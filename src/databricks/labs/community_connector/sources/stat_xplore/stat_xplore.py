"""Lakeflow Community Connector for the DWP Stat-Xplore Open Data API.

Tables
------
Built-in discovery tables (snapshot):

* ``schema_items`` - bounded walk of ``/schema`` (folders, databases and, on
  request, measures / fields / value sets) from ``schema_root``.
* ``info`` - ``/info`` (configured dataset languages).
* ``rate_limit`` - ``/rate_limit`` (free; useful as a connectivity check).

Statistical query tables are defined by configuration only - in the
connection-level ``table_registry`` or in a pipeline object's
``table_configuration`` - and executed with ``POST /table``. Each table emits
either one row per cube cell (``output=cells``, default) or one row per raw
API response (``output=responses``).

Ingestion
---------
* ``snapshot`` (default): every run re-reads the configured statistical
  definition, so revisions and restatements are picked up and vanished cells
  disappear.
* ``incremental``: period-driven. Periods are discovered from the un-recoded
  time axis, the offset stores the last fully ingested period, and every run
  re-reads ``lookback_periods`` already-ingested periods so restatements of
  recent (provisional) periods are upserted. Emitted as ``cdc`` on a stable
  cell identity with ``retrieved_at`` as the sequencing column. Stat-Xplore
  exposes no change feed or deletes, so this is *not* true CDC.

All requests run serially through one paced client; failures raise and never
advance a checkpoint.
"""

import json
from datetime import datetime, timezone
from typing import Iterator, Optional

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.stat_xplore.stat_xplore_client import (
    STAT_XPLORE_DEFAULT_BASE_URL,
    StatXploreClient,
    StatXploreResponseError,
)
from databricks.labs.community_connector.sources.stat_xplore.stat_xplore_config import (
    STAT_XPLORE_QUERY_OPTION_KEYS,
    StatXploreQueryConfig,
    stat_xplore_float_option,
    stat_xplore_int_option,
    stat_xplore_parse_query_config,
    stat_xplore_parse_registry,
)
from databricks.labs.community_connector.sources.stat_xplore.stat_xplore_cube import (
    stat_xplore_cell_records,
    stat_xplore_response_record,
)
from databricks.labs.community_connector.sources.stat_xplore.stat_xplore_schemas import (
    STAT_XPLORE_BUILTIN_METADATA,
    STAT_XPLORE_BUILTIN_SCHEMAS,
    STAT_XPLORE_BUILTIN_TABLES,
    STAT_XPLORE_CELLS_SCHEMA,
    STAT_XPLORE_RESPONSES_SCHEMA,
    STAT_XPLORE_TABLE_INFO,
    STAT_XPLORE_TABLE_SCHEMA_ITEMS,
)

_SX_DEFAULT_SCHEMA_ROOT = "str:folder:root"
_SX_DEFAULT_EXPAND_TYPES = "FOLDER"


class StatXploreLakeflowConnect(LakeflowConnect):  # pylint: disable=too-many-instance-attributes
    """LakeflowConnect implementation for DWP Stat-Xplore."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        api_key = options.get("api_key")
        if not api_key or not str(api_key).strip():
            raise ValueError(
                "Stat-Xplore connection option 'api_key' is required (sent as the APIKey header)."
            )
        self._api_key = str(api_key).strip()
        self._base_url = (options.get("base_url") or STAT_XPLORE_DEFAULT_BASE_URL).strip()
        self._client_kwargs = {
            "timeout_seconds": stat_xplore_float_option(
                options.get("timeout_seconds"), "timeout_seconds", 120.0, 1.0),
            "min_interval_seconds": stat_xplore_float_option(
                options.get("min_request_interval_seconds"), "min_request_interval_seconds", 2.0),
            "max_retries": stat_xplore_int_option(
                options.get("max_retries"), "max_retries", 5, 0, 10),
            "retry_max_delay_seconds": stat_xplore_float_option(
                options.get("retry_max_delay_seconds"), "retry_max_delay_seconds", 60.0),
            "rate_limit_reserve": stat_xplore_int_option(
                options.get("rate_limit_reserve"), "rate_limit_reserve", 25, 0, 1999),
            "rate_limit_max_wait_seconds": stat_xplore_float_option(
                options.get("rate_limit_max_wait_seconds"), "rate_limit_max_wait_seconds", 300.0),
            "accept_language": (options.get("accept_language") or "").strip() or None,
        }
        self._max_table_requests = stat_xplore_int_option(
            options.get("max_table_requests_per_run"), "max_table_requests_per_run", 200, 1, 2000)
        self._registry = stat_xplore_parse_registry(
            options.get("table_registry"), STAT_XPLORE_BUILTIN_TABLES)
        # Validate the client settings (base_url etc.) eagerly, without I/O.
        StatXploreClient(self._api_key, self._base_url, **self._client_kwargs)
        # Cap every read of this instance to a single, fixed point in time.
        self._init_ts = datetime.now(timezone.utc).isoformat()
        self._client: Optional[StatXploreClient] = None
        self._table_requests_used = 0
        self._period_cache: dict = {}

    # The client (requests.Session) is rebuilt lazily after pickling.
    def __getstate__(self):
        state = dict(self.__dict__)
        state["_client"] = None
        return state

    @property
    def client(self) -> StatXploreClient:
        if self._client is None:
            self._client = StatXploreClient(self._api_key, self._base_url, **self._client_kwargs)
        return self._client

    # ------------------------------------------------------------------ resolution

    def list_tables(self) -> list[str]:
        return list(STAT_XPLORE_BUILTIN_TABLES) + [
            n for n in self._registry if n not in STAT_XPLORE_BUILTIN_TABLES
        ]

    def _query_config(self, table_name: str, table_options: dict) -> Optional[StatXploreQueryConfig]:
        """None for built-in tables; a validated config for query tables; else raise."""
        if table_name in STAT_XPLORE_BUILTIN_TABLES:
            return None
        table_options = table_options or {}
        overrides = {
            k: table_options[k] for k in STAT_XPLORE_QUERY_OPTION_KEYS
            if k in table_options and table_options[k] not in (None, "")
        }
        base = self._registry.get(table_name)
        if base is None and "database" not in overrides:
            raise ValueError(
                f"Unknown Stat-Xplore table '{table_name}'. Use a built-in table "
                f"{list(STAT_XPLORE_BUILTIN_TABLES)}, a 'table_registry' entry "
                f"{list(self._registry)}, or supply 'database', 'measures' and "
                "'dimensions' in the table configuration."
            )
        return stat_xplore_parse_query_config(table_name, {**(base or {}), **overrides})

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        config = self._query_config(table_name, table_options)
        if config is None:
            return STAT_XPLORE_BUILTIN_SCHEMAS[table_name]
        return STAT_XPLORE_RESPONSES_SCHEMA if config.output == "responses" else STAT_XPLORE_CELLS_SCHEMA

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        config = self._query_config(table_name, table_options)
        if config is None:
            return dict(STAT_XPLORE_BUILTIN_METADATA[table_name])
        key = "response_id" if config.output == "responses" else "record_id"
        if config.incremental:
            return {"primary_keys": [key], "cursor_field": "retrieved_at", "ingestion_type": "cdc"}
        return {"primary_keys": [key], "ingestion_type": "snapshot"}

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        config = self._query_config(table_name, table_options)
        if config is None:
            if table_name == STAT_XPLORE_TABLE_SCHEMA_ITEMS:
                return iter(self._read_schema_items(table_options or {})), None
            if table_name == STAT_XPLORE_TABLE_INFO:
                return iter(self._read_info()), None
            return iter(self._read_rate_limit()), None
        if config.incremental:
            return self._read_incremental(config, start_offset or {})
        return iter(self._read_snapshot(config)), None

    # ------------------------------------------------------------------ built-ins

    def _read_schema_items(self, opts: dict) -> list:  # pylint: disable=too-many-locals
        root = (opts.get("schema_root") or _SX_DEFAULT_SCHEMA_ROOT).strip()
        expand = {
            t.strip().upper()
            for t in str(opts.get("schema_expand_types") or _SX_DEFAULT_EXPAND_TYPES).split(",")
            if t.strip()
        }
        max_depth = stat_xplore_int_option(opts.get("schema_max_depth"), "schema_max_depth", 3, 1, 10)
        max_requests = stat_xplore_int_option(
            opts.get("schema_max_requests"), "schema_max_requests", 50, 1, 1000)

        rows: dict = {}
        order: list = []
        queue = [(root, "", 0, None)]
        fetched = 0
        while queue:
            item_id, parent, depth, summary = queue.pop(0)
            if fetched >= max_requests:
                raise StatXploreResponseError(
                    f"Stat-Xplore schema walk from {root!r} needs more than schema_max_requests="
                    f"{max_requests} requests. Narrow schema_root/schema_expand_types/"
                    "schema_max_depth or raise the limit; nothing was emitted."
                )
            fetched += 1
            node = self.client.get_schema(None if item_id == _SX_DEFAULT_SCHEMA_ROOT else item_id)
            children = node.get("children") if isinstance(node.get("children"), list) else []
            detail = {k: v for k, v in node.items() if k != "children"}
            self._add_schema_row(rows, order, detail or summary or {"id": item_id}, parent,
                                 depth, True, len(children), root)
            for child in children:
                if not isinstance(child, dict) or not child.get("id"):
                    raise StatXploreResponseError(f"Stat-Xplore schema item {item_id!r} has a child without 'id'.")
                ctype = str(child.get("type") or "").upper()
                if ctype in expand and depth + 1 < max_depth:
                    queue.append((child["id"], node.get("id") or item_id, depth + 1, child))
                else:
                    self._add_schema_row(rows, order, child, node.get("id") or item_id,
                                         depth + 1, False, None, root)
        return [rows[k] for k in order]

    def _add_schema_row(self, rows, order, item, parent, depth, expanded, child_count, root):  # pylint: disable=too-many-arguments
        key = (item.get("id"), parent)
        if key in rows:
            return
        order.append(key)
        rows[key] = {
            "id": item.get("id"),
            "parent_id": parent,
            "label": item.get("label"),
            "type": item.get("type"),
            "location": item.get("location"),
            "depth": depth,
            "is_expanded": expanded,
            "child_count": child_count,
            "schema_root": root,
            "raw_json": json.dumps(item, sort_keys=True, ensure_ascii=False),
            "retrieved_at": self._init_ts,
        }

    def _read_info(self) -> list:
        info = self.client.get_info()
        languages = info.get("languages")
        if not isinstance(languages, list):
            raise StatXploreResponseError("Stat-Xplore /info response has no 'languages' list.")
        out = []
        for lang in languages:
            if not isinstance(lang, dict) or not lang.get("locale"):
                raise StatXploreResponseError("Stat-Xplore /info language entry without 'locale'.")
            out.append({
                "locale": lang["locale"],
                "display_name": lang.get("displayName"),
                "raw_json": json.dumps(lang, sort_keys=True, ensure_ascii=False),
                "retrieved_at": self._init_ts,
            })
        return out

    def _read_rate_limit(self) -> list:
        body = self.client.get_rate_limit()
        return [{
            "scope": "api_key",
            "limit": body.get("limit"),
            "remaining": body.get("remaining"),
            "reset_epoch_ms": body.get("reset"),
            "raw_json": json.dumps(body, sort_keys=True, ensure_ascii=False),
            "retrieved_at": self._init_ts,
        }]

    # ------------------------------------------------------------------ queries

    def _reserve_table_requests(self, count: int, table_name: str) -> None:
        if self._table_requests_used + count > self._max_table_requests:
            raise StatXploreResponseError(
                f"Stat-Xplore table '{table_name}' needs {count} more /table request(s); this run "
                f"has used {self._table_requests_used} of max_table_requests_per_run="
                f"{self._max_table_requests}. Reduce periods / raise period_batch_size, or "
                "raise the limit. No checkpoint was advanced."
            )

    def _post_table(self, query: dict, table_name: str) -> dict:
        self._reserve_table_requests(1, table_name)
        self._table_requests_used += 1
        return self.client.post_table(query)

    def _discover_periods(self, config: StatXploreQueryConfig) -> list:
        """Ordered time value IDs from the un-recoded time axis (1 request, cached)."""
        key = (config.database, config.measures[0], config.time_field)
        if key in self._period_cache:
            return self._period_cache[key]
        response = self._post_table(config.period_discovery_query(), config.table_name)
        fields = response.get("fields") if isinstance(response, dict) else None
        if not isinstance(fields, list) or len(fields) != 1 or fields[0].get("uri") != config.time_field:
            raise StatXploreResponseError(
                f"Stat-Xplore period discovery for {config.time_field!r} returned an unexpected "
                "field list.")
        periods = []
        for item in fields[0].get("items") or []:
            uris = item.get("uris") if isinstance(item, dict) else None
            if not isinstance(uris, list) or len(uris) != 1:
                raise StatXploreResponseError(
                    f"Stat-Xplore period discovery for {config.time_field!r}: item without a "
                    "single value URI.")
            periods.append(str(uris[0]))
        if not periods:
            raise StatXploreResponseError(f"Stat-Xplore period discovery for {config.time_field!r} returned no periods.")
        if len(set(periods)) != len(periods):
            raise StatXploreResponseError(
                f"Stat-Xplore period discovery for {config.time_field!r} returned duplicate periods.")
        self._period_cache[key] = periods
        return periods

    def _check_period_coverage(self, config, response, requested: list) -> None:
        for fld in response.get("fields") or []:
            if isinstance(fld, dict) and fld.get("uri") == config.time_field:
                returned = [u for item in fld.get("items") or [] for u in (item.get("uris") or [])]
                if sorted(returned) != sorted(requested):
                    missing = sorted(set(requested) - set(returned))
                    raise StatXploreResponseError(
                        f"Stat-Xplore table '{config.table_name}': response did not cover the "
                        f"requested period(s) {missing or requested}; refusing to checkpoint "
                        "an incomplete period.")
                return
        raise StatXploreResponseError(
            f"Stat-Xplore table '{config.table_name}': response is missing the time field.")

    def _fetch(self, config: StatXploreQueryConfig, periods: Optional[list]) -> list:
        """Execute the query for ``periods`` (None = un-managed single request)."""
        chunks = [None] if periods is None else [
            periods[i:i + config.period_batch_size]
            for i in range(0, len(periods), config.period_batch_size)
        ]
        self._reserve_table_requests(len(chunks), config.table_name)
        records = []
        for chunk in chunks:
            estimate = config.estimated_cells(len(chunk) if chunk is not None else None)
            if estimate is not None and estimate > config.max_cells_per_request:
                raise StatXploreResponseError(
                    f"Stat-Xplore table '{config.table_name}': request would return ~{estimate} "
                    f"cells (> max_cells_per_request={config.max_cells_per_request}). Not sent.")
            query = config.build_query(chunk)
            response = self._post_table(query, config.table_name)
            if chunk is not None:
                self._check_period_coverage(config, response, chunk)
            if config.output == "responses":
                records.append(stat_xplore_response_record(response, config, query, chunk, self._init_ts))
            else:
                records.extend(stat_xplore_cell_records(response, config, query, self._init_ts))
        return records

    def _select_snapshot_periods(self, config: StatXploreQueryConfig) -> Optional[list]:
        if not config.connector_manages_periods:
            return None
        if config.periods:
            return list(config.periods)
        available = self._discover_periods(config)
        return available[-config.latest_periods:] if config.latest_periods else available

    def _read_snapshot(self, config: StatXploreQueryConfig) -> list:
        return self._fetch(config, self._select_snapshot_periods(config))

    def _read_incremental(  # pylint: disable=too-many-locals
        self, config: StatXploreQueryConfig, start: dict
    ) -> tuple[Iterator[dict], dict]:
        if start.get("query_hash") not in (None, config.query_hash):
            raise ValueError(
                f"Stat-Xplore table '{config.table_name}': the statistical definition changed "
                "since the last checkpoint. Run a full refresh of this table.")
        same_cycle = start.get("cycle") == self._init_ts
        if same_cycle and start.get("complete"):
            return iter([]), start

        available = self._discover_periods(config)
        # latest_periods bounds the window (no deep bootstrap); indices stay
        # relative to the full published axis so a new release is not a resync.
        floor = max(0, len(available) - config.latest_periods) if config.latest_periods else 0
        watermark = start.get("watermark")
        wm_idx = -1
        resync = False
        if watermark is not None:
            if watermark not in available:
                raise StatXploreResponseError(
                    f"Stat-Xplore table '{config.table_name}': checkpointed period {watermark!r} "
                    "is no longer published. Run a full refresh of this table.")
            wm_idx = available.index(watermark)
            stored_idx = start.get("watermark_index")
            resync = stored_idx is not None and stored_idx != wm_idx

        if same_cycle:
            # Continue this run's window exactly where the previous batch
            # stopped - including lookback/resync periods below the watermark.
            first = int(start.get("cursor", wm_idx + 1))
        elif resync:
            # Periods were inserted/removed before the watermark: re-read the window.
            first = 0
        else:
            first = max(0, wm_idx + 1 - config.lookback_periods)
        first = max(first, floor)
        todo = available[first:]
        batch = todo[:config.max_periods_per_batch]
        # Raises on any failure, so neither watermark nor cursor can move
        # past a period whose data was not fully returned.
        records = self._fetch(config, batch) if batch else []

        new_idx = max([wm_idx] + [first + i for i in range(len(batch))])
        offset = {
            "query_hash": config.query_hash,
            "watermark": available[new_idx] if new_idx >= 0 else None,
            "watermark_index": new_idx if new_idx >= 0 else None,
            "cycle": self._init_ts,
            "cursor": first + len(batch),
            "complete": len(batch) == len(todo),
        }
        return iter(records), offset
