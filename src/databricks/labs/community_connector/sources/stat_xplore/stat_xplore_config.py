"""Configuration parsing, validation and /table query building for Stat-Xplore.

A *statistical query table* is defined entirely by configuration - either in
the connection-level ``table_registry`` JSON or in a pipeline object's
``table_configuration`` - so new Stat-Xplore datasets never need Python
changes. Every option arrives from Spark as a string; JSON-valued options
(``measures``, ``dimensions``, ``recodes``, ``periods``) may be native JSON
when they come from the registry.
"""

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Optional

STAT_XPLORE_QUERY_OPTION_KEYS = (
    "database",
    "measures",
    "dimensions",
    "recodes",
    "time_field",
    "periods",
    "latest_periods",
    "period_batch_size",
    "ingestion_mode",
    "lookback_periods",
    "max_periods_per_batch",
    "max_cells_per_request",
    "output",
)
STAT_XPLORE_INGESTION_MODES = ("snapshot", "incremental")
STAT_XPLORE_OUTPUTS = ("cells", "responses")

_SX_DEFAULT_PERIOD_BATCH_SIZE = 1
_SX_DEFAULT_LOOKBACK_PERIODS = 2
_SX_DEFAULT_MAX_PERIODS_PER_BATCH = 12
_SX_DEFAULT_MAX_CELLS_PER_REQUEST = 250_000
_SX_HARD_MAX_CELLS_PER_REQUEST = 5_000_000


def stat_xplore_canonical_json(value: Any) -> str:
    """Deterministic JSON used for hashing and for persisted request JSON."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def stat_xplore_sha256(value: Any) -> str:
    text = value if isinstance(value, str) else stat_xplore_canonical_json(value)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _sx_json(value: Any, name: str) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Stat-Xplore option '{name}' is not valid JSON: {exc.msg}") from None
    return value


def stat_xplore_int_option(
    value: Any, name: str, default: Optional[int], minimum: int = 0,
    maximum: Optional[int] = None,
) -> Optional[int]:
    if value is None or (isinstance(value, str) and not value.strip()):
        return default
    try:
        if isinstance(value, bool):
            raise ValueError
        parsed = int(str(value).strip())
    except ValueError:
        raise ValueError(f"Stat-Xplore option '{name}' must be an integer, got {value!r}.") from None
    if parsed < minimum or (maximum is not None and parsed > maximum):
        bound = f">= {minimum}" + (f" and <= {maximum}" if maximum is not None else "")
        raise ValueError(f"Stat-Xplore option '{name}' must be {bound}, got {parsed}.")
    return parsed


def stat_xplore_float_option(value: Any, name: str, default: float, minimum: float = 0.0) -> float:
    if value is None or (isinstance(value, str) and not value.strip()):
        return default
    try:
        parsed = float(str(value).strip())
    except ValueError:
        raise ValueError(f"Stat-Xplore option '{name}' must be a number, got {value!r}.") from None
    if parsed < minimum:
        raise ValueError(f"Stat-Xplore option '{name}' must be >= {minimum}, got {parsed}.")
    return parsed


def _sx_require_id(value: Any, name: str, prefix: str) -> str:
    if not isinstance(value, str) or not value.startswith(prefix) or len(value) <= len(prefix):
        raise ValueError(
            f"Stat-Xplore option '{name}' must be a Stat-Xplore ID starting with "
            f"'{prefix}' (as returned by /schema), got {value!r}."
        )
    return value


def _sx_parse_measures(raw: Any) -> list:
    value = _sx_json(raw, "measures")
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list) or not value:
        raise ValueError("Stat-Xplore option 'measures' must be a non-empty JSON array of measure IDs.")
    out = [_sx_require_id(m, "measures", "str:") for m in value]
    if len(set(out)) != len(out):
        raise ValueError("Stat-Xplore option 'measures' contains duplicates.")
    return out


def _sx_parse_dimensions(raw: Any) -> list:
    value = _sx_json(raw, "dimensions")
    if not isinstance(value, list) or not value:
        raise ValueError(
            "Stat-Xplore option 'dimensions' must be a non-empty JSON array of field-ID "
            'arrays, e.g. [["str:field:...:A"],["str:field:...:B"]].'
        )
    dims = []
    for dim in value:
        fields = [dim] if isinstance(dim, str) else dim
        if not isinstance(fields, list) or not fields:
            raise ValueError("Each Stat-Xplore dimension must be a field ID or a non-empty array of field IDs.")
        dims.append([_sx_require_id(f, "dimensions", "str:field:") for f in fields])
    flat = [f for d in dims for f in d]
    if len(set(flat)) != len(flat):
        raise ValueError("Stat-Xplore option 'dimensions' lists the same field more than once.")
    return dims


def _sx_parse_recodes(raw: Any, dimension_fields: list) -> dict:
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return {}
    value = _sx_json(raw, "recodes")
    if not isinstance(value, dict):
        raise ValueError("Stat-Xplore option 'recodes' must be a JSON object keyed by field ID.")
    out = {}
    for field_id, recode in value.items():
        _sx_require_id(field_id, "recodes", "str:field:")
        if field_id not in dimension_fields:
            raise ValueError(f"Stat-Xplore recode field {field_id!r} is not in 'dimensions'.")
        if not isinstance(recode, dict) or set(recode) - {"map", "total"}:
            raise ValueError(f"Stat-Xplore recode for {field_id!r} may only contain 'map' and 'total'.")
        clean = {}
        if "map" in recode:
            groups = recode["map"]
            if not isinstance(groups, list) or not groups:
                raise ValueError(f"Stat-Xplore recode map for {field_id!r} must be a non-empty array.")
            clean_groups = []
            for group in groups:
                members = [group] if isinstance(group, str) else group
                if not isinstance(members, list) or not members:
                    raise ValueError(f"Stat-Xplore recode map for {field_id!r} has an empty group.")
                clean_groups.append([_sx_require_id(v, "recodes", "str:value:") for v in members])
            clean["map"] = clean_groups
        if "total" in recode:
            if not isinstance(recode["total"], bool):
                raise ValueError(f"Stat-Xplore recode 'total' for {field_id!r} must be true or false.")
            clean["total"] = recode["total"]
        out[field_id] = clean
    return out


def _sx_parse_periods(raw: Any) -> Optional[list]:
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return None
    value = _sx_json(raw, "periods")
    if not isinstance(value, list) or not value:
        raise ValueError("Stat-Xplore option 'periods' must be a non-empty JSON array of time value IDs.")
    out = [_sx_require_id(p, "periods", "str:value:") for p in value]
    if len(set(out)) != len(out):
        raise ValueError("Stat-Xplore option 'periods' contains duplicates.")
    return out


@dataclass(frozen=True)
class StatXploreQueryConfig:  # pylint: disable=too-many-instance-attributes
    """Validated definition of one statistical query table."""

    table_name: str
    database: str
    measures: list
    dimensions: list
    recodes: dict = field(default_factory=dict)
    time_field: Optional[str] = None
    periods: Optional[list] = None
    latest_periods: Optional[int] = None
    period_batch_size: int = _SX_DEFAULT_PERIOD_BATCH_SIZE
    ingestion_mode: str = "snapshot"
    lookback_periods: int = _SX_DEFAULT_LOOKBACK_PERIODS
    max_periods_per_batch: int = _SX_DEFAULT_MAX_PERIODS_PER_BATCH
    max_cells_per_request: int = _SX_DEFAULT_MAX_CELLS_PER_REQUEST
    output: str = "cells"

    @property
    def incremental(self) -> bool:
        return self.ingestion_mode == "incremental"

    @property
    def connector_manages_periods(self) -> bool:
        """True when the connector recodes the time field per request."""
        return bool(self.time_field) and self.time_field not in self.recodes

    def base_query(self) -> dict:
        query = {
            "database": self.database,
            "measures": list(self.measures),
            "dimensions": [list(d) for d in self.dimensions],
        }
        if self.recodes:
            query["recodes"] = json.loads(stat_xplore_canonical_json(self.recodes))
        return query

    @property
    def query_hash(self) -> str:
        """Stable identity of the statistical definition (excludes period selection)."""
        return stat_xplore_sha256({"query": self.base_query(), "time_field": self.time_field})

    def build_query(self, period_uris: Optional[list] = None) -> dict:
        query = self.base_query()
        if period_uris is not None:
            if not self.connector_manages_periods:
                raise ValueError("build_query(period_uris) requires a connector-managed time_field.")
            recodes = dict(query.get("recodes") or {})
            recodes[self.time_field] = {"map": [[p] for p in period_uris], "total": False}
            query["recodes"] = recodes
        return query

    def period_discovery_query(self) -> dict:
        """Smallest /table query that returns the full, un-recoded time axis."""
        return {
            "database": self.database,
            "measures": [self.measures[0]],
            "dimensions": [[self.time_field]],
        }

    def estimated_cells(self, period_count: Optional[int] = None) -> Optional[int]:
        """Upper-bound cell count if every field's cardinality is known, else None."""
        total = len(self.measures)
        for dim in self.dimensions:
            for field_id in dim:
                recode = self.recodes.get(field_id) or {}
                if field_id == self.time_field and period_count is not None:
                    size = period_count
                elif "map" in recode:
                    size = len(recode["map"]) + (1 if recode.get("total") else 0)
                else:
                    return None
                total *= size
        return total


def stat_xplore_parse_query_config(table_name: str, raw: dict) -> StatXploreQueryConfig:
    """Validate a raw (registry or table_options) definition."""
    unknown = sorted(set(raw) - set(STAT_XPLORE_QUERY_OPTION_KEYS))
    if unknown:
        raise ValueError(f"Stat-Xplore table '{table_name}': unknown query option(s) {unknown}.")
    if not raw.get("database"):
        raise ValueError(f"Stat-Xplore table '{table_name}': 'database' is required.")
    database = _sx_require_id(raw.get("database"), "database", "str:database:")
    measures = _sx_parse_measures(raw.get("measures"))
    dimensions = _sx_parse_dimensions(raw.get("dimensions"))
    flat_fields = [f for d in dimensions for f in d]
    recodes = _sx_parse_recodes(raw.get("recodes"), flat_fields)

    time_field = raw.get("time_field") or None
    if time_field is not None:
        _sx_require_id(time_field, "time_field", "str:field:")
        if [time_field] not in dimensions:
            raise ValueError(
                f"Stat-Xplore table '{table_name}': time_field must appear in 'dimensions' "
                "as its own single-field dimension."
            )

    ingestion_mode = str(raw.get("ingestion_mode") or "snapshot").strip().lower()
    if ingestion_mode not in STAT_XPLORE_INGESTION_MODES:
        raise ValueError(f"Stat-Xplore 'ingestion_mode' must be one of {STAT_XPLORE_INGESTION_MODES}.")
    output = str(raw.get("output") or "cells").strip().lower()
    if output not in STAT_XPLORE_OUTPUTS:
        raise ValueError(f"Stat-Xplore 'output' must be one of {STAT_XPLORE_OUTPUTS}.")

    periods = _sx_parse_periods(raw.get("periods"))
    latest_periods = stat_xplore_int_option(raw.get("latest_periods"), "latest_periods", None, 1)
    if periods and latest_periods:
        raise ValueError("Stat-Xplore options 'periods' and 'latest_periods' are mutually exclusive.")
    managed = bool(time_field) and time_field not in recodes
    if (periods or latest_periods or ingestion_mode == "incremental") and not managed:
        raise ValueError(
            f"Stat-Xplore table '{table_name}': 'periods', 'latest_periods' and "
            "ingestion_mode=incremental need a 'time_field' that is NOT also recoded "
            "in 'recodes' (the connector recodes it per request)."
        )
    if ingestion_mode == "incremental" and periods:
        raise ValueError("Stat-Xplore incremental tables discover periods; do not set 'periods'.")

    return StatXploreQueryConfig(
        table_name=table_name,
        database=database,
        measures=measures,
        dimensions=dimensions,
        recodes=recodes,
        time_field=time_field,
        periods=periods,
        latest_periods=latest_periods,
        period_batch_size=stat_xplore_int_option(
            raw.get("period_batch_size"), "period_batch_size", _SX_DEFAULT_PERIOD_BATCH_SIZE, 1, 120
        ),
        ingestion_mode=ingestion_mode,
        lookback_periods=stat_xplore_int_option(
            raw.get("lookback_periods"), "lookback_periods", _SX_DEFAULT_LOOKBACK_PERIODS, 0, 120
        ),
        max_periods_per_batch=stat_xplore_int_option(
            raw.get("max_periods_per_batch"), "max_periods_per_batch",
            _SX_DEFAULT_MAX_PERIODS_PER_BATCH, 1, 1000,
        ),
        max_cells_per_request=stat_xplore_int_option(
            raw.get("max_cells_per_request"), "max_cells_per_request",
            _SX_DEFAULT_MAX_CELLS_PER_REQUEST, 1, _SX_HARD_MAX_CELLS_PER_REQUEST,
        ),
        output=output,
    )


def stat_xplore_parse_registry(raw: Any, reserved_names: tuple) -> dict:
    """Parse the connection-level ``table_registry`` JSON into validated configs."""
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return {}
    value = _sx_json(raw, "table_registry")
    if not isinstance(value, dict):
        raise ValueError("Stat-Xplore 'table_registry' must be a JSON object of table_name -> query.")
    out = {}
    for name, definition in value.items():
        if not isinstance(name, str) or not name.strip() or name.startswith("_community_"):
            raise ValueError(f"Invalid Stat-Xplore registry table name {name!r}.")
        if name in reserved_names:
            raise ValueError(f"Stat-Xplore registry table name {name!r} clashes with a built-in table.")
        if not isinstance(definition, dict):
            raise ValueError(f"Stat-Xplore registry entry {name!r} must be a JSON object.")
        stat_xplore_parse_query_config(name, definition)  # validate eagerly
        out[name] = dict(definition)
    return out
