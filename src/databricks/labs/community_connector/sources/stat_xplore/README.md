# DWP Stat-Xplore Community Connector

Ingests statistics from the Department for Work and Pensions
[Stat-Xplore](https://stat-xplore.dwp.gov.uk/) Open Data API into Databricks
through the Lakeflow Community Connector framework (`LakeflowConnect` +
Spark Python Data Source + managed ingestion pipelines).

The connector is **generic**: it is not tied to Universal Credit, local
authorities or any single dataset. Every statistical table is defined by
configuration (database, measures, dimensions, recodes, periods), so new
Stat-Xplore datasets need no Python changes.

## Capabilities

| Area | Supported |
| --- | --- |
| Authentication | Static API key in the `APIKey` header (UC `COMMUNITY` connection, `static` auth) |
| Discovery | `schema_items` (bounded walk of `/schema`), `info` (`/info`), `rate_limit` (`/rate_limit`) |
| Statistical queries | Any `POST /table` query: measures, multi-field dimensions, recodes (category grouping / totals), period selection |
| Ingestion | `snapshot` (default) and period-driven `incremental` (emitted as `cdc`) |
| Deletes | Not supported - Stat-Xplore exposes no change or delete feed |
| Output | `cells` (one row per measure x cell) or `responses` (one row per raw `/table` response) |

## Authentication and connection

Stat-Xplore issues one API key per account (Stat-Xplore > Account > *Open Data
API Access*). The connector sends it only in the `APIKey` header, only to the
configured HTTPS origin, never logs it and scrubs it from every error.

The spec deliberately has **no `oauth` block**, so the Community Connector CLI
creates the connection in `static` mode. The Databricks *Add data* UI currently
only offers OAuth for community connections; use the CLI (or the UC
Connections API) instead - do not add OAuth to the connector to work around
the UI.

### Create the connection (CLI)

Install the CLI from this repo (`cd tools/community_connector && pip install -e .`),
authenticate to the workspace (`DATABRICKS_CONFIG_PROFILE=<profile>`), and keep
the key out of shell history by reading it from a secret store or a prompt:

```bash
read -s SX_KEY   # paste the key; not echoed, not stored in history
community-connector create_connection stat_xplore <connection_name> \
  --auth-type static \
  --spec src/databricks/labs/community_connector/sources/stat_xplore/connector_spec.yaml \
  -o "{\"api_key\": \"${SX_KEY}\"}"
unset SX_KEY
```

`--auth-type static` is also the default derived from this spec; passing it
explicitly documents intent. The CLI validates options against
`connector_spec.yaml` and sets `externalOptionsAllowList` from
`external_options_allowlist` plus the framework options (`tableName`,
`tableNameList`, `tableConfigs`, `isDeleteFlow`).

Optional connection options are listed in `connector_spec.yaml`; see
`examples/connection_options.example.json` for a secret-free template
(including a `table_registry`). To rotate the key or change the registry, use
`community-connector update_connection stat_xplore <connection_name> ...` and
resend **all** options (updates replace the whole option map).

### Verify connectivity and permissions

1. `DESCRIBE CONNECTION <connection_name>` - confirm type `COMMUNITY` and the
   allow-list; the `api_key` value is not displayed.
2. Grant use: `GRANT USE CONNECTION ON CONNECTION <connection_name> TO <principal>`
   for whoever creates or runs the pipeline; they also need `USE CATALOG`,
   `USE SCHEMA`, `CREATE TABLE` on the destination and `READ VOLUME`/`WRITE VOLUME`
   on the wheel volume.
3. First controlled run: ingest only `rate_limit` (free - `/rate_limit` does not
   count against the hourly limit). A `401/403` fails fast with
   `StatXploreAuthError` and no retries.

## Tables

### Built-in discovery tables (snapshot)

| Table | Source | Primary key | Notes |
| --- | --- | --- | --- |
| `schema_items` | `GET /schema[/{id}]` | `id`, `parent_id` | Breadth-first walk from `schema_root` (default root folder), expanding only `schema_expand_types` (default `FOLDER`) up to `schema_max_depth` (3) and `schema_max_requests` (50). Exceeding the request cap raises instead of emitting a partial catalogue. Children pages (`Link: rel="next"`) are followed. |
| `info` | `GET /info` | `locale` | Languages configured on the service. |
| `rate_limit` | `GET /rate_limit` | `scope` | Current hourly budget. |

To discover a database's measures, fields and value IDs (needed to write a
query), point `schema_items` at it, e.g. `schema_root=str:database:UC_Monthly`,
`schema_expand_types=FIELD,VALUESET`.

### Statistical query tables

Defined either in the connection-level `table_registry` (returned by
`list_tables`) or ad hoc via a pipeline object's `table_configuration`
(table-level values override registry values). Options:

| Option | Required | Meaning |
| --- | --- | --- |
| `database` | yes | `str:database:...` ID |
| `measures` | yes | JSON array of measure IDs (`str:count:...`, `str:mean:...`, ...) |
| `dimensions` | yes | JSON array of dimensions; each dimension is an array of `str:field:...` IDs (as in `/table`) |
| `recodes` | no | JSON object keyed by field ID: `{"map": [[value IDs], ...], "total": bool}` - category selection, grouping and totals |
| `time_field` | for period options | Field ID of the time dimension; must be its own single-field dimension and not also in `recodes` (the connector recodes it per request) |
| `periods` | no | Explicit JSON array of time value IDs (snapshot only) |
| `latest_periods` | no | Only the N most recent published periods |
| `period_batch_size` | no (1) | Periods per `/table` request |
| `ingestion_mode` | no (`snapshot`) | `snapshot` or `incremental` |
| `lookback_periods` | no (2) | Incremental: already-ingested periods re-read every run |
| `max_periods_per_batch` | no (12) | Incremental: periods per micro-batch |
| `max_cells_per_request` | no (250000) | Hard cap; checked before sending when every field's cardinality is known, and always on the response |
| `output` | no (`cells`) | `cells` or `responses` |

When `time_field` is set, the connector first issues one small discovery
query (first measure x un-recoded time field) to obtain the ordered list of
published periods, then requests the data in period batches.

The connector never enumerates cube combinations automatically - each table
is exactly one configured query.

## Ingestion semantics

* **snapshot** - each run re-executes the full configured query. Revisions and
  restatements are picked up, and cells that disappear are removed by the
  framework's snapshot merge. Recommended default.
* **incremental** (`cdc`, cursor `retrieved_at`) - periods are processed in
  published order; the checkpoint stores the last fully ingested period
  (`watermark`), its index and a definition hash. Every run re-reads
  `lookback_periods` periods before the watermark so restated provisional
  months are upserted. If history is inserted before the watermark the whole
  window is re-read; if the watermark period is withdrawn, or the query
  definition changes, the read fails and asks for a full refresh. A failed or
  incomplete request raises before any offset is returned, so checkpoints never
  skip a period. This is **not true CDC**: Stat-Xplore publishes no change
  events, so revisions older than the lookback window and deleted cells are
  only reconciled by a full refresh (or by using `snapshot`).

## Bronze schema (`output=cells`)

| Column | Description |
| --- | --- |
| `record_id` | SHA-256 of definition hash + measure + item URIs (+ position/labels for URI-less items such as totals). Stable across value revisions. |
| `table_name`, `query_hash`, `database_id` | Provenance; `query_hash` excludes period selection |
| `measure_uri`, `measure_label` | Measure |
| `dimensions` | Array of `{field_uri, field_label, item_index, item_type, item_uris, item_labels, annotation_keys}` |
| `dimension_key` | Readable `field=uris` key |
| `period_field_uri`, `period_uri`, `period_code`, `period_label` | Reporting period (when `time_field` is set) |
| `value` | Double, only for finite JSON numbers |
| `value_raw` | The cell's JSON exactly as returned (`null`, numbers, markers) |
| `value_status` | `numeric`, `missing` (JSON null - unavailable or suppressed) or `non_numeric` |
| `annotation_keys`, `annotations_json` | Database, field and item annotation keys and their `annotationMap` text (disclosure-control notes, provisional flags) |
| `cube_metadata_json`, `request_json`, `response_sha256`, `retrieved_at` | Raw cube metadata, exact request, response hash, run timestamp |

Suppressed or unavailable cells are **never** converted to zero. `output=responses`
stores the complete response JSON plus cell counts by status, for maximum
fidelity or later re-flattening. A flattened analytical (Silver) model -
one column per dimension, joined to period calendars - is intentionally left
to downstream transformations.

## Reliability and API protection

* One serial client per table instance; no parallel requests.
* `min_request_interval_seconds` (default 2.0) paces requests under the
  documented 2,000 requests/hour limit.
* Timeouts (`timeout_seconds`, 120), bounded retries (`max_retries`, 5) for
  408/429/500/502/503/504 and network errors, honouring `Retry-After`
  (seconds or HTTP date), else exponential backoff with jitter capped by
  `retry_max_delay_seconds`.
* `X-RateLimit-Remaining` is tracked; at `rate_limit_reserve` the client waits
  for the reset (up to `rate_limit_max_wait_seconds`) or fails cleanly.
* `max_table_requests_per_run` refuses a plan before any request is sent;
  `max_cells_per_request` blocks oversized cubes.
* Responses are validated: field list must match the request, every cube level
  must match its field's item count, every requested period must be present.

## Deployment

No pipeline is created by this repository change. When approved:

```bash
# 1. Build and upload framework + connector wheels to a UC volume
community-connector upload stat_xplore \
  --volume-path /Volumes/<catalog>/<schema>/community_connector/packages

# 2. Create a managed ingestion pipeline from a reviewed spec
community-connector create_pipeline stat_xplore <pipeline_name> \
  -ps src/databricks/labs/community_connector/sources/stat_xplore/examples/uc_people_pipeline_spec.yaml \
  -n <connection_name> -c <catalog> -t <schema>

# 3. Run (first run: a single small table)
community-connector run_pipeline <pipeline_name>
```

The connector wheel depends only on `requests` and the framework wheel.
The single-file `_generated_stat_xplore_python_source.py` is produced by
`python3 tools/scripts/merge_python_source.py stat_xplore` for the legacy
workspace-pipeline mode.

## Examples

* `examples/connection_options.example.json` - connection options with a
  registry entry for Universal Credit (no secret).
* `examples/uc_people_pipeline_spec.yaml` - dataset 1 (Universal Credit).
* `examples/pip_pipeline_spec.yaml` - dataset 2 (PIP), defined purely in
  `table_configuration`.

Example IDs are illustrative and must be confirmed with `schema_items`
before a live run.

## Testing

```bash
PYTHONPATH=src:. pytest tests/unit/sources/stat_xplore -v
```

* `test_stat_xplore_lakeflow_connect.py` - the shared `LakeflowConnectTests`
  suite against the simulator (`source_simulator/specs/stat_xplore`, synthetic
  UC and PIP databases).
* `test_stat_xplore_unit.py` - configuration, auth header, retries/429,
  pagination, cube parsing, suppression, revisions, incremental checkpoints.

Live runs (`CONNECTOR_TEST_MODE=live` with `CONNECTOR_TEST_CONFIG_PATH`)
should use a tiny registry (one measure, one period).

## Limitations

* No change feed or deletes: incremental mode relies on period ordering and a
  lookback window; use `snapshot` or periodic full refreshes for complete
  reconciliation.
* Stat-Xplore suppresses or rounds values for disclosure control; the API
  returns them as `null` / rounded numbers without a per-cell reason, so the
  connector records the status and the annotations, not a reason code.
* Cube size for un-recoded fields is only known from the response, so the
  `max_cells_per_request` pre-check applies only when every field is recoded
  or is the managed time field.
* `/info` and rate-limit header names follow the published documentation but
  have not yet been verified against the live service from this workspace.
