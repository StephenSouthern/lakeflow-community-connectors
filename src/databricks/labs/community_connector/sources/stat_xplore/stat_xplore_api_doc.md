# Stat-Xplore Open Data API - connector research notes

Primary source: the official Stat-Xplore online help, *Open Data API* section
(`https://stat-xplore.dwp.gov.uk/webapi/online-help/Open-Data-API.html` and its
sub-pages for Schema, Table, Info and Rate Limit). Outbound HTTP from the
development workspace to `stat-xplore.dwp.gov.uk` was blocked by policy, so
nothing below has yet been re-checked against the live service; items marked
**(verify live)** must be confirmed in the first controlled run.

## Service

* Root: `https://stat-xplore.dwp.gov.uk/webapi/rest/v1` (HTTPS only).
* JSON request and response bodies.
* Authentication: header `APIKey: <key>` on every request. Keys are issued per
  Stat-Xplore account. No OAuth.
* Optional `Accept-Language` selects label language (see `/info`).

## Endpoints used

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/schema` | Root folder of the catalogue |
| GET | `/schema/{id}` | Folder, database, measure, field, value set or value; `children` lists sub-items (`id`, `label`, `location`, `type`) |
| POST | `/table` | Executes a statistical query |
| GET | `/info` | Service information (configured languages) **(verify live: `languages[].locale/displayName`)** |
| GET | `/rate_limit` | Current rate-limit state (`limit`, `remaining`, `reset`) **(verify live; documented as not counted against the limit)** |

No other endpoints are used and none are invented. There is no endpoint for
change events, release calendars, revisions or deletes.

### Schema pagination

Items with many children return them in pages, with the next page advertised
in a `Link: <url>; rel="next"` header. The client follows it (max 500 pages),
refuses to follow a link to a different origin, and fails on a malformed page.

### Identifiers

IDs are opaque strings of the form `str:<kind>:...`, e.g.
`str:database:UC_Monthly`, `str:count:...`, `str:field:...`,
`str:valueset:...`, `str:value:...`. The connector only validates the prefix
and URL-encodes IDs in paths.

## `POST /table`

Request:

```json
{
  "database": "str:database:<db>",
  "measures": ["str:count:<db>:<measure>"],
  "dimensions": [["str:field:<db>:<field>"], ["str:field:<db>:<field2>"]],
  "recodes": {
    "str:field:<db>:<field2>": {"map": [["str:value:...:A"], ["str:value:...:B", "str:value:...:C"]], "total": true}
  }
}
```

* `dimensions` - one entry per output axis; each entry is a list of field IDs.
* `recodes` - per field: `map` selects / groups value IDs (each inner list is
  one output item), `total` appends a total item.

Response objects: `query` (echo), `database` (`id`, `annotationKeys`),
`fields` (`uri`, `label`, `items[]` with `type`, `labels`, `uris`,
optional `annotationKeys`), `measures` (`uri`, `label`), `cubes` (keyed by
measure URI, each with a nested `values` array - one nesting level per entry
in `fields`, in order) and `annotationMap` (annotation key -> text).

Cell values are JSON numbers or `null`. `null` represents unavailable or
suppressed cells (statistical disclosure control); the connector stores it as
`value=null, value_status='missing'` and never as zero. Any other JSON type is
kept verbatim as `non_numeric`.

The connector checks every response against the request (fields, item
counts per nesting level, measures, requested periods) and fails rather than
emitting mis-aligned rows.

## Periods

There is no period-listing endpoint. The connector obtains the published
period axis with the smallest `/table` query that returns it (first measure x
un-recoded time field) and then selects periods by recoding the time field
with `{"map": [[period_id], ...], "total": false}`. Order is the order of the
time field's items in the response.

## Errors and limits

* 401 / 403: invalid key or no access - terminal, not retried.
* 400 / 404 / 422: invalid query or unknown ID - terminal; the client surfaces
  `errorType` / `message` from the JSON body (key scrubbed).
* 429: rate limit exceeded. Documented limit: 2,000 requests per hour per key.
  Responses carry `X-RateLimit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`
  (epoch milliseconds) **(verify live header names)**. `Retry-After` is honoured
  if present.
* 408 / 500 / 502 / 503 / 504 and network errors: retried with bounded
  exponential backoff and jitter.
* Very large cubes can be slow or rejected; the connector caps cells per
  request (`max_cells_per_request`) and requests per run
  (`max_table_requests_per_run`).

## What the API does not provide

| Need | Status | Connector approach |
| --- | --- | --- |
| Change feed / CDC | Not available | Snapshot re-read, or period watermark + lookback (`incremental`) |
| Deletes | Not available | Snapshot ingestion reconciles removals |
| Release / revision metadata | Not available per cell | Annotations retained; `response_sha256` and values allow downstream diffing |
| Period list endpoint | Not available | One small discovery `/table` query |
| Server-side pagination of `/table` | Not available | Period batching (`period_batch_size`) |
