"""Validation and flattening of Stat-Xplore ``/table`` responses.

Documented response objects: ``query``, ``database`` (``id``,
``annotationKeys``), ``fields`` (``uri``, ``label``, ``items`` of
``{type, labels, uris, annotationKeys}``, ``annotationKeys``), ``measures``
(``uri``, ``label``), ``cubes`` (one entry per requested measure) and
``annotationMap``.

``cubes`` is keyed by measure URI and each cube carries a nested ``values``
array with one nesting level per entry in ``fields`` (in ``fields`` order).
The connector does not trust that shape blindly: every level is checked
against the corresponding field's item count and any mismatch is a hard
error, never a silently mis-aligned row.

Source fidelity rules:

* A cell value is copied to ``value`` only when it is a finite JSON number.
* ``null`` (Stat-Xplore's representation of unavailable / suppressed cells)
  stays null with ``value_status='missing'`` - it is never converted to 0.
* Any other JSON value (string markers, booleans, NaN) is kept verbatim in
  ``value_raw`` with ``value_status='non_numeric'``.
"""

import json
import math
from typing import Any, Optional

from databricks.labs.community_connector.sources.stat_xplore.stat_xplore_client import (
    StatXploreResponseError,
)
from databricks.labs.community_connector.sources.stat_xplore.stat_xplore_config import (
    StatXploreQueryConfig,
    stat_xplore_canonical_json,
    stat_xplore_sha256,
)


def stat_xplore_classify_value(value: Any) -> tuple:
    """Return ``(numeric_value_or_None, status)`` without inventing zeros."""
    if value is None:
        return None, "missing"
    if isinstance(value, bool):
        return None, "non_numeric"
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return float(value), "numeric"
    return None, "non_numeric"


def stat_xplore_period_code(uri: Optional[str]) -> Optional[str]:
    """Trailing code of a value URI, e.g. ``...:C_UC_DATE:202605`` -> ``202605``."""
    if not uri:
        return None
    return str(uri).rsplit(":", 1)[-1]


def _sx_list(obj: Any, key: str, where: str) -> list:
    value = obj.get(key) if isinstance(obj, dict) else None
    if not isinstance(value, list):
        raise StatXploreResponseError(f"Stat-Xplore /table response: missing list '{key}' in {where}.")
    return value


def _sx_check_shape(values: Any, sizes: list, measure: str, level: int = 0) -> None:
    if level == len(sizes):
        if isinstance(values, list):
            raise StatXploreResponseError(
                f"Stat-Xplore cube {measure!r}: values nested deeper than the {len(sizes)} fields."
            )
        return
    if not isinstance(values, list) or len(values) != sizes[level]:
        got = len(values) if isinstance(values, list) else type(values).__name__
        raise StatXploreResponseError(
            f"Stat-Xplore cube {measure!r}: level {level} has {got} entries, "
            f"expected {sizes[level]} (field item count). Response is incomplete or malformed."
        )
    for child in values:
        _sx_check_shape(child, sizes, measure, level + 1)


def stat_xplore_validate_table_response(
    response: dict, config: StatXploreQueryConfig
) -> tuple:
    """Validate a /table response. Returns ``(fields, cubes, measure_labels)``."""
    if not isinstance(response, dict):
        raise StatXploreResponseError("Stat-Xplore /table response is not a JSON object.")
    fields = _sx_list(response, "fields", "response")
    cubes = response.get("cubes")
    if not isinstance(cubes, dict):
        raise StatXploreResponseError("Stat-Xplore /table response: missing object 'cubes'.")

    expected_fields = [f for dim in config.dimensions for f in dim]
    returned_fields = []
    for i, fld in enumerate(fields):
        if not isinstance(fld, dict) or not isinstance(fld.get("uri"), str):
            raise StatXploreResponseError(f"Stat-Xplore /table response: field #{i} has no 'uri'.")
        for j, item in enumerate(_sx_list(fld, "items", f"field {fld['uri']!r}")):
            if not isinstance(item, dict) or not isinstance(item.get("uris", []), list):
                raise StatXploreResponseError(
                    f"Stat-Xplore /table response: item #{j} of field {fld['uri']!r} is malformed."
                )
        returned_fields.append(fld["uri"])
    if sorted(returned_fields) != sorted(expected_fields):
        raise StatXploreResponseError(
            "Stat-Xplore /table response fields do not match the requested dimensions "
            f"(requested {expected_fields}, returned {returned_fields})."
        )

    sizes = [len(f["items"]) for f in fields]
    cells_per_measure = 1
    for size in sizes:
        cells_per_measure *= size
    total_cells = cells_per_measure * len(config.measures)
    if total_cells > config.max_cells_per_request:
        raise StatXploreResponseError(
            f"Stat-Xplore table '{config.table_name}': response has {total_cells} cells, above "
            f"max_cells_per_request={config.max_cells_per_request}. Narrow the query with "
            "recodes or a smaller period_batch_size."
        )

    missing = [m for m in config.measures if m not in cubes]
    if missing:
        raise StatXploreResponseError(f"Stat-Xplore /table response has no cube for measure(s) {missing}.")
    for measure in config.measures:
        cube = cubes[measure]
        if not isinstance(cube, dict) or "values" not in cube:
            raise StatXploreResponseError(f"Stat-Xplore cube {measure!r} has no 'values'.")
        _sx_check_shape(cube["values"], sizes, measure)

    labels = {}
    for measure in response.get("measures") or []:
        if isinstance(measure, dict) and measure.get("uri"):
            labels[measure["uri"]] = measure.get("label")
    return fields, cubes, labels


def _sx_iter_coords(sizes: list):
    if not sizes:
        yield ()
        return
    for i in range(sizes[0]):
        for rest in _sx_iter_coords(sizes[1:]):
            yield (i,) + rest


def _sx_cell(values: Any, coords: tuple) -> Any:
    for idx in coords:
        values = values[idx]
    return values


def _sx_annotation_map(response: dict) -> dict:
    raw = response.get("annotationMap")
    return raw if isinstance(raw, dict) else {}


def _sx_keys(obj: Any) -> list:
    keys = obj.get("annotationKeys") if isinstance(obj, dict) else None
    return [str(k) for k in keys] if isinstance(keys, list) else []


def stat_xplore_response_stats(response: dict, config: StatXploreQueryConfig) -> dict:
    """Cell counts by value status for an already-validated response."""
    fields = response["fields"]
    sizes = [len(f["items"]) for f in fields]
    counts = {"numeric": 0, "missing": 0, "non_numeric": 0}
    for measure in config.measures:
        values = response["cubes"][measure]["values"]
        for coords in _sx_iter_coords(sizes):
            counts[stat_xplore_classify_value(_sx_cell(values, coords))[1]] += 1
    return counts


def stat_xplore_cell_records(  # pylint: disable=too-many-locals
    response: dict,
    config: StatXploreQueryConfig,
    request_query: dict,
    retrieved_at: str,
) -> list:
    """Flatten a validated response into one record per (measure, cell)."""
    fields, cubes, measure_labels = stat_xplore_validate_table_response(response, config)
    response_sha = stat_xplore_sha256(response)
    request_json = stat_xplore_canonical_json(request_query)
    annotation_map = _sx_annotation_map(response)
    database = response.get("database") if isinstance(response.get("database"), dict) else {}
    database_id = database.get("id") or config.database
    database_keys = _sx_keys(database)
    sizes = [len(f["items"]) for f in fields]
    time_idx = next(
        (i for i, f in enumerate(fields) if config.time_field and f["uri"] == config.time_field), None
    )

    records = []
    for measure in config.measures:
        cube = cubes[measure]
        cube_meta = stat_xplore_canonical_json({k: v for k, v in cube.items() if k != "values"})
        for coords in _sx_iter_coords(sizes):
            raw = _sx_cell(cube["values"], coords)
            value, status = stat_xplore_classify_value(raw)
            dims = []
            keys = list(database_keys)
            for fld, idx in zip(fields, coords):
                item = fld["items"][idx]
                item_keys = _sx_keys(item)
                keys.extend(_sx_keys(fld) + item_keys)
                dims.append({
                    "field_uri": fld["uri"],
                    "field_label": fld.get("label"),
                    "item_index": idx,
                    "item_type": item.get("type"),
                    "item_uris": [str(u) for u in item.get("uris") or []],
                    "item_labels": [str(lbl) for lbl in item.get("labels") or []],
                    "annotation_keys": item_keys,
                })
            identity = [config.query_hash, measure, [[d["field_uri"], d["item_uris"]] for d in dims]]
            if any(not d["item_uris"] for d in dims):
                # Items without URIs (e.g. totals) are identified by position+label.
                identity.append([[d["item_index"], d["item_labels"]] for d in dims])
            keys = sorted(set(keys))
            period = dims[time_idx] if time_idx is not None else None
            period_uri = period["item_uris"][0] if period and period["item_uris"] else None
            records.append({
                "record_id": stat_xplore_sha256(identity),
                "table_name": config.table_name,
                "query_hash": config.query_hash,
                "database_id": database_id,
                "measure_uri": measure,
                "measure_label": measure_labels.get(measure),
                "dimensions": dims,
                "dimension_key": "|".join(
                    f"{d['field_uri']}={','.join(d['item_uris']) or '#' + str(d['item_index'])}"
                    for d in dims
                ),
                "period_field_uri": config.time_field,
                "period_uri": period_uri,
                "period_code": stat_xplore_period_code(period_uri),
                "period_label": (period["item_labels"] or [None])[0] if period else None,
                "value": value,
                "value_raw": json.dumps(raw, ensure_ascii=False),
                "value_status": status,
                "annotation_keys": keys,
                "annotations_json": stat_xplore_canonical_json(
                    {k: annotation_map[k] for k in keys if k in annotation_map}
                ),
                "cube_metadata_json": cube_meta,
                "request_json": request_json,
                "response_sha256": response_sha,
                "retrieved_at": retrieved_at,
            })
    return records


def stat_xplore_response_record(
    response: dict,
    config: StatXploreQueryConfig,
    request_query: dict,
    period_uris: Optional[list],
    retrieved_at: str,
) -> dict:
    """One Bronze row holding the complete, unmodified /table response."""
    stat_xplore_validate_table_response(response, config)
    counts = stat_xplore_response_stats(response, config)
    database = response.get("database") if isinstance(response.get("database"), dict) else {}
    periods = list(period_uris or [])
    return {
        "response_id": stat_xplore_sha256([config.query_hash, periods]),
        "table_name": config.table_name,
        "query_hash": config.query_hash,
        "database_id": database.get("id") or config.database,
        "measure_uris": list(config.measures),
        "period_field_uri": config.time_field,
        "period_uris": periods,
        "request_json": stat_xplore_canonical_json(request_query),
        "response_json": json.dumps(response, ensure_ascii=False, sort_keys=True),
        "response_sha256": stat_xplore_sha256(response),
        "cell_count": sum(counts.values()),
        "numeric_cell_count": counts["numeric"],
        "missing_cell_count": counts["missing"],
        "non_numeric_cell_count": counts["non_numeric"],
        "retrieved_at": retrieved_at,
    }
