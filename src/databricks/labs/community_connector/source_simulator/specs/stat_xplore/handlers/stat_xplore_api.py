"""Custom simulator handlers for the DWP Stat-Xplore Open Data API.

Serves a SYNTHETIC two-database catalogue (``corpus/datasets.json``):

* ``GET /schema[/{id}]`` - folder -> database -> measures/fields -> valueset
  -> values, with children paginated two per page through a
  ``Link: <...>; rel="next"`` header so the connector's pagination path runs.
* ``POST /table`` - builds the documented response (``query``, ``database``,
  ``fields``, ``measures``, ``cubes``, ``annotationMap``) from the posted
  database / measures / dimensions / recodes. Cell values are deterministic;
  cells touching a ``suppressed_values`` member are ``null``.
* ``GET /info`` and ``GET /rate_limit`` - static corpus documents.

The handler is intentionally strict (unknown IDs -> 4xx) so connector bugs
surface as errors rather than as plausible-looking data.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, unquote, urlencode, urlsplit, urlunsplit

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import ResponseRecord
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)

ROOT_ID = "str:folder:root"
_PAGE_SIZE = 2
_SERVICE_ROOT = "/webapi/rest/v1"
_RATE_HEADERS = {
    "X-RateLimit": "2000",
    "X-RateLimit-Remaining": "1990",
    "X-RateLimit-Reset": "4102444800000",
}


def _json_response(
    status: int, body: Any, prep: PreparedRequest, extra: Optional[Dict[str, str]] = None
) -> Response:
    headers = {"Content-Type": "application/json; charset=UTF-8", **_RATE_HEADERS}
    headers.update(extra or {})
    rec = ResponseRecord(
        status_code=status,
        headers=headers,
        body_text=json.dumps(body),
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(rec, prep)


def _error(status: int, error_type: str, message: str, prep: PreparedRequest) -> Response:
    return _json_response(status, {"errorType": error_type, "message": message}, prep)


def _catalogue(corpus) -> dict:
    data = corpus.get("datasets")
    if not isinstance(data, dict):
        raise ValueError("stat_xplore simulator corpus 'datasets' is missing")
    return data


def _base(prep: PreparedRequest) -> str:
    parts = urlsplit(prep.url or "")
    return f"{parts.scheme}://{parts.netloc}{_SERVICE_ROOT}"


# --------------------------------------------------------------------- schema


def _schema_index(cat: dict) -> Dict[str, dict]:
    """id -> {label, type, children: [ids]} for every schema node."""
    index: Dict[str, dict] = {
        ROOT_ID: {"label": "Root", "type": "FOLDER", "children": [f["id"] for f in cat["folders"]]}
    }
    for folder in cat["folders"]:
        index[folder["id"]] = {
            "label": folder["label"], "type": "FOLDER", "children": list(folder["databases"])
        }
    for db_id, db in cat["databases"].items():
        index[db_id] = {
            "label": db["label"],
            "type": "DATABASE",
            "children": [m["id"] for m in db["measures"]] + [f["id"] for f in db["fields"]],
        }
        for measure in db["measures"]:
            index[measure["id"]] = {"label": measure["label"], "type": measure["type"], "children": None}
        for fld in db["fields"]:
            index[fld["id"]] = {"label": fld["label"], "type": "FIELD", "children": [fld["valueset"]]}
            index[fld["valueset"]] = {
                "label": fld["label"], "type": "VALUESET", "children": [v["id"] for v in fld["values"]]
            }
            for value in fld["values"]:
                index[value["id"]] = {"label": value["label"], "type": "VALUE", "children": None}
    return index


def serve_schema(prep: PreparedRequest, spec, corpus) -> Response:  # noqa: ARG001
    parts = urlsplit(prep.url or "")
    path = parts.path
    prefix = f"{_SERVICE_ROOT}/schema"
    item_id = unquote(path[len(prefix) + 1:]) if path.startswith(prefix + "/") else ROOT_ID
    index = _schema_index(_catalogue(corpus))
    node = index.get(item_id)
    if node is None:
        return _error(404, "NotFound", f"Schema item {item_id} not found.", prep)
    base = _base(prep)

    def summary(node_id: str) -> dict:
        n = index[node_id]
        return {
            "id": node_id,
            "label": n["label"],
            "location": f"{base}/schema/{node_id}",
            "type": n["type"],
        }

    body = summary(item_id)
    extra: Dict[str, str] = {}
    if node["children"] is not None:
        page = int((parse_qs(parts.query).get("page") or ["1"])[0])
        start = (page - 1) * _PAGE_SIZE
        children = node["children"]
        body["children"] = [summary(c) for c in children[start:start + _PAGE_SIZE]]
        if start + _PAGE_SIZE < len(children):
            nxt = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode({"page": page + 1}), ""))
            extra["Link"] = f'<{nxt}>; rel="next"'
    return _json_response(200, body, prep, extra)


# ---------------------------------------------------------------------- table


def _leaf_value(measure: dict, leaf_ids: List[str]) -> float:
    digest = hashlib.sha256("|".join([measure["id"]] + leaf_ids).encode("utf-8")).hexdigest()
    spread = max(1, int(measure["base"]) // 2)
    return float(int(measure["base"]) + int(digest[:8], 16) % spread)


def _combos(groups: List[List[str]]) -> List[List[str]]:
    out: List[List[str]] = [[]]
    for members in groups:
        out = [prefix + [m] for prefix in out for m in members]
    return out


def _cell(measure: dict, member_sets: List[List[str]], suppressed: set) -> Any:
    leaves = _combos(member_sets)
    if any(v in suppressed for leaf in leaves for v in leaf):
        return None
    values = [_leaf_value(measure, leaf) for leaf in leaves]
    if measure["type"] == "COUNT":
        return int(sum(values))
    return round(sum(values) / len(values), 2)


def _nest(measure: dict, items_members: List[List[List[str]]], suppressed: set, chosen=None):
    chosen = chosen or []
    level = len(chosen)
    if level == len(items_members):
        return _cell(measure, chosen, suppressed)
    return [
        _nest(measure, items_members, suppressed, chosen + [members])
        for members in items_members[level]
    ]


def serve_table(prep: PreparedRequest, spec, corpus) -> Response:  # noqa: ARG001  # pylint: disable=too-many-locals,too-many-return-statements
    raw = prep.body or b""
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    try:
        body = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        return _error(400, "BadRequest", "Request body is not valid JSON.", prep)

    cat = _catalogue(corpus)
    db = cat["databases"].get(body.get("database"))
    if db is None:
        return _error(404, "NotFound", f"Database {body.get('database')} not found.", prep)
    measures = {m["id"]: m for m in db["measures"]}
    fields = {f["id"]: f for f in db["fields"]}
    requested_measures = body.get("measures") or []
    if not requested_measures or any(m not in measures for m in requested_measures):
        return _error(400, "BadRequest", "Unknown or missing measure.", prep)
    flat = [f for dim in body.get("dimensions") or [] for f in dim]
    if not flat or any(f not in fields for f in flat):
        return _error(400, "BadRequest", "Unknown or missing field.", prep)
    recodes = body.get("recodes") or {}

    out_fields = []
    items_members: List[List[List[str]]] = []
    for field_id in flat:
        fld = fields[field_id]
        values = {v["id"]: v for v in fld["values"]}
        recode = recodes.get(field_id) or {}
        groups = recode.get("map") or [[v["id"]] for v in fld["values"]]
        if any(m not in values for g in groups for m in g):
            return _error(400, "BadRequest", f"Unknown value in recode for {field_id}.", prep)
        items = []
        for group in groups:
            item = {
                "type": "RecodeItem",
                "labels": [values[m]["label"] for m in group],
                "uris": list(group),
            }
            keys = sorted({k for m in group for k in values[m].get("annotationKeys", [])})
            if keys:
                item["annotationKeys"] = keys
            items.append(item)
        members = [list(g) for g in groups]
        if recode.get("total"):
            items.append({"type": "Total", "labels": ["Total"], "uris": []})
            members.append([v["id"] for v in fld["values"]])
        out_fields.append({"uri": field_id, "label": fld["label"], "items": items})
        items_members.append(members)

    suppressed = set(db.get("suppressed_values") or [])
    response = {
        "query": body,
        "database": {"id": body["database"], "annotationKeys": list(db.get("annotationKeys") or [])},
        "fields": out_fields,
        "measures": [{"uri": m, "label": measures[m]["label"]} for m in requested_measures],
        "cubes": {
            m: {"values": _nest(measures[m], items_members, suppressed)} for m in requested_measures
        },
        "annotationMap": dict(db.get("annotationMap") or {}),
    }
    return _json_response(200, response, prep)


# ------------------------------------------------------------------- info etc


def serve_info(prep: PreparedRequest, spec, corpus) -> Response:  # noqa: ARG001
    return _json_response(200, corpus.get("info") or {}, prep)


def serve_rate_limit(prep: PreparedRequest, spec, corpus) -> Response:  # noqa: ARG001
    return _json_response(200, corpus.get("rate_limit") or {}, prep)
