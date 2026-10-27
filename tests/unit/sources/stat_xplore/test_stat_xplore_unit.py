"""Connector-specific tests for Stat-Xplore (no network, no credentials).

HTTP is replaced by ``FakeStatXplore``, an in-memory stand-in for ``POST
/table`` / ``GET /schema`` whose published periods and cell values can be
mutated between runs to simulate new releases, revisions and failures.
"""
# pylint: disable=missing-function-docstring,protected-access,redefined-outer-name

import json
import math
import pickle
from datetime import datetime, timezone
from pathlib import Path

import pytest
import requests

from databricks.labs.community_connector import source_simulator as _sim_pkg
from databricks.labs.community_connector.source_simulator import MODE_SIMULATE, Simulator
from databricks.labs.community_connector.sources.stat_xplore import StatXploreLakeflowConnect
from databricks.labs.community_connector.sources.stat_xplore.stat_xplore_client import (
    StatXploreAuthError,
    StatXploreClient,
    StatXploreError,
    StatXploreRateLimitError,
    StatXploreResponseError,
    stat_xplore_parse_retry_after,
)
from databricks.labs.community_connector.sources.stat_xplore.stat_xplore_config import (
    stat_xplore_parse_query_config,
)
from databricks.labs.community_connector.sources.stat_xplore.stat_xplore_cube import (
    stat_xplore_cell_records,
    stat_xplore_classify_value,
)
from databricks.labs.community_connector.sources.stat_xplore.stat_xplore_schemas import (
    STAT_XPLORE_CELLS_SCHEMA,
    STAT_XPLORE_RESPONSES_SCHEMA,
)
from tests.unit.sources.stat_xplore.test_stat_xplore_lakeflow_connect import REPLAY_CONFIG

BASE = "https://stat-xplore.dwp.gov.uk/webapi/rest/v1"
KEY = "unit-test-secret-key-0123456789"
DB = "str:database:TEST_DB"
MEASURE = "str:count:TEST_DB:V_F_TEST"
DATE = "str:field:TEST_DB:F_DATE:DATE_NAME"
EMP = "str:field:TEST_DB:V_F_TEST:EMP"
E0 = "str:value:TEST_DB:V_F_TEST:EMP:C_EMP:0"
E1 = "str:value:TEST_DB:V_F_TEST:EMP:C_EMP:1"


def period(code: str) -> str:
    return f"str:value:TEST_DB:F_DATE:DATE_NAME:C_DATE:{code}"


# ----------------------------------------------------------------- fakes


class FakeResponse:
    def __init__(self, status=200, body=None, headers=None, links=None, text=None):
        self.status_code = status
        self._body = body
        self._text = text
        self.headers = headers or {}
        self.links = links or {}

    def json(self):
        if self._text is not None:
            raise ValueError("not json")
        return self._body


class ScriptedSession:
    """Returns queued responses (or raises queued exceptions) in order."""

    def __init__(self, *responses):
        self.queue = list(responses)
        self.calls = []

    def request(self, method, url, headers=None, json=None, timeout=None):  # pylint: disable=redefined-outer-name
        self.calls.append({"method": method, "url": url, "headers": dict(headers or {}),
                           "json": json, "timeout": timeout})
        item = self.queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class FakeStatXplore:
    """Minimal /table server for one database with fields DATE x EMP."""

    def __init__(self, codes):
        self.periods = [period(c) for c in codes]
        self.values = {(p, e): 100 + i * 10 + j for i, p in enumerate(self.periods)
                       for j, e in enumerate((E0, E1))}
        self.fail_periods = set()
        self.drop_periods = set()
        self.calls = []

    def request(self, method, url, headers=None, json=None, timeout=None):  # pylint: disable=redefined-outer-name,unused-argument
        self.calls.append({"method": method, "url": url, "headers": dict(headers or {}), "json": json})
        assert method == "POST" and url == f"{BASE}/table", url
        body = json
        flat = [f for dim in body["dimensions"] for f in dim]
        recodes = body.get("recodes") or {}
        groups = {
            DATE: (recodes.get(DATE) or {}).get("map") or [[p] for p in self.periods],
            EMP: (recodes.get(EMP) or {}).get("map") or [[E0], [E1]],
        }
        requested = [g[0] for g in groups[DATE]]
        if any(p in self.fail_periods for p in requested):
            return FakeResponse(503, {"message": "unavailable"})
        groups[DATE] = [g for g in groups[DATE] if g[0] not in self.drop_periods]
        fields = [{"uri": f, "label": f.rsplit(":", 1)[-1],
                   "items": [{"type": "RecodeItem", "uris": g, "labels": [g[0][-6:]]} for g in groups[f]]}
                  for f in flat]

        def nest(level, chosen):
            if level == len(flat):
                coords = dict(zip(flat, chosen))
                return self.values.get((coords[DATE], coords.get(EMP, E0)))
            return [nest(level + 1, chosen + [g[0]]) for g in groups[flat[level]]]

        cubes = {m: {"values": nest(0, [])} for m in body["measures"]}
        return FakeResponse(200, {"query": body, "database": {"id": DB}, "fields": fields,
                                  "measures": [{"uri": MEASURE, "label": "Test count"}],
                                  "cubes": cubes, "annotationMap": {}})

    def table_calls(self):
        return [c for c in self.calls if c["method"] == "POST"]


def make_client(session, **kw):
    sleeps = []
    kw.setdefault("min_interval_seconds", 0)
    client = StatXploreClient(KEY, BASE, session=session, sleep=sleeps.append,
                              rng=lambda: 0.5, **kw)
    return client, sleeps


def make_connector(server, table=None, **conn):
    definition = {"database": DB, "measures": [MEASURE], "dimensions": [[DATE], [EMP]],
                  "time_field": DATE}
    definition.update(table or {})
    options = {"api_key": KEY, "min_request_interval_seconds": "0",
               "retry_max_delay_seconds": "0", "max_retries": "1",
               "table_registry": json.dumps({"t": definition})}
    options.update(conn)
    connector = StatXploreLakeflowConnect(options)
    connector._client_kwargs = {**connector._client_kwargs, "session": server,
                                "sleep": lambda s: None}
    return connector


def read_all(connector, start=None, max_calls=20):
    """Drive read_table like Trigger.AvailableNow; returns (records, offsets)."""
    offset, records, offsets = start or {}, [], []
    for _ in range(max_calls):
        recs, new = connector.read_table("t", offset, {})
        records.extend(recs)
        offsets.append(new)
        if new == offset:
            return records, offsets
        offset = new
    raise AssertionError("did not converge")


# ------------------------------------------------------- configuration


def test_api_key_is_required():
    for opts in ({}, {"api_key": ""}, {"api_key": "   "}):
        with pytest.raises(ValueError, match="api_key"):
            StatXploreLakeflowConnect(opts)


@pytest.mark.parametrize("url", [
    "http://stat-xplore.dwp.gov.uk/webapi/rest/v1",
    "https://user:pw@stat-xplore.dwp.gov.uk/webapi/rest/v1",
    "https://stat-xplore.dwp.gov.uk/webapi/rest/v1?x=1",
    "ftp://example.com",
])
def test_base_url_must_be_plain_https(url):
    with pytest.raises(ValueError, match="base_url"):
        StatXploreLakeflowConnect({"api_key": KEY, "base_url": url})


def test_default_base_url_is_official_endpoint():
    connector = StatXploreLakeflowConnect({"api_key": KEY})
    assert connector.client.base_url == "https://stat-xplore.dwp.gov.uk/webapi/rest/v1"


_GOOD = {"database": DB, "measures": [MEASURE], "dimensions": [[DATE], [EMP]], "time_field": DATE}


@pytest.mark.parametrize("patch,match", [
    ({"database": "UC_Monthly"}, "database"),
    ({"measures": []}, "measures"),
    ({"measures": [MEASURE, MEASURE]}, "duplicates"),
    ({"dimensions": []}, "dimensions"),
    ({"dimensions": [[DATE], [DATE]]}, "more than once"),
    ({"time_field": EMP + "X"}, "time_field"),
    ({"periods": [period("202601")], "latest_periods": "2"}, "mutually exclusive"),
    ({"time_field": None, "ingestion_mode": "incremental"}, "time_field"),
    ({"ingestion_mode": "cdc"}, "ingestion_mode"),
    ({"output": "parquet"}, "output"),
    ({"recodes": {EMP: {"map": [["not-a-value"]]}}}, "recodes"),
    ({"recodes": {"str:field:OTHER:X": {"map": [[E0]]}}}, "not in 'dimensions'"),
    ({"latest_periods": "0"}, "latest_periods"),
    ({"period_batch_size": "abc"}, "integer"),
    ({"surprise": "1"}, "unknown query option"),
])
def test_query_config_validation(patch, match):
    raw = {**_GOOD, **patch}
    raw = {k: v for k, v in raw.items() if v is not None}
    with pytest.raises(ValueError, match=match):
        stat_xplore_parse_query_config("t", raw)


def test_json_string_options_are_parsed():
    cfg = stat_xplore_parse_query_config("t", {
        "database": DB, "measures": json.dumps([MEASURE]),
        "dimensions": json.dumps([[DATE], EMP]), "time_field": DATE, "latest_periods": "3",
    })
    assert cfg.dimensions == [[DATE], [EMP]] and cfg.latest_periods == 3


@pytest.mark.parametrize("registry,match", [
    ("{not json", "not valid JSON"),
    (json.dumps([1]), "JSON object"),
    (json.dumps({"info": _GOOD}), "built-in"),
    (json.dumps({"_community_x": _GOOD}), "Invalid"),
    (json.dumps({"t": {**_GOOD, "database": "bad"}}), "database"),
])
def test_registry_validation(registry, match):
    with pytest.raises(ValueError, match=match):
        StatXploreLakeflowConnect({"api_key": KEY, "table_registry": registry})


def test_list_tables_and_unknown_table():
    connector = make_connector(None)
    assert connector.list_tables() == ["schema_items", "info", "rate_limit", "t"]
    with pytest.raises(ValueError, match="Unknown Stat-Xplore table"):
        connector.get_table_schema("nope", {})


def test_ad_hoc_table_from_table_options_and_overrides():
    connector = make_connector(None)
    adhoc = {"database": DB, "measures": json.dumps([MEASURE]),
             "dimensions": json.dumps([[DATE]]), "output": "responses"}
    assert connector.get_table_schema("adhoc", adhoc) == STAT_XPLORE_RESPONSES_SCHEMA
    assert connector.read_table_metadata("adhoc", adhoc)["primary_keys"] == ["response_id"]
    # A table_configuration override changes ingestion mode of a registry table.
    meta = connector.read_table_metadata("t", {"ingestion_mode": "incremental"})
    assert meta == {"primary_keys": ["record_id"], "cursor_field": "retrieved_at",
                    "ingestion_type": "cdc"}
    assert connector.read_table_metadata("t", {}) == {"primary_keys": ["record_id"],
                                                      "ingestion_type": "snapshot"}
    assert connector.get_table_schema("t", {}) == STAT_XPLORE_CELLS_SCHEMA


# ------------------------------------------------------------ client


def test_api_key_header_and_no_leak_in_errors():
    echo = FakeResponse(400, {"errorType": "BadRequest", "message": f"bad key {KEY}"})
    session = ScriptedSession(FakeResponse(200, {"languages": []}), echo)
    client, _ = make_client(session)
    client.get_info()
    headers = session.calls[0]["headers"]
    assert headers["APIKey"] == KEY and headers["Accept"] == "application/json"
    assert "Authorization" not in headers
    assert session.calls[0]["timeout"] == 120.0
    with pytest.raises(StatXploreError) as err:
        client.post_table({"database": DB})
    assert KEY not in str(err.value) and "***" in str(err.value)
    assert session.calls[1]["headers"]["Content-Type"] == "application/json"
    assert KEY not in repr(client)


def test_key_never_sent_to_foreign_origin():
    first = FakeResponse(200, {"id": "x", "children": []},
                         links={"next": {"url": "https://evil.example.com/schema?page=2"}})
    session = ScriptedSession(first)
    client, _ = make_client(session)
    with pytest.raises(StatXploreResponseError, match="different origin"):
        client.get_schema("x")
    assert len(session.calls) == 1


def test_schema_pagination_follows_link_next():
    p1 = FakeResponse(200, {"id": "f", "children": [{"id": "a"}]},
                      links={"next": {"url": f"{BASE}/schema/f?page=2"}})
    p2 = FakeResponse(200, {"id": "f", "children": [{"id": "b"}]})
    client, _ = make_client(ScriptedSession(p1, p2))
    assert [c["id"] for c in client.get_schema("f")["children"]] == ["a", "b"]


def test_429_honours_retry_after_seconds():
    session = ScriptedSession(FakeResponse(429, {}, headers={"Retry-After": "7"}),
                              FakeResponse(200, {"ok": 1}))
    client, sleeps = make_client(session, retry_max_delay_seconds=60)
    assert client.get_info() == {"ok": 1}
    assert sleeps == [7.0] and client.stats["responses_429"] == 1


def test_retry_after_http_date_and_garbage():
    now = datetime(2026, 10, 27, 12, 0, 0, tzinfo=timezone.utc)
    assert stat_xplore_parse_retry_after("Tue, 27 Oct 2026 12:00:30 GMT", now) == 30.0
    assert stat_xplore_parse_retry_after("-5") == 0.0
    assert stat_xplore_parse_retry_after("soon") is None
    assert stat_xplore_parse_retry_after(None) is None


def test_exponential_backoff_with_jitter_is_capped():
    session = ScriptedSession(FakeResponse(503), FakeResponse(502), FakeResponse(500),
                              FakeResponse(200, {"ok": 1}))
    client, sleeps = make_client(session, retry_max_delay_seconds=3)
    client.get_info()
    # rng=0.5 -> factor 1.0: 1, 2, then 4 capped at 3.
    assert sleeps == [1.0, 2.0, 3.0]


def test_retries_are_bounded():
    session = ScriptedSession(*[FakeResponse(503) for _ in range(3)])
    client, _ = make_client(session, max_retries=2)
    with pytest.raises(StatXploreError, match="persisted after 3") as err:
        client.get_info()
    assert err.value.status_code == 503 and len(session.calls) == 3


def test_persistent_429_raises_rate_limit_error():
    session = ScriptedSession(FakeResponse(429), FakeResponse(429))
    client, _ = make_client(session, max_retries=1)
    with pytest.raises(StatXploreRateLimitError):
        client.get_info()


@pytest.mark.parametrize("status", [401, 403])
def test_auth_errors_are_not_retried(status):
    session = ScriptedSession(FakeResponse(status, {"message": "Invalid API key"}))
    client, _ = make_client(session)
    with pytest.raises(StatXploreAuthError):
        client.get_info()
    assert len(session.calls) == 1


def test_network_errors_are_retried_then_sanitised():
    session = ScriptedSession(requests.ConnectionError(f"boom {KEY}"), FakeResponse(200, {"ok": 1}))
    client, _ = make_client(session)
    assert client.get_info() == {"ok": 1}
    session = ScriptedSession(requests.Timeout(f"boom {KEY}"), requests.Timeout("again"))
    client, _ = make_client(session, max_retries=1)
    with pytest.raises(StatXploreError) as err:
        client.get_info()
    assert KEY not in str(err.value) and err.value.__cause__ is None


def test_non_json_and_non_object_responses_rejected():
    client, _ = make_client(ScriptedSession(FakeResponse(200, text="<html>")))
    with pytest.raises(StatXploreResponseError, match="not valid JSON"):
        client.get_info()
    client, _ = make_client(ScriptedSession(FakeResponse(200, [1, 2])))
    with pytest.raises(StatXploreResponseError, match="JSON object"):
        client.post_table({})


def test_rate_limit_budget_waits_for_near_reset_or_fails():
    now = 1_000_000.0
    low = {"X-RateLimit-Remaining": "3", "X-RateLimit-Reset": str(int((now + 10) * 1000))}
    session = ScriptedSession(FakeResponse(200, {}, headers=low), FakeResponse(200, {}))
    client, sleeps = make_client(session, rate_limit_reserve=5, wall_clock=lambda: now)
    client.get_info()
    client.get_info()
    assert sleeps == [pytest.approx(10.0)]
    far = {"X-RateLimit-Remaining": "1", "X-RateLimit-Reset": str(int((now + 3600) * 1000))}
    session = ScriptedSession(FakeResponse(200, {}, headers=far))
    client, _ = make_client(session, rate_limit_reserve=5, wall_clock=lambda: now)
    client.get_info()
    with pytest.raises(StatXploreRateLimitError, match="No checkpoint was advanced"):
        client.get_info()
    assert len(session.calls) == 1


def test_requests_are_paced_serially():
    clock = {"t": 0.0}
    sleeps = []

    def sleep(s):
        sleeps.append(s)
        clock["t"] += s

    session = ScriptedSession(FakeResponse(200, {}), FakeResponse(200, {}))
    client = StatXploreClient(KEY, BASE, session=session, sleep=sleep, clock=lambda: clock["t"],
                              min_interval_seconds=2.0)
    client.get_info()
    client.get_info()
    assert sleeps == [2.0]


# -------------------------------------------------------------- cube


@pytest.mark.parametrize("raw,expected", [
    (None, (None, "missing")),
    (0, (0.0, "numeric")),
    (12.5, (12.5, "numeric")),
    ("..", (None, "non_numeric")),
    (True, (None, "non_numeric")),
    (float("nan"), (None, "non_numeric")),
])
def test_value_classification_never_invents_zero(raw, expected):
    assert stat_xplore_classify_value(raw) == expected


def _response(values, items=2):
    return {
        "database": {"id": DB},
        "fields": [{"uri": DATE, "label": "Month", "items": [
            {"type": "RecodeItem", "uris": [period(f"20260{i + 1}")], "labels": [f"m{i}"]}
            for i in range(items)]}],
        "measures": [{"uri": MEASURE, "label": "Count"}],
        "cubes": {MEASURE: {"values": values}},
        "annotationMap": {},
    }


_CFG = stat_xplore_parse_query_config(
    "t", {"database": DB, "measures": [MEASURE], "dimensions": [[DATE]], "time_field": DATE})


def test_suppressed_and_markers_preserved():
    records = stat_xplore_cell_records(_response([None, ".."]), _CFG, {}, "2026-10-27T00:00:00+00:00")
    assert [(r["value"], r["value_raw"], r["value_status"]) for r in records] == [
        (None, "null", "missing"), (None, '".."', "non_numeric")]
    zero = stat_xplore_cell_records(_response([0, 1]), _CFG, {}, "x")[0]
    assert zero["value"] == 0.0 and zero["value_status"] == "numeric"


@pytest.mark.parametrize("mutate,match", [
    (lambda r: r["cubes"][MEASURE].update(values=[1]), "level 0 has 1"),
    (lambda r: r["cubes"][MEASURE].update(values=[[1], [2]]), "deeper"),
    (lambda r: r.update(cubes={}), "no cube"),
    (lambda r: r.pop("fields"), "fields"),
    (lambda r: r["fields"][0].update(uri=EMP), "do not match"),
    (lambda r: r["fields"][0].pop("items"), "items"),
])
def test_malformed_responses_raise(mutate, match):
    response = _response([1, 2])
    mutate(response)
    with pytest.raises(StatXploreResponseError, match=match):
        stat_xplore_cell_records(response, _CFG, {}, "x")


def test_record_identity_is_stable_across_value_revisions():
    a = stat_xplore_cell_records(_response([1, 2]), _CFG, {}, "x")
    b = stat_xplore_cell_records(_response([5, 2]), _CFG, {}, "y")
    assert [r["record_id"] for r in a] == [r["record_id"] for r in b]
    assert a[0]["record_id"] != a[1]["record_id"]
    assert a[0]["response_sha256"] != b[0]["response_sha256"]
    assert a[0]["period_code"] == "202601" and a[0]["period_label"] == "m0"


def test_items_without_uris_get_distinct_identities():
    response = _response([1, 2, 3], items=2)
    response["fields"][0]["items"].append({"type": "Total", "labels": ["Total"], "uris": []})
    cfg = stat_xplore_parse_query_config("t", {"database": DB, "measures": [MEASURE],
                                                "dimensions": [[DATE]]})
    ids = [r["record_id"] for r in stat_xplore_cell_records(response, cfg, {}, "x")]
    assert len(set(ids)) == 3


# ------------------------------------------------------ snapshot reads


def test_snapshot_latest_periods_batched_and_deterministic():
    server = FakeStatXplore(["202605", "202606", "202607", "202608"])
    connector = make_connector(server, {"latest_periods": 3, "period_batch_size": 2})
    records, offset = connector.read_table("t", None, {})
    records = list(records)
    assert offset is None
    calls = server.table_calls()
    assert len(calls) == 3  # 1 period discovery + ceil(3/2) data requests
    assert calls[0]["json"] == {"database": DB, "measures": [MEASURE], "dimensions": [[DATE]]}
    assert calls[1]["json"]["recodes"][DATE] == {"map": [[period("202606")], [period("202607")]],
                                                 "total": False}
    assert sorted({r["period_code"] for r in records}) == ["202606", "202607", "202608"]
    assert len(records) == 3 * 2 and len({r["record_id"] for r in records}) == 6
    again = list(make_connector(server, {"latest_periods": 3, "period_batch_size": 2})
                 .read_table("t", None, {})[0])
    assert [(r["record_id"], r["value"]) for r in again] == [(r["record_id"], r["value"]) for r in records]


def test_snapshot_explicit_periods_skip_discovery():
    server = FakeStatXplore(["202605", "202606"])
    connector = make_connector(server, {"periods": [period("202605")]})
    records = list(connector.read_table("t", None, {})[0])
    assert len(server.table_calls()) == 1 and {r["period_code"] for r in records} == {"202605"}


def test_snapshot_picks_up_revisions_and_vanished_cells():
    server = FakeStatXplore(["202605", "202606"])
    first = {r["record_id"]: r["value"] for r in make_connector(server).read_table("t", None, {})[0]}
    server.values[(period("202605"), E0)] = 999
    server.values[(period("202606"), E1)] = None  # now suppressed
    second = {r["record_id"]: r for r in make_connector(server).read_table("t", None, {})[0]}
    assert set(first) == set(second)
    assert 999.0 in [r["value"] for r in second.values()]
    assert [r["value_status"] for r in second.values()].count("missing") == 1


def test_oversized_cube_refused_before_request():
    server = FakeStatXplore(["202605", "202606"])
    connector = make_connector(server, {
        "recodes": {EMP: {"map": [[E0], [E1]], "total": True}},
        "max_cells_per_request": "5", "period_batch_size": 2})
    with pytest.raises(StatXploreResponseError, match="max_cells_per_request"):
        list(connector.read_table("t", None, {})[0])
    assert len(server.table_calls()) == 1  # discovery only; data request not sent


def test_request_budget_refuses_plan_before_sending():
    server = FakeStatXplore(["202601", "202602", "202603", "202604"])
    connector = make_connector(server, max_table_requests_per_run="3")
    with pytest.raises(StatXploreResponseError, match="max_table_requests_per_run"):
        connector.read_table("t", None, {})
    assert len(server.table_calls()) == 1


def test_incomplete_period_response_is_rejected():
    server = FakeStatXplore(["202605", "202606"])
    server.drop_periods = {period("202606")}
    connector = make_connector(server, {"periods": [period("202605"), period("202606")],
                                        "period_batch_size": 2})
    with pytest.raises(StatXploreResponseError, match="did not cover"):
        connector.read_table("t", None, {})


# --------------------------------------------------- incremental reads

_INC = {"ingestion_mode": "incremental", "lookback_periods": 1, "max_periods_per_batch": 2}


def test_incremental_bootstrap_progresses_and_converges():
    server = FakeStatXplore(["202601", "202602", "202603"])
    connector = make_connector(server, _INC)
    records, offsets = read_all(connector)
    assert sorted({r["period_code"] for r in records}) == ["202601", "202602", "202603"]
    assert offsets[0]["watermark"] == period("202602") and offsets[0]["complete"] is False
    assert offsets[1]["watermark"] == period("202603") and offsets[1]["complete"] is True
    assert offsets[-1] == offsets[-2]
    json.dumps(offsets[-1])  # checkpointable


def test_incremental_next_run_rereads_lookback_and_new_release_with_revision():
    server = FakeStatXplore(["202601", "202602", "202603"])
    _, offsets = read_all(make_connector(server, _INC))
    checkpoint = offsets[-1]

    # No new release: only the lookback period is re-read.
    server.calls.clear()
    records, _ = read_all(make_connector(server, _INC), checkpoint)
    assert {r["period_code"] for r in records} == {"202603"}

    # New release + restatement of the previous (provisional) period.
    server.periods.append(period("202604"))
    server.values[(period("202604"), E0)] = 1
    server.values[(period("202604"), E1)] = 2
    server.values[(period("202603"), E0)] = 4242
    records, offsets2 = read_all(make_connector(server, _INC), checkpoint)
    assert {r["period_code"] for r in records} == {"202603", "202604"}
    assert 4242.0 in [r["value"] for r in records]
    assert offsets2[-1]["watermark"] == period("202604")


def test_incremental_failure_never_advances_checkpoint():
    server = FakeStatXplore(["202601", "202602", "202603"])
    connector = make_connector(server, _INC)
    _, first = connector.read_table("t", {}, {})
    server.fail_periods = {period("202603")}
    with pytest.raises(StatXploreError, match="503"):
        connector.read_table("t", first, {})
    # Recovery: the same checkpoint is replayed once the source is healthy.
    server.fail_periods = set()
    records, offsets = read_all(make_connector(server, _INC), first)
    assert "202603" in {r["period_code"] for r in records}
    assert offsets[-1]["watermark"] == period("202603")


def test_incremental_definition_change_requires_full_refresh():
    server = FakeStatXplore(["202601"])
    _, offsets = read_all(make_connector(server, _INC))
    changed = make_connector(server, {**_INC, "measures": [MEASURE, MEASURE + "_B"]})
    with pytest.raises(ValueError, match="full refresh"):
        changed.read_table("t", offsets[-1], {})


def test_incremental_withdrawn_watermark_period_raises():
    server = FakeStatXplore(["202601", "202602"])
    _, offsets = read_all(make_connector(server, _INC))
    server.periods.remove(period("202602"))
    with pytest.raises(StatXploreResponseError, match="no longer published"):
        make_connector(server, _INC).read_table("t", offsets[-1], {})


def test_incremental_resyncs_when_history_is_inserted():
    server = FakeStatXplore(["202601", "202603"])
    _, offsets = read_all(make_connector(server, _INC))
    server.periods.insert(1, period("202602"))  # back-filled period before watermark
    server.values[(period("202602"), E0)] = 7
    server.values[(period("202602"), E1)] = 8
    records, _ = read_all(make_connector(server, _INC), offsets[-1])
    assert {"202601", "202602", "202603"} <= {r["period_code"] for r in records}


def test_incremental_lookback_wider_than_batch_is_not_skipped():
    server = FakeStatXplore(["202601", "202602", "202603", "202604"])
    inc = {"ingestion_mode": "incremental", "lookback_periods": 3, "max_periods_per_batch": 1}
    _, offsets = read_all(make_connector(server, inc))
    server.calls.clear()
    records, offsets2 = read_all(make_connector(server, inc), offsets[-1])
    assert sorted({r["period_code"] for r in records}) == ["202602", "202603", "202604"]
    assert len(server.table_calls()) == 1 + 3  # discovery + one request per period
    assert offsets2[-1]["watermark"] == period("202604")


def test_connector_pickles_without_live_session():
    connector = make_connector(FakeStatXplore(["202601"]))
    _ = connector.client
    clone = pickle.loads(pickle.dumps(StatXploreLakeflowConnect({"api_key": KEY})))
    assert clone._client is None and clone.client.base_url == BASE


# ------------------------------------------------- shipped examples

_SRC = Path(__file__).resolve().parents[4] / "src/databricks/labs/community_connector/sources/stat_xplore"


def test_examples_are_valid_and_secret_free():
    import yaml  # pylint: disable=import-outside-toplevel

    conn = json.loads((_SRC / "examples/connection_options.example.json").read_text())
    assert conn["api_key"].startswith("<") and conn["api_key"].endswith(">")
    connector = StatXploreLakeflowConnect({**conn, "api_key": "placeholder"})
    assert "uc_people_by_month" in connector.list_tables()
    for name in ("uc_people_pipeline_spec.yaml", "pip_pipeline_spec.yaml"):
        spec = yaml.safe_load((_SRC / "examples" / name).read_text())
        assert spec["connection_name"].startswith("<")
        for obj in spec["objects"]:
            table = obj["table"]
            opts = {k: str(v) for k, v in (table.get("table_configuration") or {}).items()}
            assert connector.get_table_schema(table["source_table"], opts).fields
            assert connector.read_table_metadata(table["source_table"], opts)["primary_keys"]


def test_spec_declares_static_secret_key_and_allowlist():
    import yaml  # pylint: disable=import-outside-toplevel
    from databricks.labs.community_connector.sources.stat_xplore.stat_xplore_config import (  # pylint: disable=import-outside-toplevel
        STAT_XPLORE_QUERY_OPTION_KEYS,
    )
    from databricks.labs.community_connector.sources.stat_xplore.stat_xplore_schemas import (  # pylint: disable=import-outside-toplevel
        STAT_XPLORE_SCHEMA_OPTION_KEYS,
    )

    spec = yaml.safe_load((_SRC / "connector_spec.yaml").read_text())
    assert "oauth" not in spec["connection"]
    params = {p["name"]: p for p in spec["connection"]["parameters"]}
    assert params["api_key"]["required"] is True and params["api_key"]["secret"] is True
    allow = set(spec["external_options_allowlist"].split(","))
    assert allow == set(STAT_XPLORE_QUERY_OPTION_KEYS) | set(STAT_XPLORE_SCHEMA_OPTION_KEYS)


# ------------------------------------------ two datasets, one connector


def test_two_datasets_share_one_implementation():
    root = Path(_sim_pkg.__file__).parent / "specs" / "stat_xplore"
    with Simulator(mode=MODE_SIMULATE, spec_path=root / "endpoints.yaml", corpus_dir=root / "corpus"):
        connector = StatXploreLakeflowConnect(REPLAY_CONFIG)
        uc = list(connector.read_table("uc_people_by_employment", None, {})[0])
        pip = list(connector.read_table("pip_cases_by_gender", None, {})[0])
        raw = list(connector.read_table("pip_raw_responses", None, {})[0])
    assert {r["database_id"] for r in uc} == {"str:database:UC_Monthly"}
    assert {r["database_id"] for r in pip} == {"str:database:PIP_Monthly_new"}
    assert len(uc) == 3 * 3  # latest 3 periods x (2 categories + Total)
    assert len(pip) == 3 * 3 * 2  # 3 periods x 3 genders x 2 measures
    missing = [r for r in pip if r["value_status"] == "missing"]
    assert len(missing) == 6 and all(r["value"] is None for r in missing)
    assert all(r["dimensions"][1]["item_uris"][0].endswith(":U") for r in missing)
    assert all(not math.isnan(r["value"]) for r in pip if r["value"] is not None)
    assert sum(r["cell_count"] for r in raw) == 9 and sum(r["missing_cell_count"] for r in raw) == 3
