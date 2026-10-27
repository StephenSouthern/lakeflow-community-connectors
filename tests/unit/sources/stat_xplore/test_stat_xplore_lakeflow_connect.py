"""Framework contract tests for the Stat-Xplore connector.

``TestStatXploreConnector`` runs the shared ``LakeflowConnectTests`` suite
against the simulator at ``source_simulator/specs/stat_xplore/`` (synthetic,
no credentials, no network). The connection-level ``table_registry`` defines
four statistical query tables over TWO different synthetic databases
(UC_Monthly and PIP_Monthly_new) - the same Python code serves both, which
is the "generic connector" acceptance check.

Connector-specific behaviour (config validation, retries, cube parsing,
incremental checkpoints, revisions) lives in ``test_stat_xplore_unit.py``.
"""

import json

from databricks.labs.community_connector.sources.stat_xplore import StatXploreLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests

UC_DB = "str:database:UC_Monthly"
UC_COUNT = "str:count:UC_Monthly:V_F_UC_CASELOAD_FULL"
UC_DATE = "str:field:UC_Monthly:F_UC_DATE:DATE_NAME"
UC_EMP = "str:field:UC_Monthly:V_F_UC_CASELOAD_FULL:EMPLOYMENT_ID"
UC_EMP_VALUE = "str:value:UC_Monthly:V_F_UC_CASELOAD_FULL:EMPLOYMENT_ID:C_UC_EMPLOYMENT:"

PIP_DB = "str:database:PIP_Monthly_new"
PIP_COUNT = "str:count:PIP_Monthly_new:V_F_PIP_MONTHLY"
PIP_MEAN = "str:mean:PIP_Monthly_new:V_F_PIP_MONTHLY:AWARD_DURATION"
PIP_DATE = "str:field:PIP_Monthly_new:F_PIP_DATE:DATE2"
PIP_SEX = "str:field:PIP_Monthly_new:V_F_PIP_MONTHLY:SEX"

TABLE_REGISTRY = {
    # Dataset 1: Universal Credit, recoded employment with a Total item.
    "uc_people_by_employment": {
        "database": UC_DB,
        "measures": [UC_COUNT],
        "dimensions": [[UC_DATE], [UC_EMP]],
        "recodes": {UC_EMP: {"map": [[UC_EMP_VALUE + "0"], [UC_EMP_VALUE + "1"]], "total": True}},
        "time_field": UC_DATE,
        "latest_periods": 3,
    },
    # Same dataset, period-driven incremental (cdc) ingestion.
    "uc_people_incremental": {
        "database": UC_DB,
        "measures": [UC_COUNT],
        "dimensions": [[UC_DATE], [UC_EMP]],
        "time_field": UC_DATE,
        "ingestion_mode": "incremental",
        "lookback_periods": 1,
        "max_periods_per_batch": 2,
    },
    # Dataset 2: PIP, two measures, a suppressed category, all periods.
    "pip_cases_by_gender": {
        "database": PIP_DB,
        "measures": [PIP_COUNT, PIP_MEAN],
        "dimensions": [[PIP_DATE], [PIP_SEX]],
        "time_field": PIP_DATE,
        "period_batch_size": 2,
    },
    # Dataset 2 again, one raw Bronze row per /table response.
    "pip_raw_responses": {
        "database": PIP_DB,
        "measures": [PIP_COUNT],
        "dimensions": [[PIP_DATE], [PIP_SEX]],
        "time_field": PIP_DATE,
        "period_batch_size": 2,
        "output": "responses",
    },
}

REPLAY_CONFIG = {
    "api_key": "simulator-fake-api-key",
    "base_url": "https://stat-xplore.dwp.gov.uk/webapi/rest/v1",
    "min_request_interval_seconds": "0",
    "retry_max_delay_seconds": "0",
    "table_registry": json.dumps(TABLE_REGISTRY),
}


class TestStatXploreConnector(LakeflowConnectTests):
    connector_class = StatXploreLakeflowConnect
    simulator_source = "stat_xplore"
    sample_records = 200
    replay_config = REPLAY_CONFIG
    table_configs = {
        # Walk folders and databases (the default) from the root.
        "schema_items": {"schema_expand_types": "FOLDER", "schema_max_depth": "3"},
    }
