"""DWP Stat-Xplore source connector."""

from databricks.labs.community_connector.sources.stat_xplore.stat_xplore import (
    StatXploreLakeflowConnect,
)


from databricks.labs.community_connector.sparkpds import LakeflowSource


class StatXploreDataSource(LakeflowSource):
    _lakeflow_connect_cls = StatXploreLakeflowConnect
    # Kept as the default "lakeflow_connect" format name so Unity Catalog
    # connection-option injection keeps working (same as other sources).
    # _format_name = "stat_xplore"


__all__ = [
    "StatXploreLakeflowConnect",
    "StatXploreDataSource",
]
