from typing import Any, Dict

from dbt.adapters.bigquery.utility import sql_escape


def table_options(config: Dict[str, Any], node: Dict[str, Any], temporary: bool) -> Dict[str, Any]:
    opts = common_options(config, node, temporary)

    if kms_key_name := config.get("kms_key_name"):
        opts["kms_key_name"] = f"'{kms_key_name}'"

    if temporary:
        opts["expiration_timestamp"] = "TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 hour)"
    else:
        # It doesn't apply the `require_partition_filter` option for a temporary table
        # so that we avoid the error by not specifying a partition with a temporary table
        # in the incremental model.
        if require_partition_filter := config.get(
            "require_partition_filter"
        ) is not None and config.get("partition_by"):
            opts["require_partition_filter"] = require_partition_filter
        if partition_expiration_days := config.get("partition_expiration_days") is not None:
            opts["partition_expiration_days"] = partition_expiration_days

    return opts


def view_options(config: Dict[str, Any], node: Dict[str, Any]) -> Dict[str, Any]:
    return common_options(config, node)


def common_options(
    config: Dict[str, Any], node: Dict[str, Any], temporary: bool = False
) -> Dict[str, Any]:
    opts = {}

    if hours_to_expiration := config.get("hours_to_expiration") and not temporary:
        opts["expiration_timestamp"] = (
            f"TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {hours_to_expiration} hour)"
        )

    if description := node.get("description") and config.persist_relation_docs():  # type: ignore[attr-defined]
        opts["description"] = f'"""{sql_escape(description)}"""'

    if config.get("labels"):
        labels = config.get("labels", {})
        opts["labels"] = list(labels.items())  # type: ignore[assignment]

    return opts
