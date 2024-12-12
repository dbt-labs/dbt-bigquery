from dbt.tests.util import AnyString, AnyInteger


def bigquery_stats(is_table, partition=None, cluster=None):
    stats = {}

    if is_table:
        stats.update(
            {
                "num_bytes": {
                    "id": "num_bytes",
                    "label": AnyString(),
                    "value": AnyInteger(),
                    "description": AnyString(),
                    "include": True,
                },
                "num_rows": {
                    "id": "num_rows",
                    "label": AnyString(),
                    "value": AnyInteger(),
                    "description": AnyString(),
                    "include": True,
                },
            }
        )

    if partition is not None:
        stats.update(
            {
                "partitioning_type": {
                    "id": "partitioning_type",
                    "label": AnyString(),
                    "value": partition,
                    "description": AnyString(),
                    "include": True,
                }
            }
        )

    if cluster is not None:
        stats.update(
            {
                "clustering_fields": {
                    "id": "clustering_fields",
                    "label": AnyString(),
                    "value": cluster,
                    "description": AnyString(),
                    "include": True,
                }
            }
        )

    has_stats = {
        "id": "has_stats",
        "label": "Has Stats?",
        "value": bool(stats),
        "description": "Indicates whether there are statistics for this table",
        "include": False,
    }
    stats["has_stats"] = has_stats

    return stats
