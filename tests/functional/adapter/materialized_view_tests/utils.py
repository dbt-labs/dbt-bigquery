from dbt.adapters.bigquery.relation import BigQueryRelation


def query_autorefresh(project, relation: BigQueryRelation) -> bool:
    sql = f"""
        select
            case mv.autorefresh when 't' then True when 'f' then False end as autorefresh
        from stv_mv_info mv
        where trim(mv.name) ilike '{ relation.identifier }'
        and trim(mv.schema) ilike '{ relation.schema }'
        and trim(mv.db_name) ilike '{ relation.database }'
    """
    return project.run_sql(sql, fetch="one")[0]
