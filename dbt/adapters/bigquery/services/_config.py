from google.cloud.bigquery import QueryJob

from dbt.adapters.events.logging import AdapterLogger


logger = AdapterLogger("BigQuery")


DEFAULT_TIMEOUT = 300


def query_job_url(query_job: QueryJob) -> str:
    return f"https://console.cloud.google.com/bigquery?project={query_job.project}&j=bq:{query_job.location}:{query_job.job_id}&page=queryresults"
