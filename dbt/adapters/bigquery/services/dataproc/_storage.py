from google.cloud.storage import Client

from dbt.adapters.bigquery import BigQueryCredentials


def upload_model(
    client: Client,
    parsed_model: dict,
    compiled_code: str,
    credentials: BigQueryCredentials,
) -> None:
    bucket = client.get_bucket(credentials.gcs_bucket)

    schema = parsed_model["schema"]
    identifier = parsed_model["alias"]
    blob = bucket.blob(f"{schema}/{identifier}.py")

    blob.upload_from_string(compiled_code)
