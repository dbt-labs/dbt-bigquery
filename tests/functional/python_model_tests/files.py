SINGLE_RECORD = """
import pandas as pd

def model(dbt, session):

    dbt.config(
        submission_method="serverless",
        materialized="table"
    )

    df = pd.DataFrame(
        [
            {"column_name": {"name": "hello", "my_list": ["h", "e", "l", "l", "o"]}},
        ]
    )

    return df
"""


MULTI_RECORD_DEFAULT = """
import pandas as pd

def model(dbt, session):

    dbt.config(
        submission_method="serverless",
        materialized="table",
    )

    df = pd.DataFrame(
        [
            {"column_name": [{"name": "hello", "my_list": ["h", "e", "l", "l", "o"]}]},
        ]
    )

    return df
"""


ORC_FORMAT = """
import pandas as pd

def model(dbt, session):

    dbt.config(
        submission_method="serverless",
        materialized="table",
        intermediate_format="orc",
    )

    df = pd.DataFrame(
        [
            {"column_name": [{"name": "hello", "my_list": ["h", "e", "l", "l", "o"]}]},
        ]
    )

    return df
"""


ENABLE_LIST_INFERENCE = """
import pandas as pd

def model(dbt, session):

    dbt.config(
        submission_method="serverless",
        materialized="table",
        enable_list_inference="true",
    )

    df = pd.DataFrame(
        [
            {"column_name": [{"name": "hello", "my_list": ["h", "e", "l", "l", "o"]}]},
        ]
    )

    return df
"""


# this should fail
DISABLE_LIST_INFERENCE = """
import pandas as pd

def model(dbt, session):

    dbt.config(
        submission_method="serverless",
        materialized="table",
        enable_list_inference="false",
    )

    df = pd.DataFrame(
        [
            {"column_name": [{"name": "hello", "my_list": ["h", "e", "l", "l", "o"]}]},
        ]
    )

    return df

"""


ENABLE_LIST_INFERENCE_PARQUET_FORMAT = """
import pandas as pd

def model(dbt, session):

    dbt.config(
        submission_method="serverless",
        materialized="table",
        enable_list_inference="true",
        intermediate_format="parquet",
    )

    df = pd.DataFrame(
        [
            {"column_name": [{"name": "hello", "my_list": ["h", "e", "l", "l", "o"]}]},
        ]
    )

    return df
"""


DISABLE_LIST_INFERENCE_ORC_FORMAT = """
import pandas as pd

def model(dbt, session):

    dbt.config(
        submission_method="serverless",
        materialized="table",
        enable_list_inference="false",
        intermediate_format="orc",
    )

    df = pd.DataFrame(
        [
            {"column_name": [{"name": "hello", "my_list": ["h", "e", "l", "l", "o"]}]},
        ]
    )

    return df

"""
