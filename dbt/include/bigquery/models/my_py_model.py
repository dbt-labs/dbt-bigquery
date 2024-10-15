def model(dbt, session):
    return dbt.ref("my_upstream_model")
