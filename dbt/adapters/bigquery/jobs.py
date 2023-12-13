import uuid

_INVOCATION_ID = uuid.uuid4()


def define_job_id(sql, invocation_id=None):
    if invocation_id:
        job_id = str(uuid.uuid5(invocation_id, sql))
    else:
        job_id = str(uuid.uuid5(_INVOCATION_ID, sql))
    job_id = job_id.replace("-", "_")
    return job_id
