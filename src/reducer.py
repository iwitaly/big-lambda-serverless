import json

import boto3

from src.helpers import lambdautils
import api.reducer


s3 = boto3.resource('s3')
s3_client = boto3.client('s3')


TASK_MAPPER_PREFIX = "task/mapper/"
TASK_REDUCER_PREFIX = "task/reducer/"

with open('config.json', 'r') as f:
    config = json.load(f)
    REDUCER_CLASS_NAME = config.get('reducer', {}).get('class')
    Reducer = getattr(api.reducer, REDUCER_CLASS_NAME)



def handler(event, context):
    job_bucket = event['job_bucket']
    # bucket = event['bucket']
    reducer_keys = event['keys']
    job_id = event['job_id']
    r_id = event['reducer_id']
    step_id = event['step_id']
    n_reducers = event['n_reducers']


    reducer_instance = Reducer()

    results = Reducer.run(s3_client, reducer_keys, job_bucket)

    if n_reducers == 1:
        # Last reducer file, final result
        file_name = "{}/result".format(job_id)
    else:
        file_name = "{}/{}{}/{}".format(job_id, TASK_REDUCER_PREFIX, step_id, r_id)

    lambdautils.write_to_s3(s3, job_bucket, file_name,
                            json.dumps(results), {})

    return {}
