import json

import boto3

from src.helpers import lambdautils
import api.mapper

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

TASK_MAPPER_PREFIX = "task/mapper/"

with open('../config.json', 'r') as f:
    config = json.load(f)
    MAPPER_CLASS_NAME = config.get('mapper', {}).get('class')
    Mapper = getattr(api.mapper, MAPPER_CLASS_NAME)


def handler(event, context):
    job_bucket = event['job_bucket']
    src_bucket = event['bucket']
    src_keys = event['keys']
    job_id = event['job_id']
    mapper_id = event['mapper_id']

    mapper_instance = Mapper()

    output = mapper_instance.run(s3_client, src_keys, src_bucket)

    pret = [len(src_keys)]
    mapper_fname = "{}/{}{}".format(job_id, TASK_MAPPER_PREFIX, mapper_id)

    lambdautils.write_to_s3(s3, job_bucket, mapper_fname,
                            json.dumps(output), {})

    return json.dumps(pret)
