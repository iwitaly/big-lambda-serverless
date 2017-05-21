import boto3
import json
import lambdautils
from mapreduce import mapper


s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

TASK_MAPPER_PREFIX = "task/mapper/"


def handler(event, context):
    job_bucket = event['job_bucket']
    src_bucket = event['bucket']
    src_keys = event['keys']
    job_id = event['job_id']
    mapper_id = event['mapper_id']

    output = mapper(s3_client, src_keys, src_bucket)

    pret = [len(src_keys)]
    mapper_fname = "{}/{}{}".format(job_id, TASK_MAPPER_PREFIX, mapper_id)

    lambdautils.write_to_s3(s3, job_bucket, mapper_fname,
                            json.dumps(output), {})

    return json.dumps(pret)
