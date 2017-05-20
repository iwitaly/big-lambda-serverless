import boto3
import json
import lambdautils
from collections import defaultdict

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')


TASK_MAPPER_PREFIX = "task/mapper/"
TASK_REDUCER_PREFIX = "task/reducer/"


def reducer(reducer_keys, job_bucket):
    '''
    :param reducer_keys: keys of objects to process in S3
    :return: reduced dict
    '''
    results = defaultdict(int)
    # line_count = 0


    for key in reducer_keys:
        response = s3_client.get_object(Bucket=job_bucket, Key=key)
        contents = response['Body'].read()

        # results['counts'] += len(json.loads(contents))
        for k, v in json.loads(contents).iteritems():
            results[k] += v
            #line_count +=1
            #if srcIp not in results:
            #    results[srcIp] = 0
            #results[srcIp] += float(val)

    return results


def handler(event, context):
    job_bucket = event['job_bucket']
    bucket = event['bucket']
    reducer_keys = event['keys']
    job_id = event['job_id']
    r_id = event['reducer_id']
    step_id = event['step_id']
    n_reducers = event['n_reducers']

    results = reducer(reducer_keys, job_bucket)

    if n_reducers == 1:
        # Last reducer file, final result
        file_name = "{}/result".format(job_id)
    else:
        file_name = "{}/{}{}/{}".format(job_id, TASK_REDUCER_PREFIX, step_id, r_id)

    lambdautils.write_to_s3(s3, job_bucket, file_name,
                            json.dumps(results), {})

    return {}
