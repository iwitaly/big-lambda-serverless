import boto3
import json
import resource
import time
import lambdautils
from collections import defaultdict

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')


TASK_MAPPER_PREFIX = "task/mapper/"
TASK_REDUCER_PREFIX = "task/reducer/"

#### Main function you need to change #####
'''
This reducer receives (k, v) pairs, group elements by k and sum all v
'''
def reducer(reducer_keys, job_bucket):
    '''
    :param reducer_keys: keys of objects to process in S3
    :return: reduced dict
    '''
    results = defaultdict(int)

    for key in reducer_keys:
        response = s3_client.get_object(Bucket=job_bucket, Key=key)
        contents = response['Body'].read()

        for k, v in json.loads(contents).iteritems():
            results[k] += v

    return results


def handler(event, context):
    # start_time = time.time()
    
    job_bucket = event['job_bucket']
    bucket = event['bucket']
    reducer_keys = event['keys']
    job_id = event['job_id']
    r_id = event['reducer_id']
    step_id = event['step_id']
    n_reducers = event['n_reducers']

    results = reducer(reducer_keys, job_bucket)

    # time_in_secs = (time.time() - start_time)
    # pret = [len(reducer_keys), line_count, time_in_secs]
    # print "Reducer output {}".format(pret)


    ### DO NOT CHANGE, TECHNICAL CODE ###
    if n_reducers == 1:
        # Last reducer file, final result
        file_name = "{}/result".format(job_id)
    else:
        file_name = "{}/{}{}/{}".format(job_id, TASK_REDUCER_PREFIX, step_id, r_id)

    # metadata = {
    #     "linecount":  '{}'.format(line_count),
    #     "processingtime": '{}'.format(time_in_secs),
    #     "memoryUsage": '{}'.format(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    # }

    lambdautils.write_to_s3(s3, job_bucket, file_name,
                            json.dumps(results), {})

    return {}
