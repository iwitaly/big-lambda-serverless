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
    start_time = time.time()
    
    job_bucket = event['job_bucket']
    bucket = event['bucket']
    reducer_keys = event['keys']
    job_id = event['job_id']
    r_id = event['reducer_id']
    step_id = event['step_id']
    n_reducers = event['n_reducers']

    results = reducer(reducer_keys, job_bucket)

    #time_in_secs = (time.time() - start_time)
    #timeTaken = time_in_secs * 1000000000 # in 10^9 
    #s3DownloadTime = 0
    #totalProcessingTime = 0 
    # pret = [len(reducer_keys), line_count, time_in_secs]
    # print "Reducer output {}".format(pret)

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
