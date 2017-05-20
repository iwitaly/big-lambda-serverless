import boto3
import json
import time
import resource
from collections import defaultdict
import lambdautils


s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

TASK_MAPPER_PREFIX = "task/mapper/"


def mapper(src_keys, src_bucket):
    '''
    :param src_keys: keys of objects to process in S3
    :return: (k, v) pair
    '''
    output = defaultdict(int)
    # line_count = 0

    # Download and process all keys
    for key in src_keys:
        contents = lambdautils.get_object_from_s3(s3_client, src_bucket, key)

        # print contents.read(200)
        # print contents.read(512)
        # for line in contents._raw_stream:
        #    print line

        for line in contents.split('\n')[:-1]:
            # line_count += 1
            data = line.split(',')
            k = data[0]
            # k, v = srcIp, float(data[3])
            output[k] += 1

    return output


def handler(event, context):
    start_time = time.time()

    job_bucket = event['job_bucket']
    src_bucket = event['bucket']
    src_keys = event['keys']
    job_id = event['job_id']
    mapper_id = event['mapper_id']

    output = mapper(src_keys, src_bucket)


    time_in_secs = (time.time() - start_time)
    #timeTaken = time_in_secs * 1000000000 # in 10^9
    #s3DownloadTime = 0
    #totalProcessingTime = 0
    pret = [len(src_keys), time_in_secs]
    mapper_fname = "{}/{}{}".format(job_id, TASK_MAPPER_PREFIX, mapper_id)
    # metadata = {
    #     "processingtime": str(time_in_secs),
    #     "memoryUsage": '{}'.format(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    # }

    lambdautils.write_to_s3(s3, job_bucket, mapper_fname,
                            json.dumps(output), {})

    return json.dumps(pret)
