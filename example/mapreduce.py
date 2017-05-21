from collections import defaultdict
from lambdautils import get_object_from_s3
import json


def mapper(s3_client, src_keys, src_bucket):
    '''
    Perform map operation with src_keys objects from src_bucket
    WRITE YOU OWN LOGIC HERE
    :param s3_client: boto3 s3 client
    :param src_keys: keys of objects to process in S3
    :param src_bucket: data bucket
    :return: dict {(k, v)}
    '''
    output = defaultdict(int)
    # line_count = 0

    # Download and process all keys
    for key in src_keys:
        contents = get_object_from_s3(s3_client, src_bucket, key)

        # print contents.read(200)
        # print contents.read(512)
        # for line in contents._raw_stream:
        #    print line

        for line in contents.split('\n')[:-1]:
            # line_count += 1
            data = line.split(',')
            k = data[0]
            output[k] += 1

    return output


def reducer(s3_client, reducer_keys, job_bucket):
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

