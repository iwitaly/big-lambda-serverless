import json
import logging
import time
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool

import boto3
import yaml

from src.helpers import lambdautils

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


with open("local.yml", 'r') as stream:
    config_local = yaml.load(stream)
    AWS_PROFILE = config_local['aws_profile']
    AWS_REGION = config_local['aws_region']


session = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)


s3 = session.resource('s3')
s3_client = session.client('s3')
lambda_client = session.client('lambda')


with open('config.json', 'r') as f:
    config = json.load(f)

    JOB_ID = config["job_id"]
    DATA_BUCKET = config["data_bucket"]
    JOB_BUCKET = config["job_bucket"]
    LAMBDA_MEMORY = config["lambda_memory"]
    PARALLEL_LAMBDAS = config["parallel_lambdas"]
    KEY_PREFIX = config["prefix"]

    CURRENT_SERVICE = config.get('service')
    CURRENT_STAGE = config.get('stage')
    MAPPER_NAME = config.get('mapper', {}).get('name')
    REDUCER_NAME = config.get('reducer', {}).get('name')
    COORDINATOR_NAME = config.get('coordinator', {}).get('name')

    JOB_INFO = 'job_info.json'


mapper_lambda_name = '{}-{}-{}'.format(CURRENT_SERVICE, CURRENT_STAGE, MAPPER_NAME)
reducer_lambda_name = '{}-{}-{}'.format(CURRENT_SERVICE, CURRENT_STAGE, REDUCER_NAME)
rc_lambda_name = '{}-{}-{}'.format(CURRENT_SERVICE, CURRENT_STAGE, COORDINATOR_NAME)


def write_job_config(job_id, job_bucket, n_mappers, r_func):
    data = json.dumps({
        "job_id": job_id,
        "job_bucket": job_bucket,
        "map_count": n_mappers,
        "reducerFunction": r_func,
    })
    lambdautils.write_to_s3(s3, JOB_BUCKET,
                            '{}/job_info.json'.format(JOB_ID),
                            data, {})


def invoke_lambda(batches, mapper_outputs, mapper_lambda_name, m_id):
    batch = [k.key for k in batches[m_id - 1]]
    logger.info("Invoke mapper with id {} with batch len {}".format(m_id, len(batch)))

    resp = lambda_client.invoke(
        FunctionName=mapper_lambda_name,
        InvocationType='RequestResponse',
        Payload=json.dumps({
            "bucket": DATA_BUCKET,
            "keys": batch,
            "job_bucket": JOB_BUCKET,
            "job_id": JOB_ID,
            "mapper_id": m_id
        })
    )
    out = json.loads(resp['Payload'].read())
    mapper_outputs.append(out)

    logger.info("Mapper output {}".format(out))


def read_all_keys_from_s3(bucket, key_prefix):
    all_keys = []
    for obj in s3.Bucket(bucket).objects.filter(Prefix=key_prefix).all():
        # remove folders
        if not obj.key.endswith('/'):
            all_keys.append(obj)

    return all_keys


def invoke_mappers(n_mappers, batches):
    mapper_outputs = []

    logger.info("# of Mappers {}".format(n_mappers))

    pool = ThreadPool(n_mappers)

    mapper_ids = [i + 1 for i in range(n_mappers)]
    invoke_lambda_partial = partial(invoke_lambda,
                                    batches,
                                    mapper_outputs,
                                    mapper_lambda_name)

    mappers_executed = 0
    while mappers_executed < n_mappers:
        nm = min(PARALLEL_LAMBDAS, n_mappers)
        results = pool.map(invoke_lambda_partial,
                           mapper_ids[mappers_executed: mappers_executed + nm])

        mappers_executed += nm

    pool.close()
    pool.join()

    logger.info("All the mappers finished")


def main():
    all_keys = read_all_keys_from_s3(DATA_BUCKET, KEY_PREFIX)

    bsize = lambdautils.compute_batch_size(all_keys, LAMBDA_MEMORY)
    batches = lambdautils.batch_creator(all_keys, bsize)
    n_mappers = len(batches)

    write_job_config(JOB_ID, JOB_BUCKET, n_mappers, reducer_lambda_name)
    j_key = JOB_ID + "/job_data/"
    data = json.dumps({
        "map_count": n_mappers,
        "total_s3_files": len(all_keys),
        "start_time": time.time()
    })

    lambdautils.write_to_s3(s3, JOB_BUCKET, j_key, data, {})

    invoke_mappers(n_mappers, batches)

    logger.info('Driver finished')


if __name__ == '__main__':
    main()
