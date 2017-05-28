import json
import time

import boto3

from src.helpers import lambdautils

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')


with open('../config.json', "r") as f:
    config = json.load(f)
    JOB_ID = config["job_id"]
    JOB_INFO_KEY = '{}/job_info.json'.format(JOB_ID)
    JOB_BUCKET = config["job_bucket"]


job_info_file = json.loads(lambdautils.get_object_from_s3(s3_client, JOB_BUCKET, JOB_INFO_KEY))
MAP_COUNT = job_info_file["map_count"]
REDUCER_NAME = job_info_file["reducerFunction"]


def write_reducer_state(n_reducers, n_s3, bucket, fname):
    ts = time.time()
    data = json.dumps({
                "reducerCount": n_reducers,
                "total_s3_files": n_s3,
                "start_time": str(ts)
            })

    lambdautils.write_to_s3(s3, bucket, fname, data, {})


def get_mapper_files(files):
    mapper_files = filter(lambda x: "task/mapper" in x["Key"], files)
    return mapper_files


def check_job_done(files):
    for f in files:
        if "result" in f["Key"]:
            return True
    return False


def get_reducers_outputs(files, r_index):
    reducers = []
    for f in files:
        key_name = f['Key']
        parts = key_name.split('/')
        if len(parts) < 3:
            continue
        reducer_file_format = 'reducer/' + str(r_index)
        if reducer_file_format in key_name:
            reducers.append(f)

    return reducers


def get_reducer_state_info(files, job_id, job_bucket):
    reducer_states_files = filter(
        lambda x: "reducerstate." in x['Key'],
        files
    )
    reducer_step = len(reducer_states_files) > 0
    reducer_states_files_idx = map(
        lambda x: int(x['Key'].split('.')[1]),
        reducer_states_files
    )
    r_index = max(reducer_states_files_idx or [0])

    if reducer_step:
        key = "{}/reducerstate.{}".format(job_id, r_index)
        response = s3_client.get_object(Bucket=job_bucket, Key=key)
        contents = json.loads(response['Body'].read())

        reducers = get_reducers_outputs(files, r_index)

        if int(contents["reducerCount"]) == len(reducers):
            return r_index, reducers
        else:
            # Prev step didn't finish. Waiting...
            return r_index, []
    else:
        # Mappers just have finished -> return all mapper files. Thus, step number is 0
        return 0, get_mapper_files(files)


def coordinate(files, map_count, bucket):
    mapper_keys = get_mapper_files(files)
    print "Mappers done so far ", len(mapper_keys)

    if map_count == len(mapper_keys):
        step_info = get_reducer_state_info(files, JOB_ID, bucket)

        print "Step info", step_info

        step_number, reducer_keys = step_info

        if len(reducer_keys) == 0:
            print "Still waiting to finish Reducer step {}".format(step_number)
            return

        r_batch_size = lambdautils.get_reducer_batch_size(reducer_keys)

        print "Starting the reducer step", step_number
        print "Batch Size", r_batch_size

        reducer_batch_params = lambdautils.batch_creator(reducer_keys, r_batch_size)
        n_reducers = len(reducer_batch_params)

        size_of_one_batch = len(reducer_batch_params[0])
        n_s3 = n_reducers * size_of_one_batch
        step_id = step_number + 1

        for i, batch_param in enumerate(reducer_batch_params):
            batch = map(lambda x: x['Key'], batch_param)

            payload = json.dumps({
                "bucket": bucket,
                "keys": batch,
                "job_bucket": bucket,
                "job_id": JOB_ID,
                "n_reducers": n_reducers,
                "step_id": step_id,
                "reducer_id": i
            })

            resp = lambda_client.invoke(
                FunctionName=REDUCER_NAME,
                InvocationType='Event',
                Payload=payload
            )

        file_name = "{}/reducerstate.{}".format(JOB_ID, step_id)
        write_reducer_state(n_reducers, n_s3, bucket, file_name)
    else:
        print "Still waiting for all the mappers to finish .."


def validate_key(key):
    '''
    return false if not validated
    '''
    return key != JOB_INFO_KEY and not key.endswith('/')


def handler(event, context):
    print "Received event: {}".format(json.dumps(event))

    bucket = event['Records'][0]['s3']['bucket']['name']
    key_name = event['Records'][0]['s3']['object']['key']

    if not validate_key(key_name):
        print 'Key {} not validated'
        return {}

    files = s3_client.list_objects(Bucket=bucket, Prefix=JOB_ID)["Contents"]

    is_job_done = check_job_done(files)

    if not is_job_done:
        coordinate(files, MAP_COUNT, bucket)
    else:
        print 'Finished job'

    return {}
