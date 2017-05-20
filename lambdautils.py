def compute_batch_size(keys, lambda_memory):
    max_mem_for_data = 0.6 * lambda_memory * 1000 * 1000

    size = 0.
    for key in keys:
        if isinstance(key, dict):
            size += key['Size']
        else:
            size += key.size

    avg_object_size = size / len(keys)
    print "Dataset size: %s, nKeys: %s, avg: %s" %(size, len(keys), avg_object_size)

    rate_to_one_mapper = max_mem_for_data / avg_object_size

    if rate_to_one_mapper < 1.:
        print 'Your file is too large!'
        raise Exception

    b_size = int(round(rate_to_one_mapper))
    return b_size 


def batch_creator(all_keys, batch_size):
    # TODO: do in more intelligent way:)
    batches = []
    batch = []

    for key in all_keys:
        batch.append(key)

        if len(batch) >= batch_size:
            batches.append(batch)
            batch = []

    if len(batch) > 0:
        batches.append(batch)

    return batches


def write_to_s3(s3, bucket, key, data, metadata):
    s3.Bucket(bucket).put_object(Key=key, Body=data, Metadata=metadata)


def get_object_from_s3(s3_client, bucket, key):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    contents = response['Body'].read()
    return contents


def get_reducer_batch_size(keys):
    #TODO: do in more intelligent way:)
    batch_size = compute_batch_size(keys, 128)
    # At least 2 in a batch - Condition for termination
    return max(batch_size, 2)
