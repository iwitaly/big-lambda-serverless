from src.helpers.lambdautils import get_object_from_s3


class Base():
    output = {}


    def run(self, s3_client, src_keys, src_bucket):
        '''
        :param s3_client:
        :param src_keys:
        :param src_bucket:
        :return: self.output
        '''
        for key in src_keys:
            contents = get_object_from_s3(s3_client, src_bucket, key)

            self.handler(contents)
            # print contents.read(200)
            # print contents.read(512)
            # for line in contents._raw_stream:
            #    print line

        return self.output


    def handler(self, contents):
        raise NotImplemented()