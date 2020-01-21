#!/usr/bin/env python
import getopt
import sys

sys.path.extend(('.', 'lib'))
from membase.api.rest_client import Bucket, RestConnection
from memcached.helper.data_helper import  VBucketAwareMemcached
import TestInput
import logger
import os
import json



def usage(error=None):
    print("""\
Options
 -i <file>        Path to .ini file containing cluster information.
 -p <key=val,...> Comma-separated key=value info.

Available keys:
 bucket_name= bucket to load (default by default)
 path= path to json files

Example:
 doc_loader_from_file.py -i cluster.ini -p bucket_name=default,path=/tmp/items/
""")
    sys.exit(error)


class DocLoader():
    def __init__(self, servers):
        self.servers = servers
        self.master = self.servers[0]
        self.log = logger.Logger.get_logger()

    def load(self, path, bucket, prefix='test'):
        client = VBucketAwareMemcached(RestConnection(self.master), bucket)
        for file in os.listdir(path):
            f = open(path + '/' + file, 'r')
            rq_s = f.read()
            f.close()
            rq_json = json.loads(rq_s)
            key = str(file)
            try:
                o, c, d = client.set(key, 0, 0, json.dumps(rq_json))
            except Exception as ex:
                print('WARN=======================')
                print(ex)
        self.log.info("LOAD IS FINISHED")

def main():
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], 'hi:p', [])
        for o, a in opts:
            if o == "-h":
                usage()

        input = TestInput.TestInputParser.get_test_input(sys.argv)
        if not input.servers:
            usage("ERROR: no servers specified. Please use the -i parameter.")
    except IndexError:
        usage()
    except getopt.GetoptError as error:
        usage("ERROR: " + str(error))

    path = input.param("path", '')
    prefix = input.param("prefix", '')
    bucket_name = input.param("bucket_name", "default")
    
    loader = DocLoader(input.servers)
    loader.load(path, bucket_name, prefix)

if __name__ == "__main__":
    main()
