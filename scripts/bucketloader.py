# this script will connect to the node in the given ini file and create "n" buckets
# and load data until program is terminated
# example python bucketloader.py -i node.ini -p count=6,delete=True,load=True,bucket_quota=219,prefix=cloud,threads=2
# it will delete all existing buckets and create 6 buckets each 219 MB and then it will start running load
# on each bucket , each load_runner will use 2 threads
import sys
import uuid

sys.path.append('.')
sys.path.append('lib')
from threading import Thread
import TestInput
from membase.helper.bucket_helper import BucketOperationHelper
from memcached.helper.data_helper import MemcachedClientHelper
from membase.api.rest_client import RestConnection

def create_buckets(server, count, prefix, quota):
    rest = RestConnection(server)
    buckets = rest.get_buckets()
    if len(buckets) < count:
        delta = count - len(buckets)
        info = rest.get_nodes_self()
        replica = 1
        #TODO: calculate the bucket_ram from available_ram // delta
        #verify that the given quota makes sense
        #how much ram is used by all buckets
        for i in range(0, delta):
            hash_string = str(uuid.uuid4())

            name = "{0}-{1}-{2}".format(prefix, i, hash_string[:4])
            rest.create_bucket(bucket=name,
                               ramQuotaMB=quota,
                               replicaNumber=replica,
                               saslPassword="password",
                               authType="sasl",
                               proxyPort=info.memcached)
            print("created bucket {0}".format(name))


def load_buckets(server, name, get, threads, moxi):
    distro = {500: 0.5, 1024: 0.5}
    MemcachedClientHelper.load_bucket([server], name, -1, 10000000, distro, threads, -1, get, moxi)


if __name__ == "__main__":
    try:
        input = TestInput.TestInputParser.get_test_input(sys.argv)
        server = input.servers[0]
        params = input.test_params
        count = 0
        get = True
        moxi = False
        bucket_quota = 200
        run_load = False
        delete = False
        prefix = "membase-"
        no_threads = 6

        if "count" in params:
            count = int(params["count"])
            print(count)
        if "load" in params:
            if params["load"].lower() == "true":
                run_load = True
        if "threads" in params:
            no_threads = params["threads"]
        if "bucket_quota" in params:
            bucket_quota = params["bucket_quota"]
        if "moxi" in params:
            if params["moxi"].lower() == "true":
                moxi = True
        if "get" in params:
            if params["get"].lower() == "true":
                get = True
        if "prefix" in params:
            prefix = params["prefix"]
        if "delete" in params:
            if params["delete"].lower() == "true":
                delete = True

        if delete:
            BucketOperationHelper.delete_all_buckets_or_assert([server], None)

        create_buckets(server, count, prefix, bucket_quota)
        if run_load:
            rest = RestConnection(server)
            buckets = rest.get_buckets()
            threads = []
            for bucket in buckets:
                t = Thread(target=load_buckets, args=(server, bucket.name, get, no_threads, moxi))
                t.start()
                threads.append(t)

            for t in threads:
                t.join()
    except Exception as ex:
        print(ex)



