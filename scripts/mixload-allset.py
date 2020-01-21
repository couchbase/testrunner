#this script will run until stopped. it will set keys with small/large values
# python scripts/mixedload.py -p expiry=10,count=10000,prefix=something,size=1024



import sys
import uuid

sys.path.append('.')
sys.path.append('lib')
import TestInput
from memcached.helper.data_helper import MemcachedClientHelper, VBucketAwareMemcached
from membase.api.rest_client import RestConnection


if __name__ == "__main__":
    try:
        input = TestInput.TestInputParser.get_test_input(sys.argv)
        server = input.servers[0]
        params = input.test_params
        count = 0
        prefix = str(uuid.uuid4())[:6]
        count = 10 * 1000 * 1000 * 1000
        size = 512
        expiry = 0
        if "count" in params:
            count = int(params["count"])
            print(count)
        if "size" in params:
            size = int(params["size"])
        if "prefix" in params:
            prefix = params["prefix"]
        #if "expiry" in params:
        #    expiry = int(params["expiry"])
        payload = MemcachedClientHelper.create_value('*', size)
        rest = RestConnection(server)
        buckets = rest.get_buckets()
        for bucket in buckets:
            smart = VBucketAwareMemcached(rest, bucket.name)
            mc = MemcachedClientHelper.proxy_client(server, bucket.name)
            i = 0
            while i < count:
                try:
                    key = "{0}-{1}".format(prefix, i)
                    #mc = smart.memcached(key)
                    #do an expiry every 10 times
                    mc.set(key, 0, 0, payload)
                    #mc.get(key)
                    i += 1
                except Exception as ex:
                    #print ex
                    #we need to initialize smart client
                    #smart.done()
                    #smart = VBucketAwareMemcached(rest, bucket.name)
                    mc = MemcachedClientHelper.proxy_client(server, bucket.name)

    except Exception as ex:
        print(ex)




