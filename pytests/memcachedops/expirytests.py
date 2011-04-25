import unittest
from TestInput import TestInput, TestInputSingleton
import mc_bin_client
import uuid
import logger
import time
import crc32
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection


class ExpiryTests(unittest.TestCase):

    log = None
    keys = None
    clients = None
    servers = None
    input = TestInput
    bucket_port = None

    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.servers = self.input.servers

        ClusterOperationHelper.cleanup_cluster(self.servers)
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        for serverInfo in self.servers:
            remote = RemoteMachineShellConnection(serverInfo)
            info = remote.extract_remote_info()
            remote.terminate_process(info, 'memcached')
            remote.terminate_process(info, 'moxi')

            #let's first install membase server

        time.sleep(10)
        #lets create 'default' bucket
        bucket_name = 'default'
        self.bucket_port = 11211
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            rest.create_bucket(bucket=bucket_name,
                               ramQuotaMB=200,
                               replicaNumber=1,
                               proxyPort=self.bucket_port,
                               bucketType='membase')
            msg = 'create_bucket succeeded but bucket "default" does not exist'
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(bucket_name, rest), msg=msg)

        self.wait_till_memcached_is_ready(self.bucket_port)
        self.clients = self.create_mc_bin_clients_for_ips(self.servers)
        #populate key
        testuuid = uuid.uuid4()
        self.keys = ["key_%s_%d" % (testuuid, i) for i in range(100)]

    # test case to set 1000 keys and verify that those keys are stored
    def test_expiry_small_keys(self):
        self.log.info('starting ... test_expiry_small_keys for ips : {0}'.format(self.servers))
        for serverInfo in self.servers:
            self.log.info(
                'pushing 100 keys (sizes between 100 bytes to 1KB to different vbuckets in membase @ {0}'.format(serverInfo.ip))
            for key in self.keys:
                vbucketId = crc32.crc32_hash(key) & 1023 # or & 0x3FF
                self.clients[serverInfo.ip].vbucketId = vbucketId
                self.log.info("vbucket : {0}".format(vbucketId))
                try:
                    #TODO: also invoke add and update
                    self.clients[serverInfo.ip].set(key, 2, 0, key)
                    self.log.info("inserted key {0} to vBucket {1}".format(key, vbucketId))
                except mc_bin_client.MemcachedError as error:
                    self.log.info('memcachedError : {0}'.format(error.status))
                    self.log.info("unable to push key : {0} to bucket : {1}".format(key, self.clients[serverInfo.ip].vbucketId))

            self.log.info('sleep for 10 seconds wait for those items with expiry set to 10 to expire')
            time.sleep(10)
            for key in self.keys:
                try:
                    vbucketId = crc32.crc32_hash(key) & 1023 # or & 0x3FF
                    self.clients[serverInfo.ip].vbucketId = vbucketId
                    self.clients[serverInfo.ip].get(key=key)
                    self.fail("key {0} did not expire".format(key))
                except mc_bin_client.MemcachedError as error:
                    self.log.info('expected error : memcachedError : {0}'.format(error.status))


    def tearDown(self):
    #let's clean up the memcached
    #        BucketOperationHelper.delete_all_buckets_or_assert(self.ips, self)
        pass

    def generate_payload(self, pattern, size):
        return (pattern * (size / len(pattern))) + pattern[0:(size % len(pattern))]

    def create_mc_bin_clients_for_ips(self, servers):
        clients = {}
        for serverInfo in self.servers:
            clients[serverInfo.ip] = mc_bin_client.MemcachedClient(serverInfo.ip, self.bucket_port)
        return clients


    def wait_till_memcached_is_ready(self, bucket_port):
        for serverInfo in self.servers:
            start_time = time.time()
            memcached_ready = False
            #bucket port
            while time.time() <= (start_time + (5 * 60)):
                self.log.info(" port {0}:{1}".format(serverInfo.ip, bucket_port))
                client = mc_bin_client.MemcachedClient(serverInfo.ip, bucket_port)
                key = '{0}'.format(uuid.uuid4())
                vbucketId = crc32.crc32_hash(key) & 1023 # or & 0x3FF
                client.vbucketId = vbucketId
                try:
                    client.set(key, 0, 0, key)
                    self.log.info("inserted key {0} to vBucket {1}".format(key, vbucketId))
                    memcached_ready = True
                    break
                except mc_bin_client.MemcachedError as error:
                    self.log.error(
                        "memcached not ready yet .. (memcachedError : {0}) - unable to push key : {1} to bucket : {2}".format(
                            error.status, key, client.vbucketId))
                except Exception:
                    self.log.error("memcached not ready yet .. unable to push key : {0} to bucket : {1}".format(key,
                                                                                                           client.vbucketId))
                time.sleep(5)
                #close memcached
            if not memcached_ready:
                self.fail('memcached not ready for {0} after waiting for 5 minutes'.format(serverInfo.ip))
