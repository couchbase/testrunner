import time
import logger
import unittest
from TestInput import TestInput, TestInputSingleton

from memcacheConstants import ERR_NOT_FOUND
from castest.cas_base import CasBaseTest
from couchbase_helper.documentgenerator import BlobGenerator
from mc_bin_client import MemcachedError

from membase.api.rest_client import RestConnection, RestHelper
from memcached.helper.data_helper import VBucketAwareMemcached, MemcachedClientHelper
from membase.helper.bucket_helper import BucketOperationHelper
import json


from remote.remote_util import RemoteMachineShellConnection


class basic_ops(unittest.TestCase):

    def setUp(self):
        self.input = TestInputSingleton.input
        self.servers = self.input.servers
        self.time_synchronization = self.input.param("time_sync", "enabledWithoutDrift")
        self.name='bucket-1'
        BucketOperationHelper.delete_all_buckets_or_assert(servers=self.servers, test_case=self)

    def tearDown(self):
        BucketOperationHelper.delete_all_buckets_or_assert(servers=self.servers, test_case=self)

    # Usage : ./testrunner -i your.ini -t epengine.bucket_config.basic_ops.test_bucket_config -p time_sync='disabled'
    def test_bucket_config(self):

     for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            rest.create_bucket(bucket=self.name, ramQuotaMB=499, authType='sasl', timeSynchronization=self.time_synchronization)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(self.name, rest), msg='Error')

    def check_config(self):
        rest = RestConnection(self.servers[0])
        result = rest.get_bucket_json(self.name)["timeSynchronization"]
        print result
        self.assertEqual(result,self.time_synchronization, msg='ERROR, Mismatch on expected time synchronization values')

    def test_restart(self):
        self.test_bucket_config()
        self.restart_server(self.servers[:])
        self.check_config()

    def restart_server(self, servers):
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            shell.stop_couchbase()
            time.sleep(10)
            shell.start_couchbase()
            shell.disconnect()
        time.sleep(30)
