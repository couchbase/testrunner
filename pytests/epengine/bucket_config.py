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
from membase.helper.cluster_helper import ClusterOperationHelper
import logger

from remote.remote_util import RemoteMachineShellConnection


class basic_ops(unittest.TestCase):

    def setUp(self):
        self.testcase = '1'
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.servers = self.input.servers
        self.time_synchronization = self.input.param("time_sync", "enabledWithoutDrift")
        self.bucket='bucket-1'
        self.master = self.servers[0]
        self.rest = RestConnection(self.master)

        node_ram_ratio = BucketOperationHelper.base_bucket_ratio(self.servers)
        mem_quota = int(self.rest.get_nodes_self().mcdMemoryReserved *
                        node_ram_ratio)

        self.rest.init_cluster(self.master.rest_username,
            self.master.rest_password)
        self.rest.init_cluster_memoryQuota(self.master.rest_username,
            self.master.rest_password,
            memoryQuota=mem_quota)
        for server in self.servers:
            ClusterOperationHelper.cleanup_cluster([server])
            ClusterOperationHelper.wait_for_ns_servers_or_assert(
                [self.master], self.testcase)

        try:
            rebalanced = ClusterOperationHelper.add_and_rebalance(
                self.servers)

        except Exception, e:
            self.fail(e, 'cluster is not rebalanced')

        self._create_bucket()

    def tearDown(self):
        if not "skip_cleanup" in TestInputSingleton.input.test_params:
            BucketOperationHelper.delete_all_buckets_or_assert(
                self.servers, self.testcase)
            ClusterOperationHelper.cleanup_cluster(self.servers)
            ClusterOperationHelper.wait_for_ns_servers_or_assert(
                self.servers, self.testcase)


    def test_restart(self):
        self.log.info("Restarting the servers ..")
        self._restart_server(self.servers[:])
        self.log.info("Verifying bucket settings after restart ..")
        self._check_config()

    #create a bucket if it doesn't exist
    def _create_bucket(self):
        helper = RestHelper(self.rest)
        if not helper.bucket_exists(self.bucket):
            node_ram_ratio = BucketOperationHelper.base_bucket_ratio(
                self.servers)
            info = self.rest.get_nodes_self()
            available_ram = int(info.memoryQuota * node_ram_ratio)
            if available_ram < 256:
                available_ram = 256
            self.rest.create_bucket(bucket=self.bucket,
                ramQuotaMB=available_ram,authType='sasl',timeSynchronization=self.time_synchronization)
            try:
                ready = BucketOperationHelper.wait_for_memcached(self.master,
                    self.bucket)
            except Exception, e:
                self.fail(e, 'unable to create bucket')

    def _restart_server(self, servers):
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            shell.stop_couchbase()
            time.sleep(10)
            shell.start_couchbase()
            shell.disconnect()
        time.sleep(30)

    def _check_config(self):
        result = self.rest.get_bucket_json(self.bucket)["timeSynchronization"]
        print result
        self.assertEqual(result,self.time_synchronization, msg='ERROR, Mismatch on expected time synchronization values')
        self.log.info("Verified results")
