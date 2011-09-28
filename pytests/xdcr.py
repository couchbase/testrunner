from TestInput import TestInputSingleton
import logger
import time

import unittest
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection


class XDCRBaseTest(unittest.TestCase):

    @staticmethod
    def common_setup(input, testcase):
        # Resource file has 'cluster' tag
        for key, servers in input.clusters.items():
            for server in servers:
                ClusterOperationHelper.cleanup_cluster([server])
            ClusterOperationHelper.wait_for_ns_servers_or_assert(servers, testcase)
            BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)

            XDCRBaseTest.cluster_initialization(servers)
            XDCRBaseTest.create_buckets(servers, testcase, howmany=2)
            XDCRBaseTest.rebalance_servers_in(servers, input.membase_settings, testcase)

    @staticmethod
    def common_tearDown(servers, testcase):
        BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)
        for server in servers:
            ClusterOperationHelper.cleanup_cluster([server])
        ClusterOperationHelper.wait_for_ns_servers_or_assert(servers, testcase)

    @staticmethod
    def choose_nodes(master, nodes, howmany):
        selected = []
        for node in nodes:
            if not XDCRBaseTest.contains(node.ip, master.ip) and\
               not XDCRBaseTest.contains(node.ip, '127.0.0.1'):
                selected.append(node)
                if len(selected) == howmany:
                    break
        return selected

    @staticmethod
    def contains(string1, string2):
        if string1 and string2:
            return string1.find(string2) != -1
        return False

    @staticmethod
    def cluster_initialization(servers):
        log = logger.Logger().get_logger()
        master = servers[0]
        log.info('picking server : {0} as the master'.format(master))
        #if all nodes are on the same machine let's have the bucket_ram_ratio as bucket_ram_ratio * 1/len(servers)
        node_ram_ratio = BucketOperationHelper.base_bucket_ratio(servers)
        rest = RestConnection(master)
        info = rest.get_nodes_self()
        rest.init_cluster(username=master.rest_username, password=master.rest_password)
        rest.init_cluster_memoryQuota(memoryQuota=int(info.mcdMemoryReserved * node_ram_ratio))

    @staticmethod
    def create_buckets(servers, testcase, howmany=1, replica=1, bucket_ram_ratio=(2.0 / 3.0)):
        node_ram_ratio = BucketOperationHelper.base_bucket_ratio(servers)
        master = servers[0]
        BucketOperationHelper.create_multiple_buckets(master, replica, node_ram_ratio * bucket_ram_ratio, howmany=howmany)
        rest = RestConnection(master)
        buckets = rest.get_buckets()
        for bucket in buckets:
            ready = BucketOperationHelper.wait_for_memcached(master, bucket.name)
            testcase.assertTrue(ready, "wait_for_memcached failed")

    @staticmethod
    def rebalance_servers_in(servers, rest_settings, testcase):
        log = logger.Logger().get_logger()
        master = servers[0]
        rest = RestConnection(master)
        ClusterOperationHelper.add_all_nodes_or_assert(master, servers, rest_settings, testcase)

        otpNodeIds = []
        for node in rest.node_statuses():
            otpNodeIds.append(node.id)
        rebalanceStarted = rest.rebalance(otpNodeIds, [])
        testcase.assertTrue(rebalanceStarted,
                            "unable to start rebalance on master node {0}".format(master.ip))
        log.info('started rebalance operation on master node {0}'.format(master.ip))
        rebalanceSucceeded = rest.monitorRebalance()
        testcase.assertTrue(rebalanceSucceeded,
                            "rebalance operation for nodes: {0} was not successful".format(otpNodeIds))


class XDCRTests(unittest.TestCase):
    def setUp(self):
        self.log = logger.Logger().get_logger()
        self._input = TestInputSingleton.input
        self._clusters = self._input.clusters
        if not self._clusters:
            self.log.info("No Cluster tags defined in resource file")
            exit(1)
        XDCRBaseTest.common_setup(self._input, self)

    def tearDown(self):
        for id, servers in self._clusters.items():
            XDCRBaseTest.common_tearDown(servers, self)

    def test_unidirectional_setup(self):
        self.common_test_body()

    def test_bidirectional_setup(self):
        self.common_test_body(False)

    def test_scheduled_setup(self):
        self.common_test_body()

    def test_continuous_setup(self):
        self.common_test_body()

    def test_conflict_resolution(self):
        self.common_test_body()

    def test_progress(self):
        self.common_test_body()

    def test_cancellation(self):
        self.common_test_body()

    def test_existing_replication_configuration(self):
        self.common_test_body()

    def test_existing_replication_configuration(self):
        self.common_test_body()

    def common_test_body(self, unidirectional=True):
        log = logger.Logger.get_logger()
        log.info("Common Test Body")
        