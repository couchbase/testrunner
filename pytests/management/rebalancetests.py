import time
import unittest
from TestInput import TestInputSingleton
import logger
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper as ClusterHelper, ClusterOperationHelper
from memcached.helper.data_helper import MemcachedClientHelper

class RebalanceBaseTest(unittest.TestCase):
    @staticmethod
    def common_setup(input, bucket, testcase):
        log = logger.Logger.get_logger()
        servers = input.servers
        ClusterHelper.cleanup_cluster(servers)
        BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)
        serverInfo = servers[0]
        log.info('picking server : {0} as the master'.format(serverInfo))
        rest = RestConnection(serverInfo)
        info = rest.get_nodes_self()
        rest.init_cluster(username=serverInfo.rest_username,
                          password=serverInfo.rest_password)
        rest.init_cluster_memoryQuota(memoryQuota=info.mcdMemoryReserved)
        bucket_ram = info.mcdMemoryReserved * 2 / 3
        rest.create_bucket(bucket=bucket, ramQuotaMB=bucket_ram, replicaNumber=1, proxyPort=11211)
        BucketOperationHelper.wait_till_memcached_is_ready_or_assert(servers=[serverInfo],
                                                                     bucket_port=11211,
                                                                     test=testcase)

    @staticmethod
    def common_tearDown(servers, testcase):
        #let's just clean up the buckets in the master node
        ClusterOperationHelper.cleanup_cluster(servers)
        BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)


        #load data . add all node . load more data . rebalance

#load data. add one node rebalance , rebalance out
class IncrementalRebalanceInTests(unittest.TestCase):
    pass

    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()
        RebalanceBaseTest.common_setup(self._input, 'default', self)

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self._servers, self)

    #load data add one node , rebalance add another node rebalance
    def _common_test_body(self, load_ratio):
        master = self._servers[0]
        rest = RestConnection(master)
        creds = self._input.membase_settings
        items_inserted_count = 0

        self.log.info("inserting some items in the master before adding any nodes")
        distribution = {10: 0.5, 20: 0.5}
        inserted_keys, rejected_keys =\
        MemcachedClientHelper.load_bucket(serverInfo=master,
                                          ram_load_ratio=0.1,
                                          value_size_distribution=distribution,
                                          number_of_threads=20)
        items_inserted_count += len(inserted_keys)

        for server in self._servers[1:]:
            nodes = rest.node_statuses()
            otpNodeIds = [node.id for node in nodes]
            if 'ns_1@127.0.0.1' in otpNodeIds:
                otpNodeIds.remove('ns_1@127.0.0.1')
                otpNodeIds.append('ns_1@{0}'.format(master.ip))
            self.log.info("current nodes : {0}".format(otpNodeIds))
            self.log.info("adding node {0} and rebalance afterwards".format(server.ip))
            otpNode = rest.add_node(creds.rest_username,
                                    creds.rest_password,
                                    server.ip)
            msg = "unable to add node {0} to the cluster"
            self.assertTrue(otpNode, msg.format(server.ip))
            otpNodeIds.append(otpNode.id)
            inserted_keys, rejected_keys =\
            MemcachedClientHelper.load_bucket(serverInfo=master,
                                              ram_load_ratio=load_ratio,
                                              value_size_distribution=distribution,
                                              number_of_threads=20)
            self.log.info('inserted {0} keys'.format(len(inserted_keys)))
            rest.rebalance(otpNodes=otpNodeIds, ejectedNodes=[])
            self.assertTrue(rest.monitorRebalance(),
                            msg="rebalance operation failed after adding node {0}".format(server.ip))
            items_inserted_count += len(inserted_keys)
            final_replication_state = RestHelper(rest).wait_for_replication(120)
            msg = "replication state after waiting for up to 2 minutes : {0}"
            self.log.info(msg.format(final_replication_state))
            start_time = time.time()
            stats = rest.get_bucket_stats()
            while time.time() < (start_time + 120) and stats["curr_items"] != items_inserted_count:
                self.log.info("curr_items : {0} versus {1}".format(stats["curr_items"], items_inserted_count))
                time.sleep(5)
                stats = rest.get_bucket_stats()
            self.log.info("curr_items : {0} versus {1}".format(stats["curr_items"], items_inserted_count))
            msg="curr_items : {0} is not equal to actual # of keys inserted : {1}"
            self.assertEquals(stats["curr_items"], items_inserted_count,
                              msg=msg.format(stats["curr_items"], items_inserted_count))

    def test_small_load(self):
        self._common_test_body(0.1)

    def test_medium_load(self):
        self._common_test_body(10.0)

    def test_heavy_load(self):
        self._common_test_body(80.0)

#disk greater than memory
#class IncrementalRebalanceInAndOutTests(unittest.TestCase):
#    pass

