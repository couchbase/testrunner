import unittest
from TestInput import TestInputSingleton
import logger
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper as ClusterHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import MemcachedClientHelper as MemcachedHelper

class AddRebalanceNodesTest(unittest.TestCase):
    input = None
    servers = None
    bucket_name = None
    log = None

    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.assertTrue(self.input, msg="input parameters missing...")
        self.servers = self.input.servers
        ClusterHelper.cleanup_cluster(self.servers)
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        self.bucket_name = 'default'
        rest = RestConnection(self.servers[0])
        rest.init_cluster()
        rest.init_cluster_memoryQuota(username=self.input.membase_settings.rest_username,
                                      password=self.input.membase_settings.rest_password,
                                      memoryQuota=200)

        rest.create_bucket(bucket=self.bucket_name,
                           ramQuotaMB=256,
                           replicaNumber=1,
                           proxyPort=11211)
        BucketOperationHelper.wait_till_memcached_is_ready_or_assert(servers=[self.servers[0]],
                                                                     bucket_port=11211,
                                                                     test=self)

    #add them all rebalance
    def add_rebalance_remove(self, ram_load_ratio):
        #add nodes
        master = self.servers[0]
        #size distribution ?
        distribution = {1024: 0.4, 10 * 1024: 0.4, 40 * 1024: 0.2}
        MemcachedHelper.load_bucket(master, self.bucket_name, 11211, ram_load_ratio, distribution)
        rest = RestConnection(master)
        ClusterHelper.add_all_nodes_or_assert(master, self.servers,
                                              self.input.membase_settings, self)
        nodes = rest.node_statuses()
        otpNodeIds = [node.id for node in nodes]
        before_info = rest.get_bucket(self.bucket_name)
        rebalanceStarted = rest.rebalance(otpNodeIds, [])
        self.assertTrue(rebalanceStarted,
                        "unable to start rebalance on master node {0}".format(master.ip))
        self.log.info('started rebalance operation on master node {0}'.format(master.ip))
        rebalance_succeeded = rest.monitorRebalance()
        self.assertTrue(rebalance_succeeded,
                        "rebalance operation for nodes: {0} was not successful".format(otpNodeIds))
        self.log.info('rebalance operation succeeded for nodes: {0}'.format(otpNodeIds))
        after_info = rest.get_bucket(self.bucket_name)
        RebalanceHelper.verify_maps(before_info.forward_map, after_info.vbuckets)
        #now remove the nodes
        #make sure its rebalanced and node statuses are healthy
        helper = RestHelper(rest)
        self.assertTrue(helper.is_cluster_healthy(), "cluster status is not healthy")
        self.assertTrue(helper.is_cluster_rebalanced(), "cluster is not balanced")
        self.assertTrue(helper.wait_for_replication(180),
                        msg="replication did not complete")
        knownNodes, ejectedNodes =\
        ClusterHelper.rebalance_params_for_declustering(master=master,all_nodes=nodes)
        helper.remove_nodes(knownNodes, ejectedNodes)

    def tearDown(self):
        #let's just clean up the buckets in the master node
        BucketOperationHelper.delete_all_buckets_or_assert([self.servers[0]], self)


    def test_rebalance_0_1_percent_ram(self):
        self.add_rebalance_remove(0.1)

    def test_rebalance_1_percent_ram(self):
        self.add_rebalance_remove(1.0)

    def test_rebalance_5_percent_ram(self):
        self.add_rebalance_remove(5.0)

    def test_rebalance_10_percent_ram(self):
        self.add_rebalance_remove(10.0)

    def test_rebalance_50_percent_ram(self):
        self.add_rebalance_remove(50.0)

    def test_rebalance_99_percent_ram(self):
        self.add_rebalance_remove(99.0)
