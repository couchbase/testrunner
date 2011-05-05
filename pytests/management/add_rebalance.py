import unittest
from TestInput import TestInputSingleton
import logger
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper as ClusterHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import MemcachedClientHelper as MemcachedHelper

class AddRebalanceNodesTest(unittest.TestCase):

    @staticmethod
    def common_setup(input,bucket,testcase):
        log = logger.Logger.get_logger()
        servers = input.servers
        ClusterHelper.cleanup_cluster(servers)
        BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)
        serverInfo = servers[0]
        log.info('picking server : {0} as the master'.format(serverInfo))
        rest = RestConnection(serverInfo)
        info = rest.get_nodes_self()
        rest.init_cluster(username = serverInfo.rest_username,
                          password = serverInfo.rest_password)
        rest.init_cluster_memoryQuota(memoryQuota=info.mcdMemoryReserved)
        bucket_ram = info.mcdMemoryReserved * 2 / 3
        rest.create_bucket(bucket=bucket, ramQuotaMB=bucket_ram, replicaNumber=1, proxyPort=11211)
        BucketOperationHelper.wait_till_memcached_is_ready_or_assert(servers=[serverInfo],
                                                                     bucket_port=11211,
                                                                     test=testcase)

    #add them all rebalance
    def add_rebalance_remove(self, ram_load_ratio):
        #add nodes
        master = self._servers[0]
        #size distribution ?
        distribution = {1024: 0.4, 10 * 1024: 0.4, 40 * 1024: 0.2}
        MemcachedHelper.load_bucket(serverInfo=master,
                                    name=self._bucket,
                                    port=11211,
                                    ram_load_ratio=ram_load_ratio,
                                    value_size_distribution=distribution)
        rest = RestConnection(master)
        ClusterHelper.add_all_nodes_or_assert(master, self._servers,
                                              self._input.membase_settings, self)
        nodes = rest.node_statuses()
        otpNodeIds = [node.id for node in nodes]
        before_info = rest.get_bucket(self._bucket)
        rebalanceStarted = rest.rebalance(otpNodeIds, [])
        self.assertTrue(rebalanceStarted,
                        "unable to start rebalance on master node {0}".format(master.ip))
        self.log.info('started rebalance operation on master node {0}'.format(master.ip))
        rebalance_succeeded = rest.monitorRebalance()
        self.assertTrue(rebalance_succeeded,
                        "rebalance operation for nodes: {0} was not successful".format(otpNodeIds))
        self.log.info('rebalance operation succeeded for nodes: {0}'.format(otpNodeIds))
        after_info = rest.get_bucket(self._bucket)
        RebalanceHelper.verify_maps(before_info.forward_map, after_info.vbuckets)
        #now remove the nodes
        #make sure its rebalanced and node statuses are healthy
        helper = RestHelper(rest)
        self.assertTrue(helper.is_cluster_healthy(), "cluster status is not healthy")
        self.assertTrue(helper.is_cluster_rebalanced(), "cluster is not balanced")
        self.assertTrue(helper.wait_for_replication(180),
                        msg="replication did not complete")
        self.assertTrue(RebalanceHelper.wait_till_total_numbers_match(master=master,
                                                      servers=self._servers,
                                                      bucket=self._bucket,
                                                      replica_factor=1,
                                                      timeout_in_seconds=300),
                        msg="replication was completed but sum(curr_items) dont match the curr_items_total")
        knownNodes, ejectedNodes =\
        ClusterHelper.rebalance_params_for_declustering(master=master,all_nodes=nodes)
        helper.remove_nodes(knownNodes, ejectedNodes)

    @staticmethod
    def common_tearDown(servers,testcase):
        #let's just clean up the buckets in the master node
        BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)


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

    def test_add_one_by_one_and_rebalance_1_percent_ram(self):
        self.add_one_by_one_and_rebalance(1.0)

    def add_one_by_one_and_rebalance(self, ram_load_ratio):
        #add nodes
        master = self._servers[0]
        #size distribution ?
        distribution = {1024: 0.4, 10 * 1024: 0.4, 40 * 1024: 0.2}
        MemcachedHelper.load_bucket(serverInfo=master, name=self._bucket,
                                    port=11211, ram_load_ratio=ram_load_ratio,
                                    value_size_distribution=distribution)
        rest = RestConnection(master)
        helper = RestHelper(rest)
        if len(self._servers) > 1:
            for i in range(start=1,stop=len(self._servers)):
                ClusterHelper.add_all_nodes_or_assert(master,
                                                      [self._servers[i]],
                                                      self._input.membase_settings,
                                                      self)
            nodes = rest.node_statuses()
            otpNodeIds = [node.id for node in nodes]
            before_info = rest.get_bucket(self._bucket)
            rebalanceStarted = rest.rebalance(otpNodeIds, [])
            self.assertTrue(rebalanceStarted,
                            "unable to start rebalance on master node {0}".format(master.ip))
            self.log.info('started rebalance operation on master node {0}'.format(master.ip))
            rebalance_succeeded = rest.monitorRebalance()
            self.assertTrue(rebalance_succeeded,
                        "rebalance operation for nodes: {0} was not successful".format(otpNodeIds))
            self.log.info('rebalance operation succeeded for nodes: {0}'.format(otpNodeIds))
            after_info = rest.get_bucket(self._bucket)
            RebalanceHelper.verify_maps(before_info.forward_map, after_info.vbuckets)
            #now remove the nodes
            #make sure its rebalanced and node statuses are healthy

            self.assertTrue(helper.is_cluster_healthy(), "cluster status is not healthy")
            self.assertTrue(helper.is_cluster_rebalanced(), "cluster is not balanced")
            self.assertTrue(helper.wait_for_replication(180),
                            msg="replication did not complete")
            self.assertTrue(RebalanceHelper.wait_till_total_numbers_match(master=master,
                                                      servers=self._servers,
                                                      bucket=self._bucket,
                                                      replica_factor=1,
                                                      timeout_in_seconds=300),
                        msg="replication was completed but sum(curr_items) dont match the curr_items_total")

                #now rebalance ?

        #now we can remove everything
        nodes = rest.node_statuses()
        knownNodes, ejectedNodes =\
        ClusterHelper.rebalance_params_for_declustering(master=master,all_nodes=nodes)
        helper.remove_nodes(knownNodes, ejectedNodes)




#load data .add all nodes. rebalance .
#load data x percent and add all nodes
class RebalanceAllNodesTest(unittest.TestCase):

    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()
        AddRebalanceNodesTest.common_setup(self._input,
                                           bucket='default',
                                           testcase=self)

    def tearDown(self):
        AddRebalanceNodesTest.common_tearDown(self._servers,self)


    def _test_body(self):
        #add nodes
        master = self._servers[0]
        #size distribution ?
        distribution = {1024: 0.4, 10 * 1024: 0.4, 40 * 1024: 0.2}
        MemcachedHelper.load_bucket(serverInfo=master, name=self._bucket,
                                    port=11211, ram_load_ratio=ram_load_ratio,
                                    value_size_distribution=distribution)
        rest = RestConnection(master)
        helper = RestHelper(rest)
        if len(self._servers) > 1:
            for i in range(start=1,stop=len(self._servers)):
                ClusterHelper.add_all_nodes_or_assert(master,
                                                      [self._servers[i]],
                                                      self._input.membase_settings,
                                                      self)
            nodes = rest.node_statuses()
            otpNodeIds = [node.id for node in nodes]
            before_info = rest.get_bucket(self._bucket)
            rebalanceStarted = rest.rebalance(otpNodeIds, [])
            self.assertTrue(rebalanceStarted,
                            "unable to start rebalance on master node {0}".format(master.ip))
            self.log.info('started rebalance operation on master node {0}'.format(master.ip))
            rebalance_succeeded = rest.monitorRebalance()
            self.assertTrue(rebalance_succeeded,
                        "rebalance operation for nodes: {0} was not successful".format(otpNodeIds))
            self.log.info('rebalance operation succeeded for nodes: {0}'.format(otpNodeIds))
            after_info = rest.get_bucket(self._bucket)
            RebalanceHelper.verify_maps(before_info.forward_map, after_info.vbuckets)
            #now remove the nodes
            #make sure its rebalanced and node statuses are healthy

            self.assertTrue(helper.is_cluster_healthy(), "cluster status is not healthy")
            self.assertTrue(helper.is_cluster_rebalanced(), "cluster is not balanced")
            self.assertTrue(helper.wait_for_replication(180),
                            msg="replication did not complete")
            self.assertTrue(RebalanceHelper.wait_till_total_numbers_match(master=master,
                                                      servers=self._servers,
                                                      bucket=self._bucket,
                                                      replica_factor=1,
                                                      timeout_in_seconds=300),
                        msg="replication was completed but sum(curr_items) dont match the curr_items_total")

                #now rebalance ?

        #now we can remove everything
        nodes = rest.node_statuses()
        knownNodes, ejectedNodes =\
        ClusterHelper.rebalance_params_for_declustering(master=master,all_nodes=nodes)
        helper.remove_nodes(knownNodes, ejectedNodes)



    def test_no_data(self):
        pass

    def test_with_1_percent_data(self):
        pass

    def test_with_10_percent_data(self):
        pass

    def test_with_50_percent_data(self):
        pass

    def test_with_full_ram(self):
        pass





#load data . add all node . load more data . rebalance

#load data. add one node rebalance , rebalance out
class IncrementalRebalanceInTests(unittest.TestCase):
    pass
#load data add one node , rebalance add another node rebalance

class IncrementalRebalanceInAndOutTests(unittest.TestCase):
    pass

