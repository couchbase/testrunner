from .tuq import QueryTests
from pytests.fts.random_query_generator.rand_query_gen import DATASET
from collections.abc import Mapping, Sequence, Set
from collections import deque
import json
import threading
from pytests.fts.fts_base import CouchbaseCluster
from membase.api.exception import CBQError
from membase.api.rest_client import RestConnection

class ClusterOpsLargeMetaKV(QueryTests):

    users = {}

    def suite_setUp(self):
        super(ClusterOpsLargeMetaKV, self).suite_setUp()

    def runTest(self):
        pass


    def setUp(self):
        super(ClusterOpsLargeMetaKV, self).setUp()

        self.log.info("==============  ClusterOpsLargeMetaKV setuAp has started ==============")
        self.log_config_info()
        self.dataset = self.input.param("dataset", "emp")
        self.custom_map = self.input.param("custom_map", False)
        self.bucket_name = self.input.param("bucket_name", 'default')
        self.rebalance_in = self.input.param("rebalance_in", False)
        self.failover = self.input.param("failover", False)
        self.rebalance_out = self.input.param("rebalance_out", False)
        self.swap_rebalance = self.input.param("swap_rebalance", False)
        self.node_service_in = self.input.param("node_service_in", None)
        self.node_service_out = self.input.param("node_service_out", None)
        self.graceful_failover = self.input.param("graceful_failover", True)
        self.num_fts_partitions = self.input.param("num_fts_partitions", 6)
        self.num_fts_replica = self.input.param("num_fts_replica", 0)
        self.num_gsi_indexes = self.input.param("num_gsi_indexes", 200)
        self.num_fts_indexes = self.input.param("num_fts_indexes", 30)
        self.index_ram = self.input.param('index_ram', 512)
        self.index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        self.n1ql_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        for index_node in self.index_nodes:
            rest = RestConnection(index_node)
            rest.set_index_settings({"queryport.client.usePlanner": False})
            rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=self.index_ram)
        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        self.log.info("==============  ClusterOpsLargeMetaKV setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  ClusterOpsLargeMetaKV tearDown has started ==============")
        self.log_config_info()
        self.log.info("==============  ClusterOpsLargeMetaKV tearDown has completed ==============")
        super(ClusterOpsLargeMetaKV, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  ClusterOpsLargeMetaKV suite_tearDown has started ==============")
        self.log_config_info()
        self.log.info("==============  ClusterOpsLargeMetaKV suite_tearDown has completed ==============")
        super(ClusterOpsLargeMetaKV, self).suite_tearDown()

    def create_drop_simple_gsi_replica_indexes(self, num_gsi_indexes, index_prefix_num=0, drop_indexes=False):
        num_indexes_per_bucket = int(num_gsi_indexes / len(self.buckets))
        for bucket in self.buckets:
            for i in range(index_prefix_num, num_indexes_per_bucket+index_prefix_num):
                self.run_cbq_query("create index gsi_index{0} on {1}(name) "
                                   "with {{'defer_build': true,'num_replica': 1}}"
                                   .format(i, bucket))
            if drop_indexes:
                for i in range(index_prefix_num, num_indexes_per_bucket+index_prefix_num):
                    self.run_cbq_query("drop index {1}.gsi_index{0}".format(i, bucket))

    def build_indexes(self):
        for bucket in self.buckets:
            query = "build index on {0} (( select raw name from system:indexes where `keyspace_id` = '{0}' and state = 'deferred'))".format(bucket)

            try:
                self.run_cbq_query(query=query, server=self.n1ql_nodes[0])
            except Exception as err:
                self.fail('{0} failed with {1}'.format(str(query), str(err)))

    def create_fts_indexes(self, num_fts_indexes, index_prefix_num=0, drop_indexes=False):
        plan_params = {}
        if self.num_fts_partitions:
            plan_params["indexPartitions"] = self.num_fts_partitions
        if self.num_fts_replica:
            plan_params["numReplicas"] = self.num_fts_replica

        sourceParams = {"feedAllotment": "1:n"}

        num_indexes_per_bucket = int(num_fts_indexes / len(self.buckets))
        for bucket in self.buckets:
            for i in range(index_prefix_num, num_indexes_per_bucket+index_prefix_num):
                try:
                    self.create_fts_index(
                        name=bucket.name+"_custom_index"+str(i), source_name=bucket.name,
                        plan_params=plan_params, doc_count=self.num_items, cbcluster=self.cbcluster,
                        source_params=sourceParams)
                except AssertionError as err:
                    self.log.info(str(err))

            if drop_indexes:
                for i in range(index_prefix_num, num_indexes_per_bucket+index_prefix_num):
                    self.cbcluster.delete_fts_index(bucket.name+"_custom_index"+str(i))
                self.sleep(300)

            # ======================== tests =====================================================

    def test_clusterops_large_metakv(self):

        self.sleep(10)
        self._load_emp_dataset_on_all_buckets(end=self.num_items)

        self.create_drop_simple_gsi_replica_indexes(self.num_gsi_indexes)

        self.build_indexes()
        self.create_fts_indexes(self.num_fts_indexes)

        self.create_drop_simple_gsi_replica_indexes(400, index_prefix_num=100, drop_indexes=True)

        self.create_fts_indexes(50, index_prefix_num=100, drop_indexes=True)

        if self.rebalance_in:

            self.log.info("Now rebalancing in")

            try:
                self.cluster.rebalance(self.servers[:self.nodes_init],
                                             [self.servers[self.nodes_init]], [],services=[self.node_service_in])
            except Exception as e:
                    self.fail("Rebalance in failed with {0}".format(str(e)))

        elif self.failover:
            self.log.info("Now failover node")
            self.node_out = self.get_nodes_from_services_map(service_type=self.node_service_out, get_all_nodes=True)
            try:
                self.cluster.failover(self.servers[:self.nodes_init],
                                      [self.node_out[0]], graceful=self.graceful_failover)
            except Exception as e:
                self.fail("node failover failed with {0}".format(str(e)))
        elif self.rebalance_out:
            self.node_out = self.get_nodes_from_services_map(service_type=self.node_service_out, get_all_nodes=True)
            self.log.info("Now rebalancing out")
            try:
                self.cluster.rebalance(self.servers[:self.nodes_init],
                                             [], [self.node_out[0]])
            except Exception as e:
                self.fail("Rebalance out failed with {0}".format(str(e)))

        elif self.swap_rebalance:
            self.node_out = self.get_nodes_from_services_map(service_type=self.node_service_out, get_all_nodes=True)
            self.log.info("Now swap rebalance")

            try:
                self.cluster.rebalance(self.servers[:self.nodes_init],
                                       [self.servers[self.nodes_init]], [self.node_out[0]],services=[self.node_service_in])
            except Exception as e:
                self.fail("Rebalance in failed with {0}".format(str(e)))