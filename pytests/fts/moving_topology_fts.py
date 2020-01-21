from .fts_base import FTSBaseTest, FTSException
from .fts_base import NodeHelper
from TestInput import TestInputSingleton
from threading import Thread
from lib.remote.remote_util import RemoteMachineShellConnection
from lib.memcached.helper.data_helper import MemcachedClientHelper
import json

class MovingTopFTS(FTSBaseTest):

    def setUp(self):
        self.num_rebalance = TestInputSingleton.input.param("num_rebalance", 1)
        self.query = {"match": "emp", "field": "type"}
        super(MovingTopFTS, self).setUp()

    def tearDown(self):
        super(MovingTopFTS, self).tearDown()

    def suite_setUp(self):
        self.log.info("*** MovingTopFTS: suite_setUp() ***")

    def suite_tearDown(self):
        self.log.info("*** MovingTopFTS: suite_tearDown() ***")

    """ Topology change during indexing"""

    def rebalance_in_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        self._cb_cluster.rebalance_in(num_nodes=1, services=["kv,fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def rebalance_out_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(30)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        self._cb_cluster.rebalance_out()
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def rebalance_out_master_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        self._cb_cluster.rebalance_out_master()
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def swap_rebalance_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        self._cb_cluster.swap_rebalance(services=["fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def swap_rebalance_kv_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        self._cb_cluster.swap_rebalance_master(services=["kv"])
        try:
            for index in self._cb_cluster.get_indexes():
                self.is_index_partitioned_balanced(index)
            self.wait_for_indexing_complete()
            self.validate_index_count(equal_bucket_doc_count=True)
        except Exception as e:
            # to work around fts node becoming master and
            # unable to rebalance out kv node
            self._cb_cluster.swap_rebalance_master(services=["fts"])
            self.validate_index_count(equal_bucket_doc_count=True)
            raise e
        self._cb_cluster.swap_rebalance_master(services=["fts"])
        self.validate_index_count(equal_bucket_doc_count=True)

    def failover_non_master_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        self._cb_cluster.failover_and_rebalance_nodes()
        try:
            for index in self._cb_cluster.get_indexes():
                self.is_index_partitioned_balanced(index)
        except Exception as e:
            if self._cb_cluster.get_num_fts_nodes() == 0:
                self.log.info("Expected exception: %s" % e)
            else:
                raise e
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def failover_no_rebalance_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        self._cb_cluster.async_failover().result()
        self.sleep(60)
        try:
            for index in self._cb_cluster.get_indexes():
                self.is_index_partitioned_balanced(index)
        except Exception as e:
            self.log.info("Expected exception: %s" % e)

    def failover_master_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        self._cb_cluster.rebalance_in(num_nodes=1, services=["kv,fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self._cb_cluster.failover_and_rebalance_master()
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def failover_only_kv_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        try:
            self._cb_cluster.failover_and_rebalance_master()
        except Exception as e:
            self.log.info("Expected exception caught: %s" % e)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def hard_failover_and_delta_recovery_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        task = self._cb_cluster.async_failover()
        task.result()
        self._cb_cluster.add_back_node(recovery_type='delta', services=["kv,fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def hard_failover_and_full_recovery_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        task = self._cb_cluster.async_failover()
        task.result()
        self._cb_cluster.add_back_node(recovery_type='full', services=["kv,fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def graceful_failover_and_delta_recovery_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        task = self._cb_cluster.async_failover(graceful=True)
        task.result()
        self.sleep(60)
        self._cb_cluster.add_back_node(recovery_type='delta', services=["kv,fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def graceful_failover_and_full_recovery_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        task = self._cb_cluster.async_failover(graceful=True)
        task.result()
        self.sleep(30)
        self._cb_cluster.add_back_node(recovery_type='full', services=["kv, fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def warmup_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        self._cb_cluster.warmup_node()
        self.sleep(60, "waiting for fts to start...")
        self.wait_for_indexing_complete()
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.validate_index_count(equal_bucket_doc_count=True)

    def warmup_master_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        self._cb_cluster.warmup_node(master=True)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def node_reboot_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        self._cb_cluster.reboot_one_node(test_case=self)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def node_reboot_only_kv_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        self._cb_cluster.reboot_one_node(test_case=self, master=True)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def memc_crash_on_kv_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        NodeHelper.kill_memcached(self._cb_cluster.get_master_node())
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def erl_crash_on_kv_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        NodeHelper.kill_erlang(self._cb_cluster.get_master_node())
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def fts_node_crash_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        NodeHelper.kill_cbft_process(self._cb_cluster.get_random_fts_node())
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    """ Topology change between indexing and querying"""

    def rebalance_in_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        self._cb_cluster.rebalance_in(num_nodes=self.num_rebalance,
                                      services=["kv,fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._num_items)
        self.log.info("SUCCESS! Hits: %s" % hits)

    def rebalance_out_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        self._cb_cluster.rebalance_out(num_nodes=self.num_rebalance)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._num_items)
        self.log.info("SUCCESS! Hits: %s" % hits)

    def rebalance_out_master_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        self._cb_cluster.rebalance_out(master=True)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._num_items)
        self.log.info("SUCCESS! Hits: %s" % hits)

    def swap_rebalance_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        services = []
        for _ in range(self.num_rebalance):
            services.append("fts")
        self._cb_cluster.swap_rebalance(services=services,
                                        num_nodes=self.num_rebalance)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._num_items)
        self.log.info("SUCCESS! Hits: %s" % hits)

    def hard_failover_and_remove_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        self._cb_cluster.failover_and_rebalance_nodes()
        try:
            for index in self._cb_cluster.get_indexes():
                self.is_index_partitioned_balanced(index)
        except Exception as e:
            if self._cb_cluster.get_num_fts_nodes() == 0:
                self.log.info("Expected exception: %s" % e)
            else:
                raise e
        self.wait_for_indexing_complete()
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._num_items)
        self.log.info("SUCCESS! Hits: %s" % hits)

    def hard_failover_no_rebalance_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        self.wait_for_indexing_complete()
        self._cb_cluster.async_failover().result()
        self.sleep(30)
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query)
            if hits < self._num_items:
                self.log.info("SUCCESS: Fewer docs ({0}) returned after "
                              "hard-failover".format(hits))

    def hard_failover_master_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()
        self._cb_cluster.failover_and_rebalance_master()
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._num_items)
        self.log.info("SUCCESS! Hits: %s" % hits)

    def hard_failover_and_delta_recovery_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()
        task = self._cb_cluster.async_failover()
        task.result()
        self._cb_cluster.add_back_node(recovery_type='delta', services=["kv,fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._num_items)
        self.log.info("SUCCESS! Hits: %s" % hits)

    def hard_failover_and_full_recovery_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()
        task = self._cb_cluster.async_failover()
        task.result()
        self._cb_cluster.add_back_node(recovery_type='full', services=["kv,fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._num_items)
        self.log.info("SUCCESS! Hits: %s" % hits)

    def failover_no_rebalance_with_replicas_between_indexing_and_querying(self):
        index = self.create_index_generate_queries()
        self._cb_cluster.async_failover(
            graceful=self._input.param("graceful", False)).result()
        try:
            for index in self._cb_cluster.get_indexes():
                self.is_index_partitioned_balanced(index)
        except Exception as e:
            if self._cb_cluster.get_num_fts_nodes() == 0:
                self.log.info("Expected exception: %s" % e)
            else:
                raise e
        self.run_query_and_compare(index)

    def graceful_failover_and_delta_recovery_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()
        task = self._cb_cluster.async_failover(graceful=True)
        task.result()
        self.sleep(30)
        self._cb_cluster.add_back_node(recovery_type='delta', services=["kv,fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._num_items)
        self.log.info("SUCCESS! Hits: %s" % hits)

    def graceful_failover_and_full_recovery_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()
        task = self._cb_cluster.async_failover(graceful=True)
        task.result()
        self.sleep(30)
        self._cb_cluster.add_back_node(recovery_type='full', services=["kv, fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._num_items)
        self.log.info("SUCCESS! Hits: %s" % hits)

    def warmup_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        self._cb_cluster.warmup_node()
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.sleep(30, "waiting for fts process to start")
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._num_items)
        self.log.info("SUCCESS! Hits: %s" % hits)

    def warmup_master_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        self._cb_cluster.warmup_node(master=True)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._num_items)
        self.log.info("SUCCESS! Hits: %s" % hits)

    def node_reboot_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        self._cb_cluster.reboot_one_node(test_case=self)
        self.sleep(5)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.validate_index_count(equal_bucket_doc_count=True)
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._num_items)
        self.log.info("SUCCESS! Hits: %s" % hits)

    def memc_crash_between_indexing_and_querying(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        NodeHelper.kill_memcached(self._cb_cluster.get_random_fts_node())
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._num_items)
        self.log.info("SUCCESS! Hits: %s" % hits)

    def erl_crash_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        NodeHelper.kill_erlang(self._cb_cluster.get_random_fts_node())
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._num_items)
        self.log.info("SUCCESS! Hits: %s" % hits)

    def fts_node_crash_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        NodeHelper.kill_cbft_process(self._cb_cluster.get_random_fts_node())
        self.sleep(60)
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._num_items)
        self.log.info("SUCCESS! Hits: %s" % hits)

    """ Topology change during querying"""

    def create_index_generate_queries(self):
        index = self.create_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index")
        self.load_data()
        self.wait_for_indexing_complete()
        self.generate_random_queries(index, self.num_queries, self.query_types)
        return index

    def run_tasks_and_report(self, tasks, num_queries):
        fail_count = 0
        failed_queries = []
        for task in tasks:
            task.result()
            if hasattr(task, 'passed'):
                if not task.passed:
                    fail_count += 1
                    failed_queries.append(task.query_index+1)

        if fail_count:
            self.fail("%s out of %s queries failed! - %s" % (fail_count,
                                                             num_queries,
                                                             failed_queries))
        else:
            self.log.info("SUCCESS: %s out of %s queries passed"
                          %(num_queries-fail_count, num_queries))

    def rebalance_in_during_querying(self):
        #TESTED
        index = self.create_index_generate_queries()
        services = []
        for _ in range(self.num_rebalance):
            services.append("fts")
        tasks = []
        tasks.append(self._cb_cluster.async_rebalance_in(
            num_nodes=self.num_rebalance,
            services=services))
        for count in range(0, len(index.fts_queries)):
            tasks.append(self._cb_cluster.async_run_fts_query_compare(
                fts_index=index,
                es=self.es,
                es_index_name=None,
                query_index=count))
        self.run_tasks_and_report(tasks, len(index.fts_queries))
        self.is_index_partitioned_balanced(index)
        hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._num_items)
        self.log.info("SUCCESS! Hits: %s" % hits)

    def rebalance_out_during_querying(self):
        #TESTED
        index = self.create_index_generate_queries()
        #self.run_query_and_compare(index)
        tasks = []
        tasks.append(self._cb_cluster.async_rebalance_out(
            num_nodes=self.num_rebalance))
        for count in range(0, len(index.fts_queries)):
            tasks.append(self._cb_cluster.async_run_fts_query_compare(
                fts_index=index,
                es=self.es,
                es_index_name=None,
                query_index=count))
        self.run_tasks_and_report(tasks, len(index.fts_queries))
        self.is_index_partitioned_balanced(index)
        self.run_query_and_compare(index)
        hits, _, _, _ = index.execute_query(query=self.query,
                                         expected_hits=self._num_items)
        self.log.info("SUCCESS! Hits: %s" % hits)

    def swap_rebalance_during_querying(self):
        #TESTED
        index = self.create_index_generate_queries()
        services = []
        tasks = []
        for _ in range(self.num_rebalance):
            services.append("fts")
        reb_thread = Thread(
            target=self._cb_cluster.async_swap_rebalance,
            args=[self.num_rebalance, services])
        reb_thread.start()
        # wait for a bit before querying
        self.sleep(8)
        for count in range(0, len(index.fts_queries)):
            tasks.append(self._cb_cluster.async_run_fts_query_compare(
                fts_index=index,
                es=self.es,
                es_index_name=None,
                query_index=count))
        self.run_tasks_and_report(tasks, len(index.fts_queries))
        self.sleep(5)
        self.is_index_partitioned_balanced(index)
        hits, _, _, _ = index.execute_query(query=self.query,
                                         expected_hits=self._num_items)
        self.log.info("SUCCESS! Hits: %s" % hits)

    def hard_failover_no_rebalance_during_querying(self):
        #TESTED
        index = self.create_index_generate_queries()
        services = []
        for _ in range(self.num_rebalance):
            services.append("fts")
        tasks = []
        tasks.append(self._cb_cluster.async_failover(
            num_nodes=1,
            graceful=False))
        for count in range(0, len(index.fts_queries)):
            tasks.append(self._cb_cluster.async_run_fts_query_compare(
                fts_index=index,
                es=self.es,
                es_index_name=None,
                query_index=count))
        self.run_tasks_and_report(tasks, len(index.fts_queries))
        hits, _, _, _ = index.execute_query(query=self.query)
        if hits < self._num_items:
            self.log.info("SUCCESS! Hits: %s" % hits)

    def hard_failover_rebalance_out_during_querying(self):
        #TESTED
        index = self.create_index_generate_queries()
        services = []
        for _ in range(self.num_rebalance):
            services.append("fts")
        tasks = []
        tasks.append(self._cb_cluster.async_failover_and_rebalance(
            num_nodes=1,
            graceful=False))
        for count in range(0, len(index.fts_queries)):
            tasks.append(self._cb_cluster.async_run_fts_query_compare(
                fts_index=index,
                es=self.es,
                es_index_name=None,
                query_index=count))
        self.run_tasks_and_report(tasks, len(index.fts_queries))
        self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        hits, _, _, _ = index.execute_query(query=self.query,
                                         expected_hits=self._num_items)
        self.log.info("SUCCESS! Hits: %s" % hits)

    def failover_and_addback_during_querying(self):
        #TESTED
        recovery = self._input.param("recovery", None)
        graceful = self._input.param("graceful", False)
        index = self.create_index_generate_queries()
        if graceful:
            services = ['kv,fts']
        else:
            services = ['fts']
        tasks = []
        tasks.append(self._cb_cluster.async_failover_add_back_node(
            num_nodes=1,
            graceful=graceful,
            recovery_type=recovery,
            services=services))
        for count in range(0, len(index.fts_queries)):
            tasks.append(self._cb_cluster.async_run_fts_query_compare(
                fts_index=index,
                es=self.es,
                es_index_name=None,
                query_index=count))
        self.run_tasks_and_report(tasks, len(index.fts_queries))
        self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        hits, _, _, _ = index.execute_query(query=self.query,
                                         expected_hits=self._num_items)
        self.log.info("SUCCESS! Hits: %s" % hits)

    def graceful_failover_during_querying(self):
        index = self.create_index_generate_queries()
        services = []
        for _ in range(self.num_rebalance):
            services.append("fts")
        tasks = []
        tasks.append(self._cb_cluster.__async_failover_and_rebalance(
            graceful=False))
        for count in range(0, len(index.fts_queries)):
            tasks.append(self._cb_cluster.async_run_fts_query_compare(
                fts_index=index,
                es=self.es,
                es_index_name=None,
                query_index=count))
        self.run_tasks_and_report(tasks, len(index.fts_queries))
        self.is_index_partitioned_balanced(index)
        hits, _, _, _ = index.execute_query(query=self.query,
                                         expected_hits=self._num_items)
        self.log.info("SUCCESS! Hits: %s" % hits)

    def fts_node_down_with_replicas_during_querying(self):
        index = self.create_index_generate_queries()
        node = self._cb_cluster.get_random_fts_node()
        NodeHelper.stop_couchbase(node)
        try:
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._num_items)
        except Exception as e:
            self.log.info("Expected exception : %s" % e)
        NodeHelper.start_couchbase(node)
        NodeHelper.wait_warmup_completed([node])
        self.run_query_and_compare(index)

    def warmup_master_during_querying(self):
        #TESTED
        index = self.create_index_generate_queries()
        self._cb_cluster.warmup_node(master=True)
        self.is_index_partitioned_balanced(index)
        self.run_query_and_compare(index)

    def node_reboot_during_querying(self):
        #TESTED
        index = self.create_index_generate_queries()
        node = self._cb_cluster.reboot_one_node(test_case=self)
        self._cb_cluster.set_bypass_fts_node(node)
        self.is_index_partitioned_balanced(index)
        self.run_query_and_compare(index)

    def memc_crash_during_indexing_and_querying(self):
        self.load_data()
        index = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name('default'),
            index_name="default_index")
        self.generate_random_queries(index, self.num_queries, self.query_types)
        node = self._cb_cluster.get_random_fts_node()
        NodeHelper.kill_memcached(node)
        self._cb_cluster.set_bypass_fts_node(node)
        self.run_query_and_compare(index)

    def erl_crash_during_querying(self):
        #TESTED
        index = self.create_index_generate_queries()
        node = self._cb_cluster.get_random_fts_node()
        NodeHelper.kill_erlang(node)
        self.is_index_partitioned_balanced(index)
        self.run_query_and_compare(index)

    def fts_crash_during_querying(self):
        #TESTED
        index = self.create_index_generate_queries()
        self.run_query_and_compare(index)
        node = self._cb_cluster.get_random_fts_node()
        NodeHelper.kill_cbft_process(node)
        self._cb_cluster.set_bypass_fts_node(node)
        self.run_query_and_compare(index)

    def update_index_during_rebalance(self):
        """
         Perform indexing + rebalance + index defn change in parallel
        """
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        # wait till indexing is midway...
        self.wait_for_indexing_complete(self._num_items//2)
        reb_thread = Thread(target=self._cb_cluster.rebalance_out_master,
                                   name="rebalance",
                                   args=())
        reb_thread.start()
        self.sleep(15)
        index = self._cb_cluster.get_fts_index_by_name('default_index_1')
        new_plan_param = {"maxPartitionsPerPIndex": 64}
        index.index_definition['planParams'] = \
            index.build_custom_plan_params(new_plan_param)
        index.index_definition['uuid'] = index.get_uuid()
        update_index_thread = Thread(target=index.update(),
                                   name="update_index",
                                   args=())
        update_index_thread.start()
        _, defn = index.get_index_defn()
        self.log.info(defn['indexDef'])
        reb_thread.join()
        update_index_thread.join()
        hits, _, _, _ = index.execute_query(self.query,
                                            zero_results_ok=True)
        self.log.info("Hits: %s" % hits)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        hits, _, _, _ = index.execute_query(self.query,
                                            zero_results_ok=False)
        self.log.info("Hits: %s" % hits)

    def delete_index_during_rebalance(self):
        """
         Perform indexing + rebalance + index delete in parallel
        """
        import copy
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        indexes = copy.copy(self._cb_cluster.get_indexes())
        for index in indexes:
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        # wait till indexing is midway...
        self.wait_for_indexing_complete(self._num_items//2)
        reb_thread = Thread(target=self._cb_cluster.rebalance_out,
                                   name="rebalance",
                                   args=())
        reb_thread.start()
        # the first part of the rebalance is kv, wait for fts rebalance
        self.sleep(50)

        for index in indexes:
            index.delete()

        self.sleep(5)

        for index in indexes:
            try:
                _, defn = index.get_index_defn()
                self.log.info(defn['indexDef'])
            except KeyError as e:
                self.log.info("Expected exception: {0}".format(e))
                deleted = self._cb_cluster.are_index_files_deleted_from_disk(index.name)
                if deleted:
                    self.log.info("Confirmed: index files deleted from disk")
                else:
                    self.fail("ERROR: Index files still present on disk")
            else:
                self.fail("ERROR: Index definition still exists after deletion! "
                          "%s" %defn['indexDef'])


    def delete_buckets_during_rebalance(self):
        """
            Perform indexing + rebalance + bucket delete in parallel
        """
        from lib.membase.api.rest_client import RestConnection
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
        # wait till indexing is midway...
        self.wait_for_indexing_complete(self._num_items // 2)
        reb_thread = Thread(target=self._cb_cluster.rebalance_out_master,
                            name="rebalance",
                            args=())
        reb_thread.start()
        self.sleep(1)
        for bucket in self._cb_cluster.get_buckets():
            self.log.info("Deleting bucket {0}".format(bucket.name))
            if not RestConnection(self._cb_cluster.get_master_node()).delete_bucket(bucket.name):
                self.log.info("Expected error - cannot delete buckets during rebalance!")
            else:
                self.fail("Able to delete buckets during rebalance!")


    def update_index_during_failover(self):
        """
         Perform indexing + failover + index defn change in parallel
         for D,D+F,F cluster
        """
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        # wait till indexing is midway...
        self.wait_for_indexing_complete(self._num_items//2)
        fail_thread = Thread(target=self._cb_cluster.failover(),
                                   name="failover",
                                   args=())
        fail_thread.start()
        index = self._cb_cluster.get_fts_index_by_name('default_index_1')
        new_plan_param = {"maxPartitionsPerPIndex": 64}
        index.index_definition['planParams'] = \
            index.build_custom_plan_params(new_plan_param)
        index.index_definition['uuid'] = index.get_uuid()
        update_index_thread = Thread(target=index.update(),
                                   name="update_index",
                                   args=())
        update_index_thread.start()
        _, defn = index.get_index_defn()
        self.log.info(defn['indexDef'])
        fail_thread.join()
        update_index_thread.join()
        hits, _, _, _ = index.execute_query(self.query)
        self.log.info("Hits: %s" % hits)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        hits, _, _, _ = index.execute_query(
            self.query,
            expected_hits=index.get_src_bucket_doc_count())
        self.log.info("Hits: %s" % hits)

    def update_index_during_failover_and_rebalance(self):
        """
         Perform indexing + failover + index defn change in parallel
        """
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        # wait till indexing is midway...
        self.wait_for_indexing_complete(self._num_items//2)
        fail_thread = Thread(
            target=self._cb_cluster.failover_and_rebalance_nodes(),
            name="failover",
            args=())
        index = self._cb_cluster.get_fts_index_by_name('default_index_1')
        new_plan_param = {"maxPartitionsPerPIndex": 128}
        index.index_definition['planParams'] = \
            index.build_custom_plan_params(new_plan_param)
        index.index_definition['uuid'] = index.get_uuid()
        update_index_thread = Thread(target=index.update(),
                                   name="update_index",
                                   args=())
        fail_thread.start()
        update_index_thread.start()
        _, defn = index.get_index_defn()
        self.log.info(defn['indexDef'])
        fail_thread.join()
        update_index_thread.join()
        hits, _, _, _ = index.execute_query(self.query)
        self.log.info("Hits: %s" % hits)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        hits, _, _, _ = index.execute_query(
            self.query,
            expected_hits=index.get_src_bucket_doc_count())
        self.log.info("Hits: %s" % hits)


    def partial_rollback(self):
        bucket = self._cb_cluster.get_bucket_by_name("default")

        self._cb_cluster.flush_buckets([bucket])

        index = self.create_index(bucket, "default_index")
        self.load_data()
        self.wait_for_indexing_complete()

        # Stop Persistence on Node A & Node B
        self.log.info("Stopping persistence on {0}".
                      format(self._input.servers[:2]))
        mem_client = MemcachedClientHelper.direct_client(self._input.servers[0],
                                                         bucket)
        mem_client.stop_persistence()
        mem_client = MemcachedClientHelper.direct_client(self._input.servers[1],
                                                         bucket)
        mem_client.stop_persistence()

        # Perform mutations on the bucket
        self.async_perform_update_delete(self.upd_del_fields)
        if self._update:
            self.sleep(60, "Waiting for updates to get indexed...")
        self.wait_for_indexing_complete()

        # Run FTS Query to fetch the initial count of mutated items
        query = "{\"query\": \"mutated:>0\"}"
        query = json.loads(query)
        for index in self._cb_cluster.get_indexes():
            hits1, _, _, _ = index.execute_query(query)
            self.log.info("Hits before rollback: %s" % hits1)

        # Fetch count of docs in index and bucket
        before_index_doc_count = index.get_indexed_doc_count()
        before_bucket_doc_count = index.get_src_bucket_doc_count()

        self.log.info("Docs in Bucket : %s, Docs in Index : %s" % (
            before_bucket_doc_count, before_index_doc_count))

        # Kill memcached on Node A
        self.log.info("Killing memcached on {0}".format(self._master.ip))
        shell = RemoteMachineShellConnection(self._master)
        shell.kill_memcached()

        # Start persistence on Node B
        self.log.info("Starting persistence on {0}".
                      format(self._input.servers[1].ip))
        mem_client = MemcachedClientHelper.direct_client(self._input.servers[1],
                                                         bucket)
        mem_client.start_persistence()

        # Failover Node B
        failover_task = self._cb_cluster.async_failover(
            node=self._input.servers[1])
        failover_task.result()

        # Wait for Failover & FTS index rollback to complete
        self.wait_for_indexing_complete()

        # Run FTS query to fetch count of mutated items post rollback.
        for index in self._cb_cluster.get_indexes():
            hits2, _, _, _ = index.execute_query(query)
            self.log.info("Hits after rollback: %s" % hits2)

        # Fetch count of docs in index and bucket
        after_index_doc_count = index.get_indexed_doc_count()
        after_bucket_doc_count = index.get_src_bucket_doc_count()

        self.log.info("Docs in Bucket : %s, Docs in Index : %s"
                      % (after_bucket_doc_count, after_index_doc_count))

        # Validation : If there are deletes, validate the #docs in index goes
        #  up post rollback
        if self._input.param("delete", False):
            self.assertGreater(after_index_doc_count, before_index_doc_count,
                               "Deletes : Index count after rollback not "
                               "greater than before rollback")
        else:
            # For Updates, validate that #hits goes down in the query output
            # post rollback
            self.assertGreater(hits1, hits2,
                               "Mutated items before rollback are not more "
                               "than after rollback")

        # Failover FTS node
        failover_fts_node = self._input.param("failover_fts_node", False)

        if failover_fts_node:
            failover_task = self._cb_cluster.async_failover(
                node=self._input.servers[2])
            failover_task.result()
            self.sleep(10)

            # Run FTS query to fetch count of mutated items post FTS node failover.
            for index in self._cb_cluster.get_indexes():
                hits3, _, _, _ = index.execute_query(query)
                self.log.info(
                    "Hits after rollback and failover of primary FTS node: %s" % hits3)
                self.assertEqual(hits2, hits3,
                                 "Mutated items after FTS node failover are not equal to that after rollback")

    def test_cancel_node_removal_rebalance(self):
        """
            1. start rebalance out
            2. stop rebalance
            3. cancel node removal
            4. start rebalance
        """
        from lib.membase.api.rest_client import RestConnection, RestHelper
        rest = RestConnection(self._cb_cluster.get_master_node())
        nodes = rest.node_statuses()
        ejected_nodes = []

        for node in nodes:
            if node.ip != self._master.ip:
                ejected_nodes.append(node.id)
                break

        self.log.info(
                "removing node {0} from cluster".format(ejected_nodes))
        rest.rebalance(otpNodes=[node.id for node in nodes],
                           ejectedNodes=ejected_nodes)
        self.sleep(3)
        stopped = rest.stop_rebalance()
        self.assertTrue(stopped, msg="unable to stop rebalance")

        #for cases if rebalance ran fast
        if RestHelper(rest).is_cluster_rebalanced():
            self.log.info("Rebalance is finished already.")
        else:
            rest.rebalance(otpNodes=[node.id for node in nodes], ejectedNodes=[])
            self.assertTrue(rest.monitorRebalance(), msg="rebalance operation "
                                                     "failed after restarting")

    def test_stop_restart_rebalance_in_loop(self):
        """
         Kick off rebalance-out. Stop and stop rebalance in a loop
         continuously till rebalance finishes.
        :return:
        """
        count = 0

        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.wait_for_indexing_complete()

        from lib.membase.api.rest_client import RestConnection, RestHelper
        rest = RestConnection(self._cb_cluster.get_master_node())
        nodes = rest.node_statuses()
        ejected_nodes = []

        for node in nodes:
            if node.ip != self._master.ip:
                ejected_nodes.append(node.id)
                break
        self.log.info(
            "removing node(s) {0} from cluster".format(ejected_nodes))

        while count<5:
            if RestHelper(rest).is_cluster_rebalanced():
                self.log.info("Rebalance is finished already.")
                break
            rest.rebalance(otpNodes=[node.id for node in nodes],
                           ejectedNodes=ejected_nodes)
            stopped = rest.stop_rebalance()
            self.sleep(1)
            self.assertTrue(stopped, msg="unable to stop rebalance")
            count += 1

        if not RestHelper(rest).is_cluster_rebalanced():
            rest.rebalance(otpNodes=[node.id for node in nodes],
                           ejectedNodes=ejected_nodes)

    def test_rebalance_cancel_new_rebalance(self):
        """
            Load bucket, do not create index
            From a 3 kv+fts node cluster, rebalance out master + one node
            Immediately (after few secs), stop rebalance
            Rebalance out other nodes than master.
            After rebalance completes, create an index
        :return:
        """
        self.load_data()

        non_master_nodes = list(set(self._cb_cluster.get_nodes())-
                           {self._master})

        from lib.membase.api.rest_client import RestConnection, RestHelper
        rest = RestConnection(self._master)
        nodes = rest.node_statuses()
        ejected_nodes = []

        # first eject a master + non-master node
        eject_nodes = [self._master] + [non_master_nodes[0]]

        for eject in eject_nodes:
            for node in nodes:
                if eject.ip == node.ip:
                    ejected_nodes.append(node.id)
                    break

        rest.rebalance(otpNodes=[node.id for node in nodes],
                       ejectedNodes=ejected_nodes)
        self.sleep(3)
        rest._rebalance_progress()
        stopped = rest.stop_rebalance()
        self.assertTrue(stopped, msg="unable to stop rebalance")

        eject_nodes = non_master_nodes[:2]
        ejected_nodes = []

        for eject in eject_nodes:
            for node in nodes:
                if eject.ip == node.ip:
                    ejected_nodes.append(node.id)
                    break

        rest.rebalance(otpNodes=[node.id for node in nodes],
                       ejectedNodes=ejected_nodes)
        rest.monitorRebalance()

        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def test_kv_and_fts_rebalance_with_high_ops(self):
        from lib.membase.api.rest_client import RestConnection
        rest = RestConnection(self._cb_cluster.get_master_node())

        # start loading
        default_bucket = self._cb_cluster.get_bucket_by_name("default")
        load_thread = Thread(target=self._cb_cluster.load_from_high_ops_loader,
                                       name="gen_high_ops_load",
                                       args=[default_bucket])
        load_thread.start()

        # create index and query
        index = self.create_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index")
        load_thread.join()
        self.wait_for_indexing_complete()
        
        # check for dataloss
        errors = self._cb_cluster.check_dataloss_with_high_ops_loader(default_bucket)
        if errors:
            self.log.info("Printing missing keys:")
            for error in errors:
                print(error)

        if self._num_items != rest.get_active_key_count(default_bucket):
            self.fail("FATAL: Data loss detected!! Docs loaded : {0}, docs present: {1}".
                      format(self._num_items, rest.get_active_key_count(default_bucket)))
        else:
            self.log.info("SUCCESS: No traces of data loss upon verification")

        # load again
        load_thread = Thread(target=self._cb_cluster.load_from_high_ops_loader,
                                       name="gen_high_ops_load",
                                       args=[default_bucket])
        load_thread.start()


        # do a rebalance-out of kv+fts node
        self._cb_cluster.rebalance_out_master()
        load_thread.join()

        # check for dataloss
        errors = self._cb_cluster.check_dataloss_with_high_ops_loader(default_bucket)
        if errors:
            self.log.info("Printing missing keys:")
            for error in errors:
                print(error)
        if self._num_items != rest.get_active_key_count(default_bucket):
            self.fail("FATAL: Data loss detected!! Docs loaded : {0}, docs present: {1}".
                      format(self._num_items, rest.get_active_key_count(default_bucket)))
        else:
            self.log.info("SUCCESS: No traces of data loss upon verification")

        # load again
        load_thread = Thread(target=self._cb_cluster.load_from_high_ops_loader,
                                       name="gen_high_ops_load",
                                       args=[default_bucket])
        load_thread.start()

        # do a rebalance-out of fts node
        self._cb_cluster.rebalance_out()
        load_thread.join()

        # check for dataloss
        errors = self._cb_cluster.check_dataloss_with_high_ops_loader(default_bucket)
        if errors:
            self.log.info("Printing missing keys:")
            for error in errors:
                print(error)
        if self._num_items != rest.get_active_key_count(default_bucket):
            self.fail("FATAL: Data loss detected!! Docs loaded : {0}, docs present: {1}".
                      format(self._num_items, rest.get_active_key_count(default_bucket)))
        else:
            self.log.info("SUCCESS: No traces of data loss upon verification")
