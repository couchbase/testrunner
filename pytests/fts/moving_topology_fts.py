from fts_base import FTSBaseTest, FTSException
from fts_base import NodeHelper
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

    """ Topology change during indexing"""

    def rebalance_in_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name,index.get_indexed_doc_count()))
        self._cb_cluster.rebalance_in(num_nodes=1, services=["kv,fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def rebalance_out_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name,index.get_indexed_doc_count()))
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
                          %(index.name,index.get_indexed_doc_count()))
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
                          %(index.name,index.get_indexed_doc_count()))
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
                          %(index.name,index.get_indexed_doc_count()))
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
                          %(index.name,index.get_indexed_doc_count()))
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
                          %(index.name,index.get_indexed_doc_count()))
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
                          %(index.name,index.get_indexed_doc_count()))
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
                          %(index.name,index.get_indexed_doc_count()))
        task = self._cb_cluster.async_failover(graceful=True)
        task.result()
        self.sleep(30)
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
                          %(index.name,index.get_indexed_doc_count()))
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
                          %(index.name,index.get_indexed_doc_count()))
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
                          %(index.name,index.get_indexed_doc_count()))
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
                          %(index.name,index.get_indexed_doc_count()))
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
                          %(index.name,index.get_indexed_doc_count()))
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
                          %(index.name,index.get_indexed_doc_count()))
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
        for _ in xrange(self.num_rebalance):
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
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
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
        for _ in xrange(self.num_rebalance):
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
        for _ in xrange(self.num_rebalance):
            services.append("fts")
        tasks = []
        tasks.append(self._cb_cluster.async_swap_rebalance(
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

    def hard_failover_no_rebalance_during_querying(self):
        #TESTED
        index = self.create_index_generate_queries()
        services = []
        for _ in xrange(self.num_rebalance):
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
        for _ in xrange(self.num_rebalance):
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
        for _ in xrange(self.num_rebalance):
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

    def partial_rollback(self):
        bucket = self._cb_cluster.get_bucket_by_name("default")

        self._cb_cluster.flush_buckets([bucket])

        index = self.create_index(bucket, "default_index")
        self.load_data()
        self.wait_for_indexing_complete()

        # Stop Persistence on Node A & Node B
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

        # Kill memcached on Node A so that Node B becomes master
        shell = RemoteMachineShellConnection(self._master)
        shell.kill_memcached()

        # Start persistence on Node B
        mem_client = MemcachedClientHelper.direct_client(self._input.servers[1],
                                                         bucket)
        mem_client.start_persistence()

        # Failover Node B
        failover_task = self._cb_cluster.async_failover(
            node=self._input.servers[1])
        failover_task.result()

        # Wait for Failover & FTS index rollback to complete
        self.sleep(10)

        # Run FTS query to fetch count of mutated items post rollback.
        for index in self._cb_cluster.get_indexes():
            hits2, _, _, _ = index.execute_query(query)
            self.log.info("Hits after rollback: %s" % hits2)

        # Fetch count of docs in index and bucket
        after_index_doc_count = index.get_indexed_doc_count()
        after_bucket_doc_count = index.get_src_bucket_doc_count()

        self.log.info("Docs in Bucket : %s, Docs in Index : %s"
                      % (after_bucket_doc_count, after_index_doc_count))

        # Validation : If there are deletes, validate the #docs in index goes up post rollback
        if self._input.param("delete", False):
            self.assertGreater(after_index_doc_count, before_index_doc_count,
                               "Deletes : Index count after rollback not greater than before rollback")
        else:
            # For Updates, validate that #hits goes down in the query output post rollback
            self.assertGreater(hits1, hits2,
                               "Mutated items before rollback are not more than after rollback")

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
