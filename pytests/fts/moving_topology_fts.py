from fts_base import FTSBaseTest, FTSException
from fts_base import NodeHelper
from TestInput import TestInputSingleton
from threading import Thread

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
            if self._cb_cluster.get_num_fts_nodes() == 0:
                self.log.info("Expected exception: %s" % e)
            else:
                raise e
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

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
            hits, _, _ = index.execute_query(query=self.query,
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
            hits, _, _ = index.execute_query(query=self.query,
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
            hits, _, _ = index.execute_query(query=self.query,
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
            hits, _, _ = index.execute_query(query=self.query,
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
            hits, _, _ = index.execute_query(query=self.query,
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
            hits, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._num_items)
        self.log.info("SUCCESS! Hits: %s" % hits)

    def hard_failover_master_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()
        self._cb_cluster.failover_and_rebalance_master()
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        for index in self._cb_cluster.get_indexes():
            hits, _, _ = index.execute_query(query=self.query,
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
            hits, _, _ = index.execute_query(query=self.query,
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
            hits, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._num_items)
        self.log.info("SUCCESS! Hits: %s" % hits)

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
            hits, _, _ = index.execute_query(query=self.query,
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
            hits, _, _ = index.execute_query(query=self.query,
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
            hits, _, _ = index.execute_query(query=self.query,
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
            hits, _, _ = index.execute_query(query=self.query,
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
            hits, _, _ = index.execute_query(query=self.query,
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
            hits, _, _ = index.execute_query(query=self.query,
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
            hits, _, _ = index.execute_query(query=self.query,
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
        for index in self._cb_cluster.get_indexes():
            hits, _, _ = index.execute_query(query=self.query,
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
        tasks = []
        tasks.append(self._cb_cluster.async_rebalance_in(
            num_nodes=self.num_rebalance,
            services=["kv,fts"]))
        for count in range(0, len(index.fts_queries)):
            tasks.append(self._cb_cluster.async_run_fts_query_compare(
                fts_index=index,
                es=self.es,
                es_index_name=None,
                query_index=count))
        self.run_tasks_and_report(tasks, len(index.fts_queries))
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        for index in self._cb_cluster.get_indexes():
            hits, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._num_items)
        self.log.info("SUCCESS! Hits: %s" % hits)

    def rebalance_out_during_querying(self):
        #TESTED
        index = self.create_index_generate_queries()
        self.run_query_and_compare(index)
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
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        for index in self._cb_cluster.get_indexes():
            hits, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._num_items)
        self.log.info("SUCCESS! Hits: %s" % hits)

    def swap_rebalance_during_querying(self):
        index = self.create_index_generate_queries()
        self.load_data()
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
        for task in tasks:
            task.result()
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        for index in self._cb_cluster.get_indexes():
            hits, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._num_items)
        self.log.info("SUCCESS! Hits: %s" % hits)

    def hard_failover_during_querying(self):
        pass

    def swap_rebalance_during_querying(self):
        pass

    def fts_node_down_during_querying(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        node = self._cb_cluster.get_random_fts_node()
        NodeHelper.stop_couchbase(node)
        for index in self._cb_cluster.get_indexes():
            hits, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._num_items)
        self.log.info("SUCCESS! Hits: %s" % hits)
        NodeHelper.start_couchbase(node)