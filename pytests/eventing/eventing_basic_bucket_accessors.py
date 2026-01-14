from lib.membase.api.rest_client import RestConnection, RestHelper
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE
from pytests.eventing.eventing_base import EventingBaseTest, log
from membase.helper.cluster_helper import ClusterOperationHelper


class BasicBucketAccessors(EventingBaseTest):
    def setUp(self):
        super(BasicBucketAccessors, self).setUp()
        self.buckets = self.rest.get_buckets()
        self.src_bucket = self.rest.get_bucket_by_name(self.src_bucket_name)
        self.gens_load = self.generate_docs(self.docs_per_day)
        handler_code = self.input.param('handler_code', 'basic_bucket_accessors_get')
        if handler_code == 'basic_bucket_accessors_get':
            self.handler_code = HANDLER_CODE.BASIC_BUCKET_ACCESSORS_GET
        elif handler_code == 'basic_bucket_accessors_sbm':
            self.handler_code = HANDLER_CODE.BASIC_BUCKET_ACCESSORS_SBM
        elif handler_code == 'basic_bucket_accessors_timers':
            self.handler_code = HANDLER_CODE.BASIC_BUCKET_ACCESSORS_TIMERS
        elif handler_code == 'basic_bucket_accessors_curl':
            self.handler_code = HANDLER_CODE.BASIC_BUCKET_ACCESSORS_CURL
        elif handler_code == 'basic_bucket_accessors_base64':
            self.handler_code = HANDLER_CODE.BASIC_BUCKET_ACCESSORS_BASE64
        elif handler_code == 'basic_bucket_accessors_n1ql':
            self.handler_code = HANDLER_CODE.BASIC_BUCKET_ACCESSORS_N1QL
        elif handler_code == 'basic_bucket_accessors_crc':
            self.handler_code = HANDLER_CODE.BASIC_BUCKET_ACCESSORS_CRC


    def tearDown(self):
        super(BasicBucketAccessors, self).tearDown()

    def test_basic_bucket_accessors_get(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, self.handler_code, src_binding=True)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations
        self.sleep(10)
        on_update_success=self.get_stats_value(self.function_name, "execution_stats.on_update_success")
        on_update_failure=self.get_stats_value(self.function_name, "execution_stats.on_update_failure")
        self.log.info("execution_stats.on_update_success: {}".format(on_update_success))
        self.log.info("execution_stats.on_update_failure: {}".format(on_update_failure))
        if on_update_success != self.docs_per_day * self.num_docs:
            self.fail("on_update_success is not as expected {0} , actual value {1}".format(self.docs_per_day * self.num_docs, on_update_success))
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
        self.verify_doc_count_collections("default.scope0.collection1", 0)
        self.undeploy_and_delete_function(body)


    def test_basic_bucket_accessors_get_sbm(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, self.handler_code, src_binding=True)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations
        self.sleep(10)
        on_update_success=self.get_stats_value(self.function_name, "execution_stats.on_update_success")
        on_update_failure=self.get_stats_value(self.function_name, "execution_stats.on_update_failure")
        self.log.info("execution_stats.on_update_success: {}".format(on_update_success))
        self.log.info("execution_stats.on_update_failure: {}".format(on_update_failure))
        if on_update_success != self.docs_per_day * self.num_docs:
            self.fail("on_update_success is not as expected {0}, actual value {1}".format(self.docs_per_day * self.num_docs, on_update_success))
        self.verify_doc_count_collections("default.scope0.collection0", self.docs_per_day * self.num_docs * 2)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
        self.verify_doc_count_collections("default.scope0.collection0", 0)
        self.undeploy_and_delete_function(body)


    def test_vb_shuffle_during_failover(self):
        eventing_server = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        body = self.create_save_function_body(self.function_name, self.handler_code, src_binding=True)
        self.deploy_function(body)
        # load some data
        if self.non_default_collection:
            task=self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                              wait_for_loading=False)
        else:
            task=self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                              wait_for_loading=False)
        # fail over the eventing node
        fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[eventing_server[1]], graceful=False)
        self.wait_for_failover_or_rebalance()
        task.result()
        fail_over_task.result()
        # Wait for failover to complete
        self.sleep(10)

        on_update_success=self.get_stats_value(self.function_name, "execution_stats.on_update_success")
        on_update_failure=self.get_stats_value(self.function_name, "execution_stats.on_update_failure")
        self.log.info("execution_stats.on_update_success: {}".format(on_update_success))
        self.log.info("execution_stats.on_update_failure: {}".format(on_update_failure))
        # During failover, mutation processing may be partial due to VB shuffle
        # Just verify we have some successful processing
        if on_update_success == 0:
            self.fail("on_update_success is 0, expected at least some mutations processed. on_update_failure: {0}".format(on_update_failure))

        if self.non_default_collection:
            self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", self.docs_per_day * self.num_docs,expected_duplicate=True)
        else:
            self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs,expected_duplicate=True)
        self.undeploy_and_delete_function(body)


    def test_eventing_swap_rebalance_when_existing_eventing_node_is_processing_mutations(self):
        sock_batch_size = self.input.param('sock_batch_size', 1)
        worker_count = self.input.param('worker_count', 3)
        cpp_worker_thread_count = self.input.param('cpp_worker_thread_count', 1)
        body = self.create_save_function_body(self.function_name, self.handler_code,
                                              sock_batch_size=sock_batch_size, worker_count=worker_count,
                                              cpp_worker_thread_count=cpp_worker_thread_count, src_binding=True)
        self.deploy_function(body)
        # load data
        if not self.is_expired:
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
        else:
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size, exp=300)
        if self.pause_resume:
            self.pause_function(body)
        # swap rebalance an eventing node when eventing is processing mutations
        services_in = ["eventing"]
        nodes_out_ev = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]],
                                                 nodes_out_ev, services=services_in)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        if not self.cancel_timer:
            if self.is_sbm:
                self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 2, skip_stats_validation=True)
            else:
                self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        if not self.is_expired:
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size, op_type='delete')
        if self.pause_resume:
            self.pause_function(body)
            self.sleep(30)
            self.resume_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        if not self.cancel_timer:
            if self.is_sbm and (self.handler_code != HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_DELETE and
                    self.handler_code != HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_TIMERS_DELETE):
                self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
            else:
                self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        else:
                self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)

        self.undeploy_and_delete_function(body)
        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        # rebalance out all eventing nodes
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], nodes_out_list)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()