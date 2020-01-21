from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE
from pytests.eventing.eventing_base import EventingBaseTest, log
from membase.helper.cluster_helper import ClusterOperationHelper


class EventingSanity(EventingBaseTest):
    def setUp(self):
        super(EventingSanity, self).setUp()
        self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=700)
        if self.create_functions_buckets:
            self.bucket_size = 200
            log.info(self.bucket_size)
            bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size,
                                                       replicas=0)
            self.cluster.create_standard_bucket(name=self.src_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.src_bucket = RestConnection(self.master).get_buckets()
            self.cluster.create_standard_bucket(name=self.dst_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.cluster.create_standard_bucket(name=self.metadata_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.buckets = RestConnection(self.master).get_buckets()
        self.gens_load = self.generate_docs(self.docs_per_day)
        self.expiry = 3
        query = "create primary index on {}".format(self.src_bucket_name)
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)

    def tearDown(self):
        super(EventingSanity, self).tearDown()

    def test_create_mutation_for_dcp_stream_boundary_from_beginning(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE, worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        self.undeploy_and_delete_function(body)

    def test_delete_mutation_for_dcp_stream_boundary_from_beginning(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_DELETE, worker_count=3)
        self.deploy_function(body)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, on_delete=True)
        self.undeploy_and_delete_function(body)

    def test_expiry_mutation_for_dcp_stream_boundary_from_beginning(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, exp=1)
        # set expiry pager interval
        ClusterOperationHelper.flushctl_set(self.master, "exp_pager_stime", 1, bucket=self.src_bucket_name)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_DELETE, worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the expiry mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, on_delete=True)
        self.undeploy_and_delete_function(body)

    def test_update_mutation_for_dcp_stream_boundary_from_now(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE,
                                              dcp_stream_boundary="from_now", sock_batch_size=1, worker_count=4,
                                              cpp_worker_thread_count=4)
        self.deploy_function(body)
        # update all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='update')
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        self.undeploy_and_delete_function(body)

    def test_n1ql_query_execution_from_handler_code(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_INSERT_ON_UPDATE, worker_count=3)
        # Enable this after MB-26527 is fixed
        # sock_batch_size=10, worker_count=4, cpp_worker_thread_count=4)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        self.undeploy_and_delete_function(body)

    def test_doc_timer_events_from_handler_code_with_n1ql(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_INSERT_ON_UPDATE_WITH_DOC_TIMER,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, doc_timer_events=True)
        self.undeploy_and_delete_function(body)

    def test_cron_timer_events_from_handler_code_with_n1ql(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_INSERT_ON_UPDATE_WITH_CRON_TIMER,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, doc_timer_events=True)
        self.undeploy_and_delete_function(body)

    def test_doc_timer_events_from_handler_code_with_bucket_ops(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_DOC_TIMER,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, doc_timer_events=True)
        self.undeploy_and_delete_function(body)

    def test_cron_timer_events_from_handler_code_with_bucket_ops(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_CRON_TIMER,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, doc_timer_events=True)
        self.undeploy_and_delete_function(body)

    def test_delete_bucket_operation_from_handler_code(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_timers_without_context(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_TIMER_WITHOUT_CONTEXT,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_cancel_timers_with_timers_being_overwritten(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_WITH_TIMER_OVERWRITTEN,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_source_doc_mutations(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                 batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_SOURCE_DOC_MUTATION,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        #self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        #self.verify_source_bucket_mutation(self.docs_per_day * 2016)
        self.verify_source_bucket_mutation(self.docs_per_day * 2016)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        self.verify_source_bucket_mutation(self.docs_per_day * 2016, deletes=True, timeout=1200)
        self.undeploy_and_delete_function(body)

    def test_source_doc_mutations_with_timers(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                 batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_SOURCE_DOC_MUTATION_WITH_TIMERS,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        #self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        #self.verify_source_bucket_mutation(self.docs_per_day * 2016)
        self.verify_source_bucket_mutation(self.docs_per_day * 2016)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        self.verify_source_bucket_mutation(self.docs_per_day * 2016, deletes=True, timeout=1200)
        self.undeploy_and_delete_function(body)

    def test_source_bucket_mutations(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                 batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        #self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        #self.verify_source_bucket_mutation(self.docs_per_day * 2016)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 4032, skip_stats_validation=True)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_source_bucket_mutations_with_timers(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                 batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_WITH_TIMERS,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        #self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        #self.verify_source_bucket_mutation(self.docs_per_day * 2016)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 4032, skip_stats_validation=True)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_pause_resume_execution(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE, worker_count=3)
        self.deploy_function(body)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        self.pause_function(body)
        # intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(60)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")
        self.gens_load = self.generate_docs(self.docs_per_day*2)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size*2)
        self.resume_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016*2, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)


    def test_source_bucket_mutation_for_dcp_stream_boundary_from_now(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION,
                                              dcp_stream_boundary="from_now", sock_batch_size=1, worker_count=4,
                                              cpp_worker_thread_count=4)
        self.deploy_function(body)
        # update all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='update')
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016*2)
        self.undeploy_and_delete_function(body)