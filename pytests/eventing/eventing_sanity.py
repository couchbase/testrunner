from couchbase_helper.documentgenerator import SDKDataLoader
from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE
from pytests.eventing.eventing_base import EventingBaseTest, log
from membase.helper.cluster_helper import ClusterOperationHelper


class EventingSanity(EventingBaseTest):
    def setUp(self):
        super(EventingSanity, self).setUp()

    def tearDown(self):
        super(EventingSanity, self).tearDown()

    def test_create_mutation_for_dcp_stream_boundary_from_beginning(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    def test_delete_mutation_for_dcp_stream_boundary_from_beginning(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_DELETE)
        self.deploy_function(body)
        # delete all documents
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    def test_expiry_mutation_for_dcp_stream_boundary_from_now(self):
        # set expiry pager interval
        ClusterOperationHelper.flushctl_set(self.master, "exp_pager_stime", 1, bucket=self.default_bucket_name)
        body = self.create_save_function_body(self.function_name,"handler_code/ABO/insert_exp_delete_only.js",
                                              dcp_stream_boundary="from_now")
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection1", 0)
        ### update all the documents with expiry
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_update=True,expiry=10)
        # Wait for eventing to catch up with all the expiry mutations and verify results
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    def test_update_mutation_for_dcp_stream_boundary_from_now(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_WITH_RAND,
                                              dcp_stream_boundary="from_now")
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection1", 0)
        # update all documents
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_update=True)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    def test_n1ql_query_execution_from_handler_code(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, 'handler_code/collections/n1ql_insert_on_update.js')
        # Enable this after MB-26527 is fixed
        # sock_batch_size=10, worker_count=4, cpp_worker_thread_count=4)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    def test_timer_events_from_handler_code_with_n1ql(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name,'handler_code/collections/n1ql_insert_with_timer.js')
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    def test_timer_events_from_handler_code_with_bucket_ops(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_DOC_TIMER)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)


    def test_delete_bucket_operation_from_handler_code(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        # delete all documents
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_doc_count_collections("default.scope0.collection1", 0)
        self.undeploy_and_delete_function(body)

    def test_timers_without_context(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_TIMER_WITHOUT_CONTEXT)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    def test_cancel_timers_with_timers_being_overwritten(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_WITH_TIMER_OVERWRITTEN)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    def test_source_doc_mutations(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_SOURCE_DOC_MUTATION)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_source_bucket_mutation(self.docs_per_day * 2016,bucket='default.scope0.collection0')
        # delete all documents
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
        self.verify_source_bucket_mutation(self.docs_per_day * 2016, deletes=True, timeout=1200,
                                               bucket='default.scope0.collection0')
        self.undeploy_and_delete_function(body)

    def test_source_doc_mutations_with_timers(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_SOURCE_DOC_MUTATION_WITH_TIMERS)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_source_bucket_mutation(self.docs_per_day * 2016,bucket='default.scope0.collection0')
        # delete all documents
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
        self.verify_source_bucket_mutation(self.docs_per_day * 2016, deletes=True, timeout=1200,
                                               bucket='default.scope0.collection0')
        self.undeploy_and_delete_function(body)

    def test_source_bucket_mutations(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_doc_count_collections("default.scope0.collection0", self.docs_per_day * self.num_docs*2)
        # delete all documents
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
        self.verify_doc_count_collections("default.scope0.collection0", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    def test_source_bucket_mutations_with_timers(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_WITH_TIMERS)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_doc_count_collections("default.scope0.collection0", self.docs_per_day * self.num_docs*2)
        # delete all documents
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
        self.verify_doc_count_collections("default.scope0.collection0", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    def test_pause_resume_execution(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE)
        self.deploy_function(body)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",wait_for_loading=False)
        self.pause_function(body)
        # intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(60)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")
        self.load_data_to_collection(self.docs_per_day * self.num_docs*2, "default.scope0.collection0",wait_for_loading=False)
        self.resume_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_doc_count_collections("default.scope0.collection0", self.docs_per_day * self.num_docs*2, timeout=1200)
        self.undeploy_and_delete_function(body)


    def test_source_bucket_mutation_for_dcp_stream_boundary_from_now(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name,HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION ,
                                              dcp_stream_boundary="from_now")
        self.deploy_function(body)
        # update all documents
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_update=True)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_doc_count_collections("default.scope0.collection0", self.docs_per_day * self.num_docs*2)
        self.undeploy_and_delete_function(body)

    def test_compress_handler(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name,"handler_code/compress.js")
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_doc_count_collections("default.scope0.collection1", 0)
        self.undeploy_and_delete_function(body)

    def test_expired_mutation(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0", expiry=100, wait_for_loading=False)
        # set expiry pager interval
        ClusterOperationHelper.flushctl_set(self.master, "exp_pager_stime", 1, bucket=self.default_bucket_name)
        body = self.create_save_function_body(self.function_name, "handler_code/bucket_op_expired.js")
        self.deploy_function(body)
        # Wait for eventing to catch up with all the expiry mutations and verify results
        self.verify_doc_count_collections("default.scope0.collection0", 0)
        self.undeploy_and_delete_function(body)

    def test_cancel_timer(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, "handler_code/cancel_timer.js")
        self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        # print timer context and alarm
        self.print_timer_alarm_context(self.function_name)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_doc_count_collections("default.scope0.collection2", self.docs_per_day * self.num_docs)
        self.verify_doc_count_collections("default.scope0.collection1", 0)
        self.assertEqual(self.get_stats_value(self.function_name,"execution_stats.timer_cancel_counter"),self.docs_per_day * 2016)
        self.undeploy_and_delete_function(body)

    def test_advance_bucket_op(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0", expiry=300)
        # set expiry pager interval
        ClusterOperationHelper.flushctl_set(self.master, "exp_pager_stime", 3, bucket=self.default_bucket_name)
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/curl_timer_insert.js")
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        self.verify_doc_count_collections("default.scope0.collection2", 0)
        self.undeploy_and_delete_function(body)
