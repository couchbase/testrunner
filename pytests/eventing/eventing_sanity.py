from couchbase_helper.documentgenerator import SDKDataLoader
from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE
from pytests.eventing.eventing_base import EventingBaseTest, log
from membase.helper.cluster_helper import ClusterOperationHelper


class EventingSanity(EventingBaseTest):
    def setUp(self):
        super(EventingSanity, self).setUp()
        self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=2400)
        if self.create_functions_buckets:
            log.info(self.bucket_size)
            bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size, eviction_policy=self.eviction_policy)
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
        if self.non_default_collection:
            self.create_scope_collection(bucket=self.src_bucket_name,scope=self.src_bucket_name,collection=self.src_bucket_name)
            self.create_scope_collection(bucket=self.metadata_bucket_name,scope=self.metadata_bucket_name,collection=self.metadata_bucket_name)
            self.create_scope_collection(bucket=self.dst_bucket_name,scope=self.dst_bucket_name,collection=self.dst_bucket_name)
        query = "create primary index on {}".format(self.src_bucket_name)
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)
        query = "create primary index on {}".format(self.dst_bucket_name)
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)
        query = "create primary index on {}".format(self.metadata_bucket_name)
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)

    def tearDown(self):
        super(EventingSanity, self).tearDown()

    def test_create_mutation_for_dcp_stream_boundary_from_beginning(self):
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day*self.num_docs,"src_bucket.src_bucket.src_bucket")
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        if self.non_default_collection:
            self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs)
        else:
            self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    def test_delete_mutation_for_dcp_stream_boundary_from_beginning(self):
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day*self.num_docs,"src_bucket.src_bucket.src_bucket")
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_DELETE)
        self.deploy_function(body)
        # delete all documents
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day*self.num_docs,"src_bucket.src_bucket.src_bucket",is_delete=True)
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",is_delete=True)
        # Wait for eventing to catch up with all the delete mutations and verify results
        if self.non_default_collection:
            self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs)
        else:
            self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    def test_expiry_mutation_for_dcp_stream_boundary_from_now(self):
        # set expiry pager interval
        ClusterOperationHelper.flushctl_set(self.master, "exp_pager_stime", 1, bucket=self.src_bucket_name)
        body = self.create_save_function_body(self.function_name,"handler_code/ABO/insert_exp_delete_only.js",
                                              dcp_stream_boundary="from_now")
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day*self.num_docs,"src_bucket.src_bucket.src_bucket")
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        self.deploy_function(body)
        if self.non_default_collection:
            self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", 0)
        else:
            self.verify_doc_count_collections("dst_bucket._default._default", 0)
        ### update all the documents with expiry
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day*self.num_docs,"src_bucket.src_bucket.src_bucket",is_update=True,expiry=10)
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",is_update=True,expiry=10)
        # Wait for eventing to catch up with all the expiry mutations and verify results
        if self.non_default_collection:
            self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs)
        else:
            self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    def test_update_mutation_for_dcp_stream_boundary_from_now(self):
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day*self.num_docs,"src_bucket.src_bucket.src_bucket")
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_WITH_RAND,
                                              dcp_stream_boundary="from_now")
        self.deploy_function(body)
        if self.non_default_collection:
            self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", 0)
        else:
            self.verify_doc_count_collections("dst_bucket._default._default", 0)
        # update all documents
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day*self.num_docs,"src_bucket.src_bucket.src_bucket",is_update=True)
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",is_update=True)
        # Wait for eventing to catch up with all the update mutations and verify results
        if self.non_default_collection:
            self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs)
        else:
            self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    def test_n1ql_query_execution_from_handler_code(self):
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day*self.num_docs,"src_bucket.src_bucket.src_bucket")
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        body = self.create_save_function_body(self.function_name, 'handler_code/collections/n1ql_insert_on_update.js')
        # Enable this after MB-26527 is fixed
        # sock_batch_size=10, worker_count=4, cpp_worker_thread_count=4)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        if self.non_default_collection:
            self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs)
        else:
            self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    def test_timer_events_from_handler_code_with_n1ql(self):
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day*self.num_docs,"src_bucket.src_bucket.src_bucket")
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        body = self.create_save_function_body(self.function_name,'handler_code/collections/n1ql_insert_with_timer.js')
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        if self.non_default_collection:
            self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs)
        else:
            self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    def test_timer_events_from_handler_code_with_bucket_ops(self):
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day*self.num_docs,"src_bucket.src_bucket.src_bucket")
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_DOC_TIMER)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        if self.non_default_collection:
            self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs)
        else:
            self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)


    def test_delete_bucket_operation_from_handler_code(self):
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day*self.num_docs,"src_bucket.src_bucket.src_bucket")
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        if self.non_default_collection:
            self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs)
        else:
            self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * self.num_docs)
        # delete all documents
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day*self.num_docs,"src_bucket.src_bucket.src_bucket",is_delete=True)
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",is_delete=True)
        # Wait for eventing to catch up with all the delete mutations and verify results
        if self.non_default_collection:
            self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", 0)
        else:
            self.verify_doc_count_collections("dst_bucket._default._default", 0)
        self.undeploy_and_delete_function(body)

    def test_timers_without_context(self):
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day*self.num_docs,"src_bucket.src_bucket.src_bucket")
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_TIMER_WITHOUT_CONTEXT)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        if self.non_default_collection:
            self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs)
        else:
            self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    def test_cancel_timers_with_timers_being_overwritten(self):
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day*self.num_docs,"src_bucket.src_bucket.src_bucket")
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_WITH_TIMER_OVERWRITTEN)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        if self.non_default_collection:
            self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs)
        else:
            self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    def test_source_doc_mutations(self):
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day*self.num_docs,"src_bucket.src_bucket.src_bucket")
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_SOURCE_DOC_MUTATION)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        if self.non_default_collection:
            self.verify_source_bucket_mutation(self.docs_per_day * 2016,bucket='src_bucket.src_bucket.src_bucket')
        else:
            self.verify_source_bucket_mutation(self.docs_per_day * 2016,bucket='src_bucket._default._default')
        # delete all documents
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day*self.num_docs,"src_bucket.src_bucket.src_bucket",is_delete=True)
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",is_delete=True)
        if self.non_default_collection:
            self.verify_source_bucket_mutation(self.docs_per_day * 2016, deletes=True, timeout=1200,
                                               bucket='src_bucket.src_bucket.src_bucket')
        else:
            self.verify_source_bucket_mutation(self.docs_per_day * 2016, deletes=True, timeout=1200,
                                               bucket='src_bucket._default._default')
        self.undeploy_and_delete_function(body)

    def test_source_doc_mutations_with_timers(self):
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day*self.num_docs,"src_bucket.src_bucket.src_bucket")
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_SOURCE_DOC_MUTATION_WITH_TIMERS)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        if self.non_default_collection:
            self.verify_source_bucket_mutation(self.docs_per_day * 2016,bucket='src_bucket.src_bucket.src_bucket')
        else:
            self.verify_source_bucket_mutation(self.docs_per_day * 2016,bucket='src_bucket._default._default')
        # delete all documents
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day*self.num_docs,"src_bucket.src_bucket.src_bucket",is_delete=True)
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",is_delete=True)
        if self.non_default_collection:
            self.verify_source_bucket_mutation(self.docs_per_day * 2016, deletes=True, timeout=1200,
                                               bucket='src_bucket.src_bucket.src_bucket')
        else:
            self.verify_source_bucket_mutation(self.docs_per_day * 2016, deletes=True, timeout=1200,
                                               bucket='src_bucket._default._default')
        self.undeploy_and_delete_function(body)

    def test_source_bucket_mutations(self):
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day*self.num_docs,"src_bucket.src_bucket.src_bucket")
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        if self.non_default_collection:
            self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", self.docs_per_day * self.num_docs*2)
        else:
            self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs*2)
        # delete all documents
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day*self.num_docs,"src_bucket.src_bucket.src_bucket",is_delete=True)
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",is_delete=True)
        if self.non_default_collection:
            self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", self.docs_per_day * self.num_docs)
        else:
            self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    def test_source_bucket_mutations_with_timers(self):
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day*self.num_docs,"src_bucket.src_bucket.src_bucket")
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_WITH_TIMERS)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        if self.non_default_collection:
            self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", self.docs_per_day * self.num_docs*2)
        else:
            self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs*2)
        # delete all documents
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day*self.num_docs,"src_bucket.src_bucket.src_bucket",is_delete=True)
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",is_delete=True)
        if self.non_default_collection:
            self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", self.docs_per_day * self.num_docs)
        else:
            self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    def test_pause_resume_execution(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE)
        self.deploy_function(body)
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",wait_for_loading=False)
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",wait_for_loading=False)
        self.pause_function(body)
        # intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(60)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day * self.num_docs*2, "src_bucket.src_bucket.src_bucket",wait_for_loading=False)
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs*2, "src_bucket._default._default",wait_for_loading=False)
        self.resume_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        if self.non_default_collection:
            self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", self.docs_per_day * self.num_docs*2, timeout=1200)
        else:
            self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs*2, timeout=1200)
        self.undeploy_and_delete_function(body)


    def test_source_bucket_mutation_for_dcp_stream_boundary_from_now(self):
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        body = self.create_save_function_body(self.function_name,HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION ,
                                              dcp_stream_boundary="from_now")
        self.deploy_function(body)
        # update all documents
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",is_update=True)
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",is_update=True)
        # Wait for eventing to catch up with all the update mutations and verify results
        if self.non_default_collection:
            self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", self.docs_per_day * self.num_docs*2)
        else:
            self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs*2)
        self.undeploy_and_delete_function(body)

    def test_compress_handler(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
        body = self.create_save_function_body(self.function_name,"handler_code/compress.js")
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",is_delete=True)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", 0)
        self.undeploy_and_delete_function(body)

    def test_expired_mutation(self):
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day*self.num_docs,"src_bucket.src_bucket.src_bucket",expiry=100, wait_for_loading=False)
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",expiry=100, wait_for_loading=False)
        # set expiry pager interval
        ClusterOperationHelper.flushctl_set(self.master, "exp_pager_stime", 1, bucket=self.src_bucket_name)
        body = self.create_save_function_body(self.function_name, "handler_code/bucket_op_expired.js")
        self.deploy_function(body)
        # Wait for eventing to catch up with all the expiry mutations and verify results
        if self.non_default_collection:
            self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", 0)
        else:
            self.verify_doc_count_collections("src_bucket._default._default", 0)
        self.undeploy_and_delete_function(body)

    def test_cancel_timer(self):
        bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size, replicas=0)
        self.cluster.create_standard_bucket(name=self.dst_bucket_name1, port=STANDARD_BUCKET_PORT + 1,
                                            bucket_params=bucket_params)
        query = "create primary index on {}".format(self.dst_bucket_name1)
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)
        if self.non_default_collection:
            self.collection_rest.create_scope_collection(bucket=self.dst_bucket_name1,scope=self.dst_bucket_name1,
                                                     collection=self.dst_bucket_name1)
            self.load_data_to_collection(self.docs_per_day*self.num_docs,"src_bucket.src_bucket.src_bucket")
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        body = self.create_save_function_body(self.function_name, "handler_code/cancel_timer.js")
        if self.non_default_collection:
            body['depcfg']['buckets'].append({"alias": self.dst_bucket_name1, "bucket_name": self.dst_bucket_name1,
                                              "scope_name":self.dst_bucket_name1,"collection_name":self.dst_bucket_name1,
                                              "access": "rw"})
        else:
            body['depcfg']['buckets'].append({"alias": self.dst_bucket_name1, "bucket_name": self.dst_bucket_name1})
        self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        # print timer context and alarm
        self.print_timer_alarm_context()
        # Wait for eventing to catch up with all the update mutations and verify results
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",is_delete=True)
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",is_delete=True)
        # Wait for eventing to catch up with all the delete mutations and verify results
        if self.non_default_collection:
            self.verify_doc_count_collections("dst_bucket1.dst_bucket1.dst_bucket1", self.docs_per_day * self.num_docs)
            self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", 0)
        else:
            self.verify_doc_count_collections("dst_bucket1._default._default", self.docs_per_day * self.num_docs)
            self.verify_doc_count_collections("dst_bucket._default._default", 0)
        self.assertEqual(self.get_stats_value(self.function_name,"execution_stats.timer_cancel_counter"),self.docs_per_day * 2016)
        self.undeploy_and_delete_function(body)

    def test_advance_bucket_op(self):
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                         expiry=300)
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default", expiry=300)
        # set expiry pager interval
        ClusterOperationHelper.flushctl_set(self.master, "exp_pager_stime", 3, bucket=self.src_bucket_name)
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/curl_timer_insert.js")
        if self.non_default_collection:
            body['depcfg']['buckets'].append({"alias": self.src_bucket_name, "bucket_name": self.src_bucket_name,
                                              "scope_name":self.src_bucket_name,"collection_name":self.src_bucket_name})
        else:
            body['depcfg']['buckets'].append({"alias": self.src_bucket_name, "bucket_name": self.src_bucket_name})
        self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        if self.non_default_collection:
            self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs)
            self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", 0)
        else:
            self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * self.num_docs)
            self.verify_doc_count_collections("dst_bucket._default._default", 0)
        self.undeploy_and_delete_function(body)