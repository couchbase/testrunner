from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE
from pytests.eventing.eventing_base import EventingBaseTest, log
from membase.helper.cluster_helper import ClusterOperationHelper


class AdvanceBucketOp(EventingBaseTest):
    def setUp(self):
        super(AdvanceBucketOp, self).setUp()
        self.gens_load = self.generate_docs(self.docs_per_day)
        self.handler_code = self.input.param('handler_code',None)

    def tearDown(self):
        super(AdvanceBucketOp, self).tearDown()

    def test_advance_bucket_op(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, self.handler_code, src_binding=True)
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
        self.verify_doc_count_collections("default.scope0.collection1", 0)
        self.undeploy_and_delete_function(body)

    def test_advance_bucket_op_with_expiry(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0", expiry=120)
        # set expiry pager interval
        ClusterOperationHelper.flushctl_set(self.master, "exp_pager_stime", 3, bucket=self.default_bucket_name)
        body = self.create_save_function_body(self.function_name,self.handler_code, src_binding=True)
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        self.verify_doc_count_collections("default.scope0.collection1", 0)
        self.undeploy_and_delete_function(body)

    def test_increment_decrement(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, self.handler_code, src_binding=True)
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs + 1)
        self.verify_counter(self.docs_per_day * 2016)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
        self.verify_doc_count_collections("default.scope0.collection1", 1)
        self.undeploy_and_delete_function(body)

    def verify_counter(self,count):
        query = "create primary index on default.scope0.collection1"
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
        query = "select count from default.scope0.collection1"
        counter = self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
        self.log.info(counter["results"][0]["count"])
        if counter["results"][0]["count"] != count:
            self.fail("counter value is not as expected {0} , actual value {1}".format(count,counter["results"][0]["count"]))

    def test_set_expiry(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        self.verify_doc_count_collections("default.scope0.collection1", 0)
        self.undeploy_and_delete_function(body)

    def test_different_datatype(self):
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        query = "insert into default.scope0.collection0 (KEY, VALUE) VALUES (\"data\",\"doc created\")"
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
        self.verify_doc_count_collections("default.scope0.collection1", 1)
        self.verify_doc_count_collections("default.scope0.collection2", 1)
        self.undeploy_and_delete_function(body)

    def test_different_datatype_insert(self):
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        query = "insert into default.scope0.collection0 (KEY, VALUE) VALUES (\"data\",\"doc created\")"
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
        self.verify_doc_count_collections("default.scope0.collection1", 7)
        self.undeploy_and_delete_function(body)

    def test_incorrect_param_type(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        self.verify_doc_count_collections("default.scope0.collection2", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    def test_cas_errors(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
        self.verify_doc_count_collections("default.scope0.collection1", 0)
        self.undeploy_and_delete_function(body)

    def test_incorrect_bindings(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, self.handler_code, src_binding=True)
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
        self.verify_doc_count_collections("default.scope0.collection1", 0)
        self.undeploy_and_delete_function(body)

    def test_increment_decrement_error(self):
        query = "insert into default.scope0.collection0 (KEY, VALUE) VALUES (\"counter\",{\"count\": \"abc\"})"
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
        body = self.create_save_function_body(self.function_name, self.handler_code,dcp_stream_boundary="from_now",
                                              src_binding=True)
        self.deploy_function(body)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
        self.verify_doc_count_collections("default.scope0.collection1", 0)
        self.undeploy_and_delete_function(body)

    def test_source_bucket_mutations_are_not_suppressed_when_recursion_flag_is_enabled(self):
        self.load_data_to_collection(1, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, self.handler_code, src_binding=True)
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection0", 1000)
        self.undeploy_and_delete_function(body)

    def test_source_bucket_mutations_are_suppressed_when_recursion_flag_is_disabled(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, self.handler_code, src_binding=True)
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection0", self.docs_per_day * self.num_docs * 2)
        self.undeploy_and_delete_function(body)

    def test_kv_touch_bucket_operation_support(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, self.handler_code, src_binding=True)
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection0", 0)
        self.undeploy_and_delete_function(body)

    def test_array_and_field_level_subdoc_operations(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        self.assertTrue(self.check_lcb_exception,"LCB Exception occured during operation") 
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0", is_delete=True)
        self.verify_doc_count_collections("default.scope0.collection1", 0)
        self.undeploy_and_delete_function(body)
    
    
