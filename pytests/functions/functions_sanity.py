from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.functions.functions_base import FunctionsBaseTest, log


class FunctionsSanity(FunctionsBaseTest):
    def setUp(self):
        super(FunctionsSanity, self).setUp()
        if self.create_functions_buckets:
            self.bucket_size = 100
            log.info(self.bucket_size)
            bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size,
                                                       replicas=self.num_replicas)
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

    def tearDown(self):
        super(FunctionsSanity, self).tearDown()

    def test_create_mutation_for_dcp_stream_boundary_from_beginning(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name,
                                              "function OnUpdate(doc, meta) {\n    log('document', doc);\n    dst_bucket[meta.id] = 'hello world';\n}\nfunction OnDelete(doc) {\n}")
        self.deploy_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        self.undeploy_and_delete_function(body)

    def test_delete_mutation_for_dcp_stream_boundary_from_beginning(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name,
                                              "function OnDelete(doc) {\n    log('document', doc);\n    dst_bucket[doc.id] = 'hello world';\n}\n")
        self.deploy_function(body)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        self.undeploy_and_delete_function(body)

    def test_expiry_mutation_for_dcp_stream_boundary_from_beginning(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, exp=1)
        body = self.create_save_function_body(self.function_name,
                                              "function OnDelete(doc) {\n    log('document', doc);\n    dst_bucket[doc.id] = 'hello world';\n}\n")
        self.deploy_function(body)
        # Wait for eventing to catch up with all the expiry mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        self.undeploy_and_delete_function(body)

    def test_update_mutation_for_dcp_stream_boundary_from_now(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name,
                                              "function OnUpdate(doc,meta) {\n    log('document', doc);\n    dst_bucket[meta.id] = 'hello world';\n}\n",
                                              dcp_stream_boundary="from_now")
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
        body = self.create_save_function_body(self.function_name,
                                              "function OnUpdate(doc,meta) {\n    var docId = meta.id;\n    var query = INSERT INTO dst_bucket ( KEY, VALUE ) VALUES ( :docId ,'Hello World');\n    query.execQuery();\n}\n",
                                              )
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        self.undeploy_and_delete_function(body)

    def test_doc_timer_events_from_handler_code(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name,
                                              "function OnUpdate(doc,meta) {\n    expiry = Math.round((new Date()).getTime() / 1000) + 5;\n    docTimer(timerCallback, meta.id, expiry);\n}\nfunction timerCallback(docid, expiry) {\n    var query = INSERT INTO dst_bucket ( KEY, VALUE ) VALUES ( UUID() ,'timerCallback');\n    query.execQuery();\n}\n",
                                              )
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, doc_timer_events=True)
        self.undeploy_and_delete_function(body)
