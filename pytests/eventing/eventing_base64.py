from lib.couchbase_helper.stats_tools import StatsCommon
from lib.couchbase_helper.tuq_helper import N1QLHelper
from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from lib.couchbase_helper.documentgenerator import JSONNonDocGenerator, BlobGenerator
from pytests.fts.vector_dataset_generator.vector_dataset_loader import GoVectorLoader
from pytests.eventing.eventing_base import EventingBaseTest
import logging
import json


log = logging.getLogger()

class EventingBase64(EventingBaseTest):
    def setUp(self):
        super(EventingBase64, self).setUp()
        self.src_bucket = self.rest.get_bucket_by_name(self.src_bucket_name)
        self.handler_code = self.input.param('handler_code', None)
        self.docs_per_day = self.input.param('doc-per-day', 20)

    def tearDown(self):
        super(EventingBase64, self).tearDown()
    # testing accepted and unaccepted input types, valid/invalid lengths
    def test_positive_negative_scenarios(self):
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.load_data_to_collection(self.docs_per_day*self.num_docs, "src_bucket._default._default")
        self.deploy_function(body)
        self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day*self.num_docs)
        self.undeploy_and_delete_function(body)
        
    def test_end_to_end_query(self):
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.load_data_to_collection(self.docs_per_day*self.num_docs, "src_bucket._default._default")
        self.deploy_function(body)
        self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day*self.num_docs*2)
        self.undeploy_and_delete_function(body)

    def test_performance_for_continuous_operations(self):
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.load_data_to_collection(self.docs_per_day*self.num_docs, "src_bucket._default._default")
        self.deploy_function(body)
        self.print_execution_and_failure_stats(self.function_name)
        self.check_lcb_exception()
        self.verify_doc_count_collections("dst_bucket._default._default",self. docs_per_day*self.num_docs)
        self.undeploy_and_delete_function(body)

    def test_large_vectors_encoding(self):
        body = self.create_save_function_body(self.function_name, self.handler_code)
        govl = GoVectorLoader(self.master, "Administrator", "password",
                               "src_bucket", "_default",
                              "_default", "siftsmall", False, "", 0, self.num_docs*self.docs_per_day, False,
                              [0.25, 0.25, 0.25, 0.25], [1, 1000, 4096, 20000])
        govl.load_data()
        self.deploy_function(body)
        self.verify_doc_count_collections("dst_bucket._default._default",self.num_docs*self.docs_per_day)
        self.undeploy_and_delete_function(body)

    def test_crc64_iso_function(self):
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.load_data_to_collection(self.docs_per_day*self.num_docs, "src_bucket._default._default")
        self.deploy_function(body)
        self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day*self.num_docs)
        self.undeploy_and_delete_function(body)