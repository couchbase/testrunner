import copy

from lib.couchbase_helper.stats_tools import StatsCommon
from lib.couchbase_helper.tuq_helper import N1QLHelper
from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from lib.couchbase_helper.documentgenerator import JSONNonDocGenerator, BlobGenerator
from pytests.eventing.eventing_constants import HANDLER_CODE
from pytests.eventing.eventing_base import EventingBaseTest
import logging
import json

log = logging.getLogger()


class EventingBucketCache(EventingBaseTest):
    def setUp(self):
        super(EventingBucketCache, self).setUp()
        handler_code = self.input.param('handler_code', 'bucket_op')

    def tearDown(self):
        super(EventingBucketCache, self).tearDown()

    def test_compare_cached_doc_with_kv_bucket_doc(self):
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/cache_compare_values.js")
        self.rest.update_function(body['appname'], body, self.function_scope)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection2", self.docs_per_day * self.num_docs)
        stats = self.rest.get_all_eventing_stats()
        self.log.info("Stats for Eventing Node -\n{0}".format(json.dumps(stats, sort_keys=True, indent=4)))
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0", is_delete=True)
        self.verify_doc_count_collections("default.scope0.collection1", 0)
        self.verify_doc_count_collections("default.scope0.collection2", 0)
        self.undeploy_and_delete_function(body)

    def test_read_your_own_write_bucket_cache(self):
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/cache_ryow.js")
        self.rest.update_function(body['appname'], body, self.function_scope)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection2", self.docs_per_day * self.num_docs)
        stats = self.rest.get_all_eventing_stats()
        self.log.info("Stats for Eventing Node -\n{0}".format(json.dumps(stats, sort_keys=True, indent=4)))
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0", is_delete=True)
        self.verify_doc_count_collections("default.scope0.collection1", 0)
        self.verify_doc_count_collections("default.scope0.collection2", 0)
        self.undeploy_and_delete_function(body)

    def test_cache_overflow_when_total_docs_size_greater_than_cache_size(self):
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/cache_overflow.js")
        # set cache age to 1 min to ensure no expiry
        body['settings']['bucket_cache_age'] = 60000
        self.rest.update_function(body['appname'], body, self.function_scope)
        self.load_data_to_collection(1, "default.scope0.collection0")
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection2", 1)
        stats = self.rest.get_all_eventing_stats()
        self.log.info("Stats for Eventing Node -\n{0}".format(json.dumps(stats, sort_keys=True, indent=4)))
        self.load_data_to_collection(1, "default.scope0.collection0",
                                         is_delete=True)
        self.verify_doc_count_collections("default.scope0.collection1", 0)
        self.verify_doc_count_collections("default.scope0.collection2", 0)
        self.undeploy_and_delete_function(body)
        assert stats[0]["failure_stats"]["bucket_op_cache_miss_count"] > 8, "No cache miss occurred."

    def test_all_docs_are_fetched_from_cache_when_total_doc_size_less_than_cache_size(self):
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/cache_underflow.js")
        # set cache age to 1 min to ensure no expiry
        body['settings']['bucket_cache_age'] = 60000
        self.rest.update_function(body['appname'], body, self.function_scope)
        self.load_data_to_collection(1, "default.scope0.collection0")
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection2", 1)
        stats = self.rest.get_all_eventing_stats()
        self.log.info("Stats for Eventing Node -\n{0}".format(json.dumps(stats, sort_keys=True, indent=4)))
        self.load_data_to_collection(1, "default.scope0.collection0",
                                         is_delete=True)
        self.verify_doc_count_collections("default.scope0.collection1", 0)
        self.verify_doc_count_collections("default.scope0.collection2", 0)
        self.undeploy_and_delete_function(body)
        assert stats[0]["failure_stats"]["bucket_op_cache_miss_count"] == 6, "Unexpected cache miss occurred."

    def test_invalid_input_for_bucket_cache_size_and_bucket_cache_age(self):
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/cache_compare_values.js")
        try:
            body['settings']['bucket_cache_age'] = 0
            self.rest.update_function(body['appname'], body, self.function_scope)
        except Exception as e:
            self.log.info(e)
            assert "ERR_INVALID_REQUEST" in str(e) and "bucket_cache_age value should be greater than or equal to 1" in str(e), True
        try:
            body['settings']['bucket_cache_size'] = -10
            self.rest.update_function(body['appname'], body, self.function_scope)
        except Exception as e:
            self.log.info(e)
            assert "ERR_INVALID_REQUEST" in str(e) and "bucket_cache_age value should be greater than or equal to 1" in str(e), True
