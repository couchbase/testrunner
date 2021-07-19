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
        self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=1400)
        if self.create_functions_buckets:
            self.bucket_size = 250
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
            self.cluster.create_standard_bucket(name=self.dst_bucket_name1, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.buckets = RestConnection(self.master).get_buckets()
        self.gens_load = self.generate_docs(self.docs_per_day)
        self.expiry = 3
        handler_code = self.input.param('handler_code', 'bucket_op')
        # index is required for delete operation through n1ql
        self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
        self.n1ql_helper = N1QLHelper(shell=self.shell, max_verify=self.max_verify, buckets=self.buckets,
                                      item_flag=self.item_flag, n1ql_port=self.n1ql_port,
                                      full_docs_list=self.full_docs_list, log=self.log, input=self.input,
                                      master=self.master, use_rest=True)
        self.n1ql_helper.create_primary_index(using_gsi=True, server=self.n1ql_node)
        if self.non_default_collection:
            self.create_scope_collection(bucket=self.src_bucket_name, scope=self.src_bucket_name,
                                         collection=self.src_bucket_name)
            self.create_scope_collection(bucket=self.metadata_bucket_name, scope=self.metadata_bucket_name,
                                         collection=self.metadata_bucket_name)
            self.create_scope_collection(bucket=self.dst_bucket_name, scope=self.dst_bucket_name,
                                         collection=self.dst_bucket_name)
            self.create_scope_collection(bucket=self.dst_bucket_name1, scope=self.dst_bucket_name1,
                                         collection=self.dst_bucket_name1)

    def tearDown(self):
        try:
            self.undeploy_delete_all_functions()
        except:
            # This is just a cleanup API. Ignore the exceptions.
            pass
        super(EventingBucketCache, self).tearDown()

    def test_compare_cached_doc_with_kv_bucket_doc(self):
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/cache_compare_values.js")
        body['depcfg']['buckets'].append({"alias": self.dst_bucket_name1, "bucket_name": self.dst_bucket_name1, "access": "rw"})
        self.rest.update_function(body['appname'], body)
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        self.deploy_function(body)
        if self.non_default_collection:
            self.verify_doc_count_collections("dst_bucket1.dst_bucket1.dst_bucket1", self.docs_per_day * self.num_docs)
        else:
            self.verify_doc_count_collections("dst_bucket1._default._default", self.docs_per_day * self.num_docs)
        stats = self.rest.get_all_eventing_stats()
        self.log.info("Stats for Eventing Node -\n{0}".format(json.dumps(stats, sort_keys=True, indent=4)))
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket", is_delete=True)
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default", is_delete=True)
        if self.non_default_collection:
            self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", 0)
            self.verify_doc_count_collections("dst_bucket1.dst_bucket1.dst_bucket1", 0)
        else:
            self.verify_doc_count_collections("dst_bucket._default._default", 0)
            self.verify_doc_count_collections("dst_bucket1._default._default", 0)
        self.undeploy_and_delete_function(body)

    def test_read_your_own_write_bucket_cache(self):
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/cache_ryow.js")
        body['depcfg']['buckets'].append({"alias": self.dst_bucket_name1, "bucket_name": self.dst_bucket_name1, "access": "rw"})
        self.rest.update_function(body['appname'], body)
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        self.deploy_function(body)
        if self.non_default_collection:
            self.verify_doc_count_collections("dst_bucket1.dst_bucket1.dst_bucket1", self.docs_per_day * self.num_docs)
        else:
            self.verify_doc_count_collections("dst_bucket1._default._default", self.docs_per_day * self.num_docs)
        stats = self.rest.get_all_eventing_stats()
        self.log.info("Stats for Eventing Node -\n{0}".format(json.dumps(stats, sort_keys=True, indent=4)))
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket", is_delete=True)
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default", is_delete=True)
        if self.non_default_collection:
            self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", 0)
            self.verify_doc_count_collections("dst_bucket1.dst_bucket1.dst_bucket1", 0)
        else:
            self.verify_doc_count_collections("dst_bucket._default._default", 0)
            self.verify_doc_count_collections("dst_bucket1._default._default", 0)
        self.undeploy_and_delete_function(body)

    def test_cache_overflow_when_total_docs_size_greater_than_cache_size(self):
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/cache_overflow.js")
        body['depcfg']['buckets'].append(
            {"alias": self.dst_bucket_name1, "bucket_name": self.dst_bucket_name1, "access": "rw"})
        # set cache age to 1 min to ensure no expiry
        body['settings']['bucket_cache_age'] = 60000
        self.rest.update_function(body['appname'], body)
        if self.non_default_collection:
            self.load_data_to_collection(1, "src_bucket.src_bucket.src_bucket")
        else:
            self.load_data_to_collection(1, "src_bucket._default._default")
        self.deploy_function(body)
        if self.non_default_collection:
            self.verify_doc_count_collections("dst_bucket1.dst_bucket1.dst_bucket1", 1)
        else:
            self.verify_doc_count_collections("dst_bucket1._default._default", 1)
        stats = self.rest.get_all_eventing_stats()
        self.log.info("Stats for Eventing Node -\n{0}".format(json.dumps(stats, sort_keys=True, indent=4)))
        if self.non_default_collection:
            self.load_data_to_collection(1, "src_bucket.src_bucket.src_bucket",
                                         is_delete=True)
        else:
            self.load_data_to_collection(1, "src_bucket._default._default",
                                         is_delete=True)
        if self.non_default_collection:
            self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", 0)
            self.verify_doc_count_collections("dst_bucket1.dst_bucket1.dst_bucket1", 0)
        else:
            self.verify_doc_count_collections("dst_bucket._default._default", 0)
            self.verify_doc_count_collections("dst_bucket1._default._default", 0)
        self.undeploy_and_delete_function(body)
        assert stats[0]["failure_stats"]["bucket_op_cache_miss_count"] > 8, "No cache miss occurred."

    def test_all_docs_are_fetched_from_cache_when_total_doc_size_less_than_cache_size(self):
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/cache_underflow.js")
        body['depcfg']['buckets'].append(
            {"alias": self.dst_bucket_name1, "bucket_name": self.dst_bucket_name1, "access": "rw"})
        # set cache age to 1 min to ensure no expiry
        body['settings']['bucket_cache_age'] = 60000
        self.rest.update_function(body['appname'], body)
        if self.non_default_collection:
            self.load_data_to_collection(1, "src_bucket.src_bucket.src_bucket")
        else:
            self.load_data_to_collection(1, "src_bucket._default._default")
        self.deploy_function(body)
        if self.non_default_collection:
            self.verify_doc_count_collections("dst_bucket1.dst_bucket1.dst_bucket1", 1)
        else:
            self.verify_doc_count_collections("dst_bucket1._default._default", 1)
        stats = self.rest.get_all_eventing_stats()
        self.log.info("Stats for Eventing Node -\n{0}".format(json.dumps(stats, sort_keys=True, indent=4)))
        if self.non_default_collection:
            self.load_data_to_collection(1, "src_bucket.src_bucket.src_bucket",
                                         is_delete=True)
        else:
            self.load_data_to_collection(1, "src_bucket._default._default",
                                         is_delete=True)
        if self.non_default_collection:
            self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", 0)
            self.verify_doc_count_collections("dst_bucket1.dst_bucket1.dst_bucket1", 0)
        else:
            self.verify_doc_count_collections("dst_bucket._default._default", 0)
            self.verify_doc_count_collections("dst_bucket1._default._default", 0)
        self.undeploy_and_delete_function(body)
        assert stats[0]["failure_stats"]["bucket_op_cache_miss_count"] == 6, "Unexpected cache miss occurred."

    def test_invalid_input_for_bucket_cache_size_and_bucket_cache_age(self):
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/cache_compare_values.js")
        try:
            body['settings']['bucket_cache_age'] = 0
            self.rest.update_function(body['appname'], body)
        except Exception as e:
            self.log.info(e)
            assert "ERR_INVALID_CONFIG" in str(e) and "bucket_cache_age can not be zero or negative" in str(e), True
        try:
            body['settings']['bucket_cache_size'] = -10
            self.rest.update_function(body['appname'], body)
        except Exception as e:
            self.log.info(e)
            assert "ERR_INVALID_CONFIG" in str(e) and "bucket_cache_size can not be zero or negative" in str(e), True