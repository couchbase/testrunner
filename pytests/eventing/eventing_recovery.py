import copy
import json
import sys
import traceback
import urllib.parse

from lib.membase.api.rest_client import RestHelper
from lib.couchbase_helper.tuq_helper import N1QLHelper
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from lib.memcached.helper.data_helper import MemcachedClientHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from pytests.eventing.eventing_constants import HANDLER_CODE, HANDLER_CODE_CURL, HANDLER_CODE_ONDEPLOY, HANDLER_CODE_FTS_QUERY_SUPPORT, HANDLER_CODE_ANALYTICS
from pytests.eventing.eventing_base import EventingBaseTest
from pytests.fts.fts_callable import FTSCallable
from pytests.eventing.fts_query_definitions import ALL_QUERIES
from pytests.security.jwt_utils import JWTUtils
import logging
import time

log = logging.getLogger()


class EventingRecovery(EventingBaseTest):
    def setUp(self):
        super(EventingRecovery, self).setUp()
        self.buckets = self.rest.get_buckets()
        self.src_bucket = self.rest.get_bucket_by_name(self.src_bucket_name)
        self.gens_load = self.generate_docs(self.docs_per_day)
        handler_code = self.input.param('handler_code', 'bucket_op')
        if handler_code == 'bucket_op':
            self.handler_code = "handler_code/ABO/insert_recovery.js"
        elif handler_code == 'bucket_op_with_timers':
            self.handler_code = HANDLER_CODE.BUCKET_OPS_WITH_TIMERS_RECOVERY
        elif handler_code == 'bucket_op_with_cron_timers':
            self.handler_code = "handler_code/ABO/insert_recovery_timers.js"
        elif handler_code == 'n1ql_op_with_timers':
            self.handler_code = HANDLER_CODE.N1QL_OPS_WITH_TIMERS
        elif handler_code == 'source_bucket_mutation':
            self.handler_code = HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION_RECOVERY
        elif handler_code == 'source_bucket_mutation_with_timers':
            self.handler_code = HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_WITH_TIMERS_RECOVERY
        elif handler_code == 'bucket_op_curl_get':
            self.handler_code = HANDLER_CODE_CURL.BUCKET_OP_WITH_CURL_GET_RECOVERY
        elif handler_code == 'bucket_op_curl_post':
            self.handler_code = HANDLER_CODE_CURL.BUCKET_OP_WITH_CURL_POST
        elif handler_code == 'bucket_op_curl_put':
            self.handler_code = HANDLER_CODE_CURL.BUCKET_OP_WITH_CURL_PUT
        elif handler_code == 'bucket_op_curl_delete':
            self.handler_code = HANDLER_CODE_CURL.BUCKET_OP_WITH_CURL_DELETE
        elif handler_code == 'timer_op_curl_get':
            self.handler_code = HANDLER_CODE_CURL.TIMER_OP_WITH_CURL_GET
        elif handler_code == 'timer_op_curl_post':
            self.handler_code = HANDLER_CODE_CURL.TIMER_OP_WITH_CURL_POST
        elif handler_code == 'timer_op_curl_put':
            self.handler_code = HANDLER_CODE_CURL.TIMER_OP_WITH_CURL_PUT
        elif handler_code == 'timer_op_curl_delete':
            self.handler_code = HANDLER_CODE_CURL.TIMER_OP_WITH_CURL_DELETE_RECOVERY
        elif handler_code == 'cancel_timer':
            self.handler_code = HANDLER_CODE.CANCEL_TIMER_RECOVERY
        elif handler_code == 'bucket_op_expired':
            self.handler_code = HANDLER_CODE.BUCKET_OP_EXPIRED_RECOVERY
        elif handler_code == 'ondeploy_test':
            self.handler_code = HANDLER_CODE_ONDEPLOY.ONDEPLOY_BASIC_BUCKET_OP
        elif handler_code == 'ondeploy_test_pause_resume':
            self.handler_code = HANDLER_CODE_ONDEPLOY.ONDEPLOY_PAUSE_RESUME
        elif handler_code == 'fts_match_query':
            self.handler_code = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_MATCH_QUERY
            self.is_fts = True
            self.fts_query = ALL_QUERIES['match_query']
        elif handler_code == 'analytics_basic_select':
            self.handler_code = HANDLER_CODE_ANALYTICS.ANALYTICS_BASIC_SELECT
            self.is_analytics = True
        else:
            self.handler_code = "handler_code/ABO/insert_recovery.js"
        if self.is_expired:
            # set expiry pager interval
            ClusterOperationHelper.flushctl_set(self.master, "exp_pager_stime", 60, bucket=self.src_bucket_name)
        # FTS setup
        if getattr(self, 'is_fts', False):
            self.fts_index_name = "travel_sample_test"
            self.fts_doc_count = 31500
            self.fts_callable = FTSCallable(nodes=self.servers, es_validate=False)
            self.fts_memory_quota = 3000
            log.info("Setting FTS memory quota to %s MB" % self.fts_memory_quota)
            self.rest.set_service_memoryQuota(service='ftsMemoryQuota', memoryQuota=self.fts_memory_quota)
        # analytics setup
        if getattr(self, 'is_analytics', False):
            self.load_sample_buckets(self.server, "travel-sample")
            self.load_data_to_collection(1, "default.scope0.collection0")
            self._setup_analytics()

        # JWT Configuration (Optional)
        self.jwt_auth = self.input.param('jwt_auth', False)
        self.jwt_token = None
        if self.jwt_auth:
            self.jwt_algorithm = self.input.param('jwt_algorithm', 'ES256')
            self.jwt_issuer = self.input.param('jwt_issuer', 'custom-issuer')
            self.jwt_audience = self.input.param('jwt_audience', 'cb-cluster')
            self.jwt_user = self.input.param('jwt_user', 'jwt_user')
            self.jwt_group = self.input.param('jwt_group', 'admin')
            self.jwt_roles = self.input.param('jwt_roles', 'admin')
            self.jit_provisioning = self.input.param('jit_provisioning', True)
            self.jwt_ttl = self.input.param('jwt_ttl', 3600)
            self.jwt_utils = JWTUtils(log=self.log)

    def tearDown(self):
        if getattr(self, 'is_fts', False) and getattr(self, 'fts_index_name', None):
            try:
                self.fts_callable.delete_fts_index(self.fts_index_name)
                log.info("Deleted FTS index: %s" % self.fts_index_name)
            except Exception as e:
                log.warning("Failed to delete FTS index %s: %s" % (self.fts_index_name, str(e)))
        elif getattr(self, 'is_analytics', False):
            try:
                cbas_node = self.get_nodes_from_services_map(service_type="cbas")
                if cbas_node:
                    cbas_rest = RestConnection(cbas_node)
                    cbas_rest.execute_statement_on_cbas("DISCONNECT LINK Local", None)
            except Exception as e:
                log.exception("Analytics teardown cleanup failed: %s", str(e))
        super(EventingRecovery, self).tearDown()

    def test_killing_eventing_consumer_when_eventing_is_processing_mutations(self):
        # Setup JWT configuration if enabled
        jwt_token = self.setup_jwt_config() if self.jwt_auth else None

        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        # FTS setup if using FTS handler
        if getattr(self, 'is_fts', False):
            self.load_sample_buckets(self.server, "travel-sample")
            plan_params = {"indexPartitions": 1, "numReplicas": 0}
            fts_index = self.fts_callable.create_default_index(
                index_name=self.fts_index_name,
                bucket_name="travel-sample",
                plan_params=plan_params
            )
            self.fts_callable.wait_for_indexing_complete(item_count=self.fts_doc_count, idx=fts_index)
            self.sleep(30, "Waiting for FTS indexing to complete")
        body = self.create_save_function_body(self.function_name, self.handler_code, jwt_token=jwt_token)
        if self.is_curl:
            body['depcfg']['curl'] = []
            body['depcfg']['curl'].append({"hostname": self.hostname, "value": "server", "auth_type": self.auth_type,
                                           "username": self.curl_username, "password": self.curl_password,"cookies": self.cookies})
            self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body, jwt_token=jwt_token)
        if self.pause_resume:
            self.pause_function(body, jwt_token=jwt_token)
        # load some data
        if not self.is_expired:
            if self.non_default_collection:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",wait_for_loading=False)
            else:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",wait_for_loading=False)
        else:
            if self.non_default_collection:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                             expiry=10,wait_for_loading=False)
            else:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                             expiry=10,wait_for_loading=False)
        if self.pause_resume:
            self.resume_function(body, jwt_token=jwt_token)
        # kill eventing consumer when eventing is processing mutations
        self.kill_consumer(eventing_node)
        self.wait_for_handler_state(body['appname'], "deployed")
        # Run FTS validation if FTS handler is being used
        self.run_fts_validation()
        # Run analytics validation if analytics handler is being used
        if getattr(self, 'is_analytics', False):
            self.sleep(10, "Waiting for eventing to reprocess after consumer kill")
            self.verify_doc_count_collections("default.scope0.collection1", 1)
            self._verify_analytics_result_matches_direct_query()
            self.undeploy_and_delete_function(body, jwt_token=jwt_token)
            return
        # Wait for eventing to catch up with all the update mutations and verify results
        if not self.cancel_timer:
            if self.is_sbm:
                if self.non_default_collection:
                    self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket",self.docs_per_day * self.num_docs * 2)
                else:
                    self.verify_doc_count_collections("src_bucket._default._default",self.docs_per_day * self.num_docs * 2)
            else:
                if self.non_default_collection:
                    self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket",self.docs_per_day * self.num_docs)
                else:
                    self.verify_doc_count_collections("src_bucket._default._default",self.docs_per_day * self.num_docs)
        if self.pause_resume:
            self.pause_function(body, jwt_token=jwt_token)
        # delete all documents
        if not self.is_expired:
            if self.non_default_collection:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                             is_delete=True,wait_for_loading=False)
            else:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                             is_delete=True,wait_for_loading=False)
        if self.pause_resume:
            self.resume_function(body, jwt_token=jwt_token)
        # kill eventing consumer when eventing is processing mutations
        self.kill_consumer(eventing_node)
        self.wait_for_handler_state(body['appname'], "deployed")
        # Run FTS validation if FTS handler is being used
        self.run_fts_validation()
        # Wait for eventing to catch up with all the delete mutations and verify results
        if not self.cancel_timer:
            if self.is_sbm:
                if self.non_default_collection:
                    self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket",self.docs_per_day * self.num_docs)
                else:
                    self.verify_doc_count_collections("src_bucket._default._default",self.docs_per_day * self.num_docs)
            else:
                if self.non_default_collection:
                    self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket",0)
                else:
                    self.verify_doc_count_collections("src_bucket._default._default",0)
        else:
            if self.non_default_collection:
                self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", 0)
            else:
                self.verify_doc_count_collections("src_bucket._default._default", 0)
        self.undeploy_and_delete_function(body, jwt_token=jwt_token)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_killing_eventing_producer_when_eventing_is_processing_mutations(self):
        # Setup JWT configuration if enabled
        jwt_token = self.setup_jwt_config() if self.jwt_auth else None

        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        # FTS setup if using FTS handler
        if getattr(self, 'is_fts', False):
            self.load_sample_buckets(self.server, "travel-sample")
            plan_params = {"indexPartitions": 1, "numReplicas": 0}
            fts_index = self.fts_callable.create_default_index(
                index_name=self.fts_index_name,
                bucket_name="travel-sample",
                plan_params=plan_params
            )
            self.fts_callable.wait_for_indexing_complete(item_count=self.fts_doc_count, idx=fts_index)
            self.sleep(30, "Waiting for FTS indexing to complete")
        body = self.create_save_function_body(self.function_name, self.handler_code, jwt_token=jwt_token)
        if self.is_curl:
            body['depcfg']['curl'] = []
            body['depcfg']['curl'].append({"hostname": self.hostname, "value": "server", "auth_type": self.auth_type,
                                           "username": self.curl_username, "password": self.curl_password,"cookies": self.cookies})
            self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body, jwt_token=jwt_token)
        #pause handler
        if self.pause_resume:
            self.pause_function(body, jwt_token=jwt_token)
        # load some data
        if not self.is_expired:
            if self.non_default_collection:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",wait_for_loading=False)
            else:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",wait_for_loading=False)
        else:
            if self.non_default_collection:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                             expiry=10,wait_for_loading=False)
            else:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                             expiry=10,wait_for_loading=False)
        # kill eventing producer when eventing is processing mutations
        self.kill_producer(eventing_node)
        if self.pause_resume:
            self.wait_for_handler_state(body['appname'], "paused")
            self.resume_function(body, jwt_token=jwt_token)
        else:
            self.wait_for_handler_state(body['appname'], "deployed")
        # Run FTS validation if FTS handler is being used
        self.run_fts_validation()
        # Run analytics validation if analytics handler is being used
        if getattr(self, 'is_analytics', False):
            self.sleep(10, "Waiting for eventing to reprocess after producer kill")
            self.verify_doc_count_collections("default.scope0.collection1", 1)
            self._verify_analytics_result_matches_direct_query()
            self.undeploy_and_delete_function(body, jwt_token=jwt_token)
            return
        # Wait for eventing to catch up with all the update mutations and verify results
        if not self.cancel_timer:
            if self.is_sbm:
                if self.non_default_collection:
                    self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket",self.docs_per_day * self.num_docs * 2)
                else:
                    self.verify_doc_count_collections("src_bucket._default._default",self.docs_per_day * self.num_docs * 2)
            else:
                if self.non_default_collection:
                    self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket",self.docs_per_day * self.num_docs)
                else:
                    self.verify_doc_count_collections("src_bucket._default._default",self.docs_per_day * self.num_docs)
        if self.pause_resume:
            self.pause_function(body, jwt_token=jwt_token)
        # delete all documents
        if not self.is_expired:
            if self.non_default_collection:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                             is_delete=True,wait_for_loading=False)
            else:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                             is_delete=True,wait_for_loading=False)
        # kill eventing producer when eventing is processing mutations
        self.kill_producer(eventing_node)
        if self.pause_resume:
            self.wait_for_handler_state(body['appname'], "paused")
            self.resume_function(body, jwt_token=jwt_token)
        else:
            self.wait_for_handler_state(body['appname'], "deployed")
        # Run FTS validation if FTS handler is being used
        self.run_fts_validation()
        # Wait for eventing to catch up with all the delete mutations and verify results
        # See MB-30772
        if not self.cancel_timer:
            if self.is_sbm:
                if self.non_default_collection:
                    self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket",self.docs_per_day * self.num_docs)
                else:
                    self.verify_doc_count_collections("src_bucket._default._default",self.docs_per_day * self.num_docs)
            else:
                if self.non_default_collection:
                    self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket",0)
                else:
                    self.verify_doc_count_collections("src_bucket._default._default",0)
        else:
            if self.non_default_collection:
                self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", 0)
            else:
                self.verify_doc_count_collections("src_bucket._default._default", 0)
        self.undeploy_and_delete_function(body, jwt_token=jwt_token)
        # intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(5)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_killing_memcached_when_eventing_is_processing_mutations(self):
        kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        if self.is_curl:
            body['depcfg']['curl'] = []
            body['depcfg']['curl'].append({"hostname": self.hostname, "value": "server", "auth_type": self.auth_type,
                                           "username": self.curl_username, "password": self.curl_password,"cookies": self.cookies})
            self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        # pause handler
        if self.pause_resume:
            self.pause_function(body)
        # load some data
        if not self.is_expired:
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
        else:
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size,exp=60)
        # kill memcached on kv and eventing when eventing is processing mutations
        for node in [kv_node, eventing_node]:
            self.kill_memcached_service(node)
        self.warmup_check()
        if self.pause_resume:
            self.wait_for_handler_state(body['appname'], "paused")
            self.resume_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        # See MB-27115
        # self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # pause handler
        if self.pause_resume:
            self.pause_function(body)
        # delete all documents
        if not self.is_expired:
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size, op_type='delete')
        # kill memcached on kv and eventing when eventing is processing mutations
        for node in [kv_node, eventing_node]:
            self.kill_memcached_service(node)
        self.sleep(120)
        if self.pause_resume:
            self.wait_for_handler_state(body['appname'], "paused")
            self.resume_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        # See MB-27115
        # self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_killing_erlang_when_eventing_is_processing_mutations(self):
        kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        if self.is_curl:
            body['depcfg']['curl'] = []
            body['depcfg']['curl'].append({"hostname": self.hostname, "value": "server", "auth_type": self.auth_type,
                                           "username": self.curl_username, "password": self.curl_password,"cookies": self.cookies})
            self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        # pause handler
        if self.pause_resume:
            self.pause_function(body)
        # load some data
        if not self.is_expired:
            if self.non_default_collection:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                             wait_for_loading=False)
            else:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                             wait_for_loading=False)
        else:
            if self.non_default_collection:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                             expiry=150, wait_for_loading=False)
            else:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                             expiry=150, wait_for_loading=False)
        # kill erlang on eventing when eventing is processing mutations
        for node in [eventing_node]:
            self.print_eventing_stats_from_all_eventing_nodes()
            self.kill_erlang_service(node)
        if self.pause_resume:
            self.wait_for_handler_state(body['appname'], "paused")
            self.resume_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        if not self.cancel_timer:
            if self.is_sbm:
                if self.non_default_collection:
                    self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket",
                                                      self.docs_per_day * self.num_docs * 2)
                else:
                    self.verify_doc_count_collections("src_bucket._default._default",
                                                      self.docs_per_day * self.num_docs * 2)
            else:
                if self.non_default_collection:
                    self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket",
                                                      self.docs_per_day * self.num_docs)
                else:
                    self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs)
        # pause handler
        if self.pause_resume:
            self.pause_function(body)
        # delete all documents
        if not self.is_expired:
            if self.non_default_collection:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                             is_delete=True, wait_for_loading=False)
            else:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                             is_delete=True, wait_for_loading=False)
        # kill erlang on kv and eventing when eventing is processing mutations
        for node in [eventing_node]:
            self.print_eventing_stats_from_all_eventing_nodes()
            self.kill_erlang_service(node)
        if self.pause_resume:
            self.wait_for_handler_state(body['appname'], "paused")
            self.resume_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        if not self.cancel_timer:
            if self.is_sbm:
                if self.non_default_collection:
                    self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket",
                                                      self.docs_per_day * self.num_docs)
                else:
                    self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs)
            else:
                if self.non_default_collection:
                    self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", 0)
                else:
                    self.verify_doc_count_collections("src_bucket._default._default", 0)
        else:
            if self.non_default_collection:
                self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", 0)
            else:
                self.verify_doc_count_collections("src_bucket._default._default", 0)
        self.undeploy_and_delete_function(body)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_killing_kv_erlang_when_eventing_is_processing_mutations(self):
        kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, self.handler_code,execution_timeout=30)
        if self.is_curl:
            body['depcfg']['curl'] = []
            body['depcfg']['curl'].append({"hostname": self.hostname, "value": "server", "auth_type": self.auth_type,
                                           "username": self.curl_username, "password": self.curl_password,"cookies": self.cookies})
            self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        # pause handler
        if self.pause_resume:
            self.pause_function(body)
        # load some data
        if not self.is_expired:
            if self.non_default_collection:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
            else:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        else:
            if self.non_default_collection:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                             expiry=10)
            else:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                             expiry=10)
        # kill erlang on kv when eventing is processing mutations
        for node in [kv_node]:
            self.print_eventing_stats_from_all_eventing_nodes()
            self.kill_erlang_service(node)
        if self.pause_resume:
            self.wait_for_handler_state(body['appname'], "paused")
            self.resume_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        if not self.cancel_timer:
            if self.is_sbm:
                if self.non_default_collection:
                    self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket",
                                                      self.docs_per_day * self.num_docs * 2)
                else:
                    self.verify_doc_count_collections("src_bucket._default._default",
                                                      self.docs_per_day * self.num_docs * 2)
            else:
                if self.non_default_collection:
                    self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket",
                                                      self.docs_per_day * self.num_docs)
                else:
                    self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs)
        # pause handler
        if self.pause_resume:
            self.pause_function(body)
        # delete all documents
        if not self.is_expired:
            if self.non_default_collection:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                             is_delete=True)
            else:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                             is_delete=True)
        # kill erlang on kv when eventing is processing mutations
        for node in [kv_node]:
            self.print_eventing_stats_from_all_eventing_nodes()
            self.kill_erlang_service(node)
        if self.pause_resume:
            self.wait_for_handler_state(body['appname'], "paused")
            self.resume_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        if not self.cancel_timer:
            if self.is_sbm:
                if self.non_default_collection:
                    self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket",
                                                      self.docs_per_day * self.num_docs)
                else:
                    self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs)
            else:
                if self.non_default_collection:
                    self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", 0)
                else:
                    self.verify_doc_count_collections("src_bucket._default._default", 0)
        else:
            if self.non_default_collection:
                self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", 0)
            else:
                self.verify_doc_count_collections("src_bucket._default._default", 0)
        self.undeploy_and_delete_function(body)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_reboot_eventing_node_when_it_is_processing_mutations(self):
        gen_load_non_json_del = copy.deepcopy(self.gens_load)
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        if self.is_curl:
            body['depcfg']['curl'] = []
            body['depcfg']['curl'].append({"hostname": self.hostname, "value": "server", "auth_type": self.auth_type,
                                           "username": self.curl_username, "password": self.curl_password,"cookies": self.cookies})
            self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        # load some data
        if not self.is_expired:
            if self.non_default_collection:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                             wait_for_loading=False)
            else:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                             wait_for_loading=False)
        else:
            if self.non_default_collection:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                             expiry=150, wait_for_loading=False)
            else:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                             expiry=150, wait_for_loading=False)
        # pause handler
        if self.pause_resume:
            self.pause_function(body)
        # reboot eventing node when it is processing mutations
        self.reboot_server(eventing_node)
        # pause handler
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        if not self.cancel_timer:
            if self.is_sbm:
                if self.non_default_collection:
                    self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket",
                                                      self.docs_per_day * self.num_docs * 2)
                else:
                    self.verify_doc_count_collections("src_bucket._default._default",
                                                      self.docs_per_day * self.num_docs * 2)
            else:
                if self.non_default_collection:
                    self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket",
                                                      self.docs_per_day * self.num_docs)
                else:
                    self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs)
        # delete all documents
        if not self.is_expired:
            if self.non_default_collection:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                             is_delete=True, wait_for_loading=False)
            else:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                             is_delete=True, wait_for_loading=False)
        # pause handler
        if self.pause_resume:
            self.pause_function(body)
        # reboot eventing node when it is processing mutations
        self.reboot_server(eventing_node)
        # pause handler
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        if not self.cancel_timer:
            if self.is_sbm:
                if self.non_default_collection:
                    self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket",
                                                      self.docs_per_day * self.num_docs)
                else:
                    self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs)
            else:
                if self.non_default_collection:
                    self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", 0)
                else:
                    self.verify_doc_count_collections("src_bucket._default._default", 0)
        else:
            if self.non_default_collection:
                self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", 0)
            else:
                self.verify_doc_count_collections("src_bucket._default._default", 0)
        self.undeploy_and_delete_function(body)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_network_partitioning_eventing_node_when_its_processing_mutations(self):
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        if self.is_curl:
            body['depcfg']['curl'] = []
            body['depcfg']['curl'].append({"hostname": self.hostname, "value": "server", "auth_type": self.auth_type,
                                           "username": self.curl_username, "password": self.curl_password,"cookies": self.cookies})
            self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        try:
            # partition the eventing node when its processing mutations
            for i in range(5):
                self.start_firewall_on_node(eventing_node)
            # load some data
            if self.non_default_collection:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                             wait_for_loading=False)
            else:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                             wait_for_loading=False)
            self.sleep(180)
            self.stop_firewall_on_node(eventing_node)
            if self.is_sbm:
                if self.non_default_collection:
                    self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket",
                                                      self.docs_per_day * self.num_docs * 2)
                else:
                    self.verify_doc_count_collections("src_bucket._default._default",
                                                      self.docs_per_day * self.num_docs * 2)
            else:
                if self.non_default_collection:
                    self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket",
                                                      self.docs_per_day * self.num_docs)
                else:
                    self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs)
        except Exception:
            self.stop_firewall_on_node(eventing_node)
        finally:
            self.stop_firewall_on_node(eventing_node)
        # This is intentionally added
        self.sleep(60)
        # delete all documents
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                         is_delete=True, wait_for_loading=False)
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                         is_delete=True, wait_for_loading=False)
        # Wait for eventing to catch up with all the delete mutations and verify results
        if self.is_sbm:
            if self.non_default_collection:
                self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", self.docs_per_day * self.num_docs)
            else:
                self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs)
        else:
            if self.non_default_collection:
                self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", 0)
            else:
                self.verify_doc_count_collections("src_bucket._default._default", 0)
        self.undeploy_and_delete_function(body)

    def test_reboot_n1ql_node_when_eventing_node_is_querying(self):
        n1ql_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_INSERT_ON_UPDATE)
        if self.is_curl:
            body['depcfg']['curl'] = []
            body['depcfg']['curl'].append({"hostname": self.hostname, "value": "server", "auth_type": self.auth_type,
                                           "username": self.curl_username, "password": self.curl_password,"cookies": self.cookies})
            self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        # reboot eventing node when it is processing mutations
        self.reboot_server(n1ql_node)
        task.result()
        self.sleep(60)
        stats = self.rest.get_all_eventing_stats()
        on_update_failure = stats[0]["execution_stats"]["on_update_failure"]
        n1ql_op_exception_count = stats[0]["failure_stats"]["n1ql_op_exception_count"]
        self.undeploy_and_delete_function(body)
        log.info("stats : {0}".format(stats))
        if on_update_failure == 0 or n1ql_op_exception_count == 0:
            self.fail("No n1ql exceptions were found when n1ql node was rebooted while it was"
                      " processing queries from handler code or stats returned incorrect value")
        # intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(30)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_killing_erlang_on_n1ql_node_when_eventing_node_is_querying(self):
        n1ql_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_INSERT_ON_UPDATE)
        if self.is_curl:
            body['depcfg']['curl'] = []
            body['depcfg']['curl'].append({"hostname": self.hostname, "value": "server", "auth_type": self.auth_type,
                                           "username": self.curl_username, "password": self.curl_password,"cookies": self.cookies})
            self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        # load some data
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        # reboot eventing node when it is processing mutations
        self.kill_erlang_service(n1ql_node)
        stats = self.rest.get_all_eventing_stats()
        on_update_failure = stats[0]["execution_stats"]["on_update_failure"]
        n1ql_op_exception_count = stats[0]["failure_stats"]["n1ql_op_exception_count"]
        self.undeploy_and_delete_function(body)
        log.info("stats : {0}".format(stats))
        if on_update_failure == 0 or n1ql_op_exception_count == 0:
            self.fail("No n1ql exceptions were found when erlang process was killed on n1ql node while it was"
                      " processing queries from handler code or stats returned incorrect value")
        # intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(30)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_killing_memcached_on_n1ql_when_eventing_is_processing_mutations(self):
        n1ql_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_UPDATE_DELETE)
        if self.is_curl:
            body['depcfg']['curl'] = []
            body['depcfg']['curl'].append({"hostname": self.hostname, "value": "server", "auth_type": self.auth_type,
                                           "username": self.curl_username, "password": self.curl_password,"cookies": self.cookies})
            self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        # load some data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # kill memcached on n1ql and eventing when eventing is processing mutations
        for node in [n1ql_node, eventing_node]:
            self.kill_memcached_service(node)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.warmup_check()
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # kill memcached on n1ql and eventing when eventing is processing mutations
        for node in [n1ql_node, eventing_node]:
            self.kill_memcached_service(node)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        # intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(30)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_network_partitioning_eventing_node_with_n1ql_when_its_processing_mutations(self):
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_UPDATE_DELETE)
        if self.is_curl:
            body['depcfg']['curl'] = []
            body['depcfg']['curl'].append({"hostname": self.hostname, "value": "server", "auth_type": self.auth_type,
                                           "username": self.curl_username, "password": self.curl_password,"cookies": self.cookies})
            self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        try:
            # partition the eventing node when its processing mutations
            for i in range(5):
                self.start_firewall_on_node(eventing_node)
            # load some data
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            self.sleep(180)
            self.stop_firewall_on_node(eventing_node)
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        except Exception:
            self.stop_firewall_on_node(eventing_node)
        finally:
            self.stop_firewall_on_node(eventing_node)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_eventing_n1ql_in_different_time_zone(self):
        try:
            eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
            self.change_time_zone(eventing_node, timezone="Asia/Kolkata")
            kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
            self.change_time_zone(kv_node, timezone="America/Los_Angeles")
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            body = self.create_save_function_body(self.function_name, self.handler_code,
                                                  worker_count=3)
            if self.is_curl:
                body['depcfg']['curl'] = []
                body['depcfg']['curl'].append(
                    {"hostname": self.hostname, "value": "server", "auth_type": self.auth_type,
                     "username": self.curl_username, "password": self.curl_password, "cookies": self.cookies})
                self.rest.create_function(body['appname'], body, self.function_scope)
            self.deploy_function(body)
            # Wait for eventing to catch up with all the update mutations and verify results
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size, op_type='delete')
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
            self.undeploy_and_delete_function(body)
        finally:
            self.change_time_zone(eventing_node, timezone="UTC")
            self.change_time_zone(kv_node, timezone="UTC")

    def test_time_drift_between_kv_eventing(self):
        try:
            kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
            self.change_time_zone(kv_node, timezone="America/Chicago")
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            body = self.create_save_function_body(self.function_name, self.handler_code,
                                                  worker_count=3)
            if self.is_curl:
                body['depcfg']['curl'] = []
                body['depcfg']['curl'].append(
                    {"hostname": self.hostname, "value": "server", "auth_type": self.auth_type,
                     "username": self.curl_username, "password": self.curl_password, "cookies": self.cookies})
                self.rest.create_function(body['appname'], body, self.function_scope)
            self.deploy_function(body)
            # pause handler
            if self.pause_resume:
                self.pause_function(body)
                self.sleep(30)
                self.resume_function(body)
            # Wait for eventing to catch up with all the update mutations and verify results
            if self.is_sbm:
                self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 2,
                                             skip_stats_validation=True)
            else:
                self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
            # pause handler
            if self.pause_resume:
                self.pause_function(body)
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size, op_type='delete')
            if self.pause_resume:
                self.resume_function(body)
            if self.is_sbm:
                self.verify_eventing_results(self.function_name, self.docs_per_day * 2016,
                                             skip_stats_validation=True)
            else:
                self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
            self.undeploy_and_delete_function(body)
        finally:
            self.change_time_zone(kv_node, timezone="America/Los_Angeles")

    def test_partial_rollback(self):
        kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        log.info("kv nodes:{0}".format(kv_node))
        for node in kv_node:
            mem_client = MemcachedClientHelper.direct_client(node, self.src_bucket_name)
            mem_client.stop_persistence()
        body = self.create_save_function_body(self.function_name, self.handler_code,
                                              worker_count=3)
        if self.is_curl:
            body['depcfg']['curl'] = []
            body['depcfg']['curl'].append({"hostname": self.hostname, "value": "server", "auth_type": self.auth_type,
                                           "username": self.curl_username, "password": self.curl_password,"cookies": self.cookies})
            self.rest.create_function(body['appname'], body, self.function_scope)
        try:
            task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                    self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        except Exception as e:
            log.info("error while loading data")
        self.deploy_function(body, wait_for_bootstrap=False)
        # Kill memcached on Node A
        self.log.info("Killing memcached on {0}".format(kv_node[1]))
        shell = RemoteMachineShellConnection(kv_node[1])
        shell.kill_memcached()
        # Start persistence on Node B
        self.log.info("Starting persistence on {0}".
                      format(kv_node[0]))
        mem_client = MemcachedClientHelper.direct_client(kv_node[0],
                                                         self.src_bucket_name)
        mem_client.start_persistence()
        self.wait_for_handler_state(body['appname'], "deployed")
        stats_src = RestConnection(self.master).get_bucket_stats(bucket=self.src_bucket_name)
        log.info(stats_src)
        self.verify_eventing_results(self.function_name, stats_src["curr_items"], skip_stats_validation=True)

    def test_partial_rollback_pause_resume(self):
        kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        log.info("kv nodes:{0}".format(kv_node))
        for node in kv_node:
            mem_client = MemcachedClientHelper.direct_client(node, self.src_bucket_name)
            mem_client.stop_persistence()
        body = self.create_save_function_body(self.function_name, self.handler_code,
                                              worker_count=3)
        if self.is_curl:
            body['depcfg']['curl'] = []
            body['depcfg']['curl'].append({"hostname": self.hostname, "value": "server", "auth_type": self.auth_type,
                                           "username": self.curl_username, "password": self.curl_password,"cookies": self.cookies})
            self.rest.create_function(body['appname'], body, self.function_scope)
        try:
            task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                    self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        except Exception as e:
            log.info("error while loading data")
        self.deploy_function(body)
        if self.pause_resume:
            self.pause_function(body)
        # Kill memcached on Node A
        self.log.info("Killing memcached on {0}".format(kv_node[1]))
        shell = RemoteMachineShellConnection(kv_node[1])
        shell.kill_memcached()

        # Start persistence on Node B
        self.log.info("Starting persistence on {0}".
                      format(kv_node[0]))
        mem_client = MemcachedClientHelper.direct_client(kv_node[0],
                                                         self.src_bucket_name)
        mem_client.start_persistence()
        if self.pause_resume:
            self.wait_for_handler_state(body['appname'], "paused")
            self.resume_function(body)
        else:
            self.wait_for_handler_state(body['appname'], "deployed")
        stats_src = RestConnection(self.master).get_bucket_stats(bucket=self.src_bucket_name)
        log.info(stats_src)
        self.verify_eventing_results(self.function_name, stats_src["curr_items"], skip_stats_validation=True)

    def test_time_drift_between_eventing_nodes(self):
        try:
            ev_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
            self.change_time_zone(ev_node, timezone="America/Chicago")
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            body = self.create_save_function_body(self.function_name, self.handler_code,
                                                  worker_count=3)
            if self.is_curl:
                body['depcfg']['curl'] = []
                body['depcfg']['curl'].append(
                    {"hostname": self.hostname, "value": "server", "auth_type": self.auth_type,
                     "username": self.curl_username, "password": self.curl_password, "cookies": self.cookies})
                self.rest.create_function(body['appname'], body, self.function_scope)
            self.deploy_function(body)
            # pause handler
            if self.pause_resume:
                self.pause_function(body)
                self.sleep(30)
                self.resume_function(body)
            # Wait for eventing to catch up with all the update mutations and verify results
            if self.is_sbm:
                self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 2,
                                             skip_stats_validation=True)
            else:
                self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
            # pause handler
            if self.pause_resume:
                self.pause_function(body)
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size, op_type='delete')
            # resume handler
            if self.pause_resume:
                self.resume_function(body)
            if self.is_sbm:
                self.verify_eventing_results(self.function_name, self.docs_per_day * 2016,
                                             skip_stats_validation=True)
            else:
                self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
            self.undeploy_and_delete_function(body)
        finally:
            self.change_time_zone(ev_node, timezone="America/Los_Angeles")

    def warmup_check(self, timeout=1800):
        warmup=False
        i =0
        self.sleep(10)
        while warmup == False and i < 20 :
            task=self.rest.get_warming_up_tasks()
            self.sleep(10)
            if len(task) ==0:
                warmup= True
            i+=1
        if i >= 20 and len(task) !=0:
            raise Exception("Bucket won't warm up in expected time")


    def test_eventing_rebalance_in_kill_eventing_consumer(self):
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        sock_batch_size = self.input.param('sock_batch_size', 1)
        worker_count = self.input.param('worker_count', 3)
        cpp_worker_thread_count = self.input.param('cpp_worker_thread_count', 1)
        body = self.create_save_function_body(self.function_name, self.handler_code,
                                              sock_batch_size=sock_batch_size, worker_count=worker_count,
                                              cpp_worker_thread_count=cpp_worker_thread_count)
        if self.is_curl:
            body['depcfg']['curl'] = []
            body['depcfg']['curl'].append({"hostname": self.hostname, "value": "server", "auth_type": self.auth_type,
                                           "username": self.curl_username, "password": self.curl_password,"cookies": self.cookies})
            self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        # load data
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                         wait_for_loading=False)
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                         wait_for_loading=False)
        if self.pause_resume:
            self.pause_function(body)
        # rebalance in a eventing node when eventing is processing mutations
        services_in = ["eventing"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        self.sleep(5)
        reached = RestHelper(self.rest).rebalance_reached(percentage=60)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        # kill eventing consumer when eventing is processing mutations
        self.kill_consumer(eventing_node)
        if self.pause_resume:
            self.wait_for_handler_state(body['appname'], "paused")
        else:
            self.wait_for_handler_state(body['appname'], "deployed")
        rebalance.result()
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        if self.is_sbm:
            if self.non_default_collection:
                self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket",
                                                  self.docs_per_day * self.num_docs * 2)
            else:
                self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs * 2)
        else:
            if self.non_default_collection:
                self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", self.docs_per_day * self.num_docs)
            else:
                self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs)
        # delete json documents
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                         is_delete=True, wait_for_loading=False)
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                         is_delete=True, wait_for_loading=False)
        if self.pause_resume:
            self.pause_function(body)
            self.sleep(30)
            self.resume_function(body)
        # kill eventing consumer when eventing is processing mutations
        self.kill_consumer(eventing_node)
        self.wait_for_handler_state(body['appname'], "deployed")
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        if self.is_sbm:
            if self.non_default_collection:
                self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", self.docs_per_day * self.num_docs)
            else:
                self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs)
        else:
            if self.non_default_collection:
                self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", 0)
            else:
                self.verify_doc_count_collections("src_bucket._default._default", 0)
        self.undeploy_and_delete_function(body)
        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        # rebalance out all eventing nodes
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], nodes_out_list)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

    def test_eventing_rebalance_in_kill_eventing_producer(self):
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        sock_batch_size = self.input.param('sock_batch_size', 1)
        worker_count = self.input.param('worker_count', 3)
        cpp_worker_thread_count = self.input.param('cpp_worker_thread_count', 1)
        body = self.create_save_function_body(self.function_name, self.handler_code,
                                              sock_batch_size=sock_batch_size, worker_count=worker_count,
                                              cpp_worker_thread_count=cpp_worker_thread_count)
        if self.is_curl:
            body['depcfg']['curl'] = []
            body['depcfg']['curl'].append({"hostname": self.hostname, "value": "server", "auth_type": self.auth_type,
                                           "username": self.curl_username, "password": self.curl_password,"cookies": self.cookies})
            self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        # load data
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                         wait_for_loading=False)
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                         wait_for_loading=False)
        if self.pause_resume:
            self.pause_function(body)
        try:
            # rebalance in a eventing node when eventing is processing mutations
            services_in = ["eventing"]
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                     services=services_in)
            reached = RestHelper(self.rest).rebalance_reached(percentage=30)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            # kill eventing producer when eventing is processing mutations
            self.wait_for_failover_or_rebalance()
            self.kill_producer(eventing_node)
            if self.pause_resume:
                self.wait_for_handler_state(body['appname'], "paused")
                self.resume_function(body)
            else:
                self.wait_for_handler_state(body['appname'], "deployed")
            rebalance.result()
        except Exception as ex:
            log.info("Rebalance failed as expected after eventing got killed: {0}".format(str(ex)))
        else:
            self.fail("Rebalance succeeded even after killing eventing processes")
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        if self.is_sbm:
            if self.non_default_collection:
                self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket",
                                                  self.docs_per_day * self.num_docs * 2)
            else:
                self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs * 2)
        else:
            if self.non_default_collection:
                self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", self.docs_per_day * self.num_docs)
            else:
                self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs)
        # retry the failed rebalance
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], [])
        self.sleep(30)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "retry of the failed rebalance failed, stuck or did not complete")
        rebalance.result()
        # delete json documents
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                         is_delete=True, wait_for_loading=False)
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                         is_delete=True, wait_for_loading=False)
        if self.pause_resume:
            self.pause_function(body)
            self.sleep(30)
        # kill eventing producer when eventing is processing mutations
        self.kill_producer(eventing_node)
        self.sleep(120)
        if self.pause_resume:
            self.wait_for_handler_state(body['appname'], "paused")
            self.resume_function(body)
        else:
            self.wait_for_handler_state(body['appname'], "deployed")
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        if self.is_sbm:
            if self.non_default_collection:
                self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", self.docs_per_day * self.num_docs)
            else:
                self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs)
        else:
            if self.non_default_collection:
                self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", 0)
            else:
                self.verify_doc_count_collections("src_bucket._default._default", 0)
        self.undeploy_and_delete_function(body)
        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        # rebalance out all eventing nodes
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], nodes_out_list)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

    def test_auto_retry_of_failed_rebalance_when_producer_killed(self):
        gen_load_del = copy.deepcopy(self.gens_load)
        self.auto_retry_setup()
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        sock_batch_size = self.input.param('sock_batch_size', 1)
        worker_count = self.input.param('worker_count', 3)
        cpp_worker_thread_count = self.input.param('cpp_worker_thread_count', 1)
        body = self.create_save_function_body(self.function_name, self.handler_code, sock_batch_size=sock_batch_size,
                                              worker_count=worker_count,
                                              cpp_worker_thread_count=cpp_worker_thread_count)
        if self.is_curl:
            body['depcfg']['curl'] = []
            body['depcfg']['curl'].append({"hostname": self.hostname, "value": "server", "auth_type": self.auth_type,
                                           "username": self.curl_username, "password": self.curl_password,
                                           "cookies": self.cookies})
            self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        if self.pause_resume:
            self.pause_function(body)
        # load some data
        if self.non_default_collection:
            task=self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                         wait_for_loading=False)
        else:
            task=self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                         wait_for_loading=False)
        self.sleep(10)
        try:
            # rebalance in a eventing node when eventing is processing mutations
            services_in = ["eventing"]
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]],
                                                     [], services=services_in)
            reached = RestHelper(self.rest).rebalance_reached(percentage=60)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            self.wait_for_failover_or_rebalance()
            # kill eventing producer when eventing is processing mutations
            self.kill_producer(eventing_node)
            if self.pause_resume:
                self.wait_for_handler_state(body['appname'], "paused")
                self.resume_function(body)
            else:
                self.wait_for_handler_state(body['appname'], "deployed")
            rebalance.result()
        except Exception as ex:
            log.info("Rebalance failed as expected after eventing got killed: {0}".format(str(ex)))
            # auto retry the failed rebalance
            self.check_retry_rebalance_succeeded()
        else:
            self.fail("Rebalance succeeded even after killing eventing processes")
        if self.pause_resume:
            self.resume_function(body)
        task.result()
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        if self.is_sbm:
            if self.non_default_collection:
                self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket",
                                                  self.docs_per_day * self.num_docs * 2)
            else:
                self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs * 2)
        else:
            if self.non_default_collection:
                self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", self.docs_per_day * self.num_docs)
            else:
                self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs)
        # delete json documents
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                         is_delete=True, wait_for_loading=False)
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                         is_delete=True, wait_for_loading=False)
        if self.pause_resume:
            self.pause_function(body)
            self.sleep(30)
        # kill eventing producer when eventing is processing mutations
        self.kill_producer(eventing_node)
        self.sleep(120)
        if self.pause_resume:
            self.wait_for_handler_state(body['appname'], "paused")
            self.resume_function(body)
        else:
            self.wait_for_handler_state(body['appname'], "deployed")
        # Wait for eventing to catch up with all the delete mutations and verify results
        # This is required to ensure eventing works after rebalance goes through successfully
        if self.is_sbm:
            if self.non_default_collection:
                self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", self.docs_per_day * self.num_docs)
            else:
                self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs)
        else:
            if self.non_default_collection:
                self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", 0)
            else:
                self.verify_doc_count_collections("src_bucket._default._default", 0)
        self.undeploy_and_delete_function(body)
        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        # rebalance out all eventing nodes
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], nodes_out_list)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

    def test_killing_eventing_consumer_for_dcp_stream_boundary_from_now(self):
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, self.handler_code, dcp_stream_boundary="from_now")
        # load some data
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        if self.non_default_collection:
            self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", 0)
        else:
            self.verify_doc_count_collections("dst_bucket._default._default", 0)
        # load some more data
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        # kill eventing consumer when eventing is processing mutations
        self.kill_consumer(eventing_node)
        self.wait_for_handler_state(body['appname'], "deployed")
        if self.non_default_collection:
            self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs)
        else:
            self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * self.num_docs)
        # delete all documents
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                         is_delete=True, wait_for_loading=False)
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                         is_delete=True, wait_for_loading=False)
        # Wait for eventing to catch up with all the delete mutations and verify results
        if self.non_default_collection:
            self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", 0)
        else:
            self.verify_doc_count_collections("src_bucket._default._default", 0)
        self.undeploy_and_delete_function(body)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_is_balanced_after_stopping_couchbase_server(self):
        # Setup JWT configuration if enabled
        jwt_token = self.setup_jwt_config() if self.jwt_auth else None

        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        if len(nodes_out_list)<2:
            self.fail("Need two or more eventing nodes")
        # FTS setup if using FTS handler
        if getattr(self, 'is_fts', False):
            self.load_sample_buckets(self.server, "travel-sample")
            plan_params = {"indexPartitions": 1, "numReplicas": 0}
            fts_index = self.fts_callable.create_default_index(
                index_name=self.fts_index_name,
                bucket_name="travel-sample",
                plan_params=plan_params
            )
            self.fts_callable.wait_for_indexing_complete(item_count=self.fts_doc_count, idx=fts_index)
            self.sleep(30, "Waiting for FTS indexing to complete")
        body = self.create_save_function_body(self.function_name,self.handler_code, jwt_token=jwt_token)
        # load some data
        if not getattr(self, 'is_analytics', False):
            if self.non_default_collection:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
            else:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        self.deploy_function(body, jwt_token=jwt_token)
        # Wait for eventing to catch up with all the update mutations and verify results
        if not getattr(self, 'is_analytics', False):
            if self.non_default_collection:
                self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs)
            else:
                self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * self.num_docs)
        for node in nodes_out_list:
            self.stop_server(node,True)
        self.log.info("Couchbase stopped on all eventing nodes")
        for node in nodes_out_list:
            self.start_server(node)
        for node in nodes_out_list:
            rest_conn = RestConnection(node)
            json_response = rest_conn.cluster_status()
            self.log.info("Pools Default Statistics: {0}".format(json_response))
            is_balanced=json_response['balanced']
            if not is_balanced:
                servicesNeedRebalance=json_response['servicesNeedRebalance'][0]['services']
                self.assertFalse('eventing' in servicesNeedRebalance,
                        msg="Eventing Nodes are not balanced, need rebalance after starting couchbase server."  )
        # Run FTS validation if FTS handler is being used
        self.run_fts_validation()
        # Run analytics validation if analytics handler is being used
        if getattr(self, 'is_analytics', False):
            self.sleep(10, "Waiting for eventing to reprocess after server restart")
            self.verify_doc_count_collections("default.scope0.collection1", 1)
            self._verify_analytics_result_matches_direct_query()
            self.undeploy_and_delete_function(body, jwt_token=jwt_token)
            return

    def test_checkpointing_failure_by_cursor_aware_functions(self):
        body = self.create_save_function_body(self.function_name, self.handler_code, cursor_aware=True)
        self.deploy_function(body)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        nodes_kv_list = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        nodes_eventing_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        if len(nodes_kv_list) < 2:
            self.fail("Need two or more kv nodes")
        self.stop_server(nodes_kv_list[1], True)
        self.log.info("Stopping server for KV node for 5 mins")
        time.sleep(300)
        self.start_server(nodes_kv_list[1])
        actual_count = self.stat.get_collection_item_count_cumulative("dst_bucket", "_default",
                                                                      "_default", self.get_kv_nodes())
        if actual_count != (self.docs_per_day * self.num_docs):
            rest_conn = RestConnection(nodes_eventing_list[0])
            failure_list = rest_conn.get_event_failure_stats(self.function_name)
            if "dcp_delete_checkpoint_failure" in failure_list and "dcp_mutation_checkpoint_failure" in failure_list:
                dcp_delete_checkpoint_failure = failure_list["dcp_delete_checkpoint_failure"]
                dcp_mutation_checkpoint_failure = failure_list["dcp_mutation_checkpoint_failure"]
                if dcp_delete_checkpoint_failure == 0 and dcp_mutation_checkpoint_failure == 0:
                    self.fail("DCP Delete Checkpoint failure detected in cursor aware function")
                else:
                    self.log.info("Checkpointing Failure reflected on Statistics: dcp_delete_checkpoint_failure:{0}, dcp_mutation_checkpoint_failure:{1}".format(dcp_delete_checkpoint_failure, dcp_mutation_checkpoint_failure))
            else:
                self.log.info("Endpoints for dcp_delete_checkpoint_failure and dcp_mutation_checkpoint_failure are not present in the statistics.")
        else:
            self.log.info("Document Count Matched: {0} Expected: {1}".format(actual_count,self.docs_per_day * self.num_docs))
        self.undeploy_and_delete_function(body)

    def test_killing_eventing_consumer_when_eventing_ondeploy_is_processing_mutations(self):
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE_ONDEPLOY.ONDEPLOY_BASIC_BUCKET_OP)
        if self.is_curl:
            body['depcfg']['curl'] = []
            body['depcfg']['curl'].append({"hostname": self.hostname, "value": "server", "auth_type": self.auth_type,
                                           "username": self.curl_username, "password": self.curl_password,"cookies": self.cookies})
            self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        # kill eventing consumer
        self.kill_consumer(eventing_node)
        self.sleep(10)
        self.wait_for_handler_state(body['appname'], "deployed")
        self.verify_doc_count_collections("default.scope0.collection1", 1)
        self.undeploy_and_delete_function(body)

    #MB-66217 
    def test_is_balanced_after_restarting_eventing_producer(self):
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        self.deploy_function(body)
        if self.non_default_collection:
            self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs)
        else:
            self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * self.num_docs)
        self.kill_producer(eventing_node)
        self.sleep(120)
        if self.pause_resume:
            self.wait_for_handler_state(body['appname'], "paused")
            self.resume_function(body)
        else:
            self.wait_for_handler_state(body['appname'], "deployed")
        if self.non_default_collection:
            self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs)
        else:
            self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * self.num_docs)
        rest_conn = RestConnection(eventing_node)
        json_response = rest_conn.cluster_status()
        self.log.info("Pools Default Statistics: {0}".format(json_response))
        is_balanced=json_response['balanced']
        if not is_balanced:
            servicesNeedRebalance=json_response['servicesNeedRebalance'][0]['services']
            self.assertFalse('eventing' in servicesNeedRebalance,
                    msg="Eventing Nodes are not balanced after restarting eventing producer, need rebalance."  )

    def test_ondeploy_after_stopping_couchbase_server(self):
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name,self.handler_code)
        self.deploy_function(body)
        self.verify_doc_count_collections("dst_bucket._default._default", 1)
        self.stop_server(eventing_node,True)
        self.log.info("Couchbase stopped on the eventing node")
        self.start_server(eventing_node)
        rest_conn = RestConnection(eventing_node)
        json_response = rest_conn.cluster_status()
        self.log.info("Pools Default Statistics: {0}".format(json_response))
        self.sleep(10)
        self.wait_for_handler_state(body['appname'], "deployed")
        self.undeploy_and_delete_function(body)

    def test_ondeploy_after_stopping_couchbase_server_while_resume_is_triggered(self):
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name,self.handler_code)
        self.deploy_function(body)
        self.verify_doc_count_collections("dst_bucket._default._default", 1)
        self.pause_function(body)
        self.resume_function(body, wait_for_resume=False)
        #Node killed during the "resuming..." state
        self.stop_server(eventing_node, True)
        self.log.info("Couchbase stopped on the eventing node")
        self.start_server(eventing_node)
        rest_conn = RestConnection (eventing_node)
        json_response = rest_conn.cluster_status()
        self.log.info("Pools Default Statistics: {0}".format(json_response))
        self.sleep(10)
        #Function goes back to the "resuming..." state which is the previous state and then gets deployed
        self.wait_for_handler_state(body ['appname'], "deployed")
        self.undeploy_and_delete_function(body)

    def construct_fts_query(self, index_name, query):
        """Helper method to construct FTS query for validation"""
        fts_query = {
            "indexName": index_name,
            "query": query,
            "size": 10,
            "from": 0,
            "explain": False,
            "fields": [],
            "ctl": {
                "consistency": {
                    "level": "",
                    "vectors": {}
                }
            }
        }
        return fts_query

    def run_fts_validation(self):
        """Run FTS query validation to ensure FTS is working correctly"""
        if not getattr(self, 'is_fts', False):
            return

        log.info("Running FTS validation...")
        bucket = "travel-sample"
        scope = "_default"

        fts_query = self.construct_fts_query(
            self.fts_index_name,
            self.fts_query['query']
        )

        hits, _, _, _ = self.fts_callable.run_fts_query(
            index_name=self.fts_index_name,
            query_dict=fts_query,
            bucket_name=bucket,
            scope_name=scope,
            node=self.get_nodes_from_services_map(service_type="fts", get_all_nodes=False)
        )

        log.info("FTS query returned %s hits, expected %s" % (hits, self.fts_query['expected_hits']))
        self.assertEqual(
            hits,
            self.fts_query['expected_hits'],
            "FTS query should return %s hits, got %s" % (
                self.fts_query['expected_hits'],
                hits
            )
        )
        log.info("FTS validation passed successfully")

    def _setup_analytics(self):
        """
        Create analytics dataverse + collection on travel-sample airline
        data, connect the local link, and wait for data ingestion.
        """
        cbas_node = self.get_nodes_from_services_map(service_type="cbas")
        cbas_rest = RestConnection(cbas_node)
        cbas_rest.execute_statement_on_cbas("CREATE DATAVERSE `travel-sample`.`inventory`", None)
        cbas_rest.execute_statement_on_cbas("CREATE ANALYTICS COLLECTION `travel-sample`.`inventory`.`airline` ON `travel-sample`.`inventory`.`airline`", None)
        cbas_rest.execute_statement_on_cbas("CONNECT LINK Local", None)
        self.sleep(15, "Waiting for analytics to ingest travel-sample data")
        # Verify analytics returns data before deploying any handler
        result = cbas_rest.execute_statement_on_cbas("SELECT COUNT(*) AS cnt FROM `travel-sample`.`inventory`.`airline`", None)
        if isinstance(result, bytes):
            result = result.decode("utf-8")
        parsed = json.loads(result)
        count = parsed["results"][0]["cnt"]
        log.info("Analytics airline collection has %s docs" % count)
        self.assertTrue(count > 0, "Analytics airline collection has 0 docs — ingestion failed")

    def _verify_analytics_result_matches_direct_query(self):
        """
        Run the analytics basic select query directly via the CBAS REST API,
        then read the doc written by the eventing handler from collection1 via N1QL,
        and compare the results.
        """
        analytics_query = "SELECT name, country FROM `travel-sample`.`inventory`.`airline` LIMIT 5"
        data_field = 'data'

        # Run the analytics query directly via CBAS REST
        cbas_node = self.get_nodes_from_services_map(service_type="cbas")
        cbas_rest = RestConnection(cbas_node)
        direct_result = cbas_rest.execute_statement_on_cbas(analytics_query, None)
        if isinstance(direct_result, bytes):
            direct_result = direct_result.decode("utf-8")
        direct_parsed = json.loads(direct_result)
        direct_rows = direct_parsed["results"]
        log.info("Direct analytics query returned %s rows" % len(direct_rows))

        # Read the eventing-written doc from collection1 via N1QL
        try:
            self.n1ql_helper.run_cbq_query(
                query="CREATE PRIMARY INDEX IF NOT EXISTS ON default.scope0.collection1",
                server=self.n1ql_server)
        except Exception as e:
            log.info("Index creation note: %s" % str(e))

        n1ql_result = self.n1ql_helper.run_cbq_query(
            query="SELECT * FROM default.scope0.collection1",
            server=self.n1ql_server)
        self.assertTrue(len(n1ql_result["results"]) > 0, "No docs found in collection1 written by eventing handler")

        eventing_doc = n1ql_result["results"][0]["collection1"]
        eventing_data = eventing_doc.get(data_field, [])
        log.info("Eventing handler wrote %s rows in field '%s'" % (len(eventing_data), data_field))

        # Sort both result sets for deterministic comparison
        def sort_key(row):
            return json.dumps(row, sort_keys=True)

        direct_sorted = sorted(direct_rows, key=sort_key)
        eventing_sorted = sorted(eventing_data, key=sort_key)

        self.assertEqual(
            len(eventing_sorted), len(direct_sorted),
            "Row count mismatch: eventing=%d, direct=%d" % (len(eventing_sorted), len(direct_sorted)))

        for i, (ev_row, direct_row) in enumerate(zip(eventing_sorted, direct_sorted)):
            self.assertEqual(
                ev_row, direct_row,
                "Row %d mismatch:\n  eventing: %s\n  direct:   %s" % (i, ev_row, direct_row))

        log.info("Analytics result verification PASSED (%d rows match)" % len(direct_sorted))

    def setup_jwt_config(self):
        if not self.jwt_auth:
            return None

        log.info("Setting up JWT configuration")

        # Generate key pair
        log.info(f"Generating {self.jwt_algorithm} key pair")
        private_key, public_key = self.jwt_utils.generate_key_pair(self.jwt_algorithm, key_size=2048)
        self.private_key = private_key
        self.public_key = public_key

        # Create group with eventing permissions
        log.info(f"Creating group {self.jwt_group} with eventing permissions")
        self.create_jwt_group()

        # Create external user
        log.info(f"Creating external user {self.jwt_user}")
        self.create_jwt_user()

        # Configure JWT on cluster
        log.info("Configuring JWT on cluster")
        self.configure_jwt_on_cluster(public_key)

        # Generate JWT token
        log.info("Generating JWT token")
        self.jwt_token = self.jwt_utils.create_token(
            issuer_name=self.jwt_issuer,
            user_name=self.jwt_user,
            algorithm=self.jwt_algorithm,
            private_key=private_key,
            token_audience=[self.jwt_audience],
            user_groups=[self.jwt_group],
            ttl=self.jwt_ttl
        )
        log.info("JWT token generated successfully")

        return self.jwt_token

    def create_jwt_group(self):
        """Create a group with eventing permissions"""
        status, content = self.rest.add_group_role(
            group_name=self.jwt_group,
            description="Group for testing JWT permissions in recovery tests",
            roles=self.jwt_roles
        )
        if not status:
            raise Exception(f"Failed to create group {self.jwt_group}: {content}")
        log.info(f"Group {self.jwt_group} created successfully")

    def create_jwt_user(self):
        """Create external user and assign to group"""
        payload = urllib.parse.urlencode({
            "name": self.jwt_user,
            "groups": self.jwt_group
        })
        _ = self.rest.add_external_user(self.jwt_user, payload)
        log.info(f"External user {self.jwt_user} created successfully")

    def configure_jwt_on_cluster(self, public_key):
        """Configure JWT authentication on cluster"""
        jwt_config = self.jwt_utils.get_jwt_config(
            issuer_name=self.jwt_issuer,
            algorithm=self.jwt_algorithm,
            pub_key=public_key,
            token_audience=[self.jwt_audience],
            token_group_matching_rule=[f"^.{self.jwt_user}$ {self.jwt_group}"],
            jit_provisioning=self.jit_provisioning
        )
        status, content, _ = self.rest.create_jwt_with_config(jwt_config)
        if not status:
            raise Exception(f"Failed to configure JWT: {content}")
        log.info("JWT configured successfully")