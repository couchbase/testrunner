import copy
import json
import sys
import traceback

from lib.membase.api.rest_client import RestHelper
from lib.couchbase_helper.tuq_helper import N1QLHelper
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from lib.memcached.helper.data_helper import MemcachedClientHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from pytests.eventing.eventing_constants import HANDLER_CODE, HANDLER_CODE_CURL
from pytests.eventing.eventing_base import EventingBaseTest
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
        else:
            self.handler_code = "handler_code/ABO/insert_recovery.js"
        if self.is_expired:
            # set expiry pager interval
            ClusterOperationHelper.flushctl_set(self.master, "exp_pager_stime", 60, bucket=self.src_bucket_name)

    def tearDown(self):
        super(EventingRecovery, self).tearDown()

    def test_killing_eventing_consumer_when_eventing_is_processing_mutations(self):
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        if self.is_curl:
            body['depcfg']['curl'] = []
            body['depcfg']['curl'].append({"hostname": self.hostname, "value": "server", "auth_type": self.auth_type,
                                           "username": self.curl_username, "password": self.curl_password,"cookies": self.cookies})
            self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        if self.pause_resume:
            self.pause_function(body)
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
            self.resume_function(body)
        # kill eventing consumer when eventing is processing mutations
        self.kill_consumer(eventing_node)
        self.wait_for_handler_state(body['appname'], "deployed")
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
            self.pause_function(body)
        # delete all documents
        if not self.is_expired:
            if self.non_default_collection:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                             is_delete=True,wait_for_loading=False)
            else:
                self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                             is_delete=True,wait_for_loading=False)
        if self.pause_resume:
            self.resume_function(body)
        # kill eventing consumer when eventing is processing mutations
        self.kill_consumer(eventing_node)
        self.wait_for_handler_state(body['appname'], "deployed")
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
        self.undeploy_and_delete_function(body)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_killing_eventing_producer_when_eventing_is_processing_mutations(self):
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        if self.is_curl:
            body['depcfg']['curl'] = []
            body['depcfg']['curl'].append({"hostname": self.hostname, "value": "server", "auth_type": self.auth_type,
                                           "username": self.curl_username, "password": self.curl_password,"cookies": self.cookies})
            self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        #pause handler
        if self.pause_resume:
            self.pause_function(body)
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
            self.resume_function(body)
        else:
            self.wait_for_handler_state(body['appname'], "deployed")
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
            self.pause_function(body)
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
            self.resume_function(body)
        else:
            self.wait_for_handler_state(body['appname'], "deployed")
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
        self.undeploy_and_delete_function(body)
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
        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        if len(nodes_out_list)<2:
            self.fail("Need two or more eventing nodes")
        body = self.create_save_function_body(self.function_name, self.handler_code, dcp_stream_boundary="from_now")
        # load some data
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        if self.non_default_collection:
            self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs)
        else:
            self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * self.num_docs)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], nodes_out_list)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        for node in nodes_out_list:
            self.stop_server(node,True)
        self.log.info("Couchbase stopped on all eventing nodes")
        for node in nodes_out_list:
            self.start_server(node)
        for node in nodes_out_list:
            rest_conn = RestConnection(node)
            json_response = rest_conn.cluster_status()
            is_balanced=json_response['balanced']
            servicesNeedRebalance=json_response['servicesNeedRebalance']['services']
            self.assertTrue(is_balanced,
                        msg="Nodes are not balanced, need rebalance after starting couchbase server. Services requiring rebalance: "+ ','.join(servicesNeedRebalance))

          