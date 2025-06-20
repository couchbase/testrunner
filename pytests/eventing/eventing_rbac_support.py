from lib.couchbase_helper.stats_tools import StatsCommon
from lib.couchbase_helper.tuq_helper import N1QLHelper
from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from lib.couchbase_helper.documentgenerator import JSONNonDocGenerator, BlobGenerator
from pytests.eventing.eventing_constants import HANDLER_CODE, HANDLER_CODE_CURL
from pytests.eventing.eventing_base import EventingBaseTest
from pytests.security.rbac_base import RbacBase
from pytests.security.rbacmain import rbacmain
import logging
import json

log = logging.getLogger()


class EventingRBACSupport(EventingBaseTest):
    def setUp(self):
        super(EventingRBACSupport, self).setUp()
        self.users = self.input.param('users', None)
        if self.users:
            list_of_users = eval(eval(self.users))
            for user in list_of_users:
                u = [{'id': user['id'], 'password': user['password'], 'name': user['name']}]
                RbacBase().create_user_source(u, 'builtin', self.master)
                user_role_list = [{'id': user['id'], 'name': user['name'], 'roles': user['roles']}]
                RbacBase().add_user_role(user_role_list, self.rest, 'builtin')
            status, content, header = rbacmain(self.master)._retrieve_user_roles()
            self.log.info(json.loads(content))
        handler_code = self.input.param('handler_code', 'bucket_op')
        if handler_code == 'bucket_op':
            self.handler_code = "handler_code/ABO/insert_rebalance.js"
        elif handler_code == 'bucket_op_with_timers':
            self.handler_code = HANDLER_CODE.BUCKET_OPS_WITH_TIMERS
        elif handler_code == 'bucket_op_with_cron_timers':
            self.handler_code = "handler_code/ABO/insert_timer.js"
        elif handler_code == 'n1ql_op_with_timers':
            self.handler_code = HANDLER_CODE.N1QL_OPS_WITH_TIMERS
        elif handler_code == 'n1ql_op_without_timers':
            self.handler_code = HANDLER_CODE.N1QL_OPS_WITHOUT_TIMERS
        elif handler_code == 'source_bucket_mutation':
            self.handler_code = "handler_code/ABO/insert_sbm.js"
        elif handler_code == 'source_bucket_mutation_delete':
            self.handler_code = HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_DELETE
        elif handler_code == 'bucket_op_curl_get':
            self.handler_code = HANDLER_CODE_CURL.BUCKET_OP_WITH_CURL_GET
        elif handler_code == 'bucket_op_curl_post':
            self.handler_code = HANDLER_CODE_CURL.BUCKET_OP_WITH_CURL_POST
        elif handler_code == 'bucket_op_curl_put':
            self.handler_code = HANDLER_CODE_CURL.BUCKET_OP_WITH_CURL_PUT
        elif handler_code == 'bucket_op_curl_delete':
            self.handler_code = HANDLER_CODE_CURL.BUCKET_OP_WITH_CURL_DELETE
        elif handler_code == 'cancel_timer':
            self.handler_code = HANDLER_CODE.CANCEL_TIMER_REBALANCE
        elif handler_code == 'bucket_op_expired':
            self.handler_code = HANDLER_CODE.BUCKET_OP_EXPIRED
        elif handler_code == 'advance_bucket_op_auth_failure':
            self.handler_code = "handler_code/ABO/advance_bucket_op_auth_failure.js"
        elif handler_code == 'n1ql_op_auth_failure':
            self.handler_code = "handler_code/n1ql_op_auth_failure.js"

    def tearDown(self):
        super(EventingRBACSupport, self).tearDown()

    def test_to_check_user_with_sufficient_privileges_can_create_read_update_delete_eventing_functions(self):
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/insert_rebalance.js",
                                              username="john", password="asdasd")
        body['settings']['dcp_stream_boundary'] = "from_now"
        self.rest.update_function(self.function_name, body, username="john", password="asdasd")
        function_details = self.rest.get_function_details(self.function_name, self.function_scope, username="john", password="asdasd")
        body1 = json.loads(function_details)
        assert body1['settings']['dcp_stream_boundary'] == "from_now", "Function settings did not get updated."
        self.delete_function(body, username="john", password="asdasd")

    def test_to_check_user_with_insufficient_privileges_cannot_create_read_update_delete_eventing_functions(self):
        try:
            self.create_save_function_body(self.function_name, "handler_code/ABO/insert_rebalance.js",
                                           username="john", password="asdasd")
        except Exception as e:
            assert "ERR_FORBIDDEN" in str(e), True
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/insert_rebalance.js")
        try:
            self.rest.get_function_details(self.function_name, self.function_scope, username="john", password="asdasd")
        except Exception as e:
            assert "ERR_FORBIDDEN" in str(e), True
        try:
            self.delete_function(body, username="john", password="asdasd")
        except Exception as e:
            assert "ERR_FORBIDDEN" in str(e), True
        self.delete_function(body)

    def test_to_ensure_user_with_sufficient_privileges_can_perform_eventing_lifecycle_operations(self):
        body = self.create_save_function_body(self.function_name, self.handler_code,
                                              username="john", password="asdasd")
        if not self.is_expired:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default", expiry=300)
        self.deploy_function(body, username="john", password="asdasd")
        if not self.cancel_timer:
            if self.is_sbm:
                self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs * 2)
            else:
                self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * self.num_docs)
        self.pause_function(body, username="john", password="asdasd")
        if not self.is_expired:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                         is_delete=True)
        self.resume_function(body, username="john", password="asdasd")
        if not self.cancel_timer:
            if self.is_sbm:
                self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs)
            else:
                self.verify_doc_count_collections("dst_bucket._default._default", 0)
        self.undeploy_function(body, username="john", password="asdasd")
        self.delete_function(body, username="john", password="asdasd")

    def test_to_ensure_user_with_insufficient_privileges_cannot_perform_eventing_lifecycle_operations(self):
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/insert_rebalance.js")
        try:
            self.deploy_function(body, username="john", password="asdasd")
        except Exception as e:
            assert "ERR_FORBIDDEN" in str(e), True
        try:
            self.pause_function(body, username="john", password="asdasd")
        except Exception as e:
            assert "ERR_FORBIDDEN" in str(e), True
        try:
            self.resume_function(body, username="john", password="asdasd")
        except Exception as e:
            assert "ERR_FORBIDDEN" in str(e), True
        try:
            self.undeploy_function(body, username="john", password="asdasd")
        except Exception as e:
            assert "ERR_FORBIDDEN" in str(e), True
        self.delete_function(body)

    def test_to_check_function_is_undeployed_when_owner_loses_privileges(self):
        body = self.create_save_function_body(self.function_name, self.handler_code,
                                              username="john", password="asdasd")
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        self.deploy_function(body, username="john", password="asdasd")
        self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * self.num_docs)
        if self.pause_resume:
            self.pause_function(body, username="john", password="asdasd")
        payload = "name=" + "john" + "&roles=" + '''data_reader[metadata],data_writer[metadata],data_writer[dst_bucket],
                                                 data_dcp_reader[src_bucket]'''
        self.rest.add_set_builtin_user(user_id="john", payload=payload)
        self.wait_for_handler_state(self.function_name, "undeployed")
        self.delete_function(body)

    def test_to_check_function_is_undeployed_when_owner_is_deleted(self):
        body = self.create_save_function_body(self.function_name, self.handler_code,
                                              username="john", password="asdasd")
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        self.deploy_function(body, username="john", password="asdasd")
        self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * self.num_docs)
        if self.pause_resume:
            self.pause_function(body, username="john", password="asdasd")
        self.rest.delete_builtin_user(user_id="john")
        self.wait_for_handler_state(self.function_name, "undeployed")
        self.delete_function(body)

    def test_authorisation_failures_for_bucket_operations_and_n1ql_queries_if_role_is_deleted_when_handler_is_deployed(self):
        body = self.create_save_function_body(self.function_name, self.handler_code,
                                              username="john", password="asdasd")
        self.load_data_to_collection(self.docs_per_day * 20, "src_bucket._default._default")
        self.deploy_function(body, username="john", password="asdasd")
        self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * 20)
        if self.handler_code == "handler_code/ABO/advance_bucket_op_auth_failure.js":
            payload = "name=" + "john" + "&roles=" + '''data_reader[metadata],data_writer[metadata],data_dcp_reader[src_bucket],
                                                    data_writer[dst_bucket],eventing_manage_functions[src_bucket:_default]'''
        else:
            payload = "name=" + "john" + "&roles=" + '''data_reader[metadata],data_writer[metadata],data_writer[dst_bucket],
                                                     data_dcp_reader[src_bucket],data_reader[dst_bucket],
                                                     eventing_manage_functions[src_bucket:_default],query_update[dst_bucket],
                                                     query_delete[dst_bucket]'''
        self.rest.add_set_builtin_user(user_id="john", payload=payload)
        self.load_data_to_collection(self.docs_per_day * 20, "src_bucket._default._default",
                                     is_delete=True)
        self.verify_doc_count_collections("dst_bucket._default._default", 0)
        self.undeploy_function(body, username="john", password="asdasd")
        self.delete_function(body, username="john", password="asdasd")

    def test_to_ensure_user_with_sufficient_privileges_can_export_eventing_functions_and_vice_versa(self):
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/insert_rebalance.js",
                                              username="john", password="asdasd")
        self.rest.export_function(self.function_name, self.function_scope, username="john", password="asdasd")
        payload = "name=" + "john" + "&roles=" + '''data_reader[metadata],data_writer[metadata],data_writer[dst_bucket],
                                                         data_dcp_reader[src_bucket]'''
        self.rest.add_set_builtin_user(user_id="john", payload=payload)
        try:
            self.rest.export_function(self.function_name, self.function_scope, username="john", password="asdasd")
        except Exception as e:
            self.log.info(e)
        self.delete_function(body)

    def test_eventing_operations_after_deletion_and_recreation_of_function_owner(self):
        body = self.create_save_function_body(self.function_name, self.handler_code,
                                              username="john", password="asdasd")
        self.rest.delete_builtin_user(user_id="john")
        payload = "name=" + "john" + "&roles=" + '''data_reader[metadata],data_writer[metadata],data_writer[dst_bucket],
                  data_dcp_reader[src_bucket],eventing_manage_functions[src_bucket:_default]''' + "&password=" + "asdasd"
        self.rest.add_set_builtin_user(user_id="john", payload=payload)
        try:
            self.deploy_function(body, username="john", password="asdasd")
        except Exception as e:
            self.log.info(e)
            assert ("ERR_FORBIDDEN" in str(e) and "Owner of the function doesn't exist" in e.args[0].decode('utf-8'))

    def test_function_scope_modification_after_function_creation(self):
        body = self.create_save_function_body(self.function_name, self.handler_code,
                                              username="john", password="asdasd")
        body['function_scope'] = {"bucket": self.dst_bucket_name, "scope": "_default"}
        try:
            self.rest.update_function(self.function_name, body, username="john", password="asdasd")
        except Exception as e:
            assert "ERR_INVALID_REQUEST" in str(e) and "Function scope cannot be changed" in str(e), True
        self.delete_function(body)

    def test_same_function_name_in_different_function_scope(self):
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.function_scope = {"bucket": "*", "scope": "*"}
        body1 = self.create_save_function_body(self.function_name, self.handler_code)
        self.rest.delete_single_function(self.function_name, body['function_scope'])
        self.rest.delete_single_function(self.function_name, body1['function_scope'])

    def test_multiple_same_function_names_in_same_function_scope(self):
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        try:
            self.create_save_function_body(self.function_name, self.handler_code)
        except Exception as e:
            self.log.info(e)
            assert "ERR_APP_ALREADY_DEPLOYED" in str(e) and "another function with same name is already deployed" in str(e), True
        self.undeploy_and_delete_function(body)

    def test_dropping_function_scope_when_handler_is_deployed_or_paused(self):
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        if self.pause_resume:
            self.pause_function(body)
        # drop function scope
        self.rest.delete_bucket(self.src_bucket_name)
        # wait for function to undeploy and delete
        self.wait_for_handler_internal_undeployment_and_deletion(self.function_name)
        # check whether function is deleted or not
        content = json.loads(self.rest.get_list_of_eventing_functions())
        assert content['functions'] == [], "Function was not deleted"
