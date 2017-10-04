import logging
import threading
import json
import uuid
import time
import os

from tuq import QueryTests
from membase.api.rest_client import RestConnection
from membase.api.exception import CBQError, ReadDocumentException
from remote.remote_util import RemoteMachineShellConnection
from security.rbac_base import RbacBase


class QueryWhitelistTests(QueryTests):
    def setUp(self):
        super(QueryWhitelistTests, self).setUp()
        self.shell = RemoteMachineShellConnection(self.master)
        self.info = self.shell.extract_remote_info()
        if self.info.type.lower() == 'windows':
            self.curl_path = "%scurl" % self.path
            self.file_path = "Filec:\\ProgramFiles\\Couchbase\\Server\\bin\\..\\var\\lib\\couchbase\\n1qlcerts\\curl_whitelist"
            self.lowercase_file_path ="filec:\\ProgramFiles\\Couchbase\\Server\\bin\\..\\var\\lib\\couchbase\\n1qlcerts\\curl_whitelist"
        else:
            self.curl_path = "curl"
            self.file_path = "File/opt/couchbase/bin/../var/lib/couchbase/n1qlcerts/curl_whitelist"
            self.lowercase_file_path = "file/opt/couchbase/bin/../var/lib/couchbase/n1qlcerts/curl_whitelist"
        self.rest = RestConnection(self.master)
        self.cbqpath = '%scbq' % self.path + " -e %s:%s -q -u %s -p %s"\
                                             % (self.master.ip, self.n1ql_port, self.rest.username, self.rest.password)
        self.query_service_url = "'http://%s:%s/query/service'" % (self.master.ip,self.n1ql_port)
        self.api_port = self.input.param("api_port", 8094)
        self.load_sample = self.input.param("load_sample", False)
        self.create_users = self.input.param("create_users", False)
        self.full_access = self.input.param("full_access", True)
        self.run_cbq_query('delete from system:prepareds')

    def suite_setUp(self):
        super(QueryWhitelistTests, self).suite_setUp()
        # Create the users necessary for the RBAC tests in curl
        if self.create_users:
            testuser = [{'id': 'no_curl', 'name': 'no_curl', 'password': 'password'},
                        {'id': 'curl', 'name': 'curl', 'password': 'password'},
                        {'id': 'curl_no_insert', 'name': 'curl_no_insert', 'password': 'password'}]
            RbacBase().create_user_source(testuser, 'builtin', self.master)

            noncurl_permissions = 'bucket_full_access[*]:query_select[*]:query_update[*]:' \
                                  'query_insert[*]:query_delete[*]:query_manage_index[*]:' \
                                  'query_system_catalog'
            curl_permissions = 'bucket_full_access[*]:query_select[*]:query_update[*]:' \
                               'query_insert[*]:query_delete[*]:query_manage_index[*]:' \
                               'query_system_catalog:query_external_access'
            # Assign user to role
            role_list = [{'id': 'no_curl', 'name': 'no_curl','roles': '%s' % noncurl_permissions},
                         {'id': 'curl', 'name': 'curl', 'roles': '%s' % curl_permissions}]
            temp = RbacBase().add_user_role(role_list, self.rest, 'builtin')

    def tearDown(self):
        super(QueryWhitelistTests, self).tearDown()

    def suite_tearDown(self):
        super(QueryWhitelistTests, self).suite_tearDown()

    '''Test running a curl command without a whitelist present'''
    def test_no_whitelist(self):
        # The query that curl will send to couchbase
        n1ql_query = 'select * from default limit 5'
        error_msg= "Errorevaluatingprojection.-cause:%s.jsondoesnotexistonnode" \
                   "%s.CURL()endpointsshouldbewhitelisted." % (self.file_path,self.master.ip)

        error_msg_bad_vm = "Errorevaluatingprojection.-cause:%s.jsondoesnotexistonnode" \
                   "::1,127.0.0.1.CURL()endpointsshouldbewhitelisted." % self.file_path

        error_msg_windows = "Errorevaluatingprojection.-cause:%s.jsondoesnotexistonnode" \
                                   "::1,%s.CURL()endpointsshouldbewhitelisted." \
                                   % (self.file_path,self.master.ip)
        # This is the query that the cbq-engine will execute
        query = "select curl(" + self.query_service_url + \
                ", {'data' : 'statement=%s','user':'%s:%s'})" % (
                n1ql_query, self.username, self.password)
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        self.assertTrue(json_curl['errors'][0]['msg'] == error_msg
                        or json_curl['errors'][0]['msg'] == error_msg_bad_vm
                        or json_curl['errors'][0]['msg'] == error_msg_windows)

    '''Test running a curl command with an empty whitelist'''
    def test_empty_whitelist(self):
        self.shell.create_whitelist(self.n1ql_certs_path,{})
        n1ql_query = 'select * from default limit 5'
        error_msg= "Errorevaluatingprojection.-cause:%s.jsoncontainsemptyJSONobjectonnode%s.CURL()" \
                   "endpointsshouldbewhitelisted." % (self.file_path,self.master.ip)

        # Error message that appears on VMs that have incorrectly set up their hostname
        error_msg_bad_vm = "Errorevaluatingprojection.-cause:%s.jsoncontainsemptyJSONobjectonnode" \
                   "::1,127.0.0.1.CURL()endpointsshouldbewhitelisted." % self.file_path

        error_msg_windows = "Errorevaluatingprojection.-cause:%s.jsoncontainsemptyJSONobjectonnode" \
                   "::1,%s.CURL()endpointsshouldbewhitelisted." % (self.file_path,self.master.ip)

        # This is the query that the cbq-engine will execute
        query = "select curl(" + self.query_service_url + \
                ", {'data' : 'statement=%s','user':'%s:%s'})" % (
                n1ql_query, self.username, self.password)
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        self.assertTrue(json_curl['errors'][0]['msg'] == error_msg or
                        json_curl['errors'][0]['msg'] == error_msg_bad_vm
                        or json_curl['errors'][0]['msg'] == error_msg_windows)

        error_msg = "Errorevaluatingprojection.-cause:all_accessshouldbebooleanvaluein%s.jsononnode%s."\
                    % (self.lowercase_file_path,self.master.ip)

        error_msg_bad_vm = "Errorevaluatingprojection.-cause:all_accessshouldbebooleanvaluein%s." \
                           "jsononnode::1,127.0.0.1." % self.lowercase_file_path

        error_msg_windows = "Errorevaluatingprojection.-cause:all_accessshouldbebooleanvaluein%s." \
                           "jsononnode::1,%s." % (self.lowercase_file_path,self.master.ip)

        self.shell.create_whitelist(self.n1ql_certs_path, {"all_access": None, "allowed_urls": None,
                                                           "disallowed_urls": None})
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        self.assertTrue(json_curl['errors'][0]['msg'] == error_msg or
                        json_curl['errors'][0]['msg'] == error_msg_bad_vm
                        or json_curl['errors'][0]['msg'] == error_msg_windows)

    '''Test running a curl command with whitelists that are invalid'''
    def test_invalid_whitelist(self):
        self.shell.create_whitelist(self.n1ql_certs_path,"thisisnotvalid")
        n1ql_query = 'select * from default limit 5'
        error_msg= "Errorevaluatingprojection.-cause:%s.jsoncontainsinvalidJSONonnode%s." \
                   "ContentshavetobeaJSONobject." % (self.file_path,self.master.ip)

        error_msg_bad_vm= "Errorevaluatingprojection.-cause:%s.jsoncontainsinvalidJSONonnode::1,127.0.0.1." \
                   "ContentshavetobeaJSONobject." % self.file_path

        error_msg_windows= "Errorevaluatingprojection.-cause:%s.jsoncontainsinvalidJSONonnode::1,%s." \
                   "ContentshavetobeaJSONobject." % (self.file_path,self.master.ip)

        # This is the query that the cbq-engine will execute
        query = "select curl(" + self.query_service_url + \
                ", {'data' : 'statement=%s','user':'%s:%s'})" % (
                n1ql_query, self.username, self.password)
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        self.assertTrue(json_curl['errors'][0]['msg'] == error_msg
                        or json_curl['errors'][0]['msg'] == error_msg_bad_vm
                        or json_curl['errors'][0]['msg'] == error_msg_windows)

        error_msg = "Errorevaluatingprojection.-cause:all_accessshouldbebooleanvaluein%s.jsononnode%s." \
                    % (self.lowercase_file_path,self.master.ip)

        error_msg_bad_vm = "Errorevaluatingprojection.-cause:all_accessshouldbebooleanvaluein%s." \
                           "jsononnode::1,127.0.0.1." % self.lowercase_file_path

        error_msg_windows = "Errorevaluatingprojection.-cause:all_accessshouldbebooleanvaluein%s." \
                           "jsononnode::1,%s." % (self.lowercase_file_path,self.master.ip)

        self.shell.create_whitelist(self.n1ql_certs_path, {"all_access": "hello",
                                                           "allowed_urls": ["goodbye"],
                                                           "disallowed_urls": ["invalid"]})
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        self.assertTrue(json_curl['errors'][0]['msg'] == error_msg
                        or json_curl['errors'][0]['msg'] == error_msg_bad_vm
                        or json_curl['errors'][0]['msg'] == error_msg_windows)

    '''Test running a curl command with a whitelist that contains the field all_access: True and also
       inavlid/fake fields'''
    def test_basic_all_access_true(self):
        n1ql_query = 'select * from default limit 5'
        self.shell.create_whitelist(self.n1ql_certs_path, {"all_access":True})
        query = "select curl(" + self.query_service_url + \
                ", {'data' : 'statement=%s','user':'%s:%s'})" % (
                n1ql_query, self.username, self.password)
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        expected_result = self.run_cbq_query('select * from default limit 5')
        self.assertTrue(json_curl['results'][0]['$1']['results'] == expected_result['results'])

        curl_output = self.shell.execute_command("%s https://jira.atlassian.com/rest/api/latest/issue/JRA-9"
                                                 %self.curl_path)
        expected_curl = self.convert_list_to_json(curl_output[0])
        url = "'https://jira.atlassian.com/rest/api/latest/issue/JRA-9'"
        query="select curl("+ url +")"
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['results'][0]['$1'] == expected_curl)

        self.shell.create_whitelist(self.n1ql_certs_path, {"all_access": True,
                                                           "fake_field":"blahahahahaha",
                                                           "fake_url": "fake"})

        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['results'][0]['$1'] == expected_curl)

        self.shell.create_whitelist(self.n1ql_certs_path, {"fake_field":"blahahahahaha",
                                                           "all_access": True,
                                                           "fake_url": "fake"})

        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['results'][0]['$1'] == expected_curl)

    '''Test all_access: True with nonsense in the allowed/disallowed fields as well as nothing
       in the allowed/disallowed fields'''
    def test_all_access_true(self):
        self.shell.create_whitelist(self.n1ql_certs_path, {"all_access": True,
                                                           "allowed_urls":["blahahahahaha"],
                                                           "disallowed_urls": ["fake"]})
        curl_output = self.shell.execute_command("%s https://jira.atlassian.com/rest/api/latest/issue/JRA-9"
                                                 % self.curl_path)
        expected_curl = self.convert_list_to_json(curl_output[0])
        url = "'https://jira.atlassian.com/rest/api/latest/issue/JRA-9'"
        query="select curl("+ url +")"
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['results'][0]['$1'] == expected_curl)

        self.shell.create_whitelist(self.n1ql_certs_path, {"all_access": True,
                                                           "allowed_urls": None,
                                                           "disallowed_urls": None})
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['results'][0]['$1'] == expected_curl)

    '''Test what happens if you give an disallowed_url field as well as an all_access field, all_access
       should get precedence over disallowed_urls field'''
    def test_all_access_true_disallowed_url(self):
        self.shell.create_whitelist(self.n1ql_certs_path, {"all_access": True,
                                                           "disallowed_urls":
                                                               ["https://maps.googleapis.com"]})
        curl_output = self.shell.execute_command("%s --get https://maps.googleapis.com/maps/api/geocode/json "
                                                 "-d 'address=santa+cruz&components=country:ES&key=AIzaSyCT6niGCMsgegJkQSYSqpoLZ4_rSO59XQQ'"
                                                 % self.curl_path)
        expected_curl = self.convert_list_to_json(curl_output[0])
        url = "'https://maps.googleapis.com/maps/api/geocode/json'"
        options= "{'get':True,'data': 'address=santa+cruz&components=country:ES&key=AIzaSyCT6niGCMsgegJkQSYSqpoLZ4_rSO59XQQ'}"
        query="select curl("+ url +", %s" % options + ")"
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['results'][0]['$1'] == expected_curl)

    '''Test what happens if you give an allowed_url field as well as an all_access field, all_access
       should get precedence over allowed_urls field'''
    def test_all_access_true_allowed_url(self):
        self.shell.create_whitelist(self.n1ql_certs_path, {"all_access": True,
                                                           "allowed_urls":
                                                               ["https://maps.googleapis.com"]})
        curl_output = self.shell.execute_command("%s https://jira.atlassian.com/rest/api/latest/issue/JRA-9"
                                                 %self.curl_path)
        expected_curl = self.convert_list_to_json(curl_output[0])
        url = "'https://jira.atlassian.com/rest/api/latest/issue/JRA-9'"
        query="select curl("+ url +")"
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['results'][0]['$1'] == expected_curl)

    '''Test what happens when you set the all_access field multiple times, or try and give it multiple
       values'''
    def test_multiple_all_access(self):
        self.shell.create_whitelist(self.n1ql_certs_path, {"all_access": True,
                                                           "all_access": False})
        error_msg = "Errorevaluatingprojection.-cause:URLendpointisn'twhitelisted" \
                    "https://jira.atlassian.com/rest/api/latest/issue/JRA-9onnode%s." \
                    % (self.master.ip)

        error_msg_bad_vm = "Errorevaluatingprojection.-cause:URLendpointisn'twhitelisted" \
                    "https://jira.atlassian.com/rest/api/latest/issue/JRA-9onnode::1,127.0.0.1."

        error_msg_windows = "Errorevaluatingprojection.-cause:URLendpointisn'twhitelisted" \
                    "https://jira.atlassian.com/rest/api/latest/issue/JRA-9onnode::1,%s." \
                            % self.master.ip

        curl_output = self.shell.execute_command("%s https://jira.atlassian.com/rest/api/latest/issue/JRA-9"
                                                 %self.curl_path)
        expected_curl = self.convert_list_to_json(curl_output[0])
        url = "'https://jira.atlassian.com/rest/api/latest/issue/JRA-9'"
        query="select curl("+ url +")"
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['errors'][0]['msg'] == error_msg or
                        actual_curl['errors'][0]['msg'] == error_msg_bad_vm
                        or actual_curl['errors'][0]['msg'] == error_msg_windows)

        self.shell.create_whitelist(self.n1ql_certs_path, {"all_access": False,
                                                           "all_access": True})
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['results'][0]['$1'] == expected_curl)

        error_msg = "Errorevaluatingprojection.-cause:all_accessshouldbebooleanvaluein%s.jsononnode%s." \
                    % (self.lowercase_file_path,self.master.ip)

        error_msg_bad_vm = "Errorevaluatingprojection.-cause:all_accessshouldbebooleanvaluein%s." \
                           "jsononnode::1,127.0.0.1." % self.lowercase_file_path

        error_msg_windows = "Errorevaluatingprojection.-cause:all_accessshouldbebooleanvaluein%s." \
                           "jsononnode::1,%s." % (self.lowercase_file_path,self.master.ip)

        self.shell.create_whitelist(self.n1ql_certs_path, {"all_access": [True,False]})
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['errors'][0]['msg'] == error_msg or
                        actual_curl['errors'][0]['msg'] == error_msg_bad_vm
                        or actual_curl['errors'][0]['msg'] == error_msg_windows)

    '''Test to make sure that whitelist enforces that allowed_urls field must be given as a list'''
    def test_invalid_allowed_url(self):
        self.shell.create_whitelist(self.n1ql_certs_path, {"all_access": False,
                                                           "allowed_urls": "blahblahblah"})
        error_msg = "Errorevaluatingprojection.-cause:allowed_urlsshouldbelistofurlsin%s.jsononnode%s." \
                    % (self.lowercase_file_path, self.master.ip)

        error_msg_bad_vm = "Errorevaluatingprojection.-cause:allowed_urlsshouldbelistofurlsin%s." \
                           "jsononnode::1,127.0.0.1." % self.lowercase_file_path

        error_msg_windows = "Errorevaluatingprojection.-cause:allowed_urlsshouldbelistofurlsin%s." \
                           "jsononnode::1,%s." % (self.lowercase_file_path,self.master.ip)

        n1ql_query = 'select * from default limit 5'
        # This is the query that the cbq-engine will execute
        query = "select curl(" + self.query_service_url + \
                ", {'data' : 'statement=%s','user':'%s:%s'})" % (
                n1ql_query, self.username, self.password)
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        self.assertTrue(json_curl['errors'][0]['msg'] == error_msg or
                        json_curl['errors'][0]['msg'] == error_msg_bad_vm
                        or json_curl['errors'][0]['msg'] == error_msg_windows)

    '''Test the allowed_urls field, try to run curl against an endpoint not in allowed_urls and then
       try to run curl against an endpoint in allowed_urls'''
    def test_allowed_url(self):
        self.shell.create_whitelist(self.n1ql_certs_path, {"all_access": False,
                                                           "allowed_urls":
                                                               ["https://maps.googleapis.com"]})
        error_msg = "Errorevaluatingprojection.-cause:URLendpointisn'twhitelisted" \
                    "https://jira.atlassian.com/rest/api/latest/issue/JRA-9onnode%s." \
                    % (self.master.ip)

        error_msg_bad_vm = "Errorevaluatingprojection.-cause:URLendpointisn'twhitelisted" \
                    "https://jira.atlassian.com/rest/api/latest/issue/JRA-9onnode::1,127.0.0.1."

        error_msg_windows = "Errorevaluatingprojection.-cause:URLendpointisn'twhitelisted" \
                    "https://jira.atlassian.com/rest/api/latest/issue/JRA-9onnode::1,%s." \
                            % self.master.ip

        url = "'https://jira.atlassian.com/rest/api/latest/issue/JRA-9'"
        query="select curl("+ url +")"
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['errors'][0]['msg'] == error_msg
                        or actual_curl['errors'][0]['msg'] == error_msg_bad_vm
                        or actual_curl['errors'][0]['msg'] == error_msg_windows)

        curl_output = self.shell.execute_command("%s --get https://maps.googleapis.com/maps/api/geocode/json "
                                                 "-d 'address=santa+cruz&components=country:ES&key=AIzaSyCT6niGCMsgegJkQSYSqpoLZ4_rSO59XQQ'"
                                                 % self.curl_path)
        expected_curl = self.convert_list_to_json(curl_output[0])
        url = "'https://maps.googleapis.com/maps/api/geocode/json'"
        options= "{'get':True,'data': 'address=santa+cruz&components=country:ES&key=AIzaSyCT6niGCMsgegJkQSYSqpoLZ4_rSO59XQQ'}"
        query="select curl("+ url +", %s" % options + ")"
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['results'][0]['$1'] == expected_curl)

    '''Test the allowed_urls field, try to run curl against an endpoint not in disallowed_urls and then
       try to run curl against an endpoint in disallowed_urls, both should fail'''
    def test_disallowed_url(self):
        self.shell.create_whitelist(self.n1ql_certs_path, {"all_access": False,
                                                           "disallowed_urls":
                                                               ["https://maps.googleapis.com"]})
        error_msg = "Errorevaluatingprojection.-cause:URLendpointisn'twhitelisted" \
                    "https://jira.atlassian.com/rest/api/latest/issue/JRA-9onnode%s." \
                    % (self.master.ip)

        error_msg_bad_vm = "Errorevaluatingprojection.-cause:URLendpointisn'twhitelisted" \
                    "https://jira.atlassian.com/rest/api/latest/issue/JRA-9onnode::1,127.0.0.1." \

        error_msg_windows = "Errorevaluatingprojection.-cause:URLendpointisn'twhitelisted" \
                    "https://jira.atlassian.com/rest/api/latest/issue/JRA-9onnode::1,%s."\
                            % self.master.ip

        url = "'https://jira.atlassian.com/rest/api/latest/issue/JRA-9'"
        query="select curl("+ url +")"
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['errors'][0]['msg'] == error_msg
                        or actual_curl['errors'][0]['msg'] == error_msg_bad_vm
                        or actual_curl['errors'][0]['msg'] == error_msg_windows)

        error_msg = "Errorevaluatingprojection.-cause:URLendpointisn'twhitelisted" \
                    "https://maps.googleapis.com/maps/api/geocode/jsononnode%s." \
                    % (self.master.ip)

        error_msg_bad_vm = "Errorevaluatingprojection.-cause:URLendpointisn'twhitelisted" \
                    "https://maps.googleapis.com/maps/api/geocode/jsononnode::1,127.0.0.1." \

        error_msg_windows = "Errorevaluatingprojection.-cause:URLendpointisn'twhitelisted" \
                    "https://maps.googleapis.com/maps/api/geocode/jsononnode::1,%s." \
                            % self.master.ip

        url = "'https://maps.googleapis.com/maps/api/geocode/json'"
        options= "{'get':True,'data': 'address=santa+cruz&components=country:ES&key=AIzaSyCT6niGCMsgegJkQSYSqpoLZ4_rSO59XQQ'}"
        query="select curl("+ url +", %s" % options + ")"
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['errors'][0]['msg'] == error_msg
                        or actual_curl['errors'][0]['msg'] == error_msg_bad_vm
                        or actual_curl['errors'][0]['msg'] == error_msg_windows)

    '''Test that disallowed_urls field has precedence over allowed_urls'''
    def test_disallowed_precedence(self):
        self.shell.create_whitelist(self.n1ql_certs_path, {"all_access": False,
                                                           "allowed_urls":
                                                               ["https://maps.googleapis.com/maps/api/geocode/json"],
                                                           "disallowed_urls":
                                                               ["https://maps.googleapis.com"]})
        error_msg = "Errorevaluatingprojection.-cause:URLendpointisn'twhitelisted" \
                    "https://maps.googleapis.com/maps/api/geocode/jsononnode%s." \
                    % (self.master.ip)

        error_msg_bad_vm = "Errorevaluatingprojection.-cause:URLendpointisn'twhitelisted" \
                    "https://maps.googleapis.com/maps/api/geocode/jsononnode::1,127.0.0.1."

        error_msg_windows = "Errorevaluatingprojection.-cause:URLendpointisn'twhitelisted" \
                    "https://maps.googleapis.com/maps/api/geocode/jsononnode::1,%s." % self.master.ip

        url = "'https://maps.googleapis.com/maps/api/geocode/json'"
        options= "{'get':True,'data': 'address=santa+cruz&components=country:ES&key=AIzaSyCT6niGCMsgegJkQSYSqpoLZ4_rSO59XQQ'}"
        query="select curl("+ url +", %s" % options + ")"
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['errors'][0]['msg'] == error_msg
                        or actual_curl['errors'][0]['msg'] == error_msg_bad_vm
                        or actual_curl['errors'][0]['msg'] == error_msg_windows)

        self.shell.create_whitelist(self.n1ql_certs_path, {"all_access": False,
                                                           "allowed_urls":
                                                               ["https://maps.googleapis.com/maps/api/geocode/json"],
                                                           "disallowed_urls":
                                                               ["https://maps.googleapis.com/maps/api/geocode/json"]})
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['errors'][0]['msg'] == error_msg
                        or actual_curl['errors'][0]['msg'] == error_msg_bad_vm
                        or actual_curl['errors'][0]['msg'] == error_msg_windows)

    '''Test valid allowed with an invalid disallowed'''
    def test_allowed_invalid_disallowed(self):
        self.shell.create_whitelist(self.n1ql_certs_path, {"all_access": False,
                                                           "allowed_urls":
                                                               ["https://maps.googleapis.com"],
                                                           "disallowed_urls":["blahblahblah"]})
        error_msg = "Errorevaluatingprojection.-cause:URLendpointisn'twhitelisted" \
                    "https://jira.atlassian.com/rest/api/latest/issue/JRA-9onnode%s." \
                    % (self.master.ip)

        error_msg_bad_vm = "Errorevaluatingprojection.-cause:URLendpointisn'twhitelisted" \
                    "https://jira.atlassian.com/rest/api/latest/issue/JRA-9onnode::1,127.0.0.1." \

        error_msg_windows = "Errorevaluatingprojection.-cause:URLendpointisn'twhitelisted" \
                    "https://jira.atlassian.com/rest/api/latest/issue/JRA-9onnode::1,%s." \
                            % self.master.ip

        curl_output = self.shell.execute_command("%s https://jira.atlassian.com/rest/api/latest/issue/JRA-9"
                                                 %self.curl_path)
        expected_curl = self.convert_list_to_json(curl_output[0])
        url = "'https://jira.atlassian.com/rest/api/latest/issue/JRA-9'"
        query="select curl("+ url +")"
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['errors'][0]['msg'] == error_msg
                        or actual_curl['errors'][0]['msg'] == error_msg_bad_vm
                        or actual_curl['errors'][0]['msg'] == error_msg_windows)

        curl_output = self.shell.execute_command("%s --get https://maps.googleapis.com/maps/api/geocode/json "
                                                 "-d 'address=santa+cruz&components=country:ES&key=AIzaSyCT6niGCMsgegJkQSYSqpoLZ4_rSO59XQQ'"
                                                 % self.curl_path)
        expected_curl = self.convert_list_to_json(curl_output[0])
        url = "'https://maps.googleapis.com/maps/api/geocode/json'"
        options= "{'get':True,'data': 'address=santa+cruz&components=country:ES&key=AIzaSyCT6niGCMsgegJkQSYSqpoLZ4_rSO59XQQ'}"
        query="select curl("+ url +", %s" % options + ")"
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['results'][0]['$1'] == expected_curl)

        self.shell.create_whitelist(self.n1ql_certs_path, {"all_access": False,
                                                           "allowed_urls":
                                                               ["https://maps.googleapis.com"],
                                                           "disallowed_urls":"blahblahblah"})
        error_msg = "Errorevaluatingprojection.-cause:disallowed_urlsshouldbelistofurlsin%s.jsononnode%s." \
                    % (self.lowercase_file_path,self.master.ip)

        error_msg_bad_vm = "Errorevaluatingprojection.-cause:disallowed_urlsshouldbelistofurlsin%s." \
                           "jsononnode::1,127.0.0.1." % self.lowercase_file_path

        error_msg_windows = "Errorevaluatingprojection.-cause:disallowed_urlsshouldbelistofurlsin%s." \
                           "jsononnode::1,%s." % (self.lowercase_file_path,self.master.ip)

        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['errors'][0]['msg'] == error_msg
                        or actual_curl['errors'][0]['msg'] == error_msg_bad_vm
                        or actual_curl['errors'][0]['msg'] == error_msg_windows)

    '''Test a valid disallowed with an invalid allowed'''
    def test_disallowed_invalid_allowed(self):
        self.shell.create_whitelist(self.n1ql_certs_path, {"all_access": False,
                                                           "allowed_urls":
                                                               ["blahblahblah"],
                                                           "disallowed_urls":["https://maps.googleapis.com"]})

        error_msg = "Errorevaluatingprojection.-cause:URLendpointisn'twhitelisted" \
                    "https://maps.googleapis.com/maps/api/geocode/jsononnode%s." \
                    % (self.master.ip)

        error_msg_bad_vm = "Errorevaluatingprojection.-cause:URLendpointisn'twhitelisted" \
                    "https://maps.googleapis.com/maps/api/geocode/jsononnode::1,127.0.0.1." \

        error_msg_windows = "Errorevaluatingprojection.-cause:URLendpointisn'twhitelisted" \
                    "https://maps.googleapis.com/maps/api/geocode/jsononnode::1,%s." \
                            % self.master.ip

        url = "'https://maps.googleapis.com/maps/api/geocode/json'"
        options= "{'get':True,'data': 'address=santa+cruz&components=country:ES&key=AIzaSyCT6niGCMsgegJkQSYSqpoLZ4_rSO59XQQ'}"
        query="select curl("+ url +", %s" % options + ")"
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['errors'][0]['msg'] == error_msg
                        or actual_curl['errors'][0]['msg'] == error_msg_bad_vm
                        or actual_curl['errors'][0]['msg'] == error_msg_windows)

        self.shell.create_whitelist(self.n1ql_certs_path, {"all_access": False,
                                                           "allowed_urls": "blahblahblah",
                                                           "disallowed_urls":["https://maps.googleapis.com"]})
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['errors'][0]['msg'] == error_msg or
                        actual_curl['errors'][0]['msg'] == error_msg_bad_vm
                        or actual_curl['errors'][0]['msg'] == error_msg_windows)

    '''Should not be able to curl localhost even if you are on the localhost unless whitelisted'''
    def test_localhost(self):
        self.shell.create_whitelist(self.n1ql_certs_path, {"all_access": False})
        error_msg ="Errorevaluatingprojection.-cause:URLendpointisn'twhitelisted" \
                   "http://localhost:8093/query/serviceonnode%s." % (self.master.ip)

        error_msg_bad_vm ="Errorevaluatingprojection.-cause:URLendpointisn'twhitelisted" \
                   "http://localhost:8093/query/serviceonnode::1,127.0.0.1."

        error_msg_windows ="Errorevaluatingprojection.-cause:URLendpointisn'twhitelisted" \
                   "http://localhost:8093/query/serviceonnode::1,%s." % self.master.ip

        n1ql_query = 'select * from default limit 5'
        query = "select curl('http://localhost:8093/query/service', {'data' : 'statement=%s'," \
                "'user':'%s:%s'})" % (n1ql_query, self.username, self.password)
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        self.assertTrue(json_curl['errors'][0]['msg'] == error_msg
                        or json_curl['errors'][0]['msg'] == error_msg_bad_vm
                        or json_curl['errors'][0]['msg'] == error_msg_windows)