import logging
import threading
import json
import uuid
import time
import os

from .tuq import QueryTests
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
        #Whitelist error messages
        self.query_error_msg = "Errorevaluatingprojection.-cause:URLendpointisn'twhitelistedhttp://%s:%s/query/service." \
                "PleasemakesuretowhitelisttheURLontheUI." % (self.master.ip, self.n1ql_port)
        self.jira_error_msg ="Errorevaluatingprojection.-cause:URLendpointisn'twhitelistedhttps://jira.atlassian." \
                             "com/rest/api/latest/issue/JRA-9.PleasemakesuretowhitelisttheURLontheUI."
        self.google_error_msg = "Errorevaluatingprojection.-cause:URLendpointisn'twhitelisted" \
                                "https://maps.googleapis.com/maps/api/geocode/json."
        #End of whitelist error messages
        self.query_service_url = "'http://%s:%s/query/service'" % (self.master.ip, self.n1ql_port)
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
        # This is the query that the cbq-engine will execute
        query = "select curl(" + self.query_service_url + \
                ", {'data' : 'statement=%s','user':'%s:%s'})" % (
                n1ql_query, self.username, self.password)
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        self.assertTrue(self.query_error_msg in json_curl['errors'][0]['msg'],
                        "Error message is %s this is incorrect it should be %s"
                        % (json_curl['errors'][0]['msg'], self.query_error_msg))

    '''Test running a curl command with an empty whitelist'''
    def test_empty_whitelist(self):
        response, content = self.rest.create_whitelist(self.master, {})
        result = json.loads(content)
        self.assertEqual(result['errors']['all_access'], 'The value must be supplied')
        n1ql_query = 'select * from default limit 5'

        # This is the query that the cbq-engine will execute
        query = "select curl(" + self.query_service_url + \
                ", {'data' : 'statement=%s','user':'%s:%s'})" % (
                n1ql_query, self.username, self.password)
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        self.assertTrue(self.query_error_msg in json_curl['errors'][0]['msg'],
                        "Error message is %s this is incorrect it should be %s"
                        % (json_curl['errors'][0]['msg'], self.query_error_msg))

        self.rest.create_whitelist(self.master, {"all_access": None, "allowed_urls": None, "disallowed_urls": None})
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        self.assertTrue(self.query_error_msg in json_curl['errors'][0]['msg'],
                        "Error message is %s this is incorrect it should be %s"
                        % (json_curl['errors'][0]['msg'], self.query_error_msg))

    '''Test running a curl command with whitelists that are invalid'''
    def test_invalid_whitelist(self):
        response, content = self.rest.create_whitelist(self.master, "thisisnotvalid")
        result = json.loads(content)
        self.assertEqual(result['errors']['_'], 'Unexpected Json')
        n1ql_query = 'select * from default limit 5'
        # This is the query that the cbq-engine will execute
        query = "select curl(" + self.query_service_url + \
                ", {'data' : 'statement=%s','user':'%s:%s'})" % (
                n1ql_query, self.username, self.password)
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        self.assertTrue(self.query_error_msg in json_curl['errors'][0]['msg'],
                        "Error message is %s this is incorrect it should be %s"
                        % (json_curl['errors'][0]['msg'], self.query_error_msg))

        self.rest.create_whitelist(self.master, {"all_access": "hello", "allowed_urls": ["goodbye"],
                                                 "disallowed_urls": ["invalid"]})
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        self.assertTrue(self.query_error_msg in json_curl['errors'][0]['msg'],
                        "Error message is %s this is incorrect it should be %s"
                        % (json_curl['errors'][0]['msg'], self.query_error_msg))

    '''Test running a curl command with a whitelist that contains the field all_access: True and also
       inavlid/fake fields'''
    def test_basic_all_access_true(self):
        n1ql_query = 'select * from default limit 5'
        self.rest.create_whitelist(self.master, {"all_access": True})
        query = "select curl(" + self.query_service_url + \
                ", {'data' : 'statement=%s','user':'%s:%s'})" % (
                n1ql_query, self.username, self.password)
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        expected_result = self.run_cbq_query('select * from default limit 5')
        self.assertEqual(json_curl['results'][0]['$1']['results'], expected_result['results'])

        curl_output = self.shell.execute_command("%s https://jira.atlassian.com/rest/api/latest/issue/JRA-9"
                                                 % self.curl_path)
        expected_curl = self.convert_list_to_json(curl_output[0])
        url = "'https://jira.atlassian.com/rest/api/latest/issue/JRA-9'"
        query="select curl("+ url +")"
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertEqual(actual_curl['results'][0]['$1'], expected_curl)

        self.rest.create_whitelist(self.master, {"all_access": True,
                                                           "fake_field":"blahahahahaha",
                                                           "fake_url": "fake"})

        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertEqual(actual_curl['results'][0]['$1'], expected_curl)

        self.rest.create_whitelist(self.master, {"fake_field":"blahahahahaha",
                                                           "all_access": True,
                                                           "fake_url": "fake"})

        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertEqual(actual_curl['results'][0]['$1'], expected_curl)

    '''Test all_access: True with nonsense in the allowed/disallowed fields as well as nothing
       in the allowed/disallowed fields'''
    def test_all_access_true(self):
        self.rest.create_whitelist(self.master, {"all_access": True,
                                                 "allowed_urls":["blahahahahaha"], "disallowed_urls": ["fake"]})
        curl_output = self.shell.execute_command("%s https://jira.atlassian.com/rest/api/latest/issue/JRA-9"
                                                 % self.curl_path)
        expected_curl = self.convert_list_to_json(curl_output[0])
        url = "'https://jira.atlassian.com/rest/api/latest/issue/JRA-9'"
        query="select curl("+ url +")"
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertEqual(actual_curl['results'][0]['$1'], expected_curl)

        self.rest.create_whitelist(self.master, {"all_access": True,
                                                           "allowed_urls": None,
                                                           "disallowed_urls": None})
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertEqual(actual_curl['results'][0]['$1'], expected_curl)

    '''Test what happens if you give an disallowed_url field as well as an all_access field, all_access
       should get precedence over disallowed_urls field'''
    def test_all_access_true_disallowed_url(self):
        self.rest.create_whitelist(self.master, {"all_access": True, "disallowed_urls": ["https://maps.googleapis.com"]})
        curl_output = self.shell.execute_command("%s --get https://maps.googleapis.com/maps/api/geocode/json "
                                                 "-d 'address=santa+cruz&components=country:ES&key=AIzaSyCT6niGCMsgegJkQSYSqpoLZ4_rSO59XQQ'"
                                                 % self.curl_path)
        expected_curl = self.convert_list_to_json(curl_output[0])
        url = "'https://maps.googleapis.com/maps/api/geocode/json'"
        options= "{'get':True,'data': 'address=santa+cruz&components=country:ES&key=AIzaSyCT6niGCMsgegJkQSYSqpoLZ4_rSO59XQQ'}"
        query="select curl("+ url +", %s" % options + ")"
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertEqual(actual_curl['results'][0]['$1'], expected_curl)

    '''Test what happens if you give an allowed_url field as well as an all_access field, all_access
       should get precedence over allowed_urls field'''
    def test_all_access_true_allowed_url(self):
        self.rest.create_whitelist(self.master, {"all_access": True, "allowed_urls": ["https://maps.googleapis.com"]})
        curl_output = self.shell.execute_command("%s https://jira.atlassian.com/rest/api/latest/issue/JRA-9"
                                                 %self.curl_path)
        expected_curl = self.convert_list_to_json(curl_output[0])
        url = "'https://jira.atlassian.com/rest/api/latest/issue/JRA-9'"
        query="select curl("+ url +")"
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertEqual(actual_curl['results'][0]['$1'], expected_curl)

    '''Test what happens when you set the all_access field multiple times, or try and give it multiple
       values'''
    def test_multiple_all_access(self):
        self.rest.create_whitelist(self.master, {"all_access": True, "all_access": False})

        curl_output = self.shell.execute_command("%s https://jira.atlassian.com/rest/api/latest/issue/JRA-9"
                                                 % self.curl_path)
        expected_curl = self.convert_list_to_json(curl_output[0])
        url = "'https://jira.atlassian.com/rest/api/latest/issue/JRA-9'"
        query="select curl(" + url + ")"
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(self.jira_error_msg in actual_curl['errors'][0]['msg'],
                        "Error message is %s this is incorrect it should be %s"
                        % (actual_curl['errors'][0]['msg'], self.jira_error_msg))

        self.rest.create_whitelist(self.master, {"all_access": False, "all_access": True})
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertEqual(actual_curl['results'][0]['$1'], expected_curl)

        self.rest.create_whitelist(self.master, {"all_access": [True, False]})
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertEqual(actual_curl['results'][0]['$1'], expected_curl)

    '''Test to make sure that whitelist enforces that allowed_urls field must be given as a list'''
    def test_invalid_allowed_url(self):
        self.rest.create_whitelist(self.master, {"all_access": False})
        # Whitelist should not accept this setting and thus leave the above settting of all_access = False intact
        response, content = self.rest.create_whitelist(self.master, {"all_access": False, "allowed_urls": "blahblahblah"})
        result = json.loads(content)
        self.assertEqual(result['errors']['allowed_urls'], "Invalid type: Must be a list of non-empty strings")
        n1ql_query = 'select * from default limit 5'
        # This is the query that the cbq-engine will execute
        query = "select curl(" + self.query_service_url + \
                ", {'data' : 'statement=%s','user':'%s:%s'})" % (
                n1ql_query, self.username, self.password)
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        self.assertTrue(self.query_error_msg in json_curl['errors'][0]['msg'],
                        "Error message is %s this is incorrect it should be %s"
                        % (json_curl['errors'][0]['msg'], self.query_error_msg))

    '''Test the allowed_urls field, try to run curl against an endpoint not in allowed_urls and then
       try to run curl against an endpoint in allowed_urls'''
    def test_allowed_url(self):
        self.rest.create_whitelist(self.master, {"all_access": False, "allowed_urls": ["https://maps.googleapis.com"]})

        url = "'https://jira.atlassian.com/rest/api/latest/issue/JRA-9'"
        query="select curl("+ url +")"
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(self.jira_error_msg in actual_curl['errors'][0]['msg'],
                        "Error message is %s this is incorrect it should be %s"
                        % (actual_curl['errors'][0]['msg'], self.jira_error_msg))

        curl_output = self.shell.execute_command("%s --get https://maps.googleapis.com/maps/api/geocode/json "
                                                 "-d 'address=santa+cruz&components=country:ES&key=AIzaSyCT6niGCMsgegJkQSYSqpoLZ4_rSO59XQQ'"
                                                 % self.curl_path)
        expected_curl = self.convert_list_to_json(curl_output[0])
        url = "'https://maps.googleapis.com/maps/api/geocode/json'"
        options= "{'get':True,'data': 'address=santa+cruz&components=country:ES&key=AIzaSyCT6niGCMsgegJkQSYSqpoLZ4_rSO59XQQ'}"
        query="select curl("+ url +", %s" % options + ")"
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertEqual(actual_curl['results'][0]['$1'], expected_curl)

    '''Test the allowed_urls field, try to run curl against an endpoint not in disallowed_urls and then
       try to run curl against an endpoint in disallowed_urls, both should fail'''
    def test_disallowed_url(self):
        self.rest.create_whitelist(self.master, {"all_access": False, "disallowed_urls": ["https://maps.googleapis.com"]})

        url = "'https://jira.atlassian.com/rest/api/latest/issue/JRA-9'"
        query="select curl("+ url +")"
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(self.jira_error_msg in actual_curl['errors'][0]['msg'],
                        "Error message is %s this is incorrect it should be %s"
                        % (actual_curl['errors'][0]['msg'], self.jira_error_msg))

        url = "'https://maps.googleapis.com/maps/api/geocode/json'"
        options= "{'get':True,'data': 'address=santa+cruz&components=country:ES&key=AIzaSyCT6niGCMsgegJkQSYSqpoLZ4_rSO59XQQ'}"
        query="select curl("+ url +", %s" % options + ")"
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(self.google_error_msg in actual_curl['errors'][0]['msg'],
                        "Error message is %s this is incorrect it should be %s"
                        % (actual_curl['errors'][0]['msg'], self.google_error_msg))

    '''Test that disallowed_urls field has precedence over allowed_urls'''
    def test_disallowed_precedence(self):
        self.rest.create_whitelist(self.master, {"all_access": False,
                                                 "allowed_urls": ["https://maps.googleapis.com/maps/api/geocode/json"],
                                                 "disallowed_urls": ["https://maps.googleapis.com"]})

        url = "'https://maps.googleapis.com/maps/api/geocode/json'"
        options= "{'get':True,'data': 'address=santa+cruz&components=country:ES&key=AIzaSyCT6niGCMsgegJkQSYSqpoLZ4_rSO59XQQ'}"
        query="select curl("+ url +", %s" % options + ")"
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(self.google_error_msg in actual_curl['errors'][0]['msg'],
                        "Error message is %s this is incorrect it should be %s"
                        % (actual_curl['errors'][0]['msg'], self.google_error_msg))

        self.rest.create_whitelist(self.master, {"all_access": False,
                                                           "allowed_urls":
                                                               ["https://maps.googleapis.com/maps/api/geocode/json"],
                                                           "disallowed_urls":
                                                               ["https://maps.googleapis.com/maps/api/geocode/json"]})
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(self.google_error_msg in actual_curl['errors'][0]['msg'],
                        "Error message is %s this is incorrect it should be %s"
                        % (actual_curl['errors'][0]['msg'], self.google_error_msg))

    '''Test valid allowed with an invalid disallowed'''
    def test_allowed_invalid_disallowed(self):
        self.rest.create_whitelist(self.master, {"all_access": False,
                                                 "allowed_urls": ["https://maps.googleapis.com"],
                                                 "disallowed_urls":["blahblahblah"]})

        curl_output = self.shell.execute_command("%s https://jira.atlassian.com/rest/api/latest/issue/JRA-9"
                                                 %self.curl_path)
        expected_curl = self.convert_list_to_json(curl_output[0])
        url = "'https://jira.atlassian.com/rest/api/latest/issue/JRA-9'"
        query="select curl("+ url +")"
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(self.jira_error_msg in actual_curl['errors'][0]['msg'],
                        "Error message is %s this is incorrect it should be %s"
                        % (actual_curl['errors'][0]['msg'], self.jira_error_msg))

        curl_output = self.shell.execute_command("%s --get https://maps.googleapis.com/maps/api/geocode/json "
                                                 "-d 'address=santa+cruz&components=country:ES&key=AIzaSyCT6niGCMsgegJkQSYSqpoLZ4_rSO59XQQ'"
                                                 % self.curl_path)
        expected_curl = self.convert_list_to_json(curl_output[0])
        url = "'https://maps.googleapis.com/maps/api/geocode/json'"
        options= "{'get':True,'data': 'address=santa+cruz&components=country:ES&key=AIzaSyCT6niGCMsgegJkQSYSqpoLZ4_rSO59XQQ'}"
        query="select curl("+ url +", %s" % options + ")"
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertEqual(actual_curl['results'][0]['$1'], expected_curl)

    '''Test a valid disallowed with an invalid allowed'''
    def test_disallowed_invalid_allowed(self):
        self.rest.create_whitelist(self.master, {"all_access": False,
                                                           "allowed_urls":
                                                               ["blahblahblah"],
                                                           "disallowed_urls":["https://maps.googleapis.com"]})
        url = "'https://maps.googleapis.com/maps/api/geocode/json'"
        options= "{'get':True,'data': 'address=santa+cruz&components=country:ES&key=AIzaSyCT6niGCMsgegJkQSYSqpoLZ4_rSO59XQQ'}"
        query="select curl(" + url + ", %s" % options + ")"
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(self.google_error_msg in actual_curl['errors'][0]['msg'],
                        "Error message is %s this is incorrect it should be %s"
                        % (actual_curl['errors'][0]['msg'], self.google_error_msg))

        response, content = self.rest.create_whitelist(self.master, {"all_access": False,
                                                           "allowed_urls": "blahblahblah",
                                                           "disallowed_urls":["https://maps.googleapis.com"]})
        result = json.loads(content)
        self.assertEqual(result['errors']['allowed_urls'], "Invalid type: Must be a list of non-empty strings")
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(self.google_error_msg in actual_curl['errors'][0]['msg'],
                        "Error message is %s this is incorrect it should be %s"
                        % (actual_curl['errors'][0]['msg'], self.google_error_msg))

    def test_invalid_disallowed_url_validation(self):
        response, content = self.rest.create_whitelist(self.master, {"all_access": False,
                                                           "disallowed_urls":"blahblahblahblahblah"})
        result = json.loads(content)
        self.assertEqual(result['errors']['disallowed_urls'], "Invalid type: Must be a list of non-empty strings")

    '''Should not be able to curl localhost even if you are on the localhost unless whitelisted'''
    def test_localhost(self):
        self.rest.create_whitelist(self.master, {"all_access": False})
        error_msg ="Errorevaluatingprojection.-cause:URLendpointisn'twhitelistedhttp://localhost:8093/query/service." \
                   "PleasemakesuretowhitelisttheURLontheUI."

        n1ql_query = 'select * from default limit 5'
        query = "select curl('http://localhost:8093/query/service', {'data' : 'statement=%s'," \
                "'user':'%s:%s'})" % (n1ql_query, self.username, self.password)
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        self.assertTrue(error_msg in json_curl['errors'][0]['msg'],
                        "Error message is %s this is incorrect it should be %s"
                        % (json_curl['errors'][0]['msg'], error_msg))