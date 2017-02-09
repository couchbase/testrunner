import logging
import threading
import json
import uuid
import time

from tuq import QueryTests
from membase.api.rest_client import RestConnection
from membase.api.exception import CBQError, ReadDocumentException
from remote.remote_util import RemoteMachineShellConnection



class QueryCurlTests(QueryTests):
    def setUp(self):
        super(QueryCurlTests, self).setUp()
        self.shell = RemoteMachineShellConnection(self.master)

    def suite_setUp(self):
        super(QueryCurlTests, self).suite_setUp()

    def tearDown(self):
        super(QueryCurlTests, self).tearDown()

    def suite_tearDown(self):
        super(QueryCurlTests, self).suite_tearDown()

    '''Basic test for using POST in curl'''
    def test_POST(self):
        # Get the path to the cbq engine (/opt/coucbase/bin/cbq for linux)
        cbqpath = '%scbq' % self.path
        # Get the url that the curl will be using
        url = "'http://%s:8093/query/service'" % self.master.ip
        # The query that curl will send to couchbase
        n1ql_query = 'select * from default limit 5'
        # This is the query that the cbq-engine will execute
        query = "select curl('POST', "+ url +", {'data' : 'statement=%s'})" % n1ql_query
        curl = self.shell.execute_commands_inside(cbqpath,query,'', '', '', '', '')
        # Convert the output of the above command to json
        json_curl = self.convert_to_json(curl)
        # Compare the curl statement to the expected result of the n1ql query done normally
        expected_result = self.run_cbq_query('select * from default limit 5')
        self.assertTrue(json_curl['results'][0]['$1']['results'] == expected_result['results'])

    '''Basic test for using GET in curl'''
    def test_GET(self):
        # Get the path to the cbq engine (/opt/coucbase/bin/cbq for linux)
        cbqpath = '%scbq' % self.path
        # Get the url that the curl will be using
        url = "'http://%s:8091/pools/default/buckets/default'" % self.master.ip
        # This is the query that the cbq-engine will execute
        query = "select curl('GET', "+ url +")"
        curl = self.shell.execute_commands_inside(cbqpath,query,'', '', '', '', '')
        # Convert the output of the above command to json
        json_curl = self.convert_to_json(curl)
        self.assertTrue(json_curl['results'][0]['$1']['name'] == 'default')

    '''Basic test for having curl in the from clause'''
    def test_from(self):
        # Get the path to the cbq engine (/opt/coucbase/bin/cbq for linux)
        cbqpath = '%scbq' % self.path
        # Get the url that the curl will be using
        url = "'http://%s:8093/query/service'" % self.master.ip
        # The query that curl will send to couchbase
        n1ql_query = 'select * from default limit 5'
        # This is the query that the cbq-engine will execute
        select_query = "select *"
        from_query=" from curl('POST', "+ url +", {'data' : 'statement=%s'}) result" % n1ql_query
        curl = self.shell.execute_commands_inside(cbqpath,select_query + from_query,'', '', '', '', '')
        # Convert the output of the above command to json
        json_curl = self.convert_to_json(curl)
        # Compare the curl statement to the expected result of the n1ql query done normally
        expected_result = self.run_cbq_query('select * from default limit 5')
        self.assertTrue(json_curl['results'][0]['result']['results'] == expected_result['results'])

    '''Basic test for having curl in the select and from clause'''
    def test_select_and_from(self):
        # Get the path to the cbq engine (/opt/coucbase/bin/cbq for linux)
        cbqpath = '%scbq' % self.path
        # Get the url that the curl will be using
        url = "'http://%s:8093/query/service'" % self.master.ip
        # The query that curl will send to couchbase
        n1ql_query = 'select * from default limit 5'
        from_n1ql_query= 'select * from default'
        # This is the query that the cbq-engine will execute
        select_query = "select curl('POST', " + url + ", {'data' : 'statement=%s'})" % n1ql_query
        from_query = " from curl('POST', " + url + ", {'data' : 'statement=%s'}) result" % from_n1ql_query
        curl = self.shell.execute_commands_inside(cbqpath, select_query + from_query, '', '', '', '', '')
        # Convert the output of the above command to json
        json_curl = self.convert_to_json(curl)
        # Compare the curl statement to the expected result of the n1ql query done normally
        expected_result = self.run_cbq_query('select * from default limit 5')
        self.assertTrue(json_curl['results'][0]['$1']['results'] == expected_result['results'])

    '''Convert output of remote_util.execute_commands_inside to json
        For some reason you cannot treat the output of execute_commands_inside as normal unicode, this is a workaround
        to convert the output to json'''
    def convert_to_json(self,output_curl):
        new_curl = json.dumps(output_curl[47:])
        string_curl = json.loads(new_curl)
        json_curl = json.loads(string_curl)
        return json_curl
