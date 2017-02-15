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
        self.cbqpath = '%scbq' % self.path

    def suite_setUp(self):
        super(QueryCurlTests, self).suite_setUp()

    def tearDown(self):
        super(QueryCurlTests, self).tearDown()

    def suite_tearDown(self):
        super(QueryCurlTests, self).suite_tearDown()

    '''Basic test for using POST in curl'''
    def test_POST(self):
        # Get the url that the curl will be using
        url = "'http://%s:8093/query/service'" % self.master.ip
        # The query that curl will send to couchbase
        n1ql_query = 'select * from default limit 5'
        # This is the query that the cbq-engine will execute
        query = "select curl('POST', "+ url +", {'data' : 'statement=%s'})" % n1ql_query
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        # Compare the curl statement to the expected result of the n1ql query done normally
        expected_result = self.run_cbq_query('select * from default limit 5')
        self.assertTrue(json_curl['results'][0]['$1']['results'] == expected_result['results'])

    '''Basic test for using GET in curl'''
    def test_GET(self):
        url = "'http://%s:8091/pools/default/buckets/default'" % self.master.ip
        query = "select curl('GET', "+ url +")"
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        self.assertTrue(json_curl['results'][0]['$1']['name'] == 'default')

    '''Basic test for having curl in the from clause'''
    def test_from(self):
        url = "'http://%s:8093/query/service'" % self.master.ip
        n1ql_query = 'select * from default limit 5'
        select_query = "select *"
        from_query=" from curl('POST', "+ url +", {'data' : 'statement=%s'}) result" % n1ql_query
        curl = self.shell.execute_commands_inside(self.cbqpath,select_query + from_query,'', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        expected_result = self.run_cbq_query('select * from default limit 5')
        self.assertTrue(json_curl['results'][0]['result']['results'] == expected_result['results'])

    '''Test needs debugging'''
    def test_where(self):
        url = "'http://%s:8093/query/service'" % self.master.ip
        n1ql_query = 'select * from default limit 5'
        select_query = "select * "
        from_query="from default d "
        where_query="where d.name in curl('POST', "+ url +", {'data' : 'statement=%s'}).results " % n1ql_query
        curl = self.shell.execute_commands_inside(self.cbqpath,select_query+from_query+where_query,'', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        expected_result = self.run_cbq_query('select * from default')
        self.assertTrue(json_curl['results'][0]['result']['results'] == expected_result['results'])

    '''Basic test for having curl in the select and from clause'''
    def test_select_and_from(self):
        url = "'http://%s:8093/query/service'" % self.master.ip
        n1ql_query = 'select * from default limit 5'
        from_n1ql_query= 'select * from default'
        select_query = "select curl('POST', " + url + ", {'data' : 'statement=%s'})" % n1ql_query
        from_query = " from curl('POST', " + url + ", {'data' : 'statement=%s'}) result" % from_n1ql_query
        curl = self.shell.execute_commands_inside(self.cbqpath, select_query + from_query, '', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        expected_result = self.run_cbq_query('select * from default limit 5')
        self.assertTrue(json_curl['results'][0]['$1']['results'] == expected_result['results'])

    '''Test needs debugging'''
    def test_curl_subquery(self):
        url = "'http://%s:8093/query/service'" % self.master.ip
        n1ql_query = 'select * from default limit 5'
        select_query = "select * "
        from_query="from default d "
        where_query="where in (select * from curl('POST', "+ url +", {'data' : 'statement=%s'}) result) " % n1ql_query
        curl = self.shell.execute_commands_inside(self.cbqpath,select_query+from_query+where_query,'', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        expected_result = self.run_cbq_query('select * from default limit 5')
        self.assertTrue(json_curl['results'][0]['result']['results'] == expected_result['results'])

##############################################################################################
#
#   External endpoint Tests
#
##############################################################################################

    '''Test basic requests to json endpoints outside of couchbase
        -https://jsonplaceholder.typicode.com/todos
        -https://jsonplaceholder.typicode.com/users (contains more complex data than above
        -http://data.colorado.gov/resource/4ykn-tg5h.json/ (is a website with real data)'''
    def test_simple_external_json(self):
        # Get the output from the actual curl and test it against the n1ql curl query
        curl_output = self.shell.execute_command("curl https://jsonplaceholder.typicode.com/todos")
        # The above command returns a tuple, we want the first element of that tuple
        expected_curl = self.convert_list_to_json(curl_output[0])

        url = "'https://jsonplaceholder.typicode.com/todos'"
        query = "select curl('GET', "+ url + ")"
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        # Convert the output of the above command to json
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['results'][0]['$1'] == expected_curl)

        # Test for more complex data
        curl_output = self.shell.execute_command("curl https://jsonplaceholder.typicode.com/users")
        expected_curl = self.convert_list_to_json(curl_output[0])
        url = "'https://jsonplaceholder.typicode.com/users'"
        query = "select curl('GET', "+ url + ")"
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['results'][0]['$1'] == expected_curl)

        # Test for a website in production (the website above is only used to provide json endpoints with fake data)
        curl_output = self.shell.execute_command("curl http://data.colorado.gov/resource/4ykn-tg5h.json/")
        expected_curl = self.convert_list_to_json(curl_output[0])
        url = "'http://data.colorado.gov/resource/4ykn-tg5h.json/'"
        query = "select curl('GET', "+ url + ")"
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['results'][0]['$1'] == expected_curl)

    '''Test external endpoints in a the from field of a query
        -select * from curl result
        -select result.username from curl result
        -select * from curl result where result.username == 'Bret'.'''
    def test_from_external(self):
        url = "'https://jsonplaceholder.typicode.com/users'"
        select_query = "select *"
        from_query=" from curl('GET', "+ url +") result "
        curl = self.shell.execute_commands_inside(self.cbqpath,select_query + from_query,'', '', '', '', '')
        # Convert the output of the above command to json
        json_curl = self.convert_to_json(curl)
        self.assertTrue(json_curl['metrics']['resultCount'] == 10)

        # Test the use of curl in the from as a bucket, see if you can specify only usernames
        select_query = "select result.username"
        curl = self.shell.execute_commands_inside(self.cbqpath,select_query + from_query,'', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        # Need to make sure that only the usernames were stored, that correlates to a resultsize of 478
        self.assertTrue(json_curl['metrics']['resultSize'] == 478)

        # Test of the use of curl in the from as a bucket, see if you can filter results
        select_query = "select *"
        where_query ="where result.username == 'Bret'"
        curl = self.shell.execute_commands_inside(self.cbqpath,select_query + from_query+ where_query,'', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        self.assertTrue(json_curl['metrics']['resultCount'] == 1 and
                        json_curl['results'][0]['result']['username'] == 'Bret')

    '''Test requests to the google maps api
        -check with a valid api key
        -check with an invalid api key (should have an invalid api key error stored in the resuls)'''
    def test_external_json_google_api_key(self):
        # Test the google maps json endpoint with a valid api key and make sure it works
        curl_output = self.shell.execute_command("curl --get https://maps.googleapis.com/maps/api/geocode/json -d 'address=santa+cruz&components=country:ES&key=AIzaSyCT6niGCMsgegJkQSYSqpoLZ4_rSO59XQQ'")
        expected_curl = self.convert_list_to_json(curl_output[0])
        url = "'https://maps.googleapis.com/maps/api/geocode/json'"
        options= "{'data': 'address=santa+cruz&components=country:ES&key=AIzaSyCT6niGCMsgegJkQSYSqpoLZ4_rSO59XQQ'}"
        query="select curl('GET', "+ url +", %s" % options + ")"
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['results'][0]['$1'] == expected_curl)

        # Test the google maps json enpoint with an invalid api key and make sure it errors
        options= "{'data': 'address=santa+cruz&components=country:ES&key=AIzaSyCT6niGCMsgegJkQSYSqpoLZ4_'}"
        query="select curl('GET', "+ url +", %s" % options + ")"
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue("TheprovidedAPIkeyisinvalid." in actual_curl['results'][0]['$1']['error_message'])

##############################################################################################
#
#   Helper Functions
#
##############################################################################################

    '''Convert output of remote_util.execute_commands_inside to json
        For some reason you cannot treat the output of execute_commands_inside as normal unicode, this is a workaround
        to convert the output to json'''
    def convert_to_json(self,output_curl):
        # There are 48 unnecessary characters in the output of execute_commands_inside that must be removed
        new_curl = json.dumps(output_curl[47:])
        string_curl = json.loads(new_curl)
        json_curl = json.loads(string_curl)
        return json_curl

    '''Convert output of remote_util.execute_command to json'''
    def convert_list_to_json(self,output_of_curl):
        new_list = [string.replace(" ", "") for string in output_of_curl]
        concat_string = ''.join(new_list)
        json_output=json.loads(concat_string)
        return json_output
