import logging
import threading
import json
import uuid
import time

from tuq import QueryTests
from membase.api.rest_client import RestConnection
from membase.api.exception import CBQError, ReadDocumentException
from remote.remote_util import RemoteMachineShellConnection

SOURCE_CB_PARAMS = {
                      "authUser": "default",
                      "authPassword": "",
                      "authSaslUser": "",
                      "authSaslPassword": "",
                      "clusterManagerBackoffFactor": 0,
                      "clusterManagerSleepInitMS": 0,
                      "clusterManagerSleepMaxMS": 20000,
                      "dataManagerBackoffFactor": 0,
                      "dataManagerSleepInitMS": 0,
                      "dataManagerSleepMaxMS": 20000,
                      "feedBufferSizeBytes": 0,
                      "feedBufferAckThreshold": 0
                    }
INDEX_DEFINITION = {
                          "type": "fulltext-index",
                          "name": "",
                          "uuid": "",
                          "params": {},
                          "sourceType": "couchbase",
                          "sourceName": "default",
                          "sourceUUID": "",
                          "sourceParams": SOURCE_CB_PARAMS,
                          "planParams": {}
                        }

class QueryCurlTests(QueryTests):
    def setUp(self):
        super(QueryCurlTests, self).setUp()
        self.shell = RemoteMachineShellConnection(self.master)
        self.rest = RestConnection(self.master)
        self.cbqpath = '%scbq' % self.path + " -u %s -p %s" % (self.rest.username,self.rest.password)
        self.query_service_url = "'http://%s:%s/query/service'" % (self.master.ip,self.n1ql_port)
        self.api_port = self.input.param("api_port", 8094)
        self.load_sample = self.input.param("load_sample", False)

    def suite_setUp(self):
        super(QueryCurlTests, self).suite_setUp()
        if self.load_sample:
            self.shell.execute_command("""curl -v -u Administrator:password \
                         -X POST http://{0}:{1}/sampleBuckets/install \
                      -d  '["beer-sample"]'""".format(self.master.ip, self.master.port))
            time.sleep(1)
            index_definition = INDEX_DEFINITION
            index_name = index_definition['name'] = "beers"
            index_definition['sourceName'] = "beer-sample"
            index_definition['sourceParams']['authUser'] = "beer-sample"
            rest_src_fts = RestConnection(self.servers[0])
            try:
                status, _ = rest_src_fts.get_fts_index_definition(index_name)
                if status != 400:
                    rest_src_fts.delete_fts_index(index_name)
                rest_src_fts.create_fts_index(index_name, index_definition)
            except Exception, ex:
                self.fail(ex)

    def tearDown(self):
        super(QueryCurlTests, self).tearDown()

    def suite_tearDown(self):
        super(QueryCurlTests, self).suite_tearDown()

    '''Basic test for using POST in curl'''
    def test_POST(self):
        # The query that curl will send to couchbase
        n1ql_query = 'select * from default limit 5'
        # This is the query that the cbq-engine will execute
        query = "select curl('POST', "+ self.query_service_url +", {'data' : 'statement=%s','user':'Administrator:password'})" % n1ql_query
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        # Compare the curl statement to the expected result of the n1ql query done normally
        expected_result = self.run_cbq_query('select * from default limit 5')
        self.assertTrue(json_curl['results'][0]['$1']['results'] == expected_result['results'])

    '''Basic test for using GET in curl'''
    def test_GET(self):
        url = "'http://%s:%s/pools/default/buckets/default'" % (self.master.ip,self.master.port)
        query = "select curl('GET', "+ url +")"
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        self.assertTrue(json_curl['results'][0]['$1']['name'] == 'default')

    '''Basic test for having curl in the from clause'''
    def test_from(self):
        n1ql_query = 'select * from default limit 5'
        select_query = "select *"
        from_query=" from curl('POST', "+ self.query_service_url +", {'data' : 'statement=%s','user':'Administrator:password'}) result" % n1ql_query
        curl = self.shell.execute_commands_inside(self.cbqpath,select_query + from_query,'', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        expected_result = self.run_cbq_query('select * from default limit 5')
        self.assertTrue(json_curl['results'][0]['result']['results'] == expected_result['results'])

    '''Basic Test that tests if curl works inside the where clause of a query'''
    def test_where(self):
        n1ql_query = 'select raw d from default d limit 5&metrics=false&signature=false'
        select_query = "select * "
        from_query="from default d "
        where_query="where curl('POST', "+ self.query_service_url +\
                    ", {'data' : 'statement=%s','user':'Administrator:password'}).results[0].name == 'employee-9' " % n1ql_query
        curl = self.shell.execute_commands_inside(self.cbqpath,select_query+from_query+where_query,'', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        # This should be equiv to select * from default d where true, so all docs should be present, assuming doc-per-day = 1
        self.assertTrue(json_curl['metrics']['resultCount'] == 2016)

        where_query="where curl('POST', "+ self.query_service_url \
                    +", {'data' : 'statement=%s','user':'Administrator:password'}).results[0].name == 'Ajay' " % n1ql_query
        curl = self.shell.execute_commands_inside(self.cbqpath,select_query+from_query+where_query,'', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        # This should be equiv to select * from default d where false, so no docs should be present
        self.assertTrue(json_curl['metrics']['resultCount'] == 0)

    '''Basic test for having curl in the select and from clause at the same time'''
    def test_select_and_from(self):
        n1ql_query = 'select * from default limit 5'
        from_n1ql_query= 'select * from default'
        select_query = "select curl('POST', " + self.query_service_url + ", {'data' : 'statement=%s','user':'Administrator:password'})" % n1ql_query
        from_query = " from curl('POST', " + self.query_service_url + ", {'data' : 'statement=%s','user':'Administrator:password'}) result" % from_n1ql_query
        curl = self.shell.execute_commands_inside(self.cbqpath, select_query + from_query, '', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        expected_result = self.run_cbq_query('select * from default limit 5')
        self.assertTrue(json_curl['results'][0]['$1']['results'] == expected_result['results'])

    '''Basic test for having curl in the from and where clause at the same time'''
    def test_from_and_where(self):
        n1ql_query = 'select d from default d limit 5'
        where_n1ql_query = 'select raw d from default d limit 5&metrics=false&signature=false'
        select_query = "select * "
        from_query="from curl('POST', " + self.query_service_url + ", {'data' : 'statement=%s','user':'Administrator:password'}) result " % n1ql_query
        where_query="where curl('POST', "+ self.query_service_url +\
                    ", {'data' : 'statement=%s','user':'Administrator:password'}).results[0].name == 'employee-9' " % where_n1ql_query
        curl = self.shell.execute_commands_inside(self.cbqpath,select_query+from_query+where_query,'', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        self.assertTrue(json_curl['metrics']['resultCount'] == 1)

        where_query = "where curl('POST', " + self.query_service_url + \
                      ", {'data' : 'statement=%s','user':'Administrator:password'}).results[0].name == 'Ajay' " % where_n1ql_query
        curl = self.shell.execute_commands_inside(self.cbqpath,select_query+from_query+where_query,'', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        self.assertTrue(json_curl['metrics']['resultCount'] == 0)

    '''Basic test that tests if curl works while inside a subquery'''
    def test_curl_subquery(self):
        n1ql_query = 'select * from default limit 5'
        select_query = "select * "
        from_query="from default d "
        where_query="where d.name in (select raw result.results[0].default.name  from curl('POST', " + self.query_service_url + ", {'data' : 'statement=%s','user':'Administrator:password'}) result) " % n1ql_query
        curl = self.shell.execute_commands_inside(self.cbqpath,select_query+from_query+where_query,'', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        expected_result = self.run_cbq_query("select * from default d where d.name == 'employee-9'")
        self.assertTrue(json_curl['results'] == expected_result['results'])

    '''Test if you can join using curl as one of the buckets to be joined'''
    def test_curl_join(self):
        url = "'http://%s:%s/api/index/beers/query'" % (self.master.ip,self.api_port)
        select_query = "select ARRAY x.id for x in b1.result.hits end as hits, b1.result.total_hits as total," \
                       "array_length(b1.result.hits), b "
        from_query = "from (select curl('POST', "+ url +", "
        options = "{'header':'Content-Type: application/json', " \
                  "'data':{'explain':false,'fields': ['*'],'highlight': {},'query': {'query': 'beer'}},'user':'Administrator:password'}) result)b1 "
        join = "INNER JOIN \`beer-sample\` b ON KEYS b1.result.hits[*].id"
        curl = self.shell.execute_commands_inside(self.cbqpath,select_query+from_query+options+join,'', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        self.assertTrue(json_curl['metrics']['resultCount'] == 10 and json_curl['results'][0]['$1'] == 10 and
                        len(json_curl['results'][0]['hits']) == 10)

    '''Test that you can insert data from a select curl statement into couchbase
        -inserting data pulled from another bucket
        -inserting data with a key that already exists (should fail)'''
    def test_insert_curl(self):
        n1ql_query = 'select * from \`beer-sample\` limit 1'
        insert_query ="insert into default (key UUID(), value curl_result.results[0].\`beer-sample\`) "
        query = "select curl('POST', "+ self.query_service_url +", "
        options = "{'data' : 'statement=%s','user': 'Administrator:password'}) curl_result " % n1ql_query
        returning ="returning meta().id, * "
        curl = self.shell.execute_commands_inside(self.cbqpath,insert_query+query+options+returning,'', '', '','', '')
        json_curl = self.convert_to_json(curl)
        docid = json_curl['results'][0]['id']
        result = self.run_cbq_query('select * from default limit 1')
        result2= self.run_cbq_query('select * from `beer-sample` limit 1')
        self.assertTrue(result['results'][0]['default'] == result2['results'][0]['beer-sample']
                        and json_curl['metrics']['mutationCount'] == 1)

        insert_query = "insert into default (key '" + docid+"', value curl_result.results[0].\`beer-sample\`) "
        curl = self.shell.execute_commands_inside(self.cbqpath,insert_query+query+options+returning,'', '', '','', '')
        json_curl = self.convert_to_json(curl)
        self.assertTrue(json_curl['status'] == 'errors' and 'cause:DuplicateKey' in json_curl['errors'][0]['msg'])

    '''Test that you can insert data from a select curl statement using upsert
        -inserting data pulled from another bucket
        -insertign data using a key that already exists(should work)'''
    def test_upsert_curl(self):
        n1ql_query = 'select * from \`beer-sample\` limit 1'
        insert_query ="upsert into default (key UUID(), value curl_result.results[0].\`beer-sample\`) "
        query = "select curl('POST', "+ self.query_service_url +", "
        options = "{'data' : 'statement=%s','user': 'Administrator:password'}) curl_result " % n1ql_query
        returning ="returning meta().id, * "
        curl = self.shell.execute_commands_inside(self.cbqpath,insert_query+query+options+returning,'', '', '','', '')
        json_curl = self.convert_to_json(curl)
        docid = json_curl['results'][0]['id']
        result = self.run_cbq_query('select * from default limit 1')
        result2= self.run_cbq_query('select * from `beer-sample` limit 1')
        self.assertTrue(result['results'][0]['default'] == result2['results'][0]['beer-sample']
                        and json_curl['metrics']['mutationCount'] == 1)

        n1ql_query = 'select * from \`beer-sample\` offset 1 limit 1'
        insert_query = "upsert into default (key '" + docid+"', value curl_result.results[0].\`beer-sample\`) "
        options = "{'data' : 'statement=%s','user': 'Administrator:password'}) curl_result " % n1ql_query
        curl = self.shell.execute_commands_inside(self.cbqpath,insert_query+query+options+returning,'', '', '','', '')
        json_curl = self.convert_to_json(curl)
        result = self.run_cbq_query('select * from default limit 1')
        result2= self.run_cbq_query('select * from `beer-sample` offset 1 limit 1')
        self.assertTrue(result['results'][0]['default'] == result2['results'][0]['beer-sample']
                        and json_curl['metrics']['mutationCount'] == 1)

    '''WIP'''
    def test_update_curl(self):
        result = self.run_cbq_query('select meta().id from default limit 1')
        docid = result['results'][0]['id']



    '''Test if you can access a protected bucket with curl and authorization'''
    def test_protected_bucket(self):
        n1ql_query = 'select * from bucket0 limit 5'
        query = "select curl('POST', "+ self.query_service_url +", "
        options = "{'data' : 'statement=%s', 'user': 'bucket0:password'})" % n1ql_query
        curl = self.shell.execute_commands_inside(self.cbqpath,query+options,'', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        self.assertTrue(json_curl['results'][0]['$1']['metrics']['resultCount'] == 5)

    '''Test a n1ql curl query containing the use of FTS'''
    def test_basic_fts_curl(self):
        url = "'http://%s:%s/api/index/beers/query'" % (self.master.ip,self.api_port)
        select_query = "select result.total_hits, array_length(result.hits) "
        from_query = "from curl('POST', "+ url +", "
        options = "{'header':'Content-Type: application/json', " \
                  "'data':{'explain':true,'fields': ['*'],'highlight': {},'query': {'query': 'garden'}},'user':'Administrator:password'}) result"
        curl = self.shell.execute_commands_inside(self.cbqpath,select_query+from_query+options,'', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        self.assertTrue(json_curl['results'][0]['total_hits'] == 12 and json_curl['results'][0]['$1'] == 10)
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

    '''Test request to the google maps api with an api key'''
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

    '''Test request to a JIRA json endpoint'''
    def test_external_json_jira(self):
        curl_output = self.shell.execute_command("curl https://jira.atlassian.com/rest/api/latest/issue/JRA-9")
        expected_curl = self.convert_list_to_json(curl_output[0])
        url = "'https://jira.atlassian.com/rest/api/latest/issue/JRA-9'"
        query="select curl('GET', "+ url +")"
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['results'][0]['$1'] == expected_curl)

##############################################################################################
#
#   Negative tests
#
##############################################################################################

    '''Tests what happens when curl receives a protocol that isn't valid
        -misspelled protocol
        -unsupported protocol (the expected output of this test is unclear and as such is not included currently)'''
    def test_invalid_protocol(self):
        # Test invalid protocol (misspelled)
        url = "'htpps://maps.googleapis.com/maps/api/geocode/json'"
        query="select curl('GET', "+ url +")"
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['errors'][0]['msg'] =='Errorevaluatingprojection.-cause:curl:Unsupportedprotocol')

    '''Tests what happens when n1ql curl receives invalid urls
        -urls that don't exist
        -urls that don't return json'''
    def test_invalid_url(self):
        # Test url that does not exist
        url = "'http://asdsadasdsadsxfwefwefsdfqffsf.com/'"
        query="select curl('GET', "+ url +")"
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['errors'][0]['msg'] ==
                        "Errorevaluatingprojection.-cause:curl:Couldn'tresolvehostname")

        # Test a valid url that does not return json
        url = "'google.com'"
        query="select curl('GET', "+ url +")"
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['errors'][0]['msg'] ==
                        "Errorevaluatingprojection.-cause:InvalidJSONendpointgoogle.com")

    '''Test if a protected bucket can be accessed without giving its password'''
    def test_protected_bucket_noauth(self):
        cbqpath = '%scbq' % self.path
        # The query that curl will send to couchbase
        n1ql_query = 'select * from bucket0 limit 5'
        # This is the query that the cbq-engine will execute
        query = "select curl('POST', "+ self.query_service_url +", {'data' : 'statement=%s'})" % n1ql_query
        curl = self.shell.execute_commands_inside(cbqpath,query,'', '', '', '', '')
        json_curl = self.convert_to_json(curl)
        self.assertTrue(json_curl['results'][0]['$1']['errors'][0]['msg'] ==
                        "Invalidusername/password.")

    '''Test an unsupported method in n1ql curl
        -DELETE.'''
    def test_unsupported_method(self):
        url = "'http://google.com/'"
        query="select curl('DELETE', "+ url +")"
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['errors'][0]['msg'] ==
                        "Errorevaluatingprojection.-cause:Methodnotsupported")

    '''Tests what happens when you don't give an api key to a url that requires an api key
        - do not provide an api key
        - provide an incorrect api key'''
    def test_external_json_invalid_api_key(self):
        # Don't provide an apikey to a url that requires one
        url = "'https://api.themoviedb.org/3/movie/550'"
        query="select curl('GET', "+ url +")"
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['results'][0]['$1']['status_message'] == "InvalidAPIkey:Youmustbegrantedavalidkey.")

        # Test the google maps json endpoint with an invalid api key and make sure it errors
        url = "'https://maps.googleapis.com/maps/api/geocode/json'"
        options= "{'data': 'address=santa+cruz&components=country:ES&key=AIzaSyCT6niGCMsgegJkQSYSqpoLZ4_'}"
        query="select curl('GET', "+ url +", %s" % options + ")"
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(actual_curl['results'][0]['$1']['error_message'] == "TheprovidedAPIkeyisinvalid.")

    '''WIP'''
    def test_expired_cert(self):
        url = "'https://expired.badssl.com/'"
        query="select curl('GET', "+ url +")"
        curl = self.shell.execute_commands_inside(self.cbqpath,query,'', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        import pdb;pdb.set_trace()

    '''WIP'''
    def test_self_signed_cert(self):
        url = "'https://self-signed.badssl.com/'"
        query = "select curl('GET', " + url + ")"
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        import pdb;pdb.set_trace()

    '''WIP'''
    def test_revoked_cert(self):
        url = "'https://revoked.badssl.com/'"
        query = "select curl('GET', " + url + ")"
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        import pdb; pdb.set_trace()


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