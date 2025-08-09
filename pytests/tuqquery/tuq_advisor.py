
from remote.remote_util import RemoteMachineShellConnection
from .tuq import QueryTests
import time
from deepdiff import DeepDiff
from membase.api.exception import CBQError
import threading

class QueryAdvisorTests(QueryTests):

    def setUp(self):
        super(QueryAdvisorTests, self).setUp()
        self.log.info("==============  QueryAdvisorTests setup has started ==============")
        self.index_to_be_created = self.input.param("index_to_be_created", '')

        if self.load_sample:
            self.rest.load_sample("travel-sample")
            init_time = time.time()
            while True:
                next_time = time.time()
                query_response = self.run_cbq_query("SELECT COUNT(*) FROM `" + self.bucket_name + "`")
                self.log.info(f"{self.bucket_name}+ count: {query_response['results'][0]['$1']}")
                if query_response['results'][0]['$1'] == 31591:
                    break
                if next_time - init_time > 600:
                    break
                time.sleep(2)

            self.wait_for_all_indexes_online()
            list_of_indexes = self.run_cbq_query(query="select raw name from system:indexes WHERE indexes.bucket_id is missing")

            for index in list_of_indexes['results']:
                if index == "def_primary" or index == "#primary":
                    continue
                else:
                    self.run_cbq_query(query="drop index `travel-sample`.`%s`" % index)

        self.purge_all_sessions()

        self.log.info("==============  QueryAdvisorTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryAdvisorTests, self).suite_setUp()
        self.log.info("==============  QueryAdvisorTests suite_setup has started ==============")
        self.log.info("==============  QueryAdvisorTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueryAdvisorTests tearDown has started ==============")
        travel_sample = self.get_bucket_from_name("travel-sample")
        if travel_sample:
            self.delete_bucket(travel_sample)
        self.log.info("==============  QueryAdvisorTests tearDown has completed ==============")
        super(QueryAdvisorTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QueryAdvisorTests suite_tearDown has started ==============")
        self.log.info("==============  QueryAdvisorTests suite_tearDown has completed ==============")
        super(QueryAdvisorTests, self).suite_tearDown()

    def wait_for_index_online(self, bucket, index, scope='default', collection='default'):
        if scope == 'default' and collection == 'default':
            query_index = f"SELECT state FROM system:indexes where keyspace_id = '{bucket}' and name = '{index}'"
        else:
            query_index = f"SELECT state FROM system:indexes where bucket_id = '{bucket}' and scope_id = '{scope}' and keyspace_id = '{collection}' and name = '{index}'"
        init_time = time.time()
        while True:
            next_time = time.time()
            query_response = self.run_cbq_query(query=query_index)
            self.log.info(f"{index} response: {query_response}")
            if query_response['results'][0]['state'] == 'online':
                break
            if next_time - init_time > 600:
                break
            time.sleep(2)

    def get_statements(self, advisor_results):
        indexes = []
        statements = []
        for index in advisor_results['results'][0]['$1']['recommended_indexes']:
            indexes.append(index['index'])
            statements.append(index['statements'])
        return indexes, statements

    def purge_all_sessions(self):
        try:
            self.log.info("Purging all previous sessions")
            results = self.run_cbq_query(query="SELECT ADVISOR({'action':'list'}) as List", server=self.master)
            for task in results['results'][0]['List']:
                session = task['tasks_cache']['name']
                purge = self.run_cbq_query(query="SELECT ADVISOR({{'action':'purge', 'session':'{0}'}}) as Purge".format(session), server=self.master)
        except Exception as e:
            self.log.error("List/Purge sessions failed: {0}".format(e))
            self.fail()

    # Advisor on update statement
    def test_query_string(self):
        try:
            advise = self.run_cbq_query(query="SELECT ADVISOR(\"UPDATE `{0}` SET city = 'San Francisco' WHERE lower(city) = 'sanfrancisco'\")".format(self.bucket_name), server=self.master)
            simple_indexes, statements = self.get_statements(advise)
        except Exception as e:
            self.log.error("Advisor statement failed: {0}".format(e))
            self.fail()
        for index in simple_indexes:
            self.run_cbq_query(query=index)
            self.wait_for_all_indexes_online()
            try:
                results_with_advise_index = self.run_cbq_query(query="UPDATE `{0}` SET city = 'SF' WHERE lower(city) = 'san francisco'".format(self.bucket_name), server=self.master)
                self.assertEqual(results_with_advise_index['status'], 'success')
                self.assertEqual(results_with_advise_index['metrics']['mutationCount'], 938)
            finally:
                index_name = index.split("INDEX")[1].split("ON")[0].strip()
                self.run_cbq_query("DROP INDEX `{0}`.{1}".format(self.bucket_name,index_name))

    # same query: query count should be > 1
    def test_same_query_array(self):
        try:
            results_simple = self.run_cbq_query(query="SELECT ADVISOR([ \
                \"UPDATE `{0}` SET city = 'San Francisco' WHERE lower(city) = 'sanfrancisco'\", \
                \"UPDATE `{0}` SET city = 'San Francisco' WHERE lower(city) = 'sanfrancisco'\" \
            ])".format(self.bucket_name), server=self.master)
            simple_indexes, statements = self.get_statements(results_simple)
            self.assertEqual(statements[0][0]['run_count'], 2)
        except Exception as e:
            self.log.error("Advisor statement failed: {0}".format(e))
            self.fail()

    # similar query: statement count should be > 1
    def test_similar_query_array(self):    
        try:
            results_simple = self.run_cbq_query(query="SELECT ADVISOR([ \
                \"UPDATE `{0}` SET city = 'San Francisco' WHERE lower(city) = 'sanfrancisco'\", \
                \"UPDATE `{0}` SET city = 'San Francisco' WHERE lower(city) = 'saintfrancois'\" \
            ])".format(self.bucket_name), server=self.master)
            simple_indexes, statements = self.get_statements(results_simple)
            self.assertEqual(len(statements[0]), 2)
        except Exception as e:
            self.log.error("Advisor statement failed: {0}".format(e))
            self.fail()

    def test_diff_query_array(self):
        query1 = f"UPDATE `{self.bucket_name}` SET city = 'San Francisco' WHERE lower(city) = 'sanfrancisco'"
        query2 = f"SELECT name, city FROM `{self.bucket_name}` WHERE type = 'hotel' AND country = 'France'"
        query3 = f"SELECT airportname FROM `{self.bucket_name}` WHERE type = 'airport' AND lower(city) = 'lyon'"
        try:
            advise = self.run_cbq_query(query=f"SELECT ADVISOR([\"{query1}\", \"{query2}\", \"{query3}\"])", server=self.master)
            self.assertEqual(len(advise['results'][0]['$1']['recommended_indexes']), 3)
        except Exception as e:
            self.log.error("Advisor statement failed: {0}".format(e))
            self.fail()


    def test_query_output_array(self):
        # Run some queries
        query_paris = "SELECT airportname FROM `{0}` WHERE type = 'airport' and lower(city) = 'paris' AND country = 'France'".format(self.bucket_name)
        query_lyon = "SELECT airportname FROM `{0}` WHERE type ='airport' and lower(city) = 'lyon' AND country = 'France'".format(self.bucket_name)
        query_grenoble = "SELECT airportname FROM `{0}` WHERE type = 'airport' and lower(city) = 'grenoble' AND country = 'France'".format(self.bucket_name)
        results = self.run_cbq_query(query=query_paris, server=self.master)
        results = self.run_cbq_query(query=query_paris, server=self.master)
        results = self.run_cbq_query(query=query_paris, server=self.master)
        results = self.run_cbq_query(query=query_lyon, server=self.master)
        results = self.run_cbq_query(query=query_lyon, server=self.master)
        results = self.run_cbq_query(query=query_grenoble, server=self.master)
        try:
            results = self.run_cbq_query(query="select ADVISOR((SELECT RAW statement FROM system:completed_requests order by requestTime DESC limit 6)) as `Advise`".format(self.bucket_name), server=self.master)
            advises = results['results'][0]['Advise']
            query_count = dict()
            for index in advises['recommended_indexes']:
                for query in index['statements']:
                    query_count[query['statement']] = query['run_count']
            self.assertEqual(query_count[query_paris], 3)
            self.assertEqual(query_count[query_lyon], 2)
            self.assertEqual(query_count[query_grenoble], 1)
        except Exception as e:
            self.log.error("Advisor statement failed: {0}".format(e))
            self.fail()

    def test_query_array_arg_large(self,num=10):
        query_paris = "SELECT airportname FROM `{0}` WHERE type = 'airport' and lower(city) = 'paris' AND country = 'France'".format(self.bucket_name)
        query_array = [query_paris] * num
        try:
            results = self.run_cbq_query(query="select ADVISOR({0}) as `Advise`".format(query_array), server=self.master)
            advises = results['results'][0]['Advise']
            self.assertEqual(advises['recommended_indexes'][0]['statements'][0]['run_count'], num)
            self.assertEqual(advises['recommended_indexes'][0]['statements'][0]['statement'], query_paris)
        except Exception as e:
            self.log.error("Advisor statement failed: {0}".format(e))
            self.fail()

    # get session recommendation for completed session
    def test_get_session_completed(self):
        try:
            results = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '10s', 'query_count': 2 })", server=self.master)
            session = results['results'][0]['$1']['session']
            results = self.run_cbq_query(query="SELECT airportname FROM `{0}` WHERE lower(city) = 'lyon' AND country = 'France'".format(self.bucket_name), server=self.master)
            results = self.run_cbq_query(query="SELECT airportname FROM `{0}` WHERE lower(city) = 'lyon' AND country = 'France'".format(self.bucket_name), server=self.master)
            # Wait for session to complete
            self.sleep(10)
            results = self.run_cbq_query(query="SELECT ADVISOR({{'action': 'get', 'session': '{0}'}})".format(session), server=self.master)
            self.assertTrue('recommended_indexes' in results['results'][0]['$1'][0][0], "There are no recommended index: {0}".format(results['results'][0]['$1'][0][0]))
        except Exception as e:
            self.log.error("Advisor session failed: {0}".format(e))
            self.fail()

    def test_get_session_stopped(self):
        try:
            results = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '1h', 'query_count': 2 })", server=self.master)
            session = results['results'][0]['$1']['session']
            results = self.run_cbq_query(query="SELECT airportname FROM `{0}` WHERE lower(city) = 'lyon' AND country = 'France'".format(self.bucket_name), server=self.master)
            results = self.run_cbq_query(query="SELECT airportname FROM `{0}` WHERE lower(city) = 'lyon' AND country = 'France'".format(self.bucket_name), server=self.master)
            self.sleep(3)

            results = self.run_cbq_query(query="SELECT ADVISOR({{'action': 'stop', 'session': '{0}'}})".format(session), server=self.master)
            # Stop a second time to ensure no side effect (see MB-48576)
            results = self.run_cbq_query(query="SELECT ADVISOR({{'action': 'stop', 'session': '{0}'}})".format(session), server=self.master)
            results = self.run_cbq_query(query="SELECT ADVISOR({{'action': 'get', 'session': '{0}'}})".format(session), server=self.master)
            self.assertTrue('recommended_indexes' in results['results'][0]['$1'][0][0], "There are no recommended index: {0}".format(results['results'][0]['$1'][0][0]))
        except Exception as e:
            self.log.error("Advisor session failed: {0}".format(e))
            self.fail()

    def test_stop_session(self):
        try:
            results = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '1234567ms', 'query_count': 2 })", server=self.master)
            session = results['results'][0]['$1']['session']
            results = self.run_cbq_query(query="SELECT ADVISOR({{'action': 'stop', 'session': '{0}'}})".format(session), server=self.master)
            results = self.run_cbq_query(query="SELECT ADVISOR({'action':'list'}) as List", server=self.master)
            task = results['results'][0]['List'][0]['tasks_cache']
            self.log.info("Task cache is {0}".format(task))
            self.assertEqual(list(task.keys()), ['class', 'delay', 'id', 'name', 'node', 'queryContext','results', 'state', 'subClass', 'submitTime'])
            self.assertEqual(task['state'], "cancelled")
            self.assertEqual(task['delay'], "20m34.567s")
        except Exception as e:
            self.log.error("Advisor session failed: {0}".format(e))
            self.fail()

    def test_abort_session(self):
        try:
            results = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '3600s', 'query_count': 200 })", server=self.master)
            session = results['results'][0]['$1']['session']
            # Check session is active
            results = self.run_cbq_query(query="SELECT ADVISOR({'action':'list', 'status': 'active'}) as List", server=self.master)
            task = results['results'][0]['List'][0]['tasks_cache']
            self.log.info("Task cache is {0}".format(task))
            self.assertEqual(task['state'], "scheduled")
            self.assertEqual(task['delay'], "1h0m0s")
            self.assertEqual(task['name'], session)
            # Abort session
            results = self.run_cbq_query(query="SELECT ADVISOR({{'action': 'abort', 'session': '{0}'}})".format(session), server=self.master)
            results = self.run_cbq_query(query="SELECT ADVISOR({'action':'list', 'status': 'all'}) as List", server=self.master)
            self.assertEqual(results['results'][0]['List'],[])
        except Exception as e:
            self.log.error("Advisor session failed: {0}".format(e))
            self.fail()

    def test_purge_session_completed(self):
        try:
            results = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '5000ms', 'query_count': 2 })", server=self.master)
            session = results['results'][0]['$1']['session']
            results = self.run_cbq_query(query="SELECT airportname FROM `{0}` WHERE lower(city) = 'lyon' AND country = 'France'".format(self.bucket_name), server=self.master)
            results = self.run_cbq_query(query="SELECT airportname FROM `{0}` WHERE lower(city) = 'lyon' AND country = 'France'".format(self.bucket_name), server=self.master)
            # Wait for session to complete
            self.sleep(5)
            results = self.run_cbq_query(query="SELECT ADVISOR({'action':'list'}) as List", server=self.master)
            task = results['results'][0]['List'][0]['tasks_cache']
            self.assertEqual(task['state'], "completed")
            # Purge session
            results = self.run_cbq_query(query="SELECT ADVISOR({{'action': 'purge', 'session': '{0}'}})".format(session), server=self.master)
            results = self.run_cbq_query(query="SELECT ADVISOR({'action':'list', 'status': 'all'}) as List", server=self.master)
            self.assertEqual(results['results'][0]['List'],[])
        except Exception as e:
            self.log.error("Advisor session failed: {0}".format(e))
            self.fail()

    def test_purge_session_stopped(self):
        try:
            results = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '5000s', 'query_count': 2 })", server=self.master)
            session = results['results'][0]['$1']['session']
            results = self.run_cbq_query(query="SELECT airportname FROM `{0}` WHERE lower(city) = 'lyon' AND country = 'France'".format(self.bucket_name), server=self.master)
            results = self.run_cbq_query(query="SELECT airportname FROM `{0}` WHERE lower(city) = 'lyon' AND country = 'France'".format(self.bucket_name), server=self.master)
            # Stop session
            results = self.run_cbq_query(query="SELECT ADVISOR({{'action': 'stop', 'session': '{0}'}})".format(session), server=self.master)
            results = self.run_cbq_query(query="SELECT ADVISOR({'action':'list'}) as List", server=self.master)
            task = results['results'][0]['List'][0]['tasks_cache']
            self.assertEqual(task['state'], "cancelled")
            # Purge session
            results = self.run_cbq_query(query="SELECT ADVISOR({{'action': 'purge', 'session': '{0}'}})".format(session), server=self.master)
            results = self.run_cbq_query(query="SELECT ADVISOR({'action':'list', 'status': 'all'}) as List", server=self.master)
            self.assertEqual(results['results'][0]['List'],[])
        except Exception as e:
            self.log.error("Advisor session failed: {0}".format(e))
            self.fail()

    def test_purge_session_active(self):
        try:
            results = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '3600s', 'query_count': 200 })", server=self.master)
            session = results['results'][0]['$1']['session']
            # Check session is active
            list_all = self.run_cbq_query(query="SELECT ADVISOR({'action':'list', 'status': 'active'}) as List", server=self.master)
            task = list_all['results'][0]['List'][0]['tasks_cache']
            self.log.info("Task cache is {0}".format(task))
            self.assertEqual(task['state'], "scheduled")
            self.assertEqual(task['delay'], "1h0m0s")
            self.assertEqual(task['name'], session)
            # Purge session
            purge = self.run_cbq_query(query="SELECT ADVISOR({{'action': 'purge', 'session': '{0}'}})".format(session), server=self.master)
            list_all = self.run_cbq_query(query="SELECT ADVISOR({'action':'list', 'status': 'all'}) as List", server=self.master)
            self.assertEqual(list_all['results'][0]['List'],[])
        except Exception as e:
            self.log.error("Advisor session failed: {0}".format(e))
            self.fail()

    def test_list_session(self):
        try:
            results = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '99h', 'query_count': 2 })", server=self.master)
            active_session = results['results'][0]['$1']['session']
            results = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '50ms', 'query_count': 2 })", server=self.master)
            completed_session = results['results'][0]['$1']['session']
            results = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '1600m', 'query_count': 2 })", server=self.master)
            stopped_session = results['results'][0]['$1']['session']
            results = self.run_cbq_query(query="SELECT airportname FROM `{0}` WHERE lower(city) = 'lyon' AND country = 'France'".format(self.bucket_name), server=self.master)
            results = self.run_cbq_query(query="SELECT airportname FROM `{0}` WHERE lower(city) = 'lyon' AND country = 'France'".format(self.bucket_name), server=self.master)
            # Stop session
            results = self.run_cbq_query(query="SELECT ADVISOR({{'action': 'stop', 'session': '{0}'}})".format(stopped_session), server=self.master)
            # List sessions
            results = self.run_cbq_query(query="SELECT ADVISOR({'action':'list'}) as List", server=self.master)
            all_sessions = dict()
            for task in results['results'][0]['List']:
                all_sessions[task['tasks_cache']['state']] = task['tasks_cache']['name']
            self.assertEqual(len(all_sessions), 3)
            self.assertEqual(all_sessions['scheduled'], active_session)
            self.assertEqual(all_sessions['cancelled'], stopped_session)
            self.assertEqual(all_sessions['completed'], completed_session)
        except Exception as e:
            self.log.error("Advisor session failed: {0}".format(e))
            self.fail()

    def test_list_session_active(self):
        try:
            results = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '99h', 'query_count': 2 })", server=self.master)
            active_session = results['results'][0]['$1']['session']
            results = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '50ms', 'query_count': 2 })", server=self.master)
            completed_session = results['results'][0]['$1']['session']
            results = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '1600m', 'query_count': 2 })", server=self.master)
            stopped_session = results['results'][0]['$1']['session']
            results = self.run_cbq_query(query="SELECT airportname FROM `{0}` WHERE lower(city) = 'lyon' AND country = 'France'".format(self.bucket_name), server=self.master)
            results = self.run_cbq_query(query="SELECT airportname FROM `{0}` WHERE lower(city) = 'lyon' AND country = 'France'".format(self.bucket_name), server=self.master)
            # Stop session
            results = self.run_cbq_query(query="SELECT ADVISOR({{'action': 'stop', 'session': '{0}'}})".format(stopped_session), server=self.master)
            # List ACTIVE sessions
            results = self.run_cbq_query(query="SELECT ADVISOR({'action':'list', 'status':'active'}) as List", server=self.master)
            all_sessions = dict()
            for task in results['results'][0]['List']:
                all_sessions[task['tasks_cache']['state']] = task['tasks_cache']['name']
            self.assertEqual(len(all_sessions), 1)
            self.assertEqual(all_sessions['scheduled'], active_session)
        except Exception as e:
            self.log.error("Advisor session failed: {0}".format(e))
            self.fail()    
            
    def test_list_session_completed(self):
        try:
            results = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '99h', 'query_count': 2 })", server=self.master)
            active_session = results['results'][0]['$1']['session']
            results = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '50ms', 'query_count': 2 })", server=self.master)
            completed_session = results['results'][0]['$1']['session']
            results = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '1600m', 'query_count': 2 })", server=self.master)
            stopped_session = results['results'][0]['$1']['session']
            results = self.run_cbq_query(query="SELECT airportname FROM `{0}` WHERE lower(city) = 'lyon' AND country = 'France'".format(self.bucket_name), server=self.master)
            results = self.run_cbq_query(query="SELECT airportname FROM `{0}` WHERE lower(city) = 'lyon' AND country = 'France'".format(self.bucket_name), server=self.master)
            # Stop session
            results = self.run_cbq_query(query="SELECT ADVISOR({{'action': 'stop', 'session': '{0}'}})".format(stopped_session), server=self.master)
            # List COMPLETED sessions
            results = self.run_cbq_query(query="SELECT ADVISOR({'action':'list', 'status':'completed'}) as List", server=self.master)
            all_sessions = dict()
            for task in results['results'][0]['List']:
                all_sessions[task['tasks_cache']['state']] = task['tasks_cache']['name']
            self.assertEqual(len(all_sessions), 1)
            self.assertEqual(all_sessions['completed'], completed_session)
        except Exception as e:
            self.log.error("Advisor session failed: {0}".format(e))
            self.fail()    
    
    def test_list_session_all(self):
        try:
            results = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '99h', 'query_count': 2 })", server=self.master)
            active_session = results['results'][0]['$1']['session']
            results = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '50ms', 'query_count': 2 })", server=self.master)
            completed_session = results['results'][0]['$1']['session']
            results = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '1600m', 'query_count': 2 })", server=self.master)
            stopped_session = results['results'][0]['$1']['session']
            results = self.run_cbq_query(query="SELECT airportname FROM `{0}` WHERE lower(city) = 'lyon' AND country = 'France'".format(self.bucket_name), server=self.master)
            results = self.run_cbq_query(query="SELECT airportname FROM `{0}` WHERE lower(city) = 'lyon' AND country = 'France'".format(self.bucket_name), server=self.master)
            # Stop session
            results = self.run_cbq_query(query="SELECT ADVISOR({{'action': 'stop', 'session': '{0}'}})".format(stopped_session), server=self.master)
            # List ALL sessions
            results = self.run_cbq_query(query="SELECT ADVISOR({'action':'list', 'status':'all'}) as List", server=self.master)
            all_sessions = dict()
            for task in results['results'][0]['List']:
                all_sessions[task['tasks_cache']['state']] = task['tasks_cache']['name']
            self.assertEqual(len(all_sessions), 3)
            self.assertEqual(all_sessions['scheduled'], active_session)
            self.assertEqual(all_sessions['cancelled'], stopped_session)
            self.assertEqual(all_sessions['completed'], completed_session)
        except Exception as e:
            self.log.error("Advisor session failed: {0}".format(e))
            self.fail()

    def test_start_session_duration_value(self):
        durations = ['3600000000000ns','3600000000us','3600000ms','3600s','60m', '1h']
        try:
            for duration in durations:
                start = self.run_cbq_query(query="SELECT ADVISOR({{'action': 'start', 'duration': '{0}'}})".format(duration), server=self.master)
                session = start['results'][0]['$1']['session']
                active = self.run_cbq_query(query="SELECT ADVISOR({'action':'list'}) as List", server=self.master)
                delay = active['results'][0]['List'][0]['tasks_cache']['delay']
                self.assertEqual(delay, '1h0m0s')
                abort = self.run_cbq_query(query="SELECT ADVISOR({{'action':'abort', 'session':'{0}'}}) as Abort".format(session), server=self.master)
        except Exception as e:
            self.log.error("Advisor session failed: {0}".format(e))
            self.fail()

    def test_session_duration_completed(self):
        durations = ['1800000000ns','1800000us','1800ms','1.8s','0.03m', '0.0005h']
        try:
            for duration in durations:
                start = self.run_cbq_query(query="SELECT ADVISOR({{'action': 'start', 'duration': '{0}'}})".format(duration), server=self.master)
                session = start['results'][0]['$1']['session']
                self.sleep(3)
                complete = self.run_cbq_query(query="SELECT ADVISOR({'action':'list','status':'completed'}) as List", server=self.master)
                name = complete['results'][0]['List'][0]['tasks_cache']['name']
                delay = complete['results'][0]['List'][0]['tasks_cache']['delay']
                state = complete['results'][0]['List'][0]['tasks_cache']['state']
                self.assertEqual(delay, '1.8s')
                self.assertEqual(name, session)
                self.assertEqual(state, "completed")
                purge = self.run_cbq_query(query="SELECT ADVISOR({{'action':'purge', 'session':'{0}'}}) as Purge".format(session), server=self.master)
        except Exception as e:
            self.log.error("Advisor session failed: {0}".format(e))
            self.fail()

    def test_session_response_below(self):
        responses = ['100000000ns','100000us','100ms','0.1s', '0.000027h']
        query1=f"SELECT airportname FROM `{self.bucket_name}` WHERE type = 'airport' AND lower(city) = 'lyon' AND country = 'France'"
        query2=f"SELECT airportname FROM `{self.bucket_name}` WHERE type = 'airport' AND lower(city) = 'grenoble' AND country = 'France'"
        query3=f"SELECT airportname FROM `{self.bucket_name}` WHERE type = 'airport' AND lower(city) = 'nice' AND country = 'France'"
        try:
            for response in responses:
                start = self.run_cbq_query(query="SELECT ADVISOR({{'action': 'start', 'duration': '60s', 'response': '{0}'}})".format(response), server=self.master)
                session = start['results'][0]['$1']['session']
                results = self.run_cbq_query(query=query1, server=self.master)
                results = self.run_cbq_query(query=query2, server=self.master)
                results = self.run_cbq_query(query=query3, server=self.master)
                stop = self.run_cbq_query(query="SELECT ADVISOR({{'action':'stop', 'session':'{0}'}}) as Stop".format(session), server=self.master)
                get = self.run_cbq_query(query="SELECT ADVISOR({{'action':'get', 'session':'{0}'}}) as Get".format(session), server=self.master)
                run_count = get['results'][0]['Get'][0][0]['recommended_indexes'][0]['statements'][0]['run_count']
                self.assertEqual(run_count, 1)
                purge = self.run_cbq_query(query="SELECT ADVISOR({{'action':'purge', 'session':'{0}'}}) as Purge".format(session), server=self.master)
        except Exception as e:
            self.log.error("Advisor session failed: {0}".format(e))
            self.fail()

    def test_session_response_above(self):
        responses = ['9000000000000ns','9000000000us','9000000ms','9000s', '0.25h']
        query1=f"SELECT airportname FROM `{self.bucket_name}` WHERE type = 'airport' AND lower(city) = 'lyon' AND country = 'France'"
        query2=f"SELECT airportname FROM `{self.bucket_name}` WHERE type = 'airport' AND lower(city) = 'grenoble' AND country = 'France'"
        query3=f"SELECT airportname FROM `{self.bucket_name}` WHERE type = 'airport' AND lower(city) = 'nice' AND country = 'France'"
        try:
            for response in responses:
                start = self.run_cbq_query(query="SELECT ADVISOR({{'action': 'start', 'duration': '60s', 'response': '{0}'}})".format(response), server=self.master)
                session = start['results'][0]['$1']['session']
                results = self.run_cbq_query(query=query1, server=self.master)
                results = self.run_cbq_query(query=query2, server=self.master)
                results = self.run_cbq_query(query=query3, server=self.master)
                stop = self.run_cbq_query(query="SELECT ADVISOR({{'action':'stop', 'session':'{0}'}}) as Stop".format(session), server=self.master)
                get = self.run_cbq_query(query="SELECT ADVISOR({{'action':'get', 'session':'{0}'}}) as Get".format(session), server=self.master)
                advise = get['results'][0]['Get'][0]
                self.assertEqual(advise, [[]])
                purge = self.run_cbq_query(query="SELECT ADVISOR({{'action':'purge', 'session':'{0}'}}) as Purge".format(session), server=self.master)
        except Exception as e:
            self.log.error("Advisor session failed: {0}".format(e))
            self.fail()

    def test_session_profile(self):
        self.users = [{"id": "johnDoe", "name": "Jonathan Downing", "password": "password1"}]
        self.create_users()
        grant = self.run_cbq_query(query="GRANT {0} to {1}".format("admin", self.users[0]['id']),server=self.master)

        query1=f'SELECT airportname FROM `{self.bucket_name}` WHERE type = "airport" AND lower(city) = "lyon" AND country = "France"'
        query2=f'SELECT airportname FROM `{self.bucket_name}` WHERE type = "airport" AND lower(city) = "grenoble" AND country = "France"'
        query3=f'SELECT airportname FROM `{self.bucket_name}` WHERE type = "airport" AND lower(city) = "nice" AND country = "France"'

        try:
            start = self.run_cbq_query(query="SELECT ADVISOR({{'action': 'start', 'duration': '180s', 'profile': '{0}'}})".format(self.users[0]['id']), server=self.master)
            session = start['results'][0]['$1']['session']
            # Run query as other user
            # results = self.curl_with_roles(query1)
            # results = self.curl_with_roles(query1)
            results = self.run_cbq_query(query=query1, username=self.users[0]['id'], password=self.users[0]['password'], server=self.master)
            results = self.run_cbq_query(query=query1, username=self.users[0]['id'], password=self.users[0]['password'], server=self.master)
            # run query as current user
            results = self.run_cbq_query(query=query2, server=self.master)
            
            stop = self.run_cbq_query(query="SELECT ADVISOR({{'action':'stop', 'session':'{0}'}}) as Stop".format(session), server=self.master)
            get = self.run_cbq_query(query="SELECT ADVISOR({{'action':'get', 'session':'{0}'}}) as Get".format(session), server=self.master)

            for index in get['results'][0]['Get'][0][0]['recommended_indexes']:
                for statement in index['statements']:
                    self.assertEqual(statement['statement'], query1)
        except Exception as e:
            self.log.error("Advisor session failed: {0}".format(e))
            self.fail()

    def test_session_all(self):
        self.users = [{"id": "joaoDoe", "name": "Joao Downing", "password": "password1"}]
        self.create_users()
        user_id = self.users[0]['id']
        user_pwd = self.users[0]['password']
        grant = self.run_cbq_query(query=f"GRANT admin to {user_id}",server=self.master)
        query1=f'SELECT airportname FROM `{self.bucket_name}` WHERE type = "airport" AND lower(city) = "lyon" AND country = "France"'
        query2=f'SELECT airportname FROM `{self.bucket_name}` WHERE type = "airport" AND lower(city) = "grenoble" AND country = "France"'
        query3=f'SELECT airportname FROM `{self.bucket_name}` WHERE type = "airport" AND lower(city) = "nice" AND country = "France"'
        try:
            start = self.run_cbq_query(query="SELECT ADVISOR({{'action':'start', 'duration':'40m', 'profile': '{0}', 'query_count':5, 'response':'50ms'}})".format(self.users[0]['id']), server=self.master)
            session = start['results'][0]['$1']['session']
            # Run query as other user
            results = self.run_cbq_query(query=query1, username=user_id, password=user_pwd, server=self.master)
            results = self.run_cbq_query(query=query1, username=user_id, password=user_pwd, server=self.master)
            # Run query as current user
            results = self.run_cbq_query(query=query2, server=self.master)
            # Run query as other user
            results = self.run_cbq_query(query=query1, username=user_id, password=user_pwd, server=self.master)
            results = self.run_cbq_query(query=query1, username=user_id, password=user_pwd, server=self.master)
            results = self.run_cbq_query(query=query1, username=user_id, password=user_pwd, server=self.master)
            results = self.run_cbq_query(query=query1, username=user_id, password=user_pwd, server=self.master)
            # Stop and get session
            stop = self.run_cbq_query(query="SELECT ADVISOR({{'action':'stop', 'session':'{0}'}}) as Stop".format(session), server=self.master)
            get = self.run_cbq_query(query="SELECT ADVISOR({{'action':'get', 'session':'{0}'}}) as Get".format(session), server=self.master)
            # Check advise
            for index in get['results'][0]['Get'][0][0]['recommended_indexes']:
                for statement in index['statements']:
                    self.assertEqual(statement['statement'], query1)
                    self.assertEqual(statement['run_count'], 5)
            # Purge and list session
            purge = self.run_cbq_query(query=f"SELECT ADVISOR({{'action':'purge', 'session':'{session}'}}) as Get", server=self.master)
            list_all = self.run_cbq_query(query="SELECT ADVISOR({'action':'list', 'status': 'all'}) as List", server=self.master)
            self.assertEqual(list_all['results'][0]['List'],[])
        except Exception as e:
            self.log.error("Advisor session failed: {0}".format(e))
            self.fail()

    def test_session_cbo(self):
        advise_index1 = "CREATE INDEX adv_lower_city_country_type ON `travel-sample`(lower(`city`),`country`) WHERE `type` = 'airport'"
        advise_index2 = "CREATE INDEX adv_country_lower_city_type ON `travel-sample`(`country`,lower(`city`)) WHERE `type` = 'airport'"
        advise_stats = "UPDATE STATISTICS FOR `travel-sample`(lower(`city`), `country`, `type`)"
        query1=f'SELECT airportname FROM `{self.bucket_name}` WHERE type = "airport" AND lower(city) = "lyon" AND country = "France"'
        # update stats to ensure CBO is used
        stats = self.run_cbq_query(query=f"update statistics for `{self.bucket_name}`(type)", server=self.master)
        self.sleep(3)
        try:
            start = self.run_cbq_query(query="SELECT ADVISOR({'action':'start', 'duration':'40m'})", server=self.master)
            session = start['results'][0]['$1']['session']
            results = self.run_cbq_query(query=query1, server=self.master)
            stop = self.run_cbq_query(query=f"SELECT ADVISOR({{'action':'stop', 'session':'{session}'}}) as Stop", server=self.master)
            get = self.run_cbq_query(query=f"SELECT ADVISOR({{'action':'get', 'session':'{session}'}}) as Get", server=self.master)
            # Check advise
            self.log.info(f"Advise: {get['results']}")
            for index in get['results'][0]['Get'][0][0]['recommended_indexes']:
                self.assertTrue(index['index'] == advise_index1 or index['index'] == advise_index2)
                self.assertEqual(index['update_statistics'], advise_stats)
        except Exception as e:
            self.log.error(f"Advisor session failed: {e}")
            self.fail()

    def test_session_query_txn(self):
        query1=f'SELECT airportname FROM `{self.bucket_name}` WHERE type = "airport" AND lower(city) = "lyon" AND country = "France"'
        close_txn = ['ROLLBACK WORK', 'COMMIT']
        try:
            for rollback_or_commit in close_txn:
                start = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '15m'})", server=self.master)
                session = start['results'][0]['$1']['session']
                # Run query in transaction
                results = self.run_cbq_query(query="BEGIN WORK", server=self.master)
                txid = results['results'][0]['txid']
                results = self.run_cbq_query(query=query1, txnid=txid, server=self.master)
                results = self.run_cbq_query(query=rollback_or_commit, txnid=txid, server=self.master)
                # Stop and check session advise
                stop = self.run_cbq_query(query="SELECT ADVISOR({{'action':'stop', 'session':'{0}'}}) as Stop".format(session), server=self.master)
                get = self.run_cbq_query(query="SELECT ADVISOR({{'action':'get', 'session':'{0}'}}) as Get".format(session), server=self.master)
                for index in get['results'][0]['Get'][0][0]['recommended_indexes']:
                    for statement in index['statements']:
                        self.assertEqual(statement['statement'], query1)
        except Exception as e:
            self.log.error("Advisor session failed: {0}".format(e))
            self.fail()

    def test_negative_txn(self):
        results = self.run_cbq_query(query="BEGIN WORK", server=self.master)
        txid = results['results'][0]['txid']
        error = "advisor function is not supported within the transaction"
        try:
            start = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '15m'})", txnid=txid, server=self.master)
            self.fail("Start session did not fail. Error expected: {0}".format(error))
        except CBQError as ex:
            self.assertTrue(str(ex).find(error) > 0)
        else:
            self.fail("There were no errors. Error expected: {0}".format(error))

    def test_session_query_count(self):
        query_lyon=f'SELECT airportname FROM `{self.bucket_name}` WHERE type = "airport" AND lower(city) = "lyon" AND country = "France"'
        query_grenoble=f'SELECT airportname FROM `{self.bucket_name}` WHERE type = "airport" AND lower(city) = "grenoble" AND country = "France"'
        query_nice=f'SELECT airportname FROM `{self.bucket_name}` WHERE type = "airport" AND lower(city) = "nice" AND country = "France"'
        try:
            start = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '15m', 'query_count': 6})", server=self.master)
            session = start['results'][0]['$1']['session']
            # Run 9 queries 
            results = self.run_cbq_query(query=query_lyon, server=self.master)
            results = self.run_cbq_query(query=query_grenoble, server=self.master)
            results = self.run_cbq_query(query=query_nice, server=self.master)
            results = self.run_cbq_query(query=query_lyon, server=self.master)
            results = self.run_cbq_query(query=query_grenoble, server=self.master)
            results = self.run_cbq_query(query=query_lyon, server=self.master)
            results = self.run_cbq_query(query=query_nice, server=self.master)
            results = self.run_cbq_query(query=query_grenoble, server=self.master)
            results = self.run_cbq_query(query=query_nice, server=self.master)
            # Stop and check session advise. We should only see 6 queries count = 3*lyon + 2*grenoble + 1*nice
            stop = self.run_cbq_query(query="SELECT ADVISOR({{'action':'stop', 'session':'{0}'}}) as Stop".format(session), server=self.master)
            get = self.run_cbq_query(query="SELECT ADVISOR({{'action':'get', 'session':'{0}'}}) as Get".format(session), server=self.master)
            queries_count = dict()
            for index in get['results'][0]['Get'][0][0]['recommended_indexes']:
                for query in index['statements']:
                    queries_count[query['statement']] = query['run_count']
            self.assertEqual(queries_count[query_lyon], 3)
            self.assertEqual(queries_count[query_grenoble], 2)
            self.assertEqual(queries_count[query_nice], 1)
        except Exception as e:
            self.log.error("Advisor session failed: {0}".format(e))
            self.fail()

    def test_session_skip_statement(self):
        """ 
            Non advisabeable statement (explain, advise, select advisor, prepare, execute) should
            not show in session.
        """        
        explain_query = 'EXPLAIN SELECT airportname FROM `travel-sample` WHERE type = "airport" AND lower(city) = "lyon" AND country = "France"'
        advisor_list_query = "SELECT ADVISOR ({'action': 'list'})"
        advise_query = 'ADVISE SELECT airportname FROM `travel-sample` WHERE type = "airport" AND lower(city) = "lyon" AND country = "France"'
        prepare_query = 'PREPARE aeroport_de_lyon AS SELECT airportname FROM `travel-sample`.`inventory`.`airport` WHERE lower(city) = "lyon" AND country = "France"'
        execute_prepared_query = 'EXECUTE aeroport_de_lyon'
        # For prepare statement
        self.run_cbq_query("CREATE PRIMARY INDEX ON `default`:`default`")
        # Start session
        start = self.run_cbq_query("SELECT ADVISOR({'action': 'start', 'duration': '45m'})")
        session = start['results'][0]['$1']['session']
        # Run queries. Explain, advisor, advise, prepare and execute should not show up in session advise.
        self.run_cbq_query(explain_query)
        self.run_cbq_query(advise_query)
        self.run_cbq_query(advisor_list_query)
        self.run_cbq_query(prepare_query)
        self.run_cbq_query(execute_prepared_query)
        # Stop and check session
        self.run_cbq_query(query=f"SELECT ADVISOR({{'action':'stop', 'session':'{session}'}}) as Stop")
        get = self.run_cbq_query(query=f"SELECT ADVISOR({{'action':'get', 'session':'{session}'}}) as Get")
        # Check there are no errors statement in advise session
        advise = get['results'][0]['Get'][0][0]
        self.assertEqual(advise, [])

    def test_session_skip_count(self):
        """ 
            Non advisabeable statement should
            not be counted toward query_count.
            Only query_lyon should be counted.
        """        
        advisor_list_query = "SELECT ADVISOR ({'action': 'list'})"
        explain_query = 'EXPLAIN SELECT airportname FROM `travel-sample` WHERE type = "airport" AND lower(city) = "lyon" AND country = "France"'
        advise_query = 'ADVISE SELECT airportname FROM `travel-sample` WHERE type = "airport" AND lower(city) = "lyon" AND country = "France"'
        prepare_query = 'PREPARE aeroport_de_lyon AS SELECT airportname FROM `travel-sample`.`inventory`.`airport` WHERE lower(city) = "lyon" AND country = "France"'
        execute_prepared_query = 'EXECUTE aeroport_de_lyon'
        update_stats_query = 'UPDATE STATISTICS FOR `travel-sample`.inventory.airport(city)'
        create_scope = "CREATE SCOPE `travel-sample`.scope1"
        drop_scope = "DROP SCOPE `travel-sample`.scope1"
        create_collection = "CREATE COLLECTION `travel-sample`.inventory.collection1"
        drop_collection = "DROP COLLECTION `travel-sample`.inventory.collection1"

        query_lyon='SELECT airportname FROM `travel-sample` WHERE type = "airport" AND lower(city) = "lyon" AND country = "France"'
        # Start session
        start = self.run_cbq_query("SELECT ADVISOR({'action': 'start', 'duration': '45m', 'query_count': 2})")
        session = start['results'][0]['$1']['session']
        # Run queries
        self.run_cbq_query(advisor_list_query)
        self.run_cbq_query(advisor_list_query)
        self.run_cbq_query(prepare_query)
        self.run_cbq_query(advise_query)
        self.run_cbq_query(update_stats_query)
        self.run_cbq_query(create_scope)
        self.run_cbq_query(create_collection)

        # First execution of query
        self.run_cbq_query(query_lyon)

        self.run_cbq_query(advisor_list_query)
        self.run_cbq_query(execute_prepared_query)
        self.run_cbq_query(explain_query)
        self.run_cbq_query(drop_scope)
        self.run_cbq_query(drop_collection)

        # Second execution of query
        self.run_cbq_query(query_lyon)
        # Stop and check session
        self.run_cbq_query(query=f"SELECT ADVISOR({{'action':'stop', 'session':'{session}'}}) as Stop")
        get = self.run_cbq_query(query=f"SELECT ADVISOR({{'action':'get', 'session':'{session}'}}) as Get")
        # Check the query_lyon ran 2 times
        advise = get['results'][0]['Get'][0][0]
        statements = advise['recommended_covering_indexes'][0]['statements']
        self.assertEqual(statements[0]['statement'], query_lyon)
        self.assertEqual(statements[0]['run_count'], 2)

    def test_get_active_session(self):
        try:
            results = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '5000s', 'query_count': 2 })", server=self.master)
            session = results['results'][0]['$1']['session']
            results = self.run_cbq_query(query="SELECT airportname FROM `{0}` WHERE lower(city) = 'lyon' AND country = 'France'".format(self.bucket_name), server=self.master)
            results = self.run_cbq_query(query="SELECT airportname FROM `{0}` WHERE lower(city) = 'lyon' AND country = 'France'".format(self.bucket_name), server=self.master)
            # Get session
            get = self.run_cbq_query(query="SELECT ADVISOR({{'action': 'get', 'session': '{0}'}}) as Get".format(session), server=self.master)
            self.assertEqual(get['results'][0]['Get'], [])
            # Abort session
            abort = self.run_cbq_query(query="SELECT ADVISOR({{'action': 'abort', 'session': '{0}'}})".format(session), server=self.master)
            results = self.run_cbq_query(query="SELECT ADVISOR({'action':'list', 'status': 'all'}) as List", server=self.master)
            self.assertEqual(results['results'][0]['List'],[])
        except Exception as e:
            self.log.error("Advisor session failed: {0}".format(e))
            self.fail()

    def test_negative_query_syntax_error(self):
        query_syntax = f'SELECT airportname FROM `{self.bucket_name}` WERE type = \\"airport\\"'
        error = "syntax error - line 1, column 53, near '...travel-sample` WERE ', at: type"
        try:
            advise = self.run_cbq_query(query=f"SELECT ADVISOR(\"{query_syntax}\") as Advisor", server=self.master)
            self.assertEqual(advise["results"][0]["Advisor"]["errors"][0]["error"], error)
            self.assertEqual(advise["results"][0]["Advisor"]["errors"][0]["run_count"], 1)
            self.assertEqual(advise["results"][0]["Advisor"]["errors"][0]["statement"], query_syntax.replace('\\',''))
        except Exception as e:
            self.log.error("Advisor session failed: {0}".format(e))
            self.fail()

    def test_negative_invalid_arg(self):
        query = "SELECT ADVISOR({'action': 'start', 'duration': '10s', 'invalid': 10});"
        error_message = "Advisor: Invalid arguments."
        error_code = 10503
        try:
            results = self.run_cbq_query(query=query, server=self.master)
            self.fail("Start session did not fail. Error expected: {0}".format(error_message))
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error_code, error['reason']['code'], f'Error code is wrong please check the error: {error}')
            self.assertEqual(error_message, error['reason']['message'], f"The error message is not what we expected please check error: {error}")
        else:
            self.fail("There were no errors. Error expected: {0}".format(error_message))

    def test_negative_missing_arg(self):
        query = "SELECT ADVISOR({'action': 'start', 'response': '10s'});"
        error = "Error evaluating projection - cause: advisor() not valid argument for 'duration'"
        try:
            results = self.run_cbq_query(query=query, server=self.master)
            self.fail("Start session did not fail. Error expected: {0}".format(error))
        except CBQError as ex:
            self.assertTrue(str(ex).find(error) > 0)
        else:
            self.fail("There were no errors. Error expected: {0}".format(error))

    def test_negative_array(self):
        query=f'SELECT airportname FROM `{self.bucket_name}` WHERE type = "airport" AND lower(city) = "lyon" AND country = "France"'
        expected_error = "Number of arguments to function ADVISOR must be 1 (near line 1, column 16)."
        try:
            results = self.run_cbq_query(query=f"SELECT ADVISOR('{query}','{query}')", server=self.master)
            self.fail("Start session did not fail. Error expected: {0}".format(error))
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 3000)
            self.assertEqual(error['msg'], expected_error)
        else:
            self.fail("There were no errors. Error expected: {0}".format(error))

    def test_negative_invalid_value(self):
        invalid_actions = [ \
            {'cmd': {'action':'start', 'duration':'two'}, 'error_code': 5010, 'error_msg': 'Error evaluating projection - cause: time: invalid duration "two"'}, \
            {'cmd': {'action':'start', 'duration':'1hr'}, 'error_code': 5010, 'error_msg': 'Error evaluating projection - cause: time: unknown unit "hr" in duration "1hr"'}, \
            {'cmd': {'action':'start', 'duration':'1h', 'response':'nul'}, 'error_code': 5010, 'error_msg': 'Error evaluating projection - cause: time: invalid duration "nul"'}, \
            {'cmd': {'action':'start', 'duration':'1h', 'response':'1sec'}, 'error_code': 5010, 'error_msg': 'Error evaluating projection - cause: time: unknown unit "sec" in duration "1sec"'}, \
            {'cmd': {'action':'start', 'duration':'1h', 'query_count':'ten'}, 'error_code': 5010, 'error_msg': 'Error evaluating projection - cause: advisor() not valid argument for \'query_count\''}, \
            {'cmd': {'action':'start', 'duration':'1h', 'profile':9999}, 'error_code': 5010, 'error_msg': 'Error evaluating projection - cause: advisor() not valid argument for \'profile\''} ]
        for action in invalid_actions:
            try:
                self.run_cbq_query(query=f"SELECT ADVISOR({action['cmd']})", server=self.master)
                self.fail(f"There were no errors. Error expected: {action['error_msg']}")
            except CBQError as ex:
                error = self.process_CBQE(ex)
                self.assertEqual(error['code'], action['error_code'])
                self.assertEqual(error['msg'], action['error_msg'] )

    def test_negative_list(self):
        error = "Error evaluating projection - cause: advisor() not valid argument for 'status'"
        try:
            session = self.run_cbq_query(query="SELECT ADVISOR({'action':'list', 'status':'stopped'})", server=self.master)
            self.fail("Start session did not fail. Error expected: {0}".format(error))
        except CBQError as ex:
            self.assertTrue(str(ex).find(error) > 0)
        else:
            self.fail("There were no errors. Error expected: {0}".format(error))

    def test_negative_missing_session(self):
        error = "Error evaluating projection - cause: advisor() not valid argument for 'session'"
        try:
            session = self.run_cbq_query(query="SELECT ADVISOR({'action':'get'})", server=self.master)
            self.fail("Start session did not fail. Error expected: {0}".format(error))
        except CBQError as ex:
            self.assertTrue(str(ex).find(error) > 0)
        else:
            self.fail("There were no errors. Error expected: {0}".format(error))

    def test_negative_invalid_session(self):
        error = "Error evaluating projection - cause: advisor() not valid argument for 'session'"
        for action in ['get','purge','stop','abort']:
            try:
                session = self.run_cbq_query(query=f"SELECT ADVISOR({{'action':'{action}', 'session':123456}})", server=self.master)
                self.fail("Start session did not fail. Error expected: {0}".format(error))
            except CBQError as ex:
                self.assertTrue(str(ex).find(error) > 0)
            else:
                self.fail("There were no errors. Error expected: {0}".format(error))

    def test_negative_from(self):
        error_code = 3256
        error_message = "FROM clause is not allowed when Advisor function is present in projection clause."
        advisor_query = "select ADVISOR({'action':'list'}) FROM `trave-sample` WHERE city = 'Lyon'"
        try:
            self.run_cbq_query(advisor_query)
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], error_code)
            self.assertEqual(error['msg'], error_message)

    def run_async_query(self, query, username, password, server):
        results = self.run_cbq_query(query=query, username=username, password=password, server=server)
        # Check the query has been cancelled
        self.assertEqual(results['status'], "stopped")

    def test_session_query_cancel(self):
        long_query = f"SELECT DISTINCT  MIN(aport.airportname) AS Airport__Name, MIN(lmark.name) AS Landmark_Name, MIN(aport.tz) AS Landmark_Time FROM `{self.bucket_name}` aport LEFT JOIN `travel-sample` lmark ON aport.city = lmark.city AND lmark.country = 'United States' AND lmark.type = 'landmark' WHERE aport.type = 'airport' GROUP BY lmark.name ORDER BY lmark.name LIMIT 3"
        self.users = [{"id": "jimDoe", "name": "Jim Downing", "password": "password1"}]
        self.create_users()
        role = "admin"
        user_id = self.users[0]['id']
        user_pwd = self.users[0]['password']
        grant = self.run_cbq_query(query=f"GRANT {role} to {user_id}",server=self.master)
        cancel_query = f"DELETE FROM system:active_requests WHERE users = '{user_id}'"
        # Create index for join query
        create_index = f"CREATE INDEX `def_city` ON `{self.bucket_name}`(`city`)"
        results = self.run_cbq_query(query=create_index,server=self.master)
        th = threading.Thread(target=self.run_async_query,args=(long_query, user_id, user_pwd, self.master))
        try:
            start = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '1h', 'query_count': 2 })", server=self.master)
            session = start['results'][0]['$1']['session']
            # Spawn query in a thread
            th.start()
            # Cancel query
            self.sleep(1)
            cancel = self.run_cbq_query(query=cancel_query,username=user_id, password=user_pwd, server=self.master)
            th.join()
            # Stop and get session advise
            stop = self.run_cbq_query(query=f"SELECT ADVISOR({{'action': 'stop', 'session': '{session}'}}) as Stop", server=self.master)
            get = self.run_cbq_query(query=f"SELECT ADVISOR({{'action': 'get', 'session': '{session}'}}) as Get", server=self.master)
            for index in get['results'][0]['Get'][0][0]['recommended_indexes']:
                for statement in index['statements']:
                    self.assertEqual(statement['statement'], long_query)
        except Exception as e:
            self.log.error("Advisor session failed: {0}".format(e))
            self.fail()


    def test_session_query_timeout(self):
        long_query = f"SELECT DISTINCT  MIN(aport.airportname) AS Airport__Name, MIN(lmark.name) AS Landmark_Name, MIN(aport.tz) AS Landmark_Time FROM `{self.bucket_name}` aport LEFT JOIN `travel-sample` lmark ON aport.city = lmark.city AND lmark.country = 'United States' AND lmark.type = 'landmark' WHERE aport.type = 'airport' GROUP BY lmark.name ORDER BY lmark.name LIMIT 3"
        # Create index for join query
        create_index = f"CREATE INDEX `def_city` ON `{self.bucket_name}`(`city`)"
        results = self.run_cbq_query(query=create_index,server=self.master)        
        try:
            start = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '1h', 'query_count': 2 })", server=self.master)
            session = start['results'][0]['$1']['session']
            try:
                results = self.run_cbq_query(query=long_query, query_params={'timeout':'500ms'}, server=self.master)
            except CBQError as ex:
                self.assertTrue(str(ex).find("Timeout 500ms exceeded") > 0)
             # Stop and get session advise
            stop = self.run_cbq_query(query=f"SELECT ADVISOR({{'action': 'stop', 'session': '{session}'}}) as Stop", server=self.master)
            get = self.run_cbq_query(query=f"SELECT ADVISOR({{'action': 'get', 'session': '{session}'}}) as Get", server=self.master)
            for index in get['results'][0]['Get'][0][0]['recommended_indexes']:
                for statement in index['statements']:
                    self.assertEqual(statement['statement'], long_query)
        except Exception as e:
            self.log.error("Advisor session failed: {0}".format(e))
            self.fail()
           
    def test_session_collection(self):
        advise_index1 = "CREATE INDEX adv_lower_city_country ON `default`:`travel-sample`.`inventory`.`airport`(lower(`city`),`country`)"
        advise_index2 = "CREATE INDEX adv_country_lower_city ON `default`:`travel-sample`.`inventory`.`airport`(`country`,lower(`city`))"
        query1=f'SELECT airportname FROM `{self.bucket_name}`.inventory.airport WHERE lower(city) = "lyon" AND country = "France"'
        self.wait_for_index_online(bucket='travel-sample', scope='inventory', collection='airport', index='def_inventory_airport_primary')
        try:
            start = self.run_cbq_query(query="SELECT ADVISOR({'action':'start', 'duration':'40m'})", server=self.master)
            session = start['results'][0]['$1']['session']
            results = self.run_cbq_query(query=query1, server=self.master)
            stop = self.run_cbq_query(query=f"SELECT ADVISOR({{'action':'stop', 'session':'{session}'}}) as Stop", server=self.master)
            get = self.run_cbq_query(query=f"SELECT ADVISOR({{'action':'get', 'session':'{session}'}}) as Get", server=self.master)
            # Check advise
            for index in get['results'][0]['Get'][0][0]['recommended_indexes']:
                self.assertTrue(index['index'] == advise_index1 or index['index'] == advise_index2)
                self.assertEqual(index['statements'][0]['statement'], query1)
        except Exception as e:
            self.log.error(f"Advisor session failed: {e}")
            self.fail()

    def test_session_collection_context(self):
        advise_index1 = "CREATE INDEX adv_lower_city_country ON `default`:`travel-sample`.`inventory`.`airport`(lower(`city`),`country`)"
        advise_index2 = "CREATE INDEX adv_country_lower_city ON `default`:`travel-sample`.`inventory`.`airport`(`country`,lower(`city`))"

        query1='SELECT airportname FROM airport WHERE lower(city) = "lyon" AND country = "France"'
        query_contexts = ["", f"default:`{self.bucket_name}`.inventory", f"default:`{self.bucket_name}`._default"]
        self.wait_for_index_online(bucket='travel-sample', scope='inventory', collection='airport', index='def_inventory_airport_primary')
        for context in query_contexts:
            try:
                start = self.run_cbq_query(query="SELECT ADVISOR({'action':'start', 'duration':'40m'})", query_context=context, server=self.master)
                session = start['results'][0]['$1']['session']
                # Run query in bucket.collection context
                results = self.run_cbq_query(query=query1, query_context=f"default:`{self.bucket_name}`.inventory", server=self.master)
                stop = self.run_cbq_query(query=f"SELECT ADVISOR({{'action':'stop', 'session':'{session}'}}) as Stop", query_context=context, server=self.master)
                get = self.run_cbq_query(query=f"SELECT ADVISOR({{'action':'get', 'session':'{session}'}}) as Get", query_context=context, server=self.master)
                # Check advise
                for index in get['results'][0]['Get'][0][0]['recommended_indexes']:
                    self.assertTrue(index['index'] == advise_index1 or index['index'] == advise_index2)
                    self.assertEqual(index['statements'][0]['statement'], query1)
            except Exception as e:
                self.log.error(f"Advisor session failed: {e}")
                self.fail()

    def test_session_collection_join(self):
        advise_index1 = "CREATE INDEX adv_country_city ON `default`:`travel-sample`.`inventory`.`landmark`(`country`,`city`)"
        advise_index2 = "CREATE INDEX adv_city_country ON `default`:`travel-sample`.`inventory`.`landmark`(`city`,`country`)"
        advise_index3 = "CREATE INDEX adv_airportname_tz ON `default`:`travel-sample`.`inventory`.`landmark`(`airportname` INCLUDE MISSING,`tz`)"
        query1="SELECT DISTINCT MIN(aport.airportname) AS Airport_Name, MIN(lmark.name) AS Landmark_Name, MIN(aport.tz) AS Landmark_Time FROM `travel-sample`.inventory.landmark aport LEFT JOIN `travel-sample`.inventory.landmark lmark ON aport.city = lmark.city AND lmark.country = 'United States' GROUP BY lmark.name ORDER BY lmark.name LIMIT 3"
        self.wait_for_index_online(bucket='travel-sample', scope='inventory', collection='landmark', index='def_inventory_landmark_primary')
        try:
            start = self.run_cbq_query(query="SELECT ADVISOR({'action':'start', 'duration':'40m'})", server=self.master)
            session = start['results'][0]['$1']['session']
            # Run query in bucket.collection context
            results = self.run_cbq_query(query=query1, query_context=f"default:`{self.bucket_name}`.inventory", server=self.master)
            stop = self.run_cbq_query(query=f"SELECT ADVISOR({{'action':'stop', 'session':'{session}'}}) as Stop", server=self.master)
            get = self.run_cbq_query(query=f"SELECT ADVISOR({{'action':'get', 'session':'{session}'}}) as Get", server=self.master)
            # Check advise
            for index in get['results'][0]['Get'][0][0]['recommended_indexes']:
                self.assertTrue(index['index'] == advise_index1 or index['index'] == advise_index2 or index['index'] == advise_index3, f"index is {index['index']}")
                self.assertEqual(index['statements'][0]['statement'], query1)
        except Exception as e:
            self.log.error(f"Advisor session failed: {e}")
            self.fail()

    def test_session_negative_authorization(self):
        self.users = [{"id": "jackDoe", "name": "Jack Downing", "password": "password1"}]
        self.create_users()
        role = "query_select"
        user_id = self.users[0]['id']
        user_pwd = self.users[0]['password']
        grant = self.run_cbq_query(query=f"GRANT {role} on `{self.bucket_name}` to {user_id}",server=self.master)
        sessions_queries = ["SELECT ADVISOR({'action': 'start', 'duration': '1h', 'query_count': 2 })", "SELECT ADVISOR({'action': 'list', 'status': 'all'})"]
        expected_error = "User does not have credentials to run queries accessing the system tables. Add role query_system_catalog to allow the statement to run."
        for query in sessions_queries:
            try:
                results = self.run_cbq_query(query=query, username=user_id, password=user_pwd, server=self.master)
                self.fail("Start session did not fail. Error expected: {0}".format(error))
            except CBQError as ex:
                error = self.process_CBQE(ex)
                self.assertEqual(error['code'], 13014)
                self.assertEqual(error['msg'], expected_error)
            else:
                self.fail("There were no errors. Error expected: {0}".format(error))

    def test_session_authorization(self):
        self.users = [{"id": "janneDoe", "name": "Janne Downing", "password": "password1"}]
        self.create_users()
        role_ctlg = "query_system_catalog"
        role_qury = "query_select"
        user_id = self.users[0]['id']
        user_pwd = self.users[0]['password']
        grant_ctlg = self.run_cbq_query(query=f"GRANT {role_ctlg} to {user_id}",server=self.master)
        grant_qury = self.run_cbq_query(query=f"GRANT {role_qury} on `{self.bucket_name}` to {user_id}",server=self.master)

        query1=f'SELECT airportname FROM `{self.bucket_name}` WHERE type = "airport" AND lower(city) = "lyon" AND country = "France"'
        try:
            # Start session as authorized user
            start = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '1h', 'query_count': 2 })", username=user_id, password=user_pwd, server=self.master)
            session = start['results'][0]['$1']['session']
            # Run query as other user
            results = self.run_cbq_query(query=query1, server=self.master)
            self.sleep(2)
            # Stop and get session advise as authorized user
            stop = self.run_cbq_query(query=f"SELECT ADVISOR({{'action': 'stop', 'session': '{session}'}}) as Stop", username=user_id, password=user_pwd, server=self.master)
            get = self.run_cbq_query(query=f"SELECT ADVISOR({{'action': 'get', 'session': '{session}'}}) as Get", username=user_id, password=user_pwd, server=self.master)
            for index in get['results'][0]['Get'][0][0]['recommended_indexes']:
                for statement in index['statements']:
                    self.assertEqual(statement['statement'], query1)
            # Purge and list sessions as authorized user
            purge = self.run_cbq_query(query=f"SELECT ADVISOR({{'action': 'purge', 'session': '{session}'}})", username=user_id, password=user_pwd, server=self.master)
            sessions = self.run_cbq_query(query="SELECT ADVISOR({'action':'list', 'status': 'all'}) as List", username=user_id, password=user_pwd, server=self.master)
            self.assertEqual(sessions['results'][0]['List'],[])
        except Exception as e:
            self.log.error("Advisor session failed: {0}".format(e))
            self.fail()

    def test_session_authorization_other(self):
        self.users = [{"id": "jeanDoe", "name": "Jean Downing", "password": "password1"}]
        self.create_users()
        role_ctlg = "query_system_catalog"
        role_qury = "query_select"
        user_id = self.users[0]['id']
        user_pwd = self.users[0]['password']
        grant_ctlg = self.run_cbq_query(query=f"GRANT {role_ctlg} to {user_id}",server=self.master)
        grant_qury = self.run_cbq_query(query=f"GRANT {role_qury} on `{self.bucket_name}` to {user_id}",server=self.master)
        query1=f'SELECT airportname FROM `{self.bucket_name}` WHERE type = "airport" AND lower(city) = "lyon" AND country = "France"'
        try:
            # Start session as current user
            start = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '1h', 'query_count': 2 })", server=self.master)
            session = start['results'][0]['$1']['session']
            # Run query as current user
            results = self.run_cbq_query(query=query1, server=self.master)
            self.sleep(2)
            # Stop and get session advise as authorized user
            stop = self.run_cbq_query(query=f"SELECT ADVISOR({{'action': 'stop', 'session': '{session}'}}) as Stop", username=user_id, password=user_pwd, server=self.master)
            get = self.run_cbq_query(query=f"SELECT ADVISOR({{'action': 'get', 'session': '{session}'}}) as Get", username=user_id, password=user_pwd, server=self.master)
            for index in get['results'][0]['Get'][0][0]['recommended_indexes']:
                for statement in index['statements']:
                    self.assertEqual(statement['statement'], query1)
            # Purge and list sessions as authorized user
            purge = self.run_cbq_query(query=f"SELECT ADVISOR({{'action': 'purge', 'session': '{session}'}})", username=user_id, password=user_pwd, server=self.master)
            sessions = self.run_cbq_query(query="SELECT ADVISOR({'action':'list', 'status': 'all'}) as List", username=user_id, password=user_pwd, server=self.master)
            self.assertEqual(sessions['results'][0]['List'],[])
        except Exception as e:
            self.log.error("Advisor session failed: {0}".format(e))
            self.fail()

    def test_session_delete_completed_req(self):
        advise_index1 = "CREATE INDEX adv_lower_city_country_type ON `travel-sample`(lower(`city`),`country`) WHERE `type` = 'airport'"
        query1=f'SELECT airportname FROM `{self.bucket_name}` WHERE type = "airport" AND lower(city) = "lyon" AND country = "France"'
        try:
            start = self.run_cbq_query(query="SELECT ADVISOR({'action':'start', 'duration':'40m'})", server=self.master)
            session = start['results'][0]['$1']['session']
            # Run query in bucket.collection context
            results = self.run_cbq_query(query=query1, server=self.master)
            # Delete completed requests
            delete = self.run_cbq_query(query=f"DELETE FROM system:completed_requests", server=self.master)
            # Stop and get session
            stop = self.run_cbq_query(query=f"SELECT ADVISOR({{'action':'stop', 'session':'{session}'}}) as Stop", server=self.master)
            get = self.run_cbq_query(query=f"SELECT ADVISOR({{'action':'get', 'session':'{session}'}}) as Get", server=self.master)
            # Check advise
            advise = get['results'][0]['Get'][0]
            self.assertEqual(advise, [[]])
        except Exception as e:
            self.log.error(f"Advisor session failed: {e}")
            self.fail()

    def test_session_drop_collection(self):
        advise_index1 = "CREATE INDEX adv_country_city ON `default`:`travel-sample`.`inventory`.`landmark`(`country`,`city`)"
        advise_index2 = "CREATE INDEX adv_city_country ON `default`:`travel-sample`.`inventory`.`landmark`(`city`,`country`)"
        advise_index3 = "CREATE INDEX adv_airportname_tz ON `default`:`travel-sample`.`inventory`.`landmark`(`airportname` INCLUDE MISSING,`tz`)"
        query1="SELECT DISTINCT MIN(aport.airportname) AS Airport_Name, MIN(lmark.name) AS Landmark_Name, MIN(aport.tz) AS Landmark_Time FROM `travel-sample`.inventory.landmark aport LEFT JOIN `travel-sample`.inventory.landmark lmark ON aport.city = lmark.city AND lmark.country = 'United States' GROUP BY lmark.name ORDER BY lmark.name LIMIT 3"
        self.run_cbq_query("CREATE PRIMARY INDEX `def_inventory_landmark_primary` IF NOT EXISTS ON `travel-sample`.`inventory`.`landmark`")
        self.wait_for_index_online(bucket='travel-sample', scope='inventory', collection='landmark', index='def_inventory_landmark_primary')
        try:
            start = self.run_cbq_query(query="SELECT ADVISOR({'action':'start', 'duration':'40m'})", server=self.master)
            session = start['results'][0]['$1']['session']
            # Run query in bucket.collection context
            results = self.run_cbq_query(query=query1, server=self.master)
            # Drop collection
            drop_collection = self.run_cbq_query(query="DROP COLLECTION `travel-sample`.`inventory`.`landmark`", server=self.master)
            # Stop and get session
            stop = self.run_cbq_query(query=f"SELECT ADVISOR({{'action':'stop', 'session':'{session}'}}) as Stop", server=self.master)
            get = self.run_cbq_query(query=f"SELECT ADVISOR({{'action':'get', 'session':'{session}'}}) as Get", server=self.master)
            # Check advise
            for index in get['results'][0]['Get'][0][0]['recommended_indexes']:
                self.assertTrue(index['index'] == advise_index1 or index['index'] == advise_index2 or index['index'] == advise_index3)
                self.assertEqual(index['statements'][0]['statement'], query1)
        except Exception as e:
            self.log.error(f"Advisor session failed: {e}")
            self.fail()

    def test_session_drop_scope(self):
        advise_index1 = "CREATE INDEX adv_country_city ON `default`:`travel-sample`.`inventory`.`landmark`(`country`,`city`)"
        advise_index2 = "CREATE INDEX adv_city_country ON `default`:`travel-sample`.`inventory`.`landmark`(`city`,`country`)"
        advise_index3 = "CREATE INDEX adv_airportname_tz ON `default`:`travel-sample`.`inventory`.`landmark`(`airportname` INCLUDE MISSING,`tz`)"
        query1="SELECT DISTINCT MIN(aport.airportname) AS Airport_Name, MIN(lmark.name) AS Landmark_Name, MIN(aport.tz) AS Landmark_Time FROM `travel-sample`.inventory.landmark aport LEFT JOIN `travel-sample`.inventory.landmark lmark ON aport.city = lmark.city AND lmark.country = 'United States' GROUP BY lmark.name ORDER BY lmark.name LIMIT 3"
        self.run_cbq_query("CREATE PRIMARY INDEX `def_inventory_landmark_primary` IF NOT EXISTS ON `travel-sample`.`inventory`.`landmark`")
        self.wait_for_index_online(bucket='travel-sample', scope='inventory', collection='landmark', index='def_inventory_landmark_primary')
        try:
            start = self.run_cbq_query(query="SELECT ADVISOR({'action':'start', 'duration':'40m'})", server=self.master)
            session = start['results'][0]['$1']['session']
            # Run query in bucket.collection context
            results = self.run_cbq_query(query=query1, server=self.master)
            # Drop scope
            drop_scope = self.run_cbq_query(query="DROP SCOPE `travel-sample`.`inventory`", server=self.master)
            # Stop and get session
            stop = self.run_cbq_query(query=f"SELECT ADVISOR({{'action':'stop', 'session':'{session}'}}) as Stop", server=self.master)
            get = self.run_cbq_query(query=f"SELECT ADVISOR({{'action':'get', 'session':'{session}'}}) as Get", server=self.master)
            # Check advise
            for index in get['results'][0]['Get'][0][0]['recommended_indexes']:
                self.assertTrue(index['index'] == advise_index1 or index['index'] == advise_index2 or index['index'] == advise_index3)
                self.assertEqual(index['statements'][0]['statement'], query1)
        except Exception as e:
            self.log.error(f"Advisor session failed: {e}")
            self.fail()

    def test_session_kill_index(self):
        advise_index1 = "CREATE INDEX adv_country_lower_city_type ON `travel-sample`(`country`,lower(`city`)) WHERE `type` = 'airport'"
        advise_index2 = "CREATE INDEX adv_lower_city_country_type ON `travel-sample`(lower(`city`),`country`) WHERE `type` = 'airport'"
        query1 = f'SELECT airportname FROM `{self.bucket_name}` WHERE type = "airport" AND lower(city) = "lyon" AND country = "France"'
        node1 = self.servers[0]
        node2 = self.servers[1]
        try:
            # Start session on node1
            start = self.run_cbq_query(query="SELECT ADVISOR({'action':'start', 'duration':'40m'})", server=node1)
            session = start['results'][0]['$1']['session']
            # Run query on node1
            results = self.run_cbq_query(query=query1, server=node1)
            # Kill index service on node1
            remote_client = RemoteMachineShellConnection(node1)
            remote_client.terminate_process(process_name="indexer")
            self.sleep(3)
            # Stop session on node2
            stop = self.run_cbq_query(query=f"SELECT ADVISOR({{'action':'stop', 'session':'{session}'}}) as Stop", server=node2)
            # Get session on node1
            self.sleep(2)
            get = self.run_cbq_query(query=f"SELECT ADVISOR({{'action':'get', 'session':'{session}'}}) as Get", server=node1)
            # Check advise
            for index in get['results'][0]['Get'][0][0]['recommended_indexes']:
                self.assertTrue(index['index'] == advise_index1 or index['index'] == advise_index2)
                self.assertEqual(index['statements'][0]['statement'], query1)
                self.assertEqual(index['statements'][0]['run_count'], 1)
        except Exception as e:
            self.log.error(f"Advisor session failed: {e}")
            self.fail()

    def test_session_kill_query(self):
        advise_index1 = "CREATE INDEX adv_country_lower_city_type ON `travel-sample`(`country`,lower(`city`)) WHERE `type` = 'airport'"
        advise_index2 = "CREATE INDEX adv_lower_city_country_type ON `travel-sample`(lower(`city`),`country`) WHERE `type` = 'airport'"        
        query1 = f'SELECT airportname FROM `{self.bucket_name}` WHERE type = "airport" AND lower(city) = "lyon" AND country = "France"'
        node1 = self.servers[0]
        node2 = self.servers[1]
        try:
            # Start session on node2
            start = self.run_cbq_query(query="SELECT ADVISOR({'action':'start', 'duration':'40m'})", server=node2)
            session = start['results'][0]['$1']['session']
            # Run query on node2
            results = self.run_cbq_query(query=query1, server=node2)
            # Kill n1ql service on node2
            remote_client = RemoteMachineShellConnection(node2)
            remote_client.terminate_process(process_name="cbq-engine")
            self.sleep(3)
            # Stop session on node1
            stop = self.run_cbq_query(query=f"SELECT ADVISOR({{'action':'stop', 'session':'{session}'}}) as Stop", server=node1)
            # List session
            self.sleep(1)
            list_node1 = self.run_cbq_query(query=f"SELECT ADVISOR({{'action':'list'}}) as List", server=node1)
            statement1 = list_node1['results'][0]['List'][0]['tasks_cache']['results'][0]['recommended_indexes'][0]['statements'][0]['statement']
            # Check advise
            self.assertEqual(statement1, query1)
        except Exception as e:
            self.log.error(f"Advisor session failed: {e}")
            self.fail()

    def test_session_multi_node(self):
        advise_index1 = "CREATE INDEX adv_country_lower_city_type ON `travel-sample`(`country`,lower(`city`)) WHERE `type` = 'airport'"
        advise_index2 = "CREATE INDEX adv_lower_city_country_type ON `travel-sample`(lower(`city`),`country`) WHERE `type` = 'airport'"
        query1 = f'SELECT airportname FROM `{self.bucket_name}` WHERE type = "airport" AND lower(city) = "lyon" AND country = "France"'
        node1 = self.servers[0]
        node2 = self.servers[1]
        try:
            # Start session on node1
            start = self.run_cbq_query(query="SELECT ADVISOR({'action':'start', 'duration':'40m'})", server=node1)
            session = start['results'][0]['$1']['session']
            # Run query on node2
            results = self.run_cbq_query(query=query1, server=node2)
            # Run query on node1
            results = self.run_cbq_query(query=query1, server=node1)
            # Stop session on node1
            stop = self.run_cbq_query(query=f"SELECT ADVISOR({{'action':'stop', 'session':'{session}'}}) as Stop", server=node1)
            # Get session on node2
            get = self.run_cbq_query(query=f"SELECT ADVISOR({{'action':'get', 'session':'{session}'}}) as Get", server=node2)
            # Check advise
            for index in get['results'][0]['Get'][0][0]['recommended_indexes']:
                self.assertTrue(index['index'] == advise_index1 or index['index'] == advise_index2)
                self.assertEqual(index['statements'][0]['statement'], query1)
                self.assertEqual(index['statements'][0]['run_count'], 2)
        except Exception as e:
            self.log.error(f"Advisor session failed: {e}")
            self.fail()
