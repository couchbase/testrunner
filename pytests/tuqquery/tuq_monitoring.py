import json
import logging
import threading
import time

from lib.Cb_constants.CBServer import CbServer
from remote.remote_util import RemoteMachineShellConnection

from .tuq import QueryTests


class QueryMonitoringTests(QueryTests):
    def setUp(self):
        super(QueryMonitoringTests, self).setUp()
        self.threadFailure = False
        self.run_cbq_query('delete from system:completed_requests')
        self.run_cbq_query('delete from system:prepareds')
        self.use_query_context = self.input.param("use_query_context", False)
        self.rest.set_completed_requests_collection_duration(self.master, 1000)
        self.rest.set_completed_requests_max_entries(self.master, 4000)
        self.query_buckets = self.get_query_buckets(check_all_buckets=True)

    def suite_setUp(self):
        super(QueryMonitoringTests, self).suite_setUp()
        # Use a JS UDF so these monitoring tests can force a query to run for a known duration.
        functions = 'function monitoring_sleep(delay) { var start = new Date().getTime(); while (new Date().getTime() < start + delay); return delay; }'
        self.create_library("monitoring_helper", functions, ["monitoring_sleep"], replace=True)
        self.run_cbq_query(query='CREATE OR REPLACE FUNCTION monitoring_sleep(t) LANGUAGE JAVASCRIPT AS "monitoring_sleep" AT "monitoring_helper"')

    def tearDown(self):
        super(QueryMonitoringTests, self).tearDown()

    def suite_tearDown(self):
        try:
            self.run_cbq_query(query='DROP FUNCTION monitoring_sleep')
        except Exception as ex:
            self.log.info("Ignoring monitoring_sleep cleanup error: {0}".format(str(ex)))
        try:
            self.delete_library("monitoring_helper")
        except Exception as ex:
            self.log.info("Ignoring monitoring_helper cleanup error: {0}".format(str(ex)))
        super(QueryMonitoringTests, self).suite_tearDown()

    ##############################################################################################
    #
    #   Monitoring Test Cases (Normal Queries)
    #
    ##############################################################################################

    '''Runs the basic cluster monitoring checks: (2 queries will be run when calling this method, each query will be
                                                  called from a different node)
            -check if the running queries are in system:active_requests
            -check if the queries' node fields accurately reflect the node they were started from
            -check if a query can be accessed from system:active_requests using its requestId
            -check if a query can be killed from system:active_requests using its requestId
                -once the query is killed check if it is in system:completed_requests
            -check if the queries appear in system:completed_requests when they complete.'''

    def test_simple_cluster_monitoring(self):
        self.test_monitoring(test='simple')

    '''Runs basic completed_requests deletions
            -check if you can delete the whole log
            -check if you can delete by node
            -check if you can delete by requestId'''

    def test_purge_completed(self):
        self.test_monitoring(test='purge')

    '''Checks to see if active_requests and completed_requests can be filtered by node'''

    def test_filter_by_node(self):
        self.test_monitoring(test='filter')

    '''Checks to see if the queries run from a server that has been downed are removed from system:completed_requests'''

    def test_server_failure(self):
        self.test_monitoring(test='simple')

        result = self.run_cbq_query('select * from system:completed_requests')
        self.assertEqual(result['metrics']['resultCount'], 3)

        remote = RemoteMachineShellConnection(self.servers[1])
        remote.stop_server()

        time.sleep(30)
        # Check to see that completed_requests does not contain info from the downed node
        result = self.run_cbq_query('select * from system:completed_requests')
        self.assertEqual(result['metrics']['resultCount'], 2)
        result = self.run_cbq_query('select * from system:completed_requests where node = "%s:%s"'
                                    % (self.servers[1].ip, self.servers[1].port))
        self.assertEqual(result['metrics']['resultCount'], 0)

        # The info from the down node should not have been restored by the node coming back online
        remote.start_server()
        time.sleep(30)
        result = self.run_cbq_query('select * from system:completed_requests')
        self.assertEqual(result['metrics']['resultCount'], 2)
        result = self.run_cbq_query('select * from system:completed_requests where node = "%s:%s"'
                                    % (self.servers[1].ip, self.servers[1].port))
        self.assertEqual(result['metrics']['resultCount'], 0)

    def test_collections_completed_request_full_path(self):
        self.run_cbq_query(query="CREATE PRIMARY INDEX ON default:default.test.test1")
        self.run_cbq_query(query="DROP PRIMARY INDEX on default:default.test.test1")
        results = self.run_cbq_query(query="select * from system:completed_requests")
        self.assertEqual(results['metrics']['resultCount'], 1)
        self.assertEqual(results['results'][0]['completed_requests']['statement'], 'CREATE PRIMARY INDEX ON default:default.test.test1')

    def test_collections_completed_request_query_context(self):
        self.run_cbq_query(query="CREATE PRIMARY INDEX ON test1", query_context="default:default.test")
        self.run_cbq_query(query="DROP PRIMARY INDEX on default:default.test.test1")
        results = self.run_cbq_query(query="select * from system:completed_requests")
        self.assertEqual(results['metrics']['resultCount'], 1)
        self.assertEqual(results['results'][0]['completed_requests']['statement'], 'CREATE PRIMARY INDEX ON test1')
        self.assertEqual(results['results'][0]['completed_requests']['queryContext'], 'default:default.test')

    def test_collections_active_request_full_path(self):
        e = threading.Event()
        t51 = threading.Thread(name='run_second_query', target=self.run_parallel_query_collections)
        t51.start()
        e.set()
        results = self.run_cbq_query(query="SELECT * from system:active_requests")
        try:
            if results['results'][0]['active_requests']['statement'] == 'CREATE PRIMARY INDEX on default:default.test.test2' or results['results'][1]['active_requests']['statement'] == 'CREATE PRIMARY INDEX on default:default.test.test2':
                self.assertTrue(True)
            else:
                self.fail("Could not find the correct statement {0}".format(results))
        finally:
            t51.join()
            self.run_cbq_query(query="DROP PRIMARY INDEX on default:default.test.test2")

    def test_collections_active_request_query_context(self):
        e = threading.Event()
        t51 = threading.Thread(name='run_second_query', target=self.run_parallel_query_collections)
        t51.start()
        e.set()
        results = self.run_cbq_query(query="SELECT * from system:active_requests")
        try:
            if results['results'][0]['active_requests']['statement'] == 'CREATE PRIMARY INDEX on test2':
                if results['results'][0]['active_requests']['queryContext'] == 'default:default.test':
                    self.assertTrue(True)
            elif results['results'][1]['active_requests']['statement'] == 'CREATE PRIMARY INDEX on test2':
                if results['results'][1]['active_requests']['queryContext'] == 'default:default.test':
                    self.assertTrue(True)
            else:
                self.fail("Could not find the correct statement {0}".format(results))
        finally:
            t51.join()
            self.run_cbq_query(query="DROP PRIMARY INDEX on default:default.test.test2")

    def test_transaction_active(self):
        queries = ['EXECUTE FUNCTION monitoring_sleep(15000)','EXECUTE FUNCTION monitoring_sleep(15000)','COMMIT TRANSACTION']
        tx_start = self.run_cbq_query(query="BEGIN WORK", txtimeout="2m")
        txid = tx_start['results'][0]['txid']
        for query in queries:
            if query is not 'COMMIT TRANSACTION':
                t51 = threading.Thread(name='run_second_query', target=self.run_transaction_query, args=(query, txid))
                t51.start()
                time.sleep(1)
                results = self.run_cbq_query(query="SELECT * from system:active_requests")
                try:
                    if results['results'][0]['active_requests']['statement'] == query:
                        self.log.info(results['results'][0]['active_requests'])
                        self.assertEqual(results['results'][0]['active_requests']['txid'], txid)
                        self.assertTrue('transactionRemainingTime' in results['results'][0]['active_requests'],
                                        "Transaction Remaining Time not found in the entry! {0}".format(
                                            str(results['results'][0]['active_requests'])))
                        self.assertTrue('transactionElapsedTime' in results['results'][0]['active_requests'],
                                        "Transaction Elapsed Time not found in the entry! {0}".format(
                                            str(results['results'][0]['active_requests'])))
                    elif results['results'][1]['active_requests']['statement'] == query:
                        self.log.info(results['results'][1]['active_requests'])
                        self.assertEqual(results['results'][1]['active_requests']['txid'], txid)
                        self.assertTrue('transactionRemainingTime' in results['results'][1]['active_requests'],
                                        "Transaction Remaining Time not found in the entry! {0}".format(
                                            str(results['results'][0]['active_requests'])))
                        self.assertTrue('transactionElapsedTime' in results['results'][1]['active_requests'],
                                        "Transaction Elapsed Time not found in the entry! {0}".format(
                                            str(results['results'][1]['active_requests'])))
                    else:
                        self.fail("Could not find the correct statement {0}".format(results))
                finally:
                    t51.join()

    def test_transaction_completed(self):
        self.rest.set_completed_requests_collection_duration(self.master, 500)
        try:
            tx_start = self.run_cbq_query(query="BEGIN WORK", txtimeout="2m")
            txid = tx_start['results'][0]['txid']
            self.run_cbq_query(query='select * from default where name = "employee-9"', txnid=txid)
            self.run_cbq_query(query='update default SET name = "employee-9000" where name = "employee-9"', txnid=txid)
            self.run_cbq_query(query='delete from default limit 10000', txnid=txid)
            try:
                self.run_cbq_query(query='COMMIT TRANSACTION', txnid=txid)
            except Exception as ex:
                if "errors" in str(ex):
                    self.assertTrue("17018" in str(ex))
                else:
                    self.log.error(str(ex))

            select_results = self.run_cbq_query(query='select * from system:completed_requests where statement = "select * from default where name = \\"employee-9\\""')
            self.log.info(select_results)
            self.assertEqual(select_results['results'][0]['completed_requests']['txid'], txid)
            self.assertTrue('transactionRemainingTime' in select_results['results'][0]['completed_requests'],"Transaction Remaining Time not found in the entry! {0}".format(str(select_results['results'][0]['completed_requests'])))
            self.assertTrue('transactionElapsedTime' in select_results['results'][0]['completed_requests'],"Transaction Elapsed Time not found in the entry! {0}".format(str(select_results['results'][0]['completed_requests'])))

            update_results = self.run_cbq_query(query='select * from system:completed_requests where statement = "update default SET name = \\"employee-9000\\" where name = \\"employee-9\\""')
            self.log.info(update_results)
            self.assertEqual(update_results['results'][0]['completed_requests']['txid'], txid)
            self.assertTrue('transactionRemainingTime' in update_results['results'][0]['completed_requests'],"Transaction Remaining Time not found in the entry! {0}".format(str(update_results['results'][0]['completed_requests'])))
            self.assertTrue('transactionElapsedTime' in update_results['results'][0]['completed_requests'],"Transaction Elapsed Time not found in the entry! {0}".format(str(update_results['results'][0]['completed_requests'])))

            delete_results = self.run_cbq_query(query='select * from system:completed_requests where statement = "delete from default limit 10000"')
            self.log.info(delete_results)
            self.assertEqual(delete_results['results'][0]['completed_requests']['txid'], txid)
            self.assertTrue('transactionRemainingTime' in delete_results['results'][0]['completed_requests'],"Transaction Remaining Time not found in the entry! {0}".format(str(delete_results['results'][0]['completed_requests'])))
            self.assertTrue('transactionElapsedTime' in delete_results['results'][0]['completed_requests'],"Transaction Elapsed Time not found in the entry! {0}".format(str(delete_results['results'][0]['completed_requests'])))

            commit_results = self.run_cbq_query(query='select * from system:completed_requests where statement = "COMMIT TRANSACTION"')
            self.log.info(commit_results)
            self.assertEqual(commit_results['results'][0]['completed_requests']['txid'],txid)
            self.assertFalse('transactionRemainingTime' in commit_results['results'][0]['completed_requests'],"Transaction Remaining Time found in the entry but it should not be there! {0}".format(str(commit_results['results'][0]['completed_requests'])))
            self.assertTrue('transactionElapsedTime' in commit_results['results'][0]['completed_requests'],"Transaction Elapsed Time not found in the entry! {0}".format(str(commit_results['results'][0]['completed_requests'])))
        finally:
            self.rest.set_completed_requests_collection_duration(self.master, 1000)

    ##############################################################################################
    #
    #   Monitoring Helper Functions
    #
    ##############################################################################################

    def run_transaction_query(self, query='', txid=''):
        self.run_cbq_query(query=query, txnid=txid)

    def run_parallel_query(self, server):
        logging.info('parallel query is active')
        self._run_monitoring_sleep_query(delay_ms=30000, server=server)

    def run_parallel_query_collections(self):
        if self.use_query_context:
            self.run_cbq_query(query="CREATE PRIMARY INDEX on test2", query_context="default:default.test")
        else:
            self.run_cbq_query(query="CREATE PRIMARY INDEX on default:default.test.test2")

    '''Run basic cluster monitoring checks using deterministic 30-second sleep queries on specific nodes.'''

    def test_monitoring(self, test):
        for query_bucket in self.query_buckets:
            logging.info('PURGING COMPLETED REQUEST LOG')
            self.run_cbq_query('delete from system:completed_requests')
            result = self.run_cbq_query('select * from system:completed_requests')
            self.assertEqual(result['metrics']['resultCount'], 0)
            logging.info("CHECKING THAT NO REQUESTS ARE RUNNING")
            result = self.run_cbq_query('select * from system:active_requests')
            self.assertEqual(result['metrics']['resultCount'], 1)
            e = threading.Event()
            if test == 'simple':
                t50 = threading.Thread(name='run_simple_monitoring', target=self.run_simple_monitoring_check,
                                       args=(e, 2))
            elif test == 'purge':
                t50 = threading.Thread(name='run_purge', target=self.run_purge_completed_requests,
                                       args=(e, 2))
            elif test == 'filter':
                t50 = threading.Thread(name='run_filter_by_node', target=self.run_filter_by_node,
                                       args=(e, 2))
                t52 = threading.Thread(name='run_third_query', target=self.run_parallel_query,
                                       args=[self.servers[1]])
                t53 = threading.Thread(name='run_fourth_query', target=self.run_parallel_query,
                                       args=[self.servers[1]])
            else:
                raise Exception("Incorrect test provided")

            t51 = threading.Thread(name='run_second_query', target=self.run_parallel_query,
                                   args=[self.servers[2]])
            t50.start()
            t51.start()
            if test == 'filter':
                t52.start()
                t53.start()
            e.set()
            self._run_monitoring_sleep_query(delay_ms=30000, server=self.servers[1])
            logging.debug('event is set')
            t50.join(100)
            t51.join(100)
            if test == 'filter':
                t52.join(100)
                t53.join(100)
            self.assertFalse(self.threadFailure)
            query_template = 'FROM %s select $str0, $str1 ORDER BY $str0,$str1 ASC' % query_bucket
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

    def run_simple_monitoring_check(self, e, t):
        while not e.isSet():
            logging.debug('wait_for_event_timeout starting')
            event_is_set = e.wait(t)
            logging.debug('event set: %s', event_is_set)
            if event_is_set:
                time.sleep(5)
                # check if the running queries are in system:active_requests
                logging.info('CHECKING SYSTEM:ACTIVE_REQUESTS FOR THE RUNNING QUERIES')
                result = self.run_cbq_query('select * from system:active_requests')
                if not result['metrics']['resultCount'] == 3:
                    self.threadFailure = True
                    logging.error(
                        'NOT ALL ACTIVE QUERIES ARE IN ACTIVE_REQUESTS, THERE SHOULD BE 3 QUERIES ACTIVE. %s'
                        ' QUERIES ARE ACTIVE.' % result['metrics']['resultCount'])
                    self.log.error(json.dumps(result, sort_keys=True, indent=3))
                    return

                # check if the queries' node fields accurately reflect the node they were started from
                logging.info("VERIFYING THAT ACTIVE_REQUESTS HAVE THE QUERIES MARKED WITH THE CORRECT NODES")
                node1 = self.run_cbq_query('select * from system:active_requests where node  =  "%s:%s"'
                                           % (self.servers[1].ip, self.servers[1].port))
                if not node1['metrics']['resultCount'] == 1:
                    self.threadFailure = True
                    logging.error('THE QUERY ON THE REQUESTED NODE: "%s:%s" IS NOT IN SYSTEM:ACTIVE_REQUESTS'
                                  % (self.servers[1].ip, self.servers[1].port))
                    self.log.error(node1)
                    return
                node2 = self.run_cbq_query('select * from system:active_requests where node  =  "%s:%s"'
                                           % (self.servers[2].ip, self.servers[2].port))
                if not node2['metrics']['resultCount'] == 1:
                    self.threadFailure = True
                    logging.error('THE QUERY ON THE REQUESTED NODE: "%s:%s" IS NOT IN SYSTEM:ACTIVE_REQUESTS'
                                  % (self.servers[2].ip, self.servers[2].port))
                    self.log.error(node2)
                    return

                # check if a query can be accessed from system:active_requests using its requestId
                logging.info("CHECKING IF A QUERY CAN BE ACCESSED VIA ITS requestId")
                requestId = result['results'][1]['active_requests']['requestId']
                result = self.run_cbq_query('select * from system:active_requests where requestId  =  "%s"'
                                            % requestId)
                if not result['metrics']['resultCount'] == 1:
                    self.threadFailure = True
                    logging.error('THE QUERY FOR requestId "%s" IS NOT IN ACTIVE_REQUESTS' % requestId)
                    self.log.error(json.dumps(result, sort_keys=True, indent=3))
                    return

                # check if a query can be killed from system:active_requests using its requestId
                logging.info("CHECKING IF A QUERY CAN BE KILLED")
                time.sleep(1)
                self.run_cbq_query('delete from system:active_requests where requestId  =  "%s"' % requestId)
                result = self.run_cbq_query('select * from system:active_requests  where requestId  =  "%s"'
                                            % requestId)
                if not result['metrics']['resultCount'] == 0:
                    self.threadFailure = True
                    logging.error('THE QUERY FOR requestId "%s" WAS NOT KILLED AND IS STILL IN ACTIVE_REQUESTS'
                                  % requestId)
                    return

                # once the query is killed check if it is in system:completed_requests
                result = self.run_cbq_query('select * from system:completed_requests where requestId = "%s"'
                                            % requestId)
                if not result['metrics']['resultCount'] == 1:
                    self.threadFailure = True
                    logging.error('THE QUERY FOR requestId "%s" WAS REMOVED FROM ACTIVE_REQUESTS BUT NOT PUT INTO '
                                  'COMPLETED_REQUESTS' % requestId)
                    self.log.error(json.dumps(result, sort_keys=True, indent=3))
                    return

                time.sleep(60)
                # check if the queries appear in system:completed_requests when they complete.
                logging.info('CHECKING IF ALL COMPLETED QUERIES ARE IN SYSTEM:COMPLETED_REQUESTS')
                result = self.run_cbq_query('select * from system:completed_requests')
                if not result['metrics']['resultCount'] == 2:
                    self.threadFailure = True
                    logging.error('THE QUERIES EITHER DID NOT COMPLETE RUNNING OR WERE NOT ADDED TO '
                                  'SYSTEM:COMPLETED_REQUESTS')
                    self.log.error(json.dumps(result, sort_keys=True, indent=3))
                    return

    def run_purge_completed_requests(self, e, t):
        while not e.isSet():
            logging.debug('wait_for_event_timeout starting')
            event_is_set = e.wait(t)
            logging.debug('event set: %s', event_is_set)
            if event_is_set:
                time.sleep(60)
                logging.info('CHECKING IF SYSTEM:COMPLETED_REQUESTS HAS QUERIES IN IT')
                result = self.run_cbq_query('select * from system:completed_requests')
                if not result['metrics']['resultCount'] == 2:
                    self.threadFailure = True
                    logging.error('THERE ARE NO ITEMS INSIDE SYSTEM:COMPLETED_REQUESTS')
                    self.log.error(json.dumps(result, sort_keys=True, indent=3))
                    return

                # check if the queries appear in system:completed_requests when they complete.
                logging.info('CHECKING IF SYSTEM:COMPLETED_REQUESTS CAN BE PURGED')
                self.run_cbq_query("delete from system:completed_requests")
                result = self.run_cbq_query('select * from system:completed_requests')
                if not result['metrics']['resultCount'] == 0:
                    self.threadFailure = True
                    logging.error('DELETE FAILED, THERE ARE STILL ITEMS INSIDE SYSTEM:COMPLETED_REQUESTS')
                    self.log.error(json.dumps(result, sort_keys=True, indent=3))
                    return

                query1 = threading.Thread(name='run_first_query', target=self.run_parallel_query,
                                          args=[self.servers[1]])
                query2 = threading.Thread(name='run_first_query', target=self.run_parallel_query,
                                          args=[self.servers[1]])
                query3 = threading.Thread(name='run_third_query', target=self.run_parallel_query,
                                          args=[self.servers[2]])
                query4 = threading.Thread(name='run_fourth_query', target=self.run_parallel_query,
                                          args=[self.servers[2]])
                query1.start()
                query2.start()
                query3.start()
                query4.start()

                query1.join(100)
                query2.join(100)
                query3.join(100)
                query4.join(100)

                # check if the queries can be purged selectively
                logging.info('CHECKING IF SYSTEM:COMPLETED_REQUESTS CAN BE PURGED BY NODE')
                self.run_cbq_query('delete from system:completed_requests where node = "%s:%s"'
                                   % (self.servers[2].ip, self.servers[2].port))
                result = self.run_cbq_query('select * from system:completed_requests')
                if not result['metrics']['resultCount'] == 2:
                    self.threadFailure = True
                    logging.error('DELETE FAILED, THERE ARE STILL ITEMS FROM NODE: "%s:%s"'
                                  'INSIDE SYSTEM:COMPLETED_REQUESTS'
                                  % (self.servers[2].ip, self.servers[2].port))
                    self.log.error(json.dumps(result, sort_keys=True, indent=3))
                    return

                # check if the queries can be purged by requestId
                logging.info('CHECKING IF SYSTEM:COMPLETED_REQUESTS CAN BE PURGED BY REQUESTID')
                requestId = result['results'][0]['completed_requests']['requestId']
                self.run_cbq_query('delete from system:completed_requests where requestId = "%s"' % requestId)
                result = self.run_cbq_query('select * from system:completed_requests')
                if not result['metrics']['resultCount'] == 1:
                    self.threadFailure = True
                    logging.error('DELETE FAILED, THE QUERY FOR REQUESTID: "%s" IS STILL '
                                  'INSIDE SYSTEM:COMPLETED_REQUESTS' % requestId)
                    self.log.error(json.dumps(result, sort_keys=True, indent=3))
                    return

    def run_filter_by_node(self, e, t):
        while not e.isSet():
            logging.debug('wait_for_event_timeout starting')
            event_is_set = e.wait(t)
            logging.debug('event set: %s', event_is_set)
            if event_is_set:
                time.sleep(3)
                logging.info('CHECKING IF SYSTEM:ACTIVE_REQUESTS RESULTS CAN BE FILTERED BY NODE')
                result = self.run_cbq_query('select * from system:active_requests')
                node1 = self.run_cbq_query('select * from system:active_requests where node = "%s:%s"'
                                           % (self.servers[2].ip, self.servers[2].port))
                if not node1['metrics']['resultCount'] == 1:
                    self.threadFailure = True
                    logging.error('THE RESULTS OF THE QUERY ARE INCORRECT')
                    self.log.error(json.dumps(result, sort_keys=True, indent=3))
                    self.log.error(node1)
                    return

                node2 = self.run_cbq_query('select * from system:active_requests where node = "%s:%s"'
                                           % (self.servers[1].ip, self.servers[1].port))
                if not node2['metrics']['resultCount'] == 3:
                    self.threadFailure = True
                    logging.error('THE RESULTS OF THE QUERY ARE INCORRECT')
                    self.log.error(json.dumps(result, sort_keys=True, indent=3))
                    self.log.error(node2)
                    return

                time.sleep(90)

                logging.info('CHECKING IF SYSTEM:COMPLETED_REQUESTS RESULTS CAN BE FILTERED BY NODE')
                result = self.run_cbq_query('select * from system:completed_requests')
                node1 = self.run_cbq_query('select * from system:completed_requests where node = "%s:%s"'
                                           % (self.servers[2].ip, self.servers[1].port))
                if not node1['metrics']['resultCount'] == 1:
                    self.threadFailure = True
                    logging.error('THE RESULTS OF THE QUERY ARE INACCURATE')
                    self.log.error(json.dumps(result, sort_keys=True, indent=3))
                    self.log.error(node1)
                    return

                node2 = self.run_cbq_query('select * from system:completed_requests where node = "%s:%s"'
                                           % (self.servers[1].ip, self.servers[1].port))
                if not node2['metrics']['resultCount'] == 3:
                    self.threadFailure = True
                    logging.error('THE RESULTS OF THE QUERY ARE INACCURATE')
                    self.log.error(json.dumps(result, sort_keys=True, indent=3))
                    self.log.error(node2)
                    return

    ##############################################################################################
    #
    #   Monitoring Prepared Test Cases
    #
    ##############################################################################################

    '''Runs the basic prepared monitoring checks:
            -Check that the number of uses for a prepared statement is correctly updated
            -Check that prepared statements appear in system:active_requests when ran
            -Check that prepared statements appear in system:completed_requests when ran.'''

    def test_prepared_monitoring(self):
        self.test_prepared_common_body()
        time.sleep(10)
        # Check that both prepareds are in system:prepareds
        self.query = "select * from system:prepareds"
        result = self.run_cbq_query()
        self.assertEqual(result['metrics']['resultCount'], 6)

        name = result['results'][1]['prepareds']['name']
        uses = result['results'][1]['prepareds']['uses']
        self.assertEqual(uses, 1)

        secondname = result['results'][1]['prepareds']['name']

        e = threading.Event()
        thread1 = threading.Thread(name='run_simple_monitoring', target=self.run_simple_monitoring_prepared_check,
                                   args=(e, 2))
        thread2 = threading.Thread(name='run_prepared', target=self.execute_prepared, args=(name, self.servers[0]))
        thread3 = threading.Thread(name='run_prepared', target=self.execute_prepared,
                                   args=(secondname, self.servers[1]))
        thread1.start()
        thread2.start()
        thread3.start()

        e.set()

        thread1.join(100)
        thread2.join(100)
        thread3.join(100)
        self.assertFalse(self.threadFailure)

    '''Runs the basic prepared deletion checks:
            -Check if system:prepareds can be purged completely
            -Check if system:prepareds can be purged by node
            -Check if system:prepareds can be purged by preparedName.'''

    def test_prepared_deletion(self):
        self.test_prepared_common_body()
        self.query = "select * from system:prepareds"
        result = self.run_cbq_query()
        self.assertEqual(result['metrics']['resultCount'], 6)

        # Check if you can delete everything in system:prepareds
        self.run_cbq_query("delete from system:prepareds")
        result = self.run_cbq_query("select * from system:prepareds")
        self.assertEqual(result['metrics']['resultCount'], 0)

        # Reset prepared statements for next deletion check
        self.test_prepared_common_body()

        # Check if you can delete from system:prepareds by node
        self.query = "delete from system:prepareds where node = '%s:%s'" \
                     % (self.servers[0].ip, self.servers[0].port)
        self.run_cbq_query()
        result = self.run_cbq_query("select * from system:prepareds")
        self.assertEqual(result['metrics']['resultCount'], 4)

        self.query = "delete from system:prepareds where node = '%s:%s'" \
                     % (self.servers[1].ip, self.servers[1].port)
        self.run_cbq_query()
        result = self.run_cbq_query("select * from system:prepareds")
        self.assertEqual(result['metrics']['resultCount'], 2)

        self.query = "delete from system:prepareds where node = '%s:%s'" \
                     % (self.servers[2].ip, self.servers[2].port)
        self.run_cbq_query()
        result = self.run_cbq_query("select * from system:prepareds")
        self.assertEqual(result['metrics']['resultCount'], 0)

        # Reset prepared statements for next deletion check
        self.test_prepared_common_body()

        self.query = "select * from system:prepareds"
        result = self.run_cbq_query()
        prepared1 = result['results'][0]['prepareds']['name']
        prepared2 = result['results'][1]['prepareds']['name']

        # Check if system:prepareds can be deleted from by prepared name
        self.query = "delete from system:prepareds where name = '%s'" % prepared1
        self.run_cbq_query()
        result = self.run_cbq_query("select * from system:prepareds")
        self.assertEqual(result['metrics']['resultCount'], 3)

        self.query = "delete from system:prepareds where name = '%s'" % prepared2
        self.run_cbq_query()
        result = self.run_cbq_query("select * from system:prepareds")
        self.assertEqual(result['metrics']['resultCount'], 0)

    '''Runs the basic prepared filtering checks:
            -Check if system:prepareds can be filtered by prepared statement name
            -Check if system:prepareds can be purged by node.'''

    def test_prepared_filtering(self):
        self.test_prepared_common_body()
        # Check that both prepareds are in system:prepareds
        self.query = "select * from system:prepareds"

        result = self.run_cbq_query()
        self.assertEqual(result['metrics']['resultCount'], 6)
        prepared1 = result['results'][0]['prepareds']['name']
        prepared2 = result['results'][1]['prepareds']['name']

        # Check if you can access prepared statements from system:prepareds by the prepared statement's name
        self.query = "select * from system:prepareds where name = '%s'" % prepared1
        name1 = self.run_cbq_query(server=self.servers[2])
        self.assertEqual(name1['metrics']['resultCount'], 3)

        # Check if you can access prepared statements from system:prepareds by the prepared statement's name
        self.query = "select * from system:prepareds where name = '%s'" % prepared2
        name2 = self.run_cbq_query(server=self.servers[2])
        self.assertEqual(name2['metrics']['resultCount'], 3)

        # Check to see if system:prepareds can be filtered by node
        self.query = "select * from system:prepareds where node = '%s:%s'" % \
                     (self.servers[0].ip, self.servers[0].port)
        node1 = self.run_cbq_query()
        self.assertEqual(node1['metrics']['resultCount'], 2)

        # Check to see if system:prepareds can be filtered by node
        self.query = "select * from system:prepareds where node = '%s:%s'" \
                     % (self.servers[1].ip, self.servers[1].port)
        node2 = self.run_cbq_query()
        self.assertEqual(node2['metrics']['resultCount'], 2)

        # Check to see if system:prepareds can be filtered by node
        self.query = "select * from system:prepareds where node = '%s:%s'" \
                     % (self.servers[2].ip, self.servers[2].port)
        node3 = self.run_cbq_query()
        self.assertEqual(node3['metrics']['resultCount'], 2)

    def test_prepared_check_requestId(self):
        self.test_prepared_kill(self.run_check_requestId)

    def test_prepared_kill_by_requestId(self):
        self.test_prepared_kill(self.run_kill_prepared_by_requestId)

    def test_prepared_kill_by_name(self):
        self.test_prepared_kill(self.run_kill_prepared_by_name)

    def test_prepared_kill_by_node(self):
        self.test_prepared_kill(self.run_kill_prepared_by_node)

    ##############################################################################################
    #
    #   Prepared Test Helper Functions
    #
    ##############################################################################################

    def execute_prepared(self, prepared_name, server):
        result = self.run_cbq_query('EXECUTE "%s"' % prepared_name, server=server)

    '''Prepares two statements on different nodes.'''

    def test_prepared_common_body(self, test_type=None):
        self.query = "select * from system:prepareds"
        result = self.run_cbq_query()
        self.assertEqual(result['metrics']['resultCount'], 0)

        self.query = "(select * from " + self.query_buckets[0] + " union select * from " + self.query_buckets[
            0] + " union select * from " + self.query_buckets[0] + ") " \
                                                                   "union (select d from " + self.query_buckets[
                         0] + " d JOIN " + self.query_buckets[0] + " def ON KEYS d.name)"
        self.prepared_common_body()
        self.prepared_common_body(server=self.servers[1])

    def run_simple_monitoring_prepared_check(self, e, t):
        while not e.isSet():
            logging.debug('wait_for_event_timeout starting')
            event_is_set = e.wait(t)
            logging.debug('event set: %s', event_is_set)
            if event_is_set:
                logging.info('CHECKING # OF USES FOR THE PREPARED STATEMENT ON NODE "%s"' % self.servers[0])
                result = self.run_cbq_query('select * from system:prepareds')
                if not result['results'][0]['prepareds']['uses'] == 2:
                    self.threadFailure = True
                    logging.error(
                        'THE PREPARED STATEMENT SHOULD HAVE 2 USES, BUT ONLY "%s" USES HAVE BEEN REPORTED'
                        % result['results'][0]['prepareds']['uses'])
                    self.log.error(json.dumps(result, sort_keys=True, indent=3))
                    return

                # check if the running queries are in system:active_requests
                logging.info('CHECKING SYSTEM:ACTIVE_REQUESTS FOR THE RUNNING QUERIES')
                result = self.run_cbq_query('select * from system:active_requests')
                if not result['metrics']['resultCount'] == 3:
                    self.threadFailure = True
                    logging.error(
                        'NOT ALL ACTIVE QUERIES ARE IN ACTIVE_REQUESTS, THERE SHOULD BE 2 QUERIES ACTIVE. %s'
                        ' QUERIES ARE ACTIVE.' % result['metrics']['resultCount'])
                    self.log.error(json.dumps(result, sort_keys=True, indent=3))
                    return

                time.sleep(30)
                # Check if the completed query is in system:completed_requests
                logging.info('CHECKING SYSTEM:COMPLETED_REQUESTS FOR THE COMPLETED QUERIES')
                result = self.run_cbq_query('select * from system:completed_requests')
                if not result['metrics']['resultCount'] == 6:
                    self.threadFailure = True
                    logging.error(
                        'COMPLETED REQUESTS IS DIFFERENT THAN WHAT IS EXPECTED, THERE SHOULD BE 4 QUERIES COMPLETED. %s'
                        ' QUERIES ARE COMPLETED.' % result['metrics']['resultCount'])
                    self.log.error(json.dumps(result, sort_keys=True, indent=3))
                    return

    '''Checks if a prepared request can be killed from system:active_requests:
            -Check if the request can be killed by its requestId
            -Check if the request can be killed by its name.
            -Check if requests can be killed by node.'''

    def test_prepared_kill(self, test_to_run):
        self.test_prepared_common_body("kill")
        # Check that both prepareds are in system:prepareds
        self.query = "select * from system:prepareds"
        result = self.run_cbq_query()
        self.assertEqual(result['metrics']['resultCount'], 6)

        name = result['results'][0]['prepareds']['name']
        secondname = result['results'][1]['prepareds']['name']

        e = threading.Event()
        thread1 = threading.Thread(name='run_prepared_test', target=test_to_run,
                                   args=(e, 2))
        thread2 = threading.Thread(name='run_prepared', target=self.execute_prepared, args=(name, self.servers[0]))
        thread3 = threading.Thread(name='run_prepared', target=self.execute_prepared,
                                   args=(secondname, self.servers[1]))
        thread1.start()
        thread2.start()
        thread3.start()

        e.set()

        thread1.join(100)
        thread2.join(100)
        thread3.join(100)
        self.assertFalse(self.threadFailure)

    '''Helper to check if a prepared statement can be accessed by its requestId'''

    def run_check_requestId(self, e, t):
        while not e.isSet():
            logging.debug('wait_for_event_timeout starting')
            event_is_set = e.wait(t)
            logging.debug('event set: %s', event_is_set)
            if event_is_set:
                time.sleep(1)
                result = self.run_cbq_query('select * from system:active_requests')
                # check if a query can be accessed from system:active_requests using its requestId
                logging.info("CHECKING IF A QUERY CAN BE ACCESSED VIA ITS requestId")
                requestId = result['results'][2]['active_requests']['requestId']
                result = self.run_cbq_query('select * from system:active_requests where requestId  =  "%s"'
                                            % requestId)
                if not result['metrics']['resultCount'] == 1:
                    self.threadFailure = True
                    logging.error('THE QUERY FOR requestId "%s" IS NOT IN ACTIVE_REQUESTS' % requestId)
                    self.log.error(json.dumps(result, sort_keys=True, indent=3))
                    return

    '''Helper to check if a prepared statement can be killed by its requestId'''

    def run_kill_prepared_by_requestId(self, e, t):
        while not e.isSet():
            logging.debug('wait_for_event_timeout starting')
            event_is_set = e.wait(t)
            logging.debug('event set: %s', event_is_set)
            if event_is_set:
                result = self.run_cbq_query('select * from system:active_requests')
                requestId = result['results'][0]['active_requests']['requestId']

                # check if a query can be killed from system:active_requests using its requestId
                logging.info("CHECKING IF A QUERY CAN BE KILLED BY REQUESTID")
                self.run_cbq_query(
                    'delete from system:active_requests where requestId  =  "%s"' % requestId)
                result = self.run_cbq_query(
                    'select * from system:active_requests  where requestId  =  "%s"'
                    % requestId)
                if not result['metrics']['resultCount'] == 0:
                    self.threadFailure = True
                    logging.error(
                        'THE QUERY FOR requestId "%s" WAS NOT KILLED AND IS STILL IN ACTIVE_REQUESTS'
                        % requestId)
                    self.log.error(json.dumps(result, sort_keys=True, indent=3))
                    return

    '''Helper to check if a prepared statement can be killed by its preparedName'''

    def run_kill_prepared_by_name(self, e, t):
        while not e.isSet():
            logging.debug('wait_for_event_timeout starting')
            event_is_set = e.wait(t)
            logging.debug('event set: %s', event_is_set)
            if event_is_set:
                result = self.run_cbq_query('select * from system:active_requests')
                # Check if a request can be killed by query name
                logging.info("CHECKING IF A QUERY CAN BE KILLED BY NAME")
                preparedName = result['results'][2]['active_requests']['preparedName']
                self.run_cbq_query(
                    'delete from system:active_requests where preparedName  =  "%s"' % preparedName)
                result = self.run_cbq_query(
                    'select * from system:active_requests  where preparedName  =  "%s"'
                    % preparedName)
                if not result['metrics']['resultCount'] == 0:
                    self.threadFailure = True
                    logging.error(
                        'THE QUERY FOR name "%s" WAS NOT KILLED AND IS STILL IN ACTIVE_REQUESTS'
                        % preparedName)
                    self.log.error(json.dumps(result, sort_keys=True, indent=3))
                    return

    '''Helper to check if a prepared statement/multiple prepared statements can be killed by node'''

    def run_kill_prepared_by_node(self, e, t):
        while not e.isSet():
            logging.debug('wait_for_event_timeout starting')
            event_is_set = e.wait(t)
            logging.debug('event set: %s', event_is_set)
            if event_is_set:
                self.query = "select * from system:prepareds"
                result = self.run_cbq_query()
                name = result['results'][0]['prepareds']['name']
                secondname = result['results'][1]['prepareds']['name']

                thread1 = threading.Thread(name='run_prepared',
                                           target=self.execute_prepared,
                                           args=(name, self.servers[0]))
                thread2 = threading.Thread(name='run_prepared',
                                           target=self.execute_prepared,
                                           args=(name, self.servers[0]))
                thread3 = threading.Thread(name='run_prepared',
                                           target=self.execute_prepared,
                                           args=(secondname, self.servers[1]))

                thread1.start()
                thread2.start()
                thread3.start()

                # Check if a request can be killed by query node
                logging.info("CHECKING IF A QUERY CAN BE KILLED BY NODE")
                time.sleep(0.3)
                self.run_cbq_query(
                    'delete from system:active_requests where node  =  "%s:%s"'
                    % (self.servers[0].ip, self.servers[0].port))
                result = self.run_cbq_query(
                    'select * from system:active_requests  where node  =  "%s:%s"'
                    % (self.servers[0].ip, self.servers[0].port), server=self.servers[1])
                if not result['metrics']['resultCount'] == 0:
                    self.threadFailure = True
                    logging.error('THE QUERIES FOR node "%s" WERE NOT KILLED AND ARE STILL IN ACTIVE_REQUESTS'
                                  % self.servers[0].ip)
                    self.log.error(json.dumps(result, sort_keys=True, indent=3))
                    return

                result = self.run_cbq_query(
                    'select * from system:active_requests  where node  =  "%s:%s"'
                    % (self.servers[1].ip, self.servers[1].port))
                if not result['metrics']['resultCount'] == 2:
                    self.threadFailure = True
                    logging.error(
                        'THE QUERIES FOR node "%s" SHOULD NOT HAVE BEEN KILLED'
                        % self.servers[1].ip)
                    self.log.error(json.dumps(result, sort_keys=True, indent=3))
                    return

    ##############################################################################################
    #
    #   Configuration Test Settings (Completed_requests configuration settings)
    #
    ##############################################################################################

    '''Check that the configuration settings for system:completed_requests can be changed'''

    def test_set_completed_config(self):
        # Change the collection setting
        response, content = self.rest.set_completed_requests_collection_duration(self.master, 10000)
        result = json.loads(content)
        self.assertEqual(result['completed-threshold'], 10000)

        response, content = self.rest.set_completed_requests_collection_duration(self.master, 1000)
        result = json.loads(content)
        self.assertEqual(result['completed-threshold'], 1000)

        # Change the retention setting
        response, content = self.rest.set_completed_requests_max_entries(self.master, 10)
        result = json.loads(content)
        self.assertEqual(result['completed-limit'], 10)

        response, content = self.rest.set_completed_requests_max_entries(self.master, 4000)
        result = json.loads(content)
        self.assertEqual(result['completed-limit'], 4000)

    '''Check that you can change the maximum number of entries that system:completed requests keeps at one time.'''

    def test_retention_config(self):
        try:
            for server in self.servers[:self.nodes_init]:
                self.rest.set_completed_requests_max_entries(server, 4000)
            num_entries = 10
            # Change the retention setting to only hold the amount of queries specified by num_entries
            for server in self.servers[:self.nodes_init]:
                response, content = self.rest.set_completed_requests_max_entries(server, num_entries)
                result = json.loads(content)
                self.assertEqual(result['completed-limit'], num_entries)
        
            # Run more than num_entries(10) queries. No explicit duration comment here, so keep each query > 1 second.
            for _ in range(num_entries * 2):
                self._run_monitoring_sleep_query()

            time.sleep(1)
            result = self.run_cbq_query('select * from system:completed_requests')
            self.assertEqual(result['metrics']['resultCount'], 10)

            # negative should disable the limit
            num_entries = -1
            for server in self.servers[:self.nodes_init]:
                response, content = self.rest.set_completed_requests_max_entries(server, num_entries)
                result = json.loads(content)
                self.assertEqual(result['completed-limit'], num_entries)

            for _ in range(100):
                self._run_monitoring_sleep_query()

            time.sleep(1)
            result = self.run_cbq_query('select * from system:completed_requests')
            self.assertEqual(result['metrics']['resultCount'], 110)

            # 0 should disable logging
            num_entries = 0
            for server in self.servers[:self.nodes_init]:
                response, content = self.rest.set_completed_requests_max_entries(server, num_entries)
                result = json.loads(content)
                self.assertEqual(result['completed-limit'], num_entries)

            self._run_monitoring_sleep_query()

            result = self.run_cbq_query('select * from system:completed_requests')
            self.assertEqual(result['metrics']['resultCount'], 110)
        finally:
            for server in self.servers:
                self.rest.set_completed_requests_max_entries(server, 4000)

    '''Check that you can change the min duration a query has to run for to be stored in system:completed_requests'''

    def test_collection_config(self):
        try:
            for server in self.servers[:self.nodes_init]:
                self.rest.set_completed_requests_collection_duration(server, 1000)
            # Test the default setting of 1 second
            self.run_cbq_query('select * from system:active_requests')
            self._run_monitoring_sleep_query()
            result = self.run_cbq_query('select * from system:completed_requests')
            self.assertEqual(result['metrics']['resultCount'], 1)
            # Wipe the completed logs for the next test
            self.run_cbq_query('delete from system:completed_requests')

            # Change the minimum number of milliseconds a query needs to run for to be collected, in this case 3.5 seconds.
            min_duration = 3500
            # Change the collection setting
            for server in self.servers[:self.nodes_init]:
                response, content = self.rest.set_completed_requests_collection_duration(server, min_duration)
                result = json.loads(content)
                self.assertEqual(result['completed-threshold'], min_duration)
            # Construct sleep queries that run for 5 seconds.
            self._run_monitoring_sleep_query(5000)
            self._run_monitoring_sleep_query(5000)
            # Run a query that runs for a normal amount of time ~1.5 seconds
            self._run_monitoring_sleep_query()
            self._run_monitoring_sleep_query()

            # Only the queries run for longer than 3.5 seconds should show up.
            result = self.run_cbq_query('select * from system:completed_requests')
            self.assertEqual(result['metrics']['resultCount'], 2)
            # Wipe the completed logs for the next test
            self.run_cbq_query('delete from system:completed_requests')

            # Check 1 millisecond, basically any query should show up here
            min_duration = 1
            for server in self.servers[:self.nodes_init]:
                response, content = self.rest.set_completed_requests_collection_duration(server, min_duration)
                result = json.loads(content)
                self.assertEqual(result['completed-threshold'], min_duration)

            self.run_cbq_query('select * from system:active_requests')
            self._run_monitoring_sleep_query()
            result = self.run_cbq_query('select * from system:completed_requests')
            self.assertEqual(result['metrics']['resultCount'], 2)

            # Disable logging
            min_duration = -1
            # Change the collection setting
            for server in self.servers[:self.nodes_init]:
                response, content = self.rest.set_completed_requests_collection_duration(server, min_duration)
            result = json.loads(content)
            self.assertTrue(result['completed-threshold'] == min_duration)
            self.run_cbq_query('delete from system:completed_requests')
            self._run_monitoring_sleep_query()
            self._run_monitoring_sleep_query()

            # No queries should appear
            result = self.run_cbq_query('select * from system:completed_requests')
            self.assertEqual(result['metrics']['resultCount'], 0)
        finally:
            for server in self.servers:
                self.rest.set_completed_requests_collection_duration(server, 1000)

    def test_completed_requests_large_parameters(self):
        """MB-75217: system:completed_requests must not pin the request body of a large parameter.

        Before the fix each logged entry held the request's own namedArgs/positionalArgs and
        statement (zero-copy slices of the POST body), so the query heap grew by ~1 body per
        request (10 x 9.6 MB -> +109 MB on 8.5.0-1441). After the fix (8.5.0-1459) parameters are
        stored as a detached JSON copy capped at 64 KiB and replaced by a WARNING above it.
        """
        num_requests = self.input.param("num_requests", 10)
        num_docs = self.input.param("num_docs", 1000)
        statement = "SELECT RAW 1"
        warning_text = "exceed maximum permitted (64.0 KiB) size"
        large_data = {"_default": [{"docId": "k%05d" % i, "doc": {"id": i, "pad": "x" * 10000}}
                                   for i in range(num_docs)]}
        large_body = json.dumps({"statement": statement, "$data": large_data})
        body_size = len(large_body)
        try:
            self.rest.set_completed_requests_collection_duration(self.master, 0)
            self.run_cbq_query('DELETE FROM system:completed_requests')

            heap_before = self._query_heap_after_gc()
            for i in range(num_requests):
                result = self._post_query_body(large_body)
                self.assertEqual(result['status'], 'success', f"MB-75217: request {i} failed: {result}")
                self.assertEqual(result['results'], [1], f"MB-75217: request {i} wrong results: {result}")
            heap_after = self._query_heap_after_gc()
            heap_growth = heap_after - heap_before
            self.log.info(f"MB-75217: body {body_size} bytes x {num_requests}; heap {heap_before} -> "
                          f"{heap_after} (+{heap_growth / 1048576:.1f} MB)")
            # Fixed builds grow ~0.1 MB; the bug pins ~1 body per request, so allow less than one body.
            self.assertLess(heap_growth, body_size,
                            f"MB-75217: completed_requests pins request bodies, heap grew "
                            f"{heap_growth / 1048576:.1f} MB for {num_requests} x {body_size} byte requests")

            result = self.run_cbq_query(f"SELECT COUNT(*) AS entries, MAX(ENCODED_SIZE(namedArgs)) AS max_size, "
                                        f"ARRAY_AGG(DISTINCT namedArgs) AS args FROM system:completed_requests "
                                        f"WHERE statement = '{statement}'")
            row = result['results'][0]
            self.assertEqual(row['entries'], num_requests,
                             f"MB-75217: expected {num_requests} completed_requests entries, got {row}")
            self.assertLess(row['max_size'], 65536,
                            f"MB-75217: namedArgs not capped at 64 KiB, size {row['max_size']}")
            self.assertEqual(len(row['args']), 1, f"MB-75217: unexpected namedArgs {row['args']}")
            self.assertIn(warning_text, row['args'][0].get('WARNING', ''),
                          f"MB-75217: oversized namedArgs not replaced by WARNING: {row['args'][0]}")

            # Positional args over the cap get the same WARNING.
            self.run_cbq_query('DELETE FROM system:completed_requests')
            result = self._post_query_body(json.dumps({"statement": "SELECT RAW 2", "args": [large_data]}))
            self.assertEqual(result['status'], 'success', f"MB-75217: positional request failed: {result}")
            result = self.run_cbq_query("SELECT RAW positionalArgs FROM system:completed_requests "
                                        "WHERE statement = 'SELECT RAW 2'")
            self.assertEqual(len(result['results']), 1, f"MB-75217: positional entry missing: {result}")
            self.assertIn(warning_text, json.dumps(result['results'][0]),
                          f"MB-75217: oversized positionalArgs not replaced by WARNING: {result['results'][0]}")

            # Small parameters are still logged in full with the same shape.
            small_args = {"a": 1, "b": ["x", "y"]}
            result = self._post_query_body(json.dumps({"statement": "SELECT RAW 3", "$p": small_args, "args": [7]}))
            self.assertEqual(result['status'], 'success', f"MB-75217: small-args request failed: {result}")
            result = self.run_cbq_query("SELECT namedArgs, positionalArgs FROM system:completed_requests "
                                        "WHERE statement = 'SELECT RAW 3'")
            self.assertEqual(result['results'], [{"namedArgs": {"$p": small_args}, "positionalArgs": [7]}],
                             f"MB-75217: small parameters not logged as-is: {result['results']}")
        finally:
            self.rest.set_completed_requests_collection_duration(self.master, 1000)
            self.run_cbq_query('DELETE FROM system:completed_requests')

    # POSTs a raw JSON request body to the master's query service; query_tool puts parameters in the
    # URL, which cannot carry a multi-MB parameter.
    def _post_query_body(self, body):
        protocol, port = "http", CbServer.n1ql_port
        if CbServer.use_https:
            protocol, port = "https", CbServer.ssl_port_map.get(str(port), str(port))
        headers = self.rest._create_capi_headers()
        status, content, _ = self.rest._http_request(f"{protocol}://{self.master.ip}:{port}/query/service",
                                                     'POST', params=body, headers=headers, timeout=300)
        return json.loads(content)

    # Forces a GC on the master's query service and returns memory.usage (live heap bytes) from admin/vitals.
    def _query_heap_after_gc(self):
        protocol, port = "http", CbServer.n1ql_port
        if CbServer.use_https:
            protocol, port = "https", CbServer.ssl_port_map.get(str(port), str(port))
        for _ in range(3):
            self.rest._http_request(f"{protocol}://{self.master.ip}:{port}/debug/pprof/heap?gc=1", 'GET')
            time.sleep(1)
        status, vitals = self.rest.get_query_vitals(self.master)
        self.assertTrue(status, f"admin/vitals failed on {self.master.ip}: {vitals}")
        return json.loads(vitals)['memory.usage']

    def test_dml_does_not_retain_documents_after_request(self):
        """INSERT/UPSERT/UPDATE/DELETE must not keep the last batch of keys and documents reachable
        from the query service's pooled pair slices once the request has finished.

        Without the fix (8.5.0-1458) the query heap after GC stayed +4/+8/+46/+123 MB above baseline
        after each statement of 10 x 4 MB docs; with the fix (8.5.0-1459) it stays within 0.3 MB.
        Documents are built on the server with REPEAT so no large request body is involved.
        """
        num_docs = self.input.param("num_docs", 10)
        doc_size = self.input.param("doc_size", 4000000)
        query_bucket = self.query_buckets[0]
        keys = json.dumps(["dml_heap_%d" % i for i in range(num_docs)])
        statements = [
            ("INSERT", f"INSERT INTO {query_bucket} (KEY k, VALUE v) SELECT 'dml_heap_' || TOSTR(i) AS k, "
                       f"{{'pad': REPEAT('x', {doc_size})}} AS v FROM ARRAY_RANGE(0, {num_docs}) AS i"),
            ("UPSERT", f"UPSERT INTO {query_bucket} (KEY k, VALUE v) SELECT 'dml_heap_' || TOSTR(i) AS k, "
                       f"{{'pad': REPEAT('u', {doc_size})}} AS v FROM ARRAY_RANGE(0, {num_docs}) AS i"),
            ("UPDATE", f"UPDATE {query_bucket} USE KEYS {keys} SET pad = REPEAT('y', {doc_size})"),
            # The field predicate makes DELETE fetch the documents, so their values go into the pairs.
            ("DELETE", f"DELETE FROM {query_bucket} USE KEYS {keys} WHERE pad IS NOT NULL"),
        ]
        try:
            for _ in range(3):
                self.run_cbq_query('SELECT RAW 1')
            heap_before = self._query_heap_after_gc()
            heap_growth = {}
            for op, statement in statements:
                result = self.run_cbq_query(statement)
                self.assertEqual(result['metrics']['mutationCount'], num_docs,
                                 f"{op} of {num_docs} large documents did not mutate all of them: {result['metrics']}")
                # The heap right after the DML is noisy; measure once unrelated requests have run.
                for _ in range(20):
                    self.run_cbq_query('SELECT RAW 1')
                heap_after = self._query_heap_after_gc()
                heap_growth[op] = heap_after - heap_before
                self.log.info(f"after {op}: heap {heap_before} -> {heap_after} "
                              f"(+{heap_growth[op] / 1048576:.1f} MB)")
            result = self.run_cbq_query(f"SELECT RAW COUNT(*) FROM {query_bucket} USE KEYS {keys}")
            self.assertEqual(result['results'], [0], f"DELETE left documents behind: {result['results']}")
            # Growth is cumulative over the baseline, so every step is checked against the same bound.
            growth_mb = ", ".join(f"{op} +{growth / 1048576:.1f} MB" for op, growth in heap_growth.items())
            retained = [op for op, growth in heap_growth.items() if growth >= doc_size]
            self.assertFalse(retained, f"query heap still holds documents after {', '.join(retained)} finished "
                                       f"({num_docs} x {doc_size} byte docs; growth over baseline: {growth_mb})")
        finally:
            self.run_cbq_query(f"DELETE FROM {query_bucket} USE KEYS {keys}")

    # This helper executes the JS UDF above, which busy-waits for delay_ms milliseconds
    # so the monitoring tests can deterministically control query duration. If no
    # server is specified, it runs on the first server.
    def _run_monitoring_sleep_query(self, delay_ms=1500, server=None):
        if server is None:
            server = self.servers[1]
        return self.run_cbq_query('EXECUTE FUNCTION monitoring_sleep({0})'.format(delay_ms), server=server)
