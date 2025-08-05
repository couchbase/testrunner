from .tuq import QueryTests
import threading
import json
import time
from membase.api.rest_client import RestHelper
from membase.api.exception import CBQError

class QueryGracefulFailoverTests(QueryTests):

    def setUp(self):
        super(QueryGracefulFailoverTests, self).setUp()
        self.log.info("==============  QueryFilterTests setup has started ==============")
        self.graceful = self.input.param("graceful", False)
        self.action = self.input.param("action", "")
        self.thread_count = self.input.param("thread_count", 12)
        self.servicers_count = self.input.param("servicers_count", 8)

        # Because we want to have queries in running and queued state, number of query thread should be
        # greater than the number of servicers.
        self.assertTrue(self.thread_count > self.servicers_count, f"Make sure thread_count [{self.thread_count}] is greater than servicers_count [{self.servicers_count}]")

        # Let's make the query node (second node) has proper services per services_init
        query_node = self.servers[1]
        current_node_services = self.get_nodes_services()
        node_services = self.get_services(self.servers, self.services_init, start_node=1)
        if query_node.ip not in current_node_services.keys() or not current_node_services[query_node.ip] == node_services[0]:
            self.log.info(f"Remove and add node:{query_node} to the cluster with services: {node_services}")
            self.cluster.rebalance(self.servers, [], [query_node])
            self.cluster.rebalance(self.servers, [query_node], [], services = node_services)
        else:
            self.log.info(f"Query node aready has services: {current_node_services[query_node.ip]}")

        self.log.info("==============  QueryGracefulFailoverTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryGracefulFailoverTests, self).suite_setUp()
        self.log.info("==============  QueryGracefulFailoverTests suite_setup has started ==============")
        # Add Javascript library and sleep function. This will be used to simulate long running queries.
        functions = 'function sleep(delay) { var start = new Date().getTime(); while (new Date().getTime() < start + delay); return delay; }'
        function_names = ["sleep"]
        self.log.info("Create helper library")
        self.create_library("helper", functions, function_names)
        self.run_cbq_query('CREATE OR REPLACE FUNCTION sleep(t) LANGUAGE JAVASCRIPT AS "sleep" AT "helper"')
        self.log.info("==============  QueryGracefulFailoverTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueryGracefulFailoverTests tearDown has started ==============")
        self.log.info("==============  QueryGracefulFailoverTests tearDown has completed ==============")
        super(QueryGracefulFailoverTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QueryGracefulFailoverTests suite_tearDown has started ==============")
        self.log.info("==============  QueryGracefulFailoverTests suite_tearDown has completed ==============")
        super(QueryGracefulFailoverTests, self).suite_tearDown()

    def run_query(self, query, server, results, index=1, waitfor=0, txid=None):
        try:
            self.sleep(waitfor)
            if txid:
                response = self.run_cbq_query(query=query,server=server,txnid=txid)
                commit = self.run_cbq_query(query="COMMIT",server=server,txnid=txid)
                try:
                    # We expect the query to fail here with error: "Service shut down"
                    # as we expect to still be in middle of the failover at this point
                    # Update: because of MB-56314 we don't expect to fail now
                    fail = self.run_cbq_query(query="select 10",server=server)
                    # self.log.fail("Query should have failed with error code 1180/1181")
                except CBQError as ex:
                    error = self.process_CBQE(ex)
                    self.assertTrue(error['code'] == 1180 or error['code'] == 1181)
                    self.assertTrue(error['msg'] == "Service shut down" or error['msg'] == "Service shutting down")
            else:
                response = self.run_cbq_query(query=query,server=server)
            results[index] = response['results']
            self.log.info(results[index])
        except Exception as e:
            self.log.error(f"Query failed: {e}")

    def test_failover(self):
        sleep_time_ms = 10000
        threads = [None] * self.thread_count
        results = [None] * self.thread_count
        query_node = self.servers[1]
        self.servicers_count = 8

        # Lower query servicers setting to limit number of query threads that will be running
        self.log.info(f"Set query servicers to {self.servicers_count}")
        response, content = self.rest.set_query_servicers(query_node, self.servicers_count, servicers="plus-servicers")
        self.assertEqual(response['status'], '200')
        settings = json.loads(content)
        self.assertEqual(settings['plus-servicers'], self.servicers_count)

        # Launch query threads
        select_statement = f"select sleep({sleep_time_ms})"
        for i in range(len(threads)):
            self.log.info(f"Lauching query thread {i}")
            threads[i] = threading.Thread(target=self.run_query,args=(select_statement, query_node, results, i))
            threads[i].start()

        # Check active requests for overall cluster: running should be equal to number of servicers while submitted will be remaining that are in effect queued.
        self.sleep(3)    
        active_requests = self.run_cbq_query(f"SELECT state, count(*) as count FROM system:active_requests WHERE statement = '{select_statement}' and node like '{query_node.ip}%' GROUP BY state ORDER BY state")
        self.log.info(active_requests['results'])
        self.assertEqual(active_requests['results'][0]['state'], "running")
        self.assertEqual(active_requests['results'][0]['count'], self.servicers_count)
        self.assertEqual(active_requests['results'][1]['state'], "submitted")
        self.assertEqual(active_requests['results'][1]['count'], max(0, self.thread_count - self.servicers_count))

        # Check stats for query node: active are all executed query thread (running + queued).
        # Queued will be the diff between active count and servicers.
        query_stats = self.rest.query_tool_stats(query_node)
        self.log.info(f"active_requests: {query_stats['active_requests.count']}")
        self.log.info(f"queued_requests: {query_stats['queued_requests.count']}")
        self.assertEqual(query_stats['active_requests.count'], self.thread_count)
        self.assertEqual(query_stats['queued_requests.count'], max(0, self.thread_count - self.servicers_count))
        
        # Perform failover or remove of query node
        if self.action == 'failover':
            failover = self.cluster.failover(servers=self.servers, failover_nodes=[query_node], graceful=self.graceful)
        elif self.action == 'remove':
            rebalance = self.cluster.async_rebalance(servers=self.servers, to_add=[], to_remove=[query_node])
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()

        # Check that at least 70% of the query threads completed successfully
        for i in range(len(threads)):
            threads[i].join()
        self.log.info(results)
        success_count = 0
        for i in range(len(threads)):
            if results[i] == [{'$1': sleep_time_ms}]:
                success_count += 1
        required_successes = int(0.7 * self.thread_count)
        self.log.info(f"Successful queries: {success_count} out of {self.thread_count} (required: {required_successes})")
        self.assertGreaterEqual(success_count, required_successes, 
            f"Expected at least {required_successes} successful queries, but got {success_count}")

    def test_failover_transaction(self):
        query_node = self.servers[1]
        sleep_time_ms = 10000
        threads = [None] * self.thread_count
        results = [None] * self.thread_count

        # Start a transaction
        begin_work = self.run_cbq_query(query="BEGIN WORK", server=query_node, txtimeout="2m")
        txid = begin_work['results'][0]['txid']

        # Launch query thread/s (should be single)
        select_statement = f"select {sleep_time_ms}"
        for i in range(len(threads)):
            self.log.info(f"Lauching query thread {i}")
            threads[i] = threading.Thread(target=self.run_query,args=(select_statement, query_node, results, i, 60, txid))
            threads[i].start()

        # Perform failover or removal of query node
        self.sleep(2)
        if self.action == 'failover':
            failover = self.cluster.failover(servers=self.servers, failover_nodes=[query_node], graceful=self.graceful)
        elif self.action == 'remove':
            rebalance = self.cluster.async_rebalance(servers=self.servers, to_add=[], to_remove=[query_node])
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()

        # Check query thread/s completed successfuly
        for i in range(len(threads)):
            threads[i].join()
        self.log.info(results)
        for i in range(len(threads)):
            self.assertEqual(results[i], [{'$1': sleep_time_ms}])

    def test_failover_transaction_timeout(self):
        query_node = self.servers[1]

        # Start a transaction and let it time out
        begin_work = self.run_cbq_query(query="BEGIN WORK", server=query_node, txtimeout="1m")
        txid = begin_work['results'][0]['txid']
        self.log.info(f"Started transaction ID:{txid}")

        # Perform failover or removal of query node
        self.sleep(2)
        start_time = time.time()
        if self.action == 'failover':
            failover = self.cluster.failover(servers=self.servers, failover_nodes=[query_node], graceful=self.graceful)
            self.assertTrue(failover)
        elif self.action == 'remove':
            rebalance = self.cluster.async_rebalance(servers=self.servers, to_add=[], to_remove=[query_node])
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
        end_time = time.time()
        time_elapsed = (end_time - start_time)
        self.assertTrue(60 <= time_elapsed <= 180, "Failover should have completed once transaction timed out after 1min")