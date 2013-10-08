import math

from tuqquery.tuq import QueryTests
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection

class QueriesOpsTests(QueryTests):
    def setUp(self):
        super(QueriesOpsTests, self).setUp()

    def suite_setUp(self):
        super(QueriesOpsTests, self).suite_setUp()

    def tearDown(self):
        super(QueriesOpsTests, self).tearDown()

    def suite_tearDown(self):
        super(QueriesOpsTests, self).suite_tearDown()

    def test_incr_rebalance_in(self):
        self.assertTrue(len(self.servers) >= self.nodes_in + 1, "Servers are not enough")
        self.test_order_by_over()
        for i in xrange(1, self.nodes_in + 1):
            rebalance = self.cluster.async_rebalance(self.servers[:i],
                                                     self.servers[i:i+1], [])
            self.test_order_by_over()
            rebalance.result()
            self.test_order_by_over()

    def test_incr_rebalance_out(self):
        self.assertTrue(len(self.servers[:self.nodes_init]) > self.nodes_out + 1,
                        "Servers are not enough")
        self.test_order_by_over()
        for i in xrange(1, self.nodes_out + 1):
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init - (i-1)],
                                    [],
                                    self.servers[self.nodes_init - i:self.nodes_init - (i-1)])
            self.test_order_by_over()
            rebalance.result()
            self.test_order_by_over()

    def test_swap_rebalance(self):
        self.assertTrue(len(self.servers) >= self.nodes_init + self.nodes_in,
                        "Servers are not enough")
        self.test_order_by_over()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                               self.servers[self.nodes_init:self.nodes_init + self.nodes_in],
                               self.servers[self.nodes_init - self.nodes_out:self.nodes_init])
        self.test_order_by_over()
        rebalance.result()
        self.test_order_by_over()

    def test_rebalance_with_server_crash(self):
        servr_in = self.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        servr_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
        self.test_group_by_over()
        for i in xrange(3):
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                     servr_in, servr_out)
            self.sleep(5, "Wait some time for rebalance process and then kill memcached")
            remote = RemoteMachineShellConnection(self.servers[self.nodes_init -1])
            remote.terminate_process(process_name='memcached')
            self.test_group_by_over()
            try:
                rebalance.result()
            except:
                pass
        self.cluster.rebalance(self.servers[:self.nodes_init], servr_in, servr_out)
        self.test_group_by_over()

    def test_failover(self):
        servr_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
        self.test_group_by_aggr_fn()
        self.cluster.failover(self.servers[:self.nodes_init], servr_out)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                               [], servr_out)
        self.test_group_by_aggr_fn()
        rebalance.result()
        self.test_group_by_aggr_fn()

    def test_failover_add_back(self):
        servr_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
        self.test_group_by_aggr_fn()

        nodes_all = RestConnection(self.master).node_statuses()
        nodes = []
        for failover_node in servr_out:
            nodes.extend([node for node in nodes_all
                if node.ip != failover_node.ip or str(node.port) != failover_node.port])
        self.cluster.failover(self.servers[:self.nodes_init], servr_out)
        for node in nodes:
            RestConnection(self.master).add_back_node(node.id)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [])
        self.test_group_by_aggr_fn()
        rebalance.result()
        self.test_group_by_aggr_fn()

    def test_autofailover(self):
        autofailover_timeout = 30
        status = self.rest.update_autofailover_settings(True, autofailover_timeout)
        self.assertTrue(status, 'failed to change autofailover_settings!')
        servr_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
        self.test_group_by_aggr_fn()
        remote = RemoteMachineShellConnection(self.servers[self.nodes_init -1])
        try:
            remote.stop_server()
            self.sleep(autofailover_timeout + 10, "Wait for autofailover")
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                   [], servr_out)
            self.test_group_by_aggr_fn()
            rebalance.result()
            self.test_group_by_aggr_fn()
        finally:
            remote.start_server()

    def test_cancel_query_mb_9223(self):
        for bucket in self.buckets:
            self.query = 'SELECT tasks_points.task1 AS points FROM %s AS test ' %(bucket.name) +\
                         'GROUP BY test.tasks_points.task1 ORDER BY points'
            self.log.info("run query to cancel")
            try:
                RestConnection(self.master).query_tool(self.query, timeout=5)
            except:
                self.log.info("query is cancelled")
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"points" : doc["tasks_points"]["task1"]} for doc in full_list]
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: doc['points'])
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['resultset'], expected_result)