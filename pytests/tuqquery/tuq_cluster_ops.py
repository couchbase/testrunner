import math

from tuqquery.tuq import QueryTests


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
