from membase.api.rest_client import RestConnection, RestHelper
from tuq import QueryTests
from remote.remote_util import RemoteMachineShellConnection
from membase.api.exception import CBQError
import logger


class QueryAutoPrepareTests(QueryTests):
    def setUp(self):
        super(QueryAutoPrepareTests, self).setUp()
        self.log.info("==============  QueryAutoPrepareTests setup has started ==============")
        self.run_cbq_query('delete from system:prepareds')
        self.log.info("==============  QueryAutoPrepareTests setup has completed ==============")
        self.log_config_info()


    def suite_setUp(self):
        super(QueryAutoPrepareTests, self).suite_setUp()
        self.log.info("==============  QueryAutoPrepareTests suite_setup has started ==============")
        self.log.info("==============  QueryAutoPrepareTests suite_setup has completed ==============")
        self.log_config_info()

    def tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryAutoPrepareTests tearDown has started ==============")
        self.log.info("==============  QueryAutoPrepareTests tearDown has completed ==============")
        super(QueryAutoPrepareTests, self).tearDown()

    def suite_tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryAutoPrepareTests suite_tearDown has started ==============")
        self.log.info("==============  QueryAutoPrepareTests suite_tearDown has completed ==============")
        super(QueryAutoPrepareTests, self).suite_tearDown()

    '''Test auto-prepare, prepare on first node, check if it is prepared on both nodes and that it can be executed on 
       both nodes'''
    def test_basic_auto_prepare(self):
        self.run_cbq_query(query="PREPARE P1 FROM select * from default limit 5", server=self.servers[0])
        self.sleep(2)
        prepared_results = self.run_cbq_query(query="select * from system:prepareds")
        self.assertEqual(prepared_results['metrics']['resultCount'], 2)
        query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
        self.assertEqual(query_results['metrics']['resultCount'], 5)
        query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
        self.assertEqual(query_results2['metrics']['resultCount'], 5)

    '''Test if you can execute a prepared statement by its name in clustered format [ip:port]<prepared_name> , if a node
       doesn't have the prepared statement it should be able to pull it from a node that does'''
    def test_pull_prepare(self):
        prepared_result = self.run_cbq_query(query="PREPARE P1 FROM select * from default limit 5", server=self.servers[0])
        self.sleep(2)
        self.query = "delete from system:prepareds where node = '%s:%s'" \
                     % (self.servers[1].ip,self.servers[1].port)
        self.run_cbq_query()
        query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
        self.assertEqual(query_results['metrics']['resultCount'], 5)

        query_results2 = self.run_cbq_query(query="execute '[%s:%s]P1'"
                                                  % (self.servers[0].ip, self.servers[0].port), server=self.servers[1])
        self.assertEqual(query_results2['metrics']['resultCount'], 5)

    '''Delete docs to change the index that the index is using, should be able to execute prepareds without repreparing'''
    def test_change_index_delete_docs(self):
        try:
            self.run_cbq_query(query= "CREATE INDEX idx on default(join_day)")
            self.run_cbq_query(query="PREPARE P1 FROM select * from default WHERE join_day = 10 limit 5",
                               server=self.servers[0])
            self.sleep(2)
            prepared_results = self.run_cbq_query(query="select * from system:prepareds")
            self.assertEqual(prepared_results['metrics']['resultCount'], 2)
            query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
            self.assertEqual(query_results['metrics']['resultCount'], 5)
            query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
            self.assertEqual(query_results2['metrics']['resultCount'], 5)

            self.run_cbq_query(query="DELETE FROM default LIMIT 10")

            self.assertEqual(prepared_results['metrics']['resultCount'], 2)
            query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
            self.assertEqual(query_results['metrics']['resultCount'], 5)
            query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
            self.assertEqual(query_results2['metrics']['resultCount'], 5)
        finally:
            self.run_cbq_query(query="DROP INDEX default.idx")

    '''Drop an index and create a new index on the same field, this is a new index that the prepared needs to use,
       this should trigger a re-prepare'''
    def test_recreate_index(self):
        try:
            self.run_cbq_query(query="CREATE INDEX idx on default(join_day)")
            self.run_cbq_query(query="PREPARE P1 FROM select * from default WHERE join_day = 10 limit 5",
                               server=self.servers[0])
            self.sleep(2)
            prepared_results = self.run_cbq_query(query="select * from system:prepareds")
            self.assertEqual(prepared_results['metrics']['resultCount'], 2)
            query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
            self.assertEqual(query_results['metrics']['resultCount'], 5)
            query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
            self.assertEqual(query_results2['metrics']['resultCount'], 5)

            self.run_cbq_query(query="DROP INDEX default.idx")
            self.sleep(5)
            self.run_cbq_query(query="CREATE INDEX idx2 on default(join_day)")

            query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
            self.assertEqual(query_results['metrics']['resultCount'], 5)
            query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
            self.assertEqual(query_results2['metrics']['resultCount'], 5)
        finally:
            self.run_cbq_query(query="DROP INDEX default.idx2")

    '''Run a prepared statement using primary index, then drop primary index and create a new index that the query will
       use instead'''
    def test_new_index(self):
        try:
            self.run_cbq_query(query="PREPARE P1 FROM select * from default WHERE join_day = 10 limit 5",
                               server=self.servers[0])
            self.sleep(2)
            prepared_results = self.run_cbq_query(query="select * from system:prepareds")
            self.assertEqual(prepared_results['metrics']['resultCount'], 2)
            query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
            self.assertEqual(query_results['metrics']['resultCount'], 5)
            query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
            self.assertEqual(query_results2['metrics']['resultCount'], 5)

            self.run_cbq_query(query="DROP PRIMARY INDEX on default")
            self.sleep(5)
            self.run_cbq_query(query="CREATE INDEX idx on default(join_day)")

            query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
            self.assertEqual(query_results['metrics']['resultCount'], 5)
            query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
            self.assertEqual(query_results2['metrics']['resultCount'], 5)
        finally:
            self.run_cbq_query(query="DROP INDEX default.idx")
            self.run_cbq_query(query="CREATE PRIMARY INDEX ON default")

    '''Alter the node the index is present on to trigger a re-prepare'''
    def test_alter_index(self):
        try:
            self.run_cbq_query(query="CREATE INDEX idx on default(join_day)")
            self.run_cbq_query(query="PREPARE P1 FROM select * from default WHERE join_day = 10 limit 5",
                               server=self.servers[0])
            self.sleep(2)
            prepared_results = self.run_cbq_query(query="select * from system:prepareds")
            self.assertEqual(prepared_results['metrics']['resultCount'], 2)
            query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
            self.assertEqual(query_results['metrics']['resultCount'], 5)
            query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
            self.assertEqual(query_results2['metrics']['resultCount'], 5)

            self.run_cbq_query(query="ALTER INDEX default.idx WITH {'action':'move','nodes':['%s:%s']}" % (self.servers[0].ip, self.servers[0].port))
            self.sleep(5)

            query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
            self.assertEqual(query_results['metrics']['resultCount'], 5)
            query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
            self.assertEqual(query_results2['metrics']['resultCount'], 5)
        finally:
            self.run_cbq_query(query="DROP INDEX default.idx")

    def test_delete_recreate_bucket(self):
        try:
            self.run_cbq_query(query="CREATE INDEX idx on default(join_day)")
            self.run_cbq_query(query="PREPARE P1 FROM select * from default WHERE join_day = 10 limit 5",
                               server=self.servers[0])
            self.sleep(2)
            prepared_results = self.run_cbq_query(query="select * from system:prepareds")
            self.assertEqual(prepared_results['metrics']['resultCount'], 3)
            query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
            self.assertEqual(query_results['metrics']['resultCount'], 5)
            query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
            self.assertEqual(query_results2['metrics']['resultCount'], 5)

            self.rest.delete_bucket("default")
            self.sleep(5)

            self.rest.create_bucket(bucket="default", ramQuotaMB=100)
            self.sleep(5)
            self.run_cbq_query(query="CREATE INDEX idx on default(join_day)")

            query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
            self.assertEqual(query_results['metrics']['resultCount'], 0)
            query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
            self.assertEqual(query_results2['metrics']['resultCount'], 0)
        finally:
            self.run_cbq_query(query="DROP INDEX default.idx")

    def test_add_node_no_rebalance(self):
        services_in = ["index", "n1ql", "kv"]
        # rebalance in a node
        rest = RestConnection(self.master)
        rest.add_node(self.master.rest_username, self.master.rest_password, self.servers[self.nodes_init].ip,
                      self.servers[self.nodes_init].port, services=services_in)
        self.sleep(30)
        self.run_cbq_query(query="PREPARE p1 from select * from default limit 5", server=self.servers[0])
        self.sleep(5)
        nodes = rest.node_statuses()
        rest.rebalance(otpNodes=[node.id for node in nodes], ejectedNodes=[])
        self.sleep(30)
        for i in range(self.nodes_init + 1):
            try:
                self.run_cbq_query(query="execute p1", server=self.servers[i])
            except CBQError,ex:
                self.assertTrue("No such prepared statement: p1" in str(ex), "There error should be no such prepared "
                                                                             "statement, it really is %s" % ex)
                self.log.info(ex)
                self.log.info("node: %s:%s does not have the statement" % (self.servers[i].ip, self.servers[i].port))

    def test_server_drop(self):
        remote = RemoteMachineShellConnection(self.servers[1])
        remote.stop_server()
        self.sleep(30)

        try:
            self.run_cbq_query(query="PREPARE p1 from select * from default limit 5", server=self.servers[1])
            self.sleep(5)
        finally:
            remote.start_server()
            self.sleep(30)

        for i in range(self.nodes_init + 1):
            try:
                self.run_cbq_query(query="execute p1", server=self.servers[i])
            except CBQError,ex:
                self.assertTrue("No such prepared statement: p1" in str(ex), "There error should be no such prepared "
                                                                             "statement, it really is %s" % ex)
                self.log.info(ex)
                self.log.info("node: %s:%s does not have the statement" % (self.servers[i].ip, self.servers[i].port))

    def test_rebalance_in_query_node(self):
        self.run_cbq_query(query="PREPARE p1 from select * from default limit 5", server=self.servers[0])
        self.sleep(5)
        for i in range(self.nodes_init):
            self.run_cbq_query(query="execute p1", server=self.servers[i])
        services_in = ["n1ql", "index", "data"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init + 1]],[],
                                                 services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)
        for i in range(self.nodes_init + 2):
            self.run_cbq_query(query="execute '[%s:%s]p1'" % (self.servers[0].ip, self.servers[0].port), server=self.servers[i])

    def test_query_swap_rebalance(self):
        self.run_cbq_query(query="PREPARE p1 from select * from default limit 5", server=self.servers[0])
        self.sleep(5)
        for i in range(self.nodes_init):
            if not self.servers[i] == self.servers[1]:
                self.run_cbq_query(query="execute p1", server=self.servers[i])
        nodes_out_list = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        to_add_nodes = [self.servers[self.nodes_init + 2]]
        to_remove_nodes = [nodes_out_list]
        services_in = ["index", "n1ql", "data"]
        self.log.info(self.servers[:self.nodes_init])
        # do a swap rebalance
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 2], [], to_remove_nodes)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)
        for i in range(self.nodes_init):
            if not self.servers[i] == self.servers[1]:
                self.n1ql_helper.run_cbq_query(query="execute '[%s:%s]p1'" % (self.servers[2].ip, self.servers[2].port),
                                               server=self.servers[i])
