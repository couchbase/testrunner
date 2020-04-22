from membase.api.rest_client import RestConnection, RestHelper
from .tuq import QueryTests
from remote.remote_util import RemoteMachineShellConnection
from membase.api.exception import CBQError


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

    ''' Helper function to see if the prepared statements are fully prepared '''
    def check_prepared_finished(self):
        prepared_statements = self.run_cbq_query('select * from system:prepareds')
        return prepared_statements['metrics']['resultCount']

    ''' Helper function that executes the steps involved in running prepared queries with positional/named params'''
    def prepared_common(self, query='', named=False, name='', args=''):
        if named:
            if name == '':
                name = 'named'
            prepared_query = 'PREPARE %s FROM %s' % (name, query)
        else:
            prepared_query = 'PREPARE %s' % (query)

        self.shell.execute_command("%s -u Administrator:password %s:%s/query/service -d statement='%s'"
                                    % (self.curl_path, self.master.ip, self.n1ql_port, prepared_query))

        # Make sure that the prepared statement got prepared on all active nodes (2)
        self.with_retry(lambda: self.check_prepared_finished(), eval=2, delay=1, tries=30)

        # execute the non prepared version of the query to compare results
        curl_output = self.shell.execute_command("%s -u Administrator:password %s:%s/query/service -d statement='%s&%s'"
                                                 % (self.curl_path, self.master.ip, self.n1ql_port, query, args))
        expected_results = self.convert_list_to_json(curl_output[0])

        for i in range(self.nodes_init):
            if named:
                # execute the prepared statement
                curl_output = self.shell.execute_command(
                    "%s -u Administrator:password %s:%s/query/service -d 'prepared =\"%s\"&%s'"
                    % (self.curl_path, self.servers[i].ip, self.n1ql_port, name, args))
                prepared_results = self.convert_list_to_json(curl_output[0])
            else:
                # pull the prepared_name to execute it and ensure it returns the correct results
                node_prepared_name = self.run_cbq_query(
                    'select * from system:prepareds where node = "%s:%s"' % (self.servers[i].ip, self.servers[i].port))
                prepared_name = node_prepared_name['results'][0]['prepareds']['name']

                # execute the prepared statement
                curl_output = self.shell.execute_command(
                    "%s -u Administrator:password %s:%s/query/service -d 'prepared =\"%s\"&%s'"
                    % (self.curl_path, self.servers[i].ip, self.n1ql_port, prepared_name, args))
                prepared_results = self.convert_list_to_json(curl_output[0])


            self.assertEqual(sorted(prepared_results), sorted(expected_results),
                             "Results are not equal server number %s is not returning correct results" % str(i))

    ''' Test anonmyous prepareds with named parameters '''
    def test_anonymous_prepared_named_parameters(self):
        query = 'select * from default where name=$name and join_day=$join_day'
        args = '$name=\"employee-8\"&$join_day=8'
        self.prepared_common(query=query, args=args)

    ''' Test named parameters with a prepared statement explicitly named'''
    def test_named_prepared_named_parameters(self):
        query = 'select * from default where name=$name and join_mo=$join_mo'
        args = '$name=\"employee-9\"&$join_mo=10'
        self.prepared_common(query=query, named=True, name='named', args=args)


    ''' Test anonmyous prepareds with named parameters '''
    def test_anonymous_prepared_positional_parameters_dollar(self):
        query = 'select * from default where name=$1 and join_day=$2'
        args = 'args=[\"employee-8\",8]'
        self.prepared_common(query=query, args=args)

        args = '$1=\"employee-8\"&$2=8'
        self.prepared_common(query=query, args=args)

    ''' Test named parameters with a prepared statement explicitly named'''
    def test_named_prepared_positional_parameters_dollar(self):
        query = 'select * from default where name=$1 and join_mo=$2'
        args = 'args=[\"employee-9\",10]'
        self.prepared_common(query=query, named=True, name='named', args=args)

        args = '$1=\"employee-9\"&$2=10'
        self.prepared_common(query=query, named=True, name='named', args=args)

    ''' Test anonmyous prepareds with named parameters '''
    def test_anonymous_prepared_positional_parameters_question_mark(self):
        query = 'select * from default where name=? and join_day=?'
        args = 'args=[\"employee-8\",8]'
        self.prepared_common(query=query, args=args)

    ''' Test named parameters with a prepared statement explicitly named'''
    def test_named_prepared_positional_parameters_question_mark(self):
        query = 'select * from default where name=? and join_mo=?'
        args = 'args=[\"employee-9\",10]'
        self.prepared_common(query=query, named=True, name='named', args=args)

    ''' Test that you can attempt to prepare the same prepared statement twice'''
    def test_duplicate_prepare(self):
        self.run_cbq_query(query="PREPARE P1 FROM select * from default limit 5", server=self.servers[0])
        self.run_cbq_query(query="PREPARE P1 FROM select * from default limit 5", server=self.servers[0])
        self.with_retry(lambda: self.check_prepared_finished(), eval=2, delay=1, tries=30)

    ''' Test if you can force a prepared statement to be reprepared'''
    def test_prepare_force(self):
        self.run_cbq_query(query="PREPARE P1 FROM select * from default limit 5", server=self.servers[0])
        self.run_cbq_query(query="PREPARE FORCE P1 FROM select * from default limit 5", server=self.servers[0])
        self.with_retry(lambda: self.check_prepared_finished(), eval=2, delay=1, tries=30)


    ''' Test that you can prepare two statements with different names but the same text'''
    def test_different_prepared(self):
        self.run_cbq_query(query="PREPARE P1 FROM select * from default limit 5", server=self.servers[0])
        self.run_cbq_query(query="PREPARE P2 FROM select * from default limit 5", server=self.servers[0])

        self.with_retry(lambda: self.check_prepared_finished(), eval=4, delay=1, tries=30)

        prepared_name = self.run_cbq_query('select * from system:prepareds where name = "P1" ')
        self.assertEqual(prepared_name['metrics']['resultCount'], self.nodes_init)

        second_prepared_name = self.run_cbq_query('select * from system:prepareds where name = "P2" ')
        self.assertEqual(second_prepared_name['metrics']['resultCount'], self.nodes_init)

    ''' Try to prepare two separate queries under one name, should error'''
    def test_negative_prepare(self):
        try:
            self.run_cbq_query(query="PREPARE P1 FROM select * from default limit 5", server=self.servers[0])
            self.run_cbq_query(query="PREPARE P1 FROM select * from default limit 10", server=self.servers[0])
        except CBQError as ex:
            self.log.error(ex)
            self.assertTrue(str(ex).find("Unable to add name: duplicate name: P1") != -1,
                            "Error is incorrect.")

    ''' Try to prepare a query with a syntax error in it '''
    def test_prepare_syntax_error(self):
        try:
            self.run_cbq_query(query="PREPARE P1 FROM select * fro default", server=self.servers[0])
        except CBQError as ex:
            self.log.error(ex)
            self.assertTrue(str(ex).find("syntax error - at fro") != -1,
                            "Error is incorrect.")
    ''' Change query settings so that normal queries are automatically cached'''
    def test_auto_prepare(self):
        # Set the queries run to be automatically prepared
        self.shell.execute_command("%s -u Administrator:password %s:%s/admin/settings -d '{\"auto-prepare\":true}'"
                                   % (self.curl_path, self.master.ip, self.n1ql_port))

        self.run_cbq_query('select * from default', server=self.master)
        self.run_cbq_query('select * from default limit 10', server=self.master)

        # Ensure the two above queries were automatically prepared
        query_1 = self.run_cbq_query('select * from system:prepareds where statement = "select * from default"')
        query_2 = self.run_cbq_query('select * from system:prepareds where statement = "select * from default limit 10"')

        self.assertEqual(query_1['metrics']['resultCount'], 1, "Count mismatch dumping results from system:prepareds: " % query_1)
        self.assertEqual(query_2['metrics']['resultCount'], 1, "Count mismatch dumping results from system:prepareds: " % query_2)

        self.run_cbq_query('select * from default', server=self.master)
        self.run_cbq_query('select * from default limit 10', server=self.master)

        # Make sure the uses goes up since these queries are already prepared
        query_1 = self.run_cbq_query('select * from system:prepareds where statement = "select * from default"')
        query_2 = self.run_cbq_query('select * from system:prepareds where statement = "select * from default limit 10"')

        self.assertEqual(query_1['results'][0]['prepareds']['uses'], 2)
        self.assertEqual(query_2['results'][0]['prepareds']['uses'], 2)

    '''Test auto-prepare, prepare on first node, check if it is prepared on both nodes and that it can be executed on 
       both nodes'''
    def test_basic_auto_prepare(self):
        self.run_cbq_query(query="PREPARE P1 FROM select * from default limit 5", server=self.servers[0])
        self.sleep(2)
        prepared_results = self.run_cbq_query(query="select * from system:prepareds")
        self.assertEqual(prepared_results['metrics']['resultCount'], 2, "Count mismatch dumping results from system:prepareds: " % prepared_results)
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
                     % (self.servers[1].ip, self.servers[1].port)
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
            self._wait_for_index_online("default", "idx")
            self.run_cbq_query(query="PREPARE P1 FROM select * from default WHERE join_day = 10 limit 5",
                               server=self.servers[0])
            self.sleep(2)
            prepared_results = self.run_cbq_query(query="select * from system:prepareds")
            self.assertEqual(prepared_results['metrics']['resultCount'], 2, "Count mismatch dumping results from system:prepareds: " % prepared_results)
            query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
            self.assertEqual(query_results['metrics']['resultCount'], 5)
            query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
            self.assertEqual(query_results2['metrics']['resultCount'], 5)

            self.run_cbq_query(query="DELETE FROM default LIMIT 10")

            self.assertEqual(prepared_results['metrics']['resultCount'], 2, "Count mismatch dumping results from system:prepareds: " % prepared_results)
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
            self._wait_for_index_online("default", "idx")
            self.run_cbq_query(query="PREPARE P1 FROM select * from default WHERE join_day = 10 limit 5",
                               server=self.servers[0])
            self.sleep(2)
            prepared_results = self.run_cbq_query(query="select * from system:prepareds")
            self.assertEqual(prepared_results['metrics']['resultCount'], 2, "Count mismatch dumping results from system:prepareds: " % prepared_results)
            query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
            self.assertEqual(query_results['metrics']['resultCount'], 5)
            query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
            self.assertEqual(query_results2['metrics']['resultCount'], 5)
        finally:
            self.run_cbq_query(query="DROP INDEX default.idx")
            self.wait_for_index_drop("default", "idx", [("join_day", 0)], self.index_type.lower())


        try:
            self.run_cbq_query(query="CREATE INDEX idx2 on default(join_day)")
            self._wait_for_index_online("default", "idx2")
            query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
            self.assertEqual(query_results['metrics']['resultCount'], 5)
            query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
            self.assertEqual(query_results2['metrics']['resultCount'], 5)
        finally:
            self.run_cbq_query(query="DROP INDEX default.idx2")
            self.wait_for_index_drop("default", "idx2", [("join_day", 0)], self.index_type.lower())

    '''Run a prepared statement using primary index, then drop primary index and create a new index that the query will
       use instead'''
    def test_new_index(self):
        try:
            self.run_cbq_query(query="PREPARE P1 FROM select * from default WHERE join_day = 10 limit 5",
                               server=self.servers[0])
            self.sleep(2)
            prepared_results = self.run_cbq_query(query="select * from system:prepareds")
            self.assertEqual(prepared_results['metrics']['resultCount'], 2, "Count mismatch dumping results from system:prepareds: " % prepared_results)
            query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
            self.assertEqual(query_results['metrics']['resultCount'], 5)
            query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
            self.assertEqual(query_results2['metrics']['resultCount'], 5)
        finally:
            self.run_cbq_query(query="DROP PRIMARY INDEX on default")
            self.sleep(5)

        try:
            self.run_cbq_query(query="CREATE INDEX idx on default(join_day)")
            self._wait_for_index_online("default", "idx")
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
            self.run_cbq_query(query="CREATE INDEX idx on default(join_day) WITH {'nodes':['%s:%s']}" % (self.servers[0].ip, self.servers[0].port))
            self._wait_for_index_online("default", "idx")
            self.run_cbq_query(query="PREPARE P1 FROM select * from default WHERE join_day = 10 limit 5",
                               server=self.servers[0])
            self.sleep(2)
            prepared_results = self.run_cbq_query(query="select * from system:prepareds")
            self.assertEqual(prepared_results['metrics']['resultCount'], 2, "Count mismatch dumping results from system:prepareds: " % prepared_results)
            query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
            self.assertEqual(query_results['metrics']['resultCount'], 5)
            query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
            self.assertEqual(query_results2['metrics']['resultCount'], 5)

            self.run_cbq_query(query="ALTER INDEX default.idx WITH {'action':'move','nodes':['%s:%s']}" % (self.servers[1].ip, self.servers[1].port))
            self.sleep(5)

            query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
            self.assertEqual(query_results['metrics']['resultCount'], 5)
            query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
            self.assertEqual(query_results2['metrics']['resultCount'], 5)
        finally:
            self.sleep(5)
            self.run_cbq_query(query="DROP INDEX default.idx")

    def test_delete_recreate_bucket(self):
        try:
            self.run_cbq_query(query="CREATE INDEX idx on default(join_day)")
            self._wait_for_index_online("default", "idx")
            expected_results = self.run_cbq_query(query="select * from default WHERE join_day = 10 limit 5", server=self.servers[0])

            self.run_cbq_query(query="PREPARE P1 FROM select * from default WHERE join_day = 10 limit 5",
                               server=self.servers[0])
            self.sleep(30)
            prepared_results = self.run_cbq_query(query="select * from system:prepareds")
            self.assertEqual(prepared_results['metrics']['resultCount'], self.nodes_init, "Count mismatch dumping results from system:prepareds: " % prepared_results)

            query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
            self._verify_results(query_results['results'], expected_results['results'])
            self.assertEqual(query_results['metrics']['resultCount'], 5)

            query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
            self._verify_results(query_results2['results'], expected_results['results'])
            self.assertEqual(query_results2['metrics']['resultCount'], 5)

            self.ensure_bucket_does_not_exist("default", using_rest=True)
            self.rest.create_bucket(bucket="default", ramQuotaMB=100)
            self.wait_for_buckets_status({"default": "healthy"}, 5, 120)
            # this sleep is need because index deletion after bucket deletion is async
            self.sleep(60)
            self.wait_for_index_drop("default", "idx", [("join_day", 0)], self.index_type.lower())
            self.run_cbq_query(query="CREATE INDEX idx on default(join_day)")
            self._wait_for_index_online("default", "idx")
            expected_results = self.run_cbq_query(query="select * from default WHERE join_day = 10 limit 5", server=self.servers[0])

            query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
            self._verify_results(query_results['results'], expected_results['results'])
            self.assertEqual(query_results['metrics']['resultCount'], 0)

            query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
            self._verify_results(query_results2['results'], expected_results['results'])
            self.assertEqual(query_results2['metrics']['resultCount'], 0)
        finally:
            self.run_cbq_query(query="DROP INDEX default.idx")

    ''' Test that if a node is in the cluster but not currently taking traffic, it will not receive the auto-prepare'''
    def test_add_node_no_rebalance(self):
        services_in = ["index", "n1ql", "kv"]
        # rebalance in a node
        rest = RestConnection(self.master)
        rest.add_node(self.master.rest_username, self.master.rest_password, self.servers[self.nodes_init].ip,
                      self.servers[self.nodes_init].port, services=services_in)
        self.sleep(30)
        self.run_cbq_query(query="PREPARE p1 from select * from default limit 5", server=self.servers[0])
        self.sleep(5)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)
        try:
            for i in range(self.nodes_init + 1):
                try:
                    self.run_cbq_query(query="execute p1", server=self.servers[i])
                except CBQError as ex:
                    self.assertTrue("No such prepared statement: p1" in str(ex), "There error should be no such prepared "
                                                                                 "statement, it really is %s" % ex)
                    self.log.info(ex)
                    self.log.info("node: %s:%s does not have the statement" % (self.servers[i].ip, self.servers[i].port))
        finally:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], to_remove=[self.servers[self.nodes_init]])
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()

    ''' If you drop a server the prepareds on that node should be removed'''
    def test_server_drop(self):
        self.with_retry(lambda: self.ensure_primary_indexes_exist(), eval=None, delay=3, tries=5)
        # try to move index to self.servers[0]
        try:
            query = """ALTER INDEX `default`.`#primary` WITH {"action":"move","nodes": ["%s:8091"]}""" % str(self.servers[0].ip)
            self.run_cbq_query(query=query, server=self.servers[0])
            self.sleep(30)
        except Exception as ex:
            self.assertTrue("GSI AlterIndex() - cause: No Index Movement Required for Specified Destination List" in str(ex))

        remote = RemoteMachineShellConnection(self.servers[1])
        remote.stop_server()
        self.sleep(30)
        try:
            self.run_cbq_query(query="PREPARE p1 from select * from default limit 5", server=self.servers[0])
            self.sleep(5)
        finally:
            remote.start_server()
            self.sleep(30)

        for i in range(1, self.nodes_init):
            try:
                self.run_cbq_query(query="execute p1", server=self.servers[i])
            except CBQError as ex:
                self.assertTrue("No such prepared statement: p1" in str(ex), "There error should be no such prepared "
                                                                             "statement, it really is %s" % ex)
                self.log.info(ex)
                self.log.info("node: %s:%s does not have the statement" % (self.servers[i].ip, self.servers[i].port))

    ''' Test that you can execute a prepared statement on a node freshly added, meaning it has no prepareds on it'''
    def test_rebalance_in_query_node(self):
        self.with_retry(lambda: self.ensure_primary_indexes_exist(), eval=None, delay=3, tries=5)
        self.run_cbq_query(query="PREPARE p1 from select * from default limit 5", server=self.servers[0])
        self.sleep(5)
        for i in range(self.nodes_init):
            self.run_cbq_query(query="execute p1", server=self.servers[i])
        services_in = ["n1ql", "index", "data"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)
        try:
            for i in range(self.nodes_init + 1):
                self.run_cbq_query(query="execute '[%s:%s]p1'" % (self.servers[0].ip, self.servers[0].port), server=self.servers[i])
        finally:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], to_remove=[self.servers[self.nodes_init]])
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()

    ''' Test that prepared works on swap rebalance, meaning that the node being added in does not have the prepared'''
    def test_query_swap_rebalance(self):
        self.run_cbq_query(query="PREPARE p1 from select * from default limit 5", server=self.servers[0])
        self.sleep(5)
        for i in range(self.nodes_init):
            if not self.servers[i] == self.servers[1]:
                self.run_cbq_query(query="execute p1", server=self.servers[i])
        nodes_out_list = self.servers[1]
        to_add_nodes = [self.servers[self.nodes_init + 1]]
        to_remove_nodes = [nodes_out_list]
        services_in = ["index", "n1ql", "data"]
        self.log.info(self.servers[:self.nodes_init])
        # do a swap rebalance
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], to_remove_nodes)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)
        try:
            for i in range(self.nodes_init):
                if not self.servers[i] == self.servers[1]:
                    self.run_cbq_query(query="execute '[%s:%s]p1'" % (self.servers[2].ip, self.servers[2].port),
                                                   server=self.servers[i])
        finally:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], to_remove=to_add_nodes)
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()

            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_remove_nodes, [], services=services_in)
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
