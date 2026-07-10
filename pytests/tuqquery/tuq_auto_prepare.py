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
        self.query_bucket = self.get_query_buckets(check_all_buckets=True)[0]

    def suite_setUp(self):
        super(QueryAutoPrepareTests, self).suite_setUp()
        self.log.info("==============  QueryAutoPrepareTests suite_setup has started ==============")
        if self.load_collections:
            self.run_cbq_query(query='CREATE INDEX idx on default(name)')
            self.sleep(5)
            self.wait_for_all_indexes_online()
            self.collections_helper.create_scope(bucket_name="default", scope_name="test2")
            self.collections_helper.create_collection(bucket_name="default", scope_name="test2",
                                                      collection_name=self.collections[0])
            self.collections_helper.create_collection(bucket_name="default", scope_name="test2",
                                                      collection_name=self.collections[1])
            self.run_cbq_query(
                query="CREATE INDEX idx1 on default:default.test2.{0}(name)".format(self.collections[0]))
            self.run_cbq_query(
                query="CREATE INDEX idx2 on default:default.test2.{0}(name)".format(self.collections[1]))
            self.sleep(5)
            self.wait_for_all_indexes_online()
            self.run_cbq_query(
                query=('INSERT INTO default:default.test2.{0}'.format(self.collections[
                    1]) + '(KEY, VALUE) VALUES ("key1", { "type" : "hotel", "name" : "old hotel" })'))
            self.run_cbq_query(
                query=('INSERT INTO default:default.test2.{0}'.format(self.collections[1]) + '(KEY, VALUE) VALUES ("key2", { "type" : "hotel", "name" : "new hotel" })'))
            self.run_cbq_query(
                query=('INSERT INTO default:default.test2.{0}'.format(self.collections[1]) + '(KEY, VALUE) VALUES ("key3", { "type" : "hotel", "name" : "new hotel" })'))
            self.sleep(20)
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
            prepared_query = 'PREPARE %s' % query

        self.shell.execute_command("%s -u Administrator:password %s:%s/query/service -d statement='%s'"
                                   % (self.curl_path, self.master.ip, self.n1ql_port, prepared_query))

        # Make sure that the prepared statement got prepared on all active nodes (2)
        self.with_retry(lambda: self.check_prepared_finished(), eval=2, delay=1, tries=20)

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
        query = 'select * from {0} where name=$name and join_day=$join_day'.format(self.query_bucket)
        args = '$name=\"employee-8\"&$join_day=8'
        self.prepared_common(query=query, args=args)

    ''' Test named parameters with a prepared statement explicitly named'''

    def test_named_prepared_named_parameters(self):
        query = 'select * from {0} where name=$name and join_mo=$join_mo'.format(self.query_bucket)
        args = '$name=\"employee-9\"&$join_mo=10'
        self.prepared_common(query=query, named=True, name='named', args=args)

    ''' Test anonmyous prepareds with named parameters '''

    def test_anonymous_prepared_positional_parameters_dollar(self):
        query = 'select * from {0} where name=$1 and join_day=$2'.format(self.query_bucket)
        args = 'args=[\"employee-8\",8]'
        self.prepared_common(query=query, args=args)

        args = '$1=\"employee-8\"&$2=8'
        self.prepared_common(query=query, args=args)

    ''' Test named parameters with a prepared statement explicitly named'''

    def test_named_prepared_positional_parameters_dollar(self):
        query = 'select * from {0} where name=$1 and join_mo=$2'.format(self.query_bucket)
        args = 'args=[\"employee-9\",10]'
        self.prepared_common(query=query, named=True, name='named', args=args)

        args = '$1=\"employee-9\"&$2=10'
        self.prepared_common(query=query, named=True, name='named', args=args)

    ''' Test anonmyous prepareds with named parameters '''

    def test_anonymous_prepared_positional_parameters_question_mark(self):
        query = 'select * from {0} where name=? and join_day=?'.format(self.query_bucket)
        args = 'args=[\"employee-8\",8]'
        self.prepared_common(query=query, args=args)

    ''' Test named parameters with a prepared statement explicitly named'''

    def test_named_prepared_positional_parameters_question_mark(self):
        query = 'select * from {0} where name=? and join_mo=?'.format(self.query_bucket)
        args = 'args=[\"employee-9\",10]'
        self.prepared_common(query=query, named=True, name='named', args=args)

    ''' Test that you can attempt to prepare the same prepared statement twice'''

    def test_duplicate_prepare(self):
        self.run_cbq_query(query="PREPARE P1 FROM select * from {0} limit 5".format(self.query_bucket),
                           server=self.servers[0])
        self.run_cbq_query(query="PREPARE P1 FROM select * from {0} limit 5".format(self.query_bucket),
                           server=self.servers[0])
        self.with_retry(lambda: self.check_prepared_finished(), eval=2, delay=1, tries=20)

    ''' Test if you can force a prepared statement to be reprepared'''

    def test_prepare_force(self):
        self.run_cbq_query(query="PREPARE P1 FROM select * from {0} limit 5".format(self.query_bucket),
                           server=self.servers[0])
        self.run_cbq_query(query="PREPARE FORCE P1 FROM select * from {0} limit 5".format(self.query_bucket),
                           server=self.servers[0])
        self.with_retry(lambda: self.check_prepared_finished(), eval=2, delay=1, tries=20)

    ''' Test that you can prepare two statements with different names but the same text'''

    def test_different_prepared(self):
        self.run_cbq_query(query="PREPARE P1 FROM select * from {0} limit 5".format(self.query_bucket),
                           server=self.servers[0])
        self.run_cbq_query(query="PREPARE P2 FROM select * from {0} limit 5".format(self.query_bucket),
                           server=self.servers[0])

        self.with_retry(lambda: self.check_prepared_finished(), eval=4, delay=1, tries=20)

        prepared_name = self.run_cbq_query('select * from system:prepareds where name = "P1" ')
        self.assertEqual(prepared_name['metrics']['resultCount'], self.nodes_init)

        second_prepared_name = self.run_cbq_query('select * from system:prepareds where name = "P2" ')
        self.assertEqual(second_prepared_name['metrics']['resultCount'], self.nodes_init)

    ''' Try to prepare two separate queries under one name, should error'''

    def test_negative_prepare(self):
        try:
            self.run_cbq_query(query="PREPARE P1 FROM select * from {0} limit 5".format(self.query_bucket),
                               server=self.servers[0])
            self.run_cbq_query(query="PREPARE P1 FROM select * from {0} limit 10".format(self.query_bucket),
                               server=self.servers[0])
        except CBQError as ex:
            self.log.error(ex)
            self.assertTrue(str(ex).find("Unable to add name: duplicate name: P1") != -1,
                            "Error is incorrect.")

    ''' Try to prepare a query with a syntax error in it '''

    def test_prepare_syntax_error(self):
        try:
            self.run_cbq_query(query="PREPARE P1 FROM select * fro {0}".format(self.query_bucket),
                               server=self.servers[0])
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 3000, f"Expected code is 3000 but got {error['code']}")
            self.assertEqual(error['msg'], "syntax error - line 1, column 26, near '...RE P1 FROM select * ', at: fro", f"Got {error['msg']} instead of expected")

    ''' Change query settings so that normal queries are automatically cached'''

    def test_auto_prepare(self):
        # Set the queries run to be automatically prepared
        self.shell.execute_command("%s -u Administrator:password %s:%s/admin/settings -d '{\"auto-prepare\":true}'"
                                   % (self.curl_path, self.master.ip, self.n1ql_port))

        self.run_cbq_query('select * from {0}'.format(self.query_bucket), server=self.master)
        self.run_cbq_query('select * from {0} limit 10'.format(self.query_bucket), server=self.master)

        # Ensure the two above queries were automatically prepared
        query_1 = self.run_cbq_query(
            'select * from system:prepareds where statement = "select * from {0}"'.format(self.query_bucket))
        query_2 = self.run_cbq_query(
            'select * from system:prepareds where statement = "select * from {0} limit 10"'.format(self.query_bucket))

        self.assertEqual(query_1['metrics']['resultCount'], 1,
                         "Count mismatch dumping results from system:prepareds: " % query_1)
        self.assertEqual(query_2['metrics']['resultCount'], 1,
                         "Count mismatch dumping results from system:prepareds: " % query_2)

        self.run_cbq_query('select * from {0}'.format(self.query_bucket), server=self.master)
        self.run_cbq_query('select * from {0} limit 10'.format(self.query_bucket), server=self.master)

        # Make sure the uses goes up since these queries are already prepared
        query_1 = self.run_cbq_query(
            'select * from system:prepareds where statement = "select * from {0}"'.format(self.query_bucket))
        query_2 = self.run_cbq_query(
            'select * from system:prepareds where statement = "select * from {0} limit 10"'.format(self.query_bucket))

        self.assertEqual(query_1['results'][0]['prepareds']['uses'], 2)
        self.assertEqual(query_2['results'][0]['prepareds']['uses'], 2)

    def test_auto_prepare_advise(self):
        # MB-71296: with auto-prepare enabled, ADVISE statements must NOT be auto-prepared.
        # Enable auto-prepare so every ad-hoc query run is automatically prepared.
        self.shell.execute_command("%s -u Administrator:password %s:%s/admin/settings -d '{\"auto-prepare\":true}'"
                                   % (self.curl_path, self.master.ip, self.n1ql_port))

        # Positive control: a plain SELECT must be auto-prepared while auto-prepare is on.
        self.run_cbq_query('select c1, c2 from {0} where c1 > 0'.format(self.query_bucket), server=self.master)
        select_prepared = self.run_cbq_query(
            'select * from system:prepareds where statement = "select c1, c2 from {0} where c1 > 0"'.format(self.query_bucket))
        self.assertEqual(select_prepared['metrics']['resultCount'], 1,
                         "Auto-prepare not working: plain SELECT was not prepared: {0}".format(select_prepared))

        # Run the ADVISE variant of the same query.
        self.run_cbq_query('advise select c1, c2 from {0} where c1 > 0'.format(self.query_bucket), server=self.master)

        # MB-71296: the ADVISE statement must NOT be present in system:prepareds.
        advise_prepared = self.run_cbq_query(
            'select * from system:prepareds where lower(statement) like "advise%"')
        self.assertEqual(advise_prepared['metrics']['resultCount'], 0,
                         "MB-71296 regression: ADVISE statement was auto-prepared: {0}".format(advise_prepared))

    '''Test auto-prepare, prepare on first node, check if it is prepared on both nodes and that it can be executed on
       both nodes'''

    def test_basic_auto_prepare(self):
        self.run_cbq_query(query="PREPARE P1 FROM select * from {0} limit 5".format(self.query_bucket),
                           server=self.servers[0])
        self.sleep(2)
        prepared_results = self.run_cbq_query(query="select * from system:prepareds")
        self.assertEqual(prepared_results['metrics']['resultCount'], 2,
                         "Count mismatch dumping results from system:prepareds: " % prepared_results)
        query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
        self.assertEqual(query_results['metrics']['resultCount'], 5)
        query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
        self.assertEqual(query_results2['metrics']['resultCount'], 5)

    '''Test if you can execute a prepared statement by its name in clustered format [ip:port]<prepared_name> , if a node
       doesn't have the prepared statement it should be able to pull it from a node that does'''

    def test_pull_prepare(self):
        prepared_result = self.run_cbq_query(query="PREPARE P1 FROM select * from {0} limit 5".format(self.query_bucket),
                           server=self.servers[0])
        self.sleep(2)
        self.query = "delete from system:prepareds where node = '%s:%s'" \
                     % (self.servers[1].ip, self.servers[1].port)
        self.run_cbq_query()
        query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
        self.assertEqual(query_results['metrics']['resultCount'], 5)

        query_results2 = self.run_cbq_query(query="execute '[%s:%s]P1'"
                                                  % (self.servers[0].ip, self.servers[0].port), server=self.servers[1])
        self.assertEqual(query_results2['metrics']['resultCount'], 5)

    '''Delete docs to change the index that the index is using, should be able to execute prepareds without
     repreparing'''

    def test_change_index_delete_docs(self):
        try:
            self.run_cbq_query(query="CREATE INDEX idx on {0}(join_day)".format(self.query_bucket))
            self._wait_for_index_online(self.default_bucket_name, "idx")
            self.run_cbq_query(
                query="PREPARE P1 FROM select * from {0} WHERE join_day = 10 limit 5".format(self.query_bucket),
                server=self.servers[0])
            self.sleep(2)
            prepared_results = self.run_cbq_query(query="select * from system:prepareds where name = 'P1'")
            self.assertEqual(prepared_results['metrics']['resultCount'], 2,
                             "Count mismatch dumping results from system:prepareds: " % prepared_results)
            query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
            self.assertEqual(query_results['metrics']['resultCount'], 5)
            query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
            self.assertEqual(query_results2['metrics']['resultCount'], 5)

            self.run_cbq_query(query="DELETE FROM {0} LIMIT 10".format(self.query_bucket))

            self.assertEqual(prepared_results['metrics']['resultCount'], 2,
                             "Count mismatch dumping results from system:prepareds: " % prepared_results)
            query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
            self.assertEqual(query_results['metrics']['resultCount'], 5)
            query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
            self.assertEqual(query_results2['metrics']['resultCount'], 5)
        finally:
            self.run_cbq_query(query="DROP INDEX idx IF EXISTS ON {0}".format(self.query_bucket))

    '''Drop an index and create a new index on the same field, this is a new index that the prepared needs to use,
       this should trigger a re-prepare'''

    def test_recreate_index(self):
        try:
            self.run_cbq_query(query="CREATE INDEX idx on {0}(join_day)".format(self.query_bucket))
            self._wait_for_index_online(self.default_bucket_name, "idx")
            self.run_cbq_query(
                query="PREPARE P1 FROM select * from {0} WHERE join_day = 10 limit 5".format(self.query_bucket),
                server=self.servers[0])
            self.sleep(2)
            prepared_results = self.run_cbq_query(query="select * from system:prepareds where name = 'P1'")
            self.assertEqual(prepared_results['metrics']['resultCount'], 2,
                             "Count mismatch dumping results from system:prepareds: " % prepared_results)
            query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
            self.assertEqual(query_results['metrics']['resultCount'], 5)
            query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
            self.assertEqual(query_results2['metrics']['resultCount'], 5)
        finally:
            self.run_cbq_query(query="DROP INDEX idx IF EXISTS ON {0}".format(self.query_bucket))
            self.wait_for_index_drop(self.default_bucket_name, "idx", [("join_day", 0)], self.index_type.lower())

        try:
            self.run_cbq_query(query="CREATE INDEX idx2 on {0}(join_day)".format(self.query_bucket))
            self._wait_for_index_online(self.default_bucket_name, "idx2")
            query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
            self.assertEqual(query_results['metrics']['resultCount'], 5)
            query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
            self.assertEqual(query_results2['metrics']['resultCount'], 5)
        finally:
            self.run_cbq_query(query="DROP INDEX idx2 IF EXISTS ON {0}".format(self.query_bucket))
            self.wait_for_index_drop(self.default_bucket_name, "idx2", [("join_day", 0)], self.index_type.lower())

    '''Run a prepared statement using primary index, then drop primary index and create a new index that the query will
       use instead'''

    def test_new_index(self):
        try:
            self.run_cbq_query(
                query="PREPARE P1 FROM select * from {0} WHERE join_day = 10 limit 5".format(self.query_bucket),
                server=self.servers[0])
            self.sleep(2)
            prepared_results = self.run_cbq_query(query="select * from system:prepareds where name = 'P1'")
            self.assertEqual(prepared_results['metrics']['resultCount'], 2,
                             "Count mismatch dumping results from system:prepareds: " % prepared_results)
            query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
            self.assertEqual(query_results['metrics']['resultCount'], 5)
            query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
            self.assertEqual(query_results2['metrics']['resultCount'], 5)
        finally:
            self.run_cbq_query(query="DROP PRIMARY INDEX on {0}".format(self.query_bucket))
            self.sleep(5)

        try:
            self.run_cbq_query(query="CREATE INDEX idx on {0}(join_day)".format(self.query_bucket))
            self._wait_for_index_online(self.default_bucket_name, "idx")
            query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
            self.assertEqual(query_results['metrics']['resultCount'], 5)
            query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
            self.assertEqual(query_results2['metrics']['resultCount'], 5)
        finally:
            self.run_cbq_query(query="DROP INDEX idx IF EXISTS ON {0}".format(self.query_bucket))
            self.run_cbq_query(query="CREATE PRIMARY INDEX ON {0}".format(self.query_bucket))

    '''Alter the node the index is present on to trigger a re-prepare'''

    def test_alter_index(self):
        try:
            self.run_cbq_query(query="CREATE INDEX idx on %s(join_day) WITH {'nodes':['%s:%s']}" % (self.query_bucket,
                                                                                                    self.servers[0].ip,
                                                                                                    self.servers[
                                                                                                        0].port))
            self._wait_for_index_online(self.default_bucket_name, "idx")
            self.run_cbq_query(
                query="PREPARE P1 FROM select * from {0} WHERE join_day = 10 limit 5".format(self.query_bucket),
                server=self.servers[0])
            self.sleep(2)
            prepared_results = self.run_cbq_query(query="select * from system:prepareds where name = 'P1'")
            self.assertEqual(prepared_results['metrics']['resultCount'], 2,
                             "Count mismatch dumping results from system:prepareds: " % prepared_results)
            query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
            self.assertEqual(query_results['metrics']['resultCount'], 5)
            query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
            self.assertEqual(query_results2['metrics']['resultCount'], 5)

            self.run_cbq_query(query="ALTER INDEX idx ON %s WITH {'action':'move', "
                                     "'nodes':['%s:%s']}" % (self.query_bucket, self.servers[1].ip,
                                                             self.servers[1].port))
            self.sleep(5)

            query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
            self.assertEqual(query_results['metrics']['resultCount'], 5)
            query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
            self.assertEqual(query_results2['metrics']['resultCount'], 5)
        finally:
            self.sleep(5)
            self.run_cbq_query(query="DROP INDEX idx IF EXISTS ON {0}".format(self.query_bucket))

    def test_delete_recreate_bucket(self):
        try:
            self.run_cbq_query(query="CREATE INDEX idx on {0}(join_day)".format(self.query_bucket))
            self._wait_for_index_online(self.default_bucket_name, "idx")
            expected_results = self.run_cbq_query(
                query="select * from {0} WHERE join_day = 10 limit 5".format(self.query_bucket), server=self.servers[0])

            self.run_cbq_query(
                query="PREPARE P1 FROM select * from {0} WHERE join_day = 10 limit 5".format(self.query_bucket),
                server=self.servers[0])
            self.sleep(30)
            prepared_results = self.run_cbq_query(query="select * from system:prepareds")
            self.assertEqual(prepared_results['metrics']['resultCount'], self.nodes_init,
                             "Count mismatch dumping results from system:prepareds: " % prepared_results)

            query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
            self._verify_results(query_results['results'], expected_results['results'])
            self.assertEqual(query_results['metrics']['resultCount'], 5)

            query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
            self._verify_results(query_results2['results'], expected_results['results'])
            self.assertEqual(query_results2['metrics']['resultCount'], 5)

            self.ensure_bucket_does_not_exist(self.default_bucket_name, using_rest=True)
            self.rest.create_bucket(bucket=self.default_bucket_name, ramQuotaMB=self.bucket_size)
            self.wait_for_buckets_status({self.default_bucket_name: "healthy"}, 5, 120)
            # this sleep is need because index deletion after bucket deletion is async
            self.sleep(60)
            self.wait_for_index_drop(self.default_bucket_name, "idx", [("join_day", 0)], self.index_type.lower())
            self.run_cbq_query(query="CREATE INDEX idx on {0}(join_day)".format(self.query_bucket))
            self._wait_for_index_online(self.default_bucket_name, "idx")
            expected_results = self.run_cbq_query(
                query="select * from {0} WHERE join_day = 10 limit 5".format(self.query_bucket), server=self.servers[0])

            query_results = self.run_cbq_query(query="execute P1", server=self.servers[0])
            self._verify_results(query_results['results'], expected_results['results'])
            self.assertEqual(query_results['metrics']['resultCount'], 0)

            query_results2 = self.run_cbq_query(query="execute P1", server=self.servers[1])
            self._verify_results(query_results2['results'], expected_results['results'])
            self.assertEqual(query_results2['metrics']['resultCount'], 0)
        finally:
            self.run_cbq_query(query="DROP INDEX idx IF EXISTS ON {0}".format(self.query_bucket))

    ''' Test that if a node is in the cluster but not currently taking traffic, it will not receive the auto-prepare'''
    def test_add_node_no_rebalance(self):
        services_in = ["index", "n1ql", "kv"]
        # rebalance in a node
        rest = RestConnection(self.master)
        rest.add_node(self.master.rest_username, self.master.rest_password, self.servers[self.nodes_init].ip,
                      self.servers[self.nodes_init].port, services=services_in)
        self.sleep(30)
        self.run_cbq_query(query="PREPARE p1 from select * from {0} limit 5".format(self.query_bucket),
                           server=self.servers[0])
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
                    self.assertTrue("No such prepared statement: p1" in str(ex),
                                    "There error should be no such prepared "
                                    "statement, it really is %s" % ex)
                    self.log.info(ex)
                    self.log.info(
                        "node: %s:%s does not have the statement" % (self.servers[i].ip, self.servers[i].port))
        finally:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [],
                                                     to_remove=[self.servers[self.nodes_init]])
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()

    ''' If you drop a server the prepareds on that node should be removed'''

    def test_server_drop(self):
        self.with_retry(lambda: self.ensure_primary_indexes_exist(), eval=None, delay=3, tries=20)
        # try to move index to self.servers[0]
        try:
            query = """ALTER INDEX `#primary` ON %s WITH {"action":"move", 
            "nodes": ["%s:8091"]}""" % (self.query_bucket, str(self.servers[0].ip))
            self.run_cbq_query(query=query, server=self.servers[0])
            self.sleep(30)
        except Exception as ex:
            self.assertTrue(
                "GSI AlterIndex() - cause: No Index Movement Required for Specified Destination List" in str(ex))

        remote = RemoteMachineShellConnection(self.servers[1])
        remote.stop_server()
        self.sleep(30)
        try:
            self.run_cbq_query(query="PREPARE p1 from select * from {0} limit 5".format(self.query_bucket),
                               server=self.servers[0])
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
        self.with_retry(lambda: self.ensure_primary_indexes_exist(), eval=None, delay=3, tries=20)
        self.run_cbq_query(query="PREPARE p1 from select * from {0} limit 5".format(self.query_bucket),
                           server=self.servers[0])
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
                self.run_cbq_query(query="execute '[%s:%s]p1'" % (self.servers[0].ip, self.servers[0].port),
                                   server=self.servers[i])
        finally:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [],
                                                     to_remove=[self.servers[self.nodes_init]])
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

            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_remove_nodes, [],
                                                     services=services_in)
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()

    def test_prepared_collection_query_context(self):
        try:
            self.run_cbq_query(query="PREPARE p1 AS SELECT * FROM test1 b WHERE b.name = 'old hotel'", query_context='default:default.test')
            results = self.run_cbq_query(query="EXECUTE p1")
            self.assertEqual(results['results'][0]['b'], {'name': 'old hotel', 'type': 'hotel'})
        except Exception as e:
            self.log.info("Prepared statement failed {0}".format(str(e)))
        try:
            results = self.run_cbq_query(query="EXECUTE p1", query_context='default.test')
            self.fail()
        except Exception as e:
            self.assertTrue( "{'code': 4040, 'msg': 'No such prepared statement: p1, context: default.test'}" in str(e))
        try:
            results = self.run_cbq_query(query="EXECUTE p1", query_context='default.test2')
            self.fail()
        except Exception as e:
            self.assertTrue("{'code': 4040, 'msg': 'No such prepared statement: p1, context: default.test2'}" in str(e))

    def test_prepared_collection_query_context_switch(self):
        try:
            self.run_cbq_query(query="PREPARE p1 AS SELECT * FROM test1 b WHERE b.name = 'old hotel'", query_context='default:default.test')
            results = self.run_cbq_query(query="EXECUTE p1", query_context='default:default.test')
            self.assertEqual(results['results'][0]['b'], {'name': 'old hotel', 'type': 'hotel'})
            self.run_cbq_query(query="PREPARE p2 AS SELECT * FROM test1 b WHERE b.name = 'old hotel'", query_context='default:default.test2')
            results = self.run_cbq_query(query="EXECUTE p2", query_context='default:default.test2')
            self.assertEqual(results['results'], [])
        except Exception as e:
            self.log.info("Prepared statement failed {0}".format(str(e)))
            self.fail()
        try:
            results = self.run_cbq_query(query="EXECUTE p1", query_context='default.test')
            self.fail()
        except Exception as e:
            self.assertTrue("{'code': 4040, 'msg': 'No such prepared statement: p1, context: default.test'}" in str(e))
        try:
            results = self.run_cbq_query(query="EXECUTE p1", query_context='default.test2')
            self.fail()
        except Exception as e:
            self.assertTrue("{'code': 4040, 'msg': 'No such prepared statement: p1, context: default.test2'}" in str(e))
        try:
            results = self.run_cbq_query(query="EXECUTE p2", query_context='default.test')
            self.fail()
        except Exception as e:
            self.assertTrue("{'code': 4040, 'msg': 'No such prepared statement: p2, context: default.test'}" in str(e))
        try:
            results = self.run_cbq_query(query="EXECUTE p2", query_context='default.test2')
            self.fail()
        except Exception as e:
            self.assertTrue("{'code': 4040, 'msg': 'No such prepared statement: p2, context: default.test2'}" in str(e))

    def test_prepared_context_join(self):
        results = self.run_cbq_query(query='PREPARE p1 as select * from default:default.test.test1 t1 INNER JOIN test2 t2 ON t1.name = t2.name where t1.name = "new hotel"', query_context='default:default.test2')
        results = self.run_cbq_query(query="EXECUTE p1", query_context='default:default.test2')
        self.assertEqual(results['results'][0], {'t1': {'name': 'new hotel', 'type': 'hotel'}, 't2': {'name': 'new hotel', 'type': 'hotel'}}, {'t1': {'name': 'new hotel', 'type': 'hotel'}, 't2': {'name': 'new hotel', 'type': 'hotel'}})

        results2 = self.run_cbq_query(query='PREPARE p2 as select * from default:default.test.test1 t1 INNER JOIN test2 t2 ON t1.name = t2.name where t1.name = "new hotel"', query_context='default:default.test')
        results2 = self.run_cbq_query(query="EXECUTE p2", query_context='default:default.test')
        self.assertEqual(results2['results'][0], {'t1': {'name': 'new hotel', 'type': 'hotel'}, 't2': {'name': 'new hotel', 'type': 'hotel'}})

        try:
            results = self.run_cbq_query(query="EXECUTE p1", query_context='default.test')
            self.fail()
        except Exception as e:
            self.assertTrue("{'code': 4040, 'msg': 'No such prepared statement: p1, context: default.test'}" in str(e))
        try:
            results = self.run_cbq_query(query="EXECUTE p1", query_context='default.test2')
            self.fail()
        except Exception as e:
            self.assertTrue("{'code': 4040, 'msg': 'No such prepared statement: p1, context: default.test2'}" in str(e))
        try:
            results = self.run_cbq_query(query="EXECUTE p2", query_context='default.test')
            self.fail()
        except Exception as e:
            self.assertTrue("{'code': 4040, 'msg': 'No such prepared statement: p2, context: default.test'}" in str(e))
        try:
            results = self.run_cbq_query(query="EXECUTE p2", query_context='default.test2')
            self.fail()
        except Exception as e:
            self.assertTrue("{'code': 4040, 'msg': 'No such prepared statement: p2, context: default.test2'}" in str(e))

    def test_prepared_join_full_path(self):
        results = self.run_cbq_query(
            query='PREPARE p1 as select * from default:default.test.test1 t1 INNER JOIN default:default.test2.test2 t2 ON t1.name = t2.name where t1.name = "new hotel"')
        results = self.run_cbq_query(query="EXECUTE p1")
        self.assertEqual(results['results'][0], {'t1': {'name': 'new hotel', 'type': 'hotel'}, 't2': {'name': 'new hotel', 'type': 'hotel'}}, {'t1': {'name': 'new hotel', 'type': 'hotel'}, 't2': {'name': 'new hotel', 'type': 'hotel'}})
        try:
            results = self.run_cbq_query(query="EXECUTE p1", query_context='default.test')
            self.fail()
        except Exception as e:
            self.assertTrue("{'code': 4040, 'msg': 'No such prepared statement: p1, context: default.test'}" in str(e))

    def test_prepared_context_bucket_scope(self):
        results = self.run_cbq_query(query='PREPARE p1 as select * from test1 where name = "new hotel"', query_context='default.test')
        results = self.run_cbq_query(query="EXECUTE p1", query_context='default.test')
        self.assertEqual(results['results'][0], {'test1': {'name': 'new hotel', 'type': 'hotel'}})
        try:
            results = self.run_cbq_query(query="EXECUTE p1", query_context='default:default.test')
            self.fail()
        except Exception as e:
            self.assertTrue("{'code': 4040, 'msg': 'No such prepared statement: p1, context: default:default.test'}" in str(e))

    def test_prepared_context_name_bucket_scope(self):
        results = self.run_cbq_query(query='PREPARE p1 as select * from test1 where name = "new hotel"', query_context='default:default.test')
        results = self.run_cbq_query(query="EXECUTE p1", query_context='default:default.test')
        self.assertEqual(results['results'][0],{'test1': {'name': 'new hotel', 'type': 'hotel'}})
        try:
            results = self.run_cbq_query(query="EXECUTE p1", query_context='default.test')
            self.fail()
        except Exception as e:
            self.assertTrue("{'code': 4040, 'msg': 'No such prepared statement: p1, context: default.test'}" in str(e))

    def test_prepared_context_namespace(self):
        results = self.run_cbq_query(query='PREPARE p1 as select * from default.test.test1 where name = "new hotel"', query_context='default:')
        results = self.run_cbq_query(query="EXECUTE p1", query_context='default:')
        self.assertEqual(results['results'][0], {'test1': {'name': 'new hotel', 'type': 'hotel'}})
        try:
            results = self.run_cbq_query(query="EXECUTE p1", query_context='default.test')
            self.fail()
        except Exception as e:
            self.assertTrue("{'code': 4040, 'msg': 'No such prepared statement: p1, context: default.test'}" in str(e))

    def test_prepared_context_semicolon_bucket_scope(self):
        results = self.run_cbq_query(query='PREPARE p1 as select * from test1 where name = "new hotel"', query_context=':default.test')
        results = self.run_cbq_query(query="EXECUTE p1", query_context=':default.test')
        self.assertEqual(results['results'][0], {'test1': {'name': 'new hotel', 'type': 'hotel'}})
        try:
            results = self.run_cbq_query(query="EXECUTE p1", query_context='default.test')
            self.fail()
        except Exception as e:
            self.assertTrue("{'code': 4040, 'msg': 'No such prepared statement: p1, context: default.test'}" in str(e))

    def test_prepared_default(self):
        results = self.run_cbq_query(query='PREPARE p1 as select * from default:default where name = "employee-9"')
        results = self.run_cbq_query(query="EXECUTE p1")
        self.assertEqual(results['metrics']['resultCount'], 72)
        try:
            results = self.run_cbq_query(query="EXECUTE p1")
            self.assertEqual(results['metrics']['resultCount'], 72)
        except Exception as e:
            self.log.error(str(e))
            self.fail()

    def test_prepared_default_full_path(self):
        results = self.run_cbq_query(query='PREPARE p1 as select * from default:default.test.test1 where name = "new hotel"')
        results = self.run_cbq_query(query="EXECUTE p1")
        self.assertEqual(results['results'][0], {'test1': {'name': 'new hotel', 'type': 'hotel'}})
        try:
            results = self.run_cbq_query(query="EXECUTE p1", query_context='default.test')
            self.fail()
        except Exception as e:
            self.log.error(str(e))
            self.assertTrue("{'code': 4040, 'msg': 'No such prepared statement: p1, context: default.test'}" in str(e))

    def test_prepared_collection_query_context_rebalance(self):
        try:
            self.run_cbq_query(query="PREPARE p1 AS SELECT * FROM test1 b WHERE b.name = 'old hotel'", query_context='default:default.test')
            results = self.run_cbq_query(query="EXECUTE p1")
            self.assertEqual(results['results'][0]['b'], {'name': 'old hotel', 'type': 'hotel'})
        except Exception as e:
            self.log.info("Prepared statement failed {0}".format(str(e)))

        # Rebalance in an index node
        rebalance = self.cluster.async_rebalance(self.servers, [self.servers[2]], [], services=["n1ql"])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

        # THe prepared statement should be auto propogated
        results = self.run_cbq_query("SELECT * from system:prepareds")
        self.assertEqual(results['metrics']['resultCount'], 3)

    def test_system_prepareds(self):
        validated = False
        try:
            self.run_cbq_query(query="PREPARE p3 AS SELECT * FROM default where name = 'employee-9'")
        except Exception as e:
            self.log.error("Prepared statement failed {0}".format(str(e)))
            self.fail()
        try:
            self.run_cbq_query(
                query='PREPARE p2 as select * from default:default.test.test1 t1 INNER JOIN default:default.test2.test2 t2 ON t1.name = t2.name where t1.name = "new hotel"')
            self.run_cbq_query(query="PREPARE p1 AS SELECT * FROM test1 b WHERE b.name = 'old hotel'", query_context='default:default.test')
        except Exception as e:
            self.log.error("Prepared statement failed {0}".format(str(e)))
            self.fail()
        attempts = 0
        tries = 20
        while attempts < tries:
            attempts = attempts + 1
            try:
                results = self.run_cbq_query("SELECT * from system:prepareds")
                self.log.info(str(results))
                if results['metrics']['resultCount'] == 6:
                    validated = True
                    break
                else:
                    self.log.info(str(results))
                    self.sleep(1)
            except Exception as ex:
                self.log.error(str(ex))
                self.sleep(1)
        if not validated:
            self.fail("System:prepareds was not properly updated, please check logs above.")

    def test_prepared_name_consistency_with_query_context(self):
        """
        MB-68868: Verify prepared statement name is consistent across all query nodes when query_context is set.
        
        Bug: When a prepared statement is created with query_context, the name was different on different nodes.
        One node would show 'prepared_name(default:`bucket`.`scope`)' while others showed just 'prepared_name'.
        Fix ensures all nodes have the same prepared statement name.
        """
        prepared_name = "fl_airline"
        query_context = "default:default.test"
        
        try:
            self.run_cbq_query(
                query=f"PREPARE {prepared_name} AS SELECT * FROM test1 WHERE name = 'new hotel'",
                query_context=query_context
            )
            self.sleep(5)
            
            prepared_results = self.run_cbq_query("SELECT name, node FROM system:prepareds")
            
            for result in prepared_results['results']:
                self.assertEqual(
                    result['name'], prepared_name,
                    f"MB-68868: Expected prepared name '{prepared_name}' but got '{result['name']}' on node {result['node']}"
                )
                
        except Exception as e:
            self.log.error(f"Test failed with exception: {str(e)}")
            self.fail(f"MB-68868 test failed: {str(e)}")

    def test_set_plan_stability_mode(self):
        """
        set the plan stability mode to different values and verify that the value is set correctly and that the QUERY_METADATA keyspace is created as expected.
        """
        mode_variants = [("prepared_only", "prepared_only"),
            ("PREPARED-ONLY", "prepared_only"),
            ("Prepared Only", "prepared_only"),
            ("ad_hoc", "ad_hoc"),
            ("AD-HOC", "ad_hoc"),
            ("Ad Hoc", "ad_hoc"),
            ("off", "off"),
            ("OFF", "off"),
            ("ad_hoc_read_only", "ad_hoc_read_only"),
            ("AD-HOC-READ-ONLY", "ad_hoc_read_only"),
            ("Ad Hoc Read Only", "ad_hoc_read_only")]
        try:
            self._cleanup_plan_stability_state()
            pre_update_metadata_result = self.run_cbq_query(query='SELECT COUNT(*) AS keyspace_count FROM system:keyspaces WHERE name = "QUERY_METADATA"')
            self.assertEqual(pre_update_metadata_result['results'][0]['keyspace_count'], 0,
                             "QUERY_METADATA keyspace exists before plan_stability.mode update")

            for mode_value, expected_mode in mode_variants:
                self.log.info(f"Setting plan_stability.mode using value: {mode_value}")
                self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "{0}"'.format(mode_value))

                settings_result = self.run_cbq_query(query='SELECT * FROM system:settings')
                actual_mode = settings_result['results'][0]['settings']['plan_stability']['mode']
                self.assertEqual(actual_mode, expected_mode,
                                 "Expected mode {0} but got {1} for input value {2}".format(expected_mode, actual_mode, mode_value))

                metadata_result = self.run_cbq_query(query='SELECT COUNT(*) AS keyspace_count FROM system:keyspaces WHERE name = "QUERY_METADATA"')
                self.assertTrue(metadata_result['results'][0]['keyspace_count'] > 0,
                                "QUERY_METADATA keyspace is missing after setting plan_stability.mode to {0}".format(mode_value))
        finally:
            self._cleanup_plan_stability_state()

    def test_set_plan_stability_negative(self):
        """
        verify invalid values (including null) are rejected and the existing mode remains unchanged.
        also verify QUERY_METADATA is not created by rejected updates.
        """
        invalid_values = [("invalid string", '"invalid_mode"'),
            ("wrong separator", '"prepared.only"'),
            ("nonsense value", '"totally_not_supported"'),
            ("blank string", '"   "'),
            ("null value", "null")]
        try:
            self._cleanup_plan_stability_state()

            for value_label, value_expression in invalid_values:
                try:
                    self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = {0}'.format(value_expression))
                except CBQError as ex:
                    self.log.info("Received expected error for invalid mode value {0} ({1}): {2}".format(value_expression, value_label, str(ex)))
                else:
                    self.fail("Expected invalid mode update to fail for {0}: {1}".format(value_label, value_expression))

                settings_result = self.run_cbq_query(query='SELECT * FROM system:settings')
                actual_mode = settings_result['results'][0]['settings']['plan_stability']['mode']
                print("Actual mode after attempting invalid update {0}: {1}".format(value_expression, actual_mode))
                self.assertEqual(actual_mode, "off",
                                 "plan_stability.mode changed unexpectedly after invalid update {0}. Actual mode: {1}".format(value_expression, actual_mode))

                metadata_result = self.run_cbq_query(query='SELECT COUNT(*) AS keyspace_count FROM system:keyspaces WHERE name = "QUERY_METADATA"')
                self.assertEqual(metadata_result['results'][0]['keyspace_count'], 0,
                                 "QUERY_METADATA should not exist after invalid update {0}".format(value_expression))
        finally:
            self._cleanup_plan_stability_state()

    def test_set_plan_stability_mode_insufficient_ram(self):
        """
        fill KV RAM quota with a regular bucket and verify enabling plan stability fails
        when QUERY_METADATA cannot be created due to insufficient RAM.
        """
        filler_bucket = "kv_quota_exhaust_bucket"
        filler_bucket_created = False
        try:
            # Start from off mode so the next update requires QUERY_METADATA creation.
            self._cleanup_plan_stability_state()
            self.ensure_bucket_does_not_exist(filler_bucket, using_rest=True)

            system_stats = self.rest.fetch_system_stats()
            num_nodes = len(system_stats['nodes'])
            quota_total = system_stats['storageTotals']['ram']['quotaTotal']
            quota_used = system_stats['storageTotals']['ram']['quotaUsed']
            quota_remaining_mb = (quota_total - quota_used) // 1024 // 1024
            per_node_remaining_mb = int(quota_remaining_mb // num_nodes)

            self.assertTrue(per_node_remaining_mb >= 100,
                            "Not enough remaining RAM quota to create a filler bucket. Remaining per-node quota: {0} MB".format(per_node_remaining_mb))

            try:
                self.rest.create_bucket(bucket=filler_bucket, ramQuotaMB=per_node_remaining_mb, replicaNumber=0)
                filler_bucket_created = True
                self.log.info("Created filler bucket {0} with per-node RAM quota {1} MB".format(filler_bucket, per_node_remaining_mb))
            except Exception as create_ex:
                self.fail("Failed to create filler bucket to exhaust RAM quota. Error: {0}".format(str(create_ex)))

            self.wait_for_buckets_status({filler_bucket: "healthy"}, 5, 120)

            expected_error_substring = ("Error occured while creating the 'QUERY_METADATA' bucket - cause: "
                "Error while creating system bucket QUERY_METADATA - cause: (ramQuota) RAM quota specified is too large to be provisioned into this cluster")
            self.sleep(60)

            try:
                self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "prepared_only"')
                self.fail("Expected plan_stability.mode update to fail when KV RAM quota is exhausted")
            except CBQError as ex:
                error_message = str(ex)
                self.log.info("Received expected update failure under quota exhaustion: {0}".format(error_message))
                self.assertTrue(expected_error_substring in error_message,
                    "Unexpected error for insufficient RAM while creating QUERY_METADATA. Expected substring: {0}. Actual error: {1}".format(expected_error_substring, error_message))

            metadata_result = self.run_cbq_query(query='SELECT COUNT(*) AS keyspace_count FROM system:keyspaces WHERE name = "QUERY_METADATA"')
            self.assertEqual(metadata_result['results'][0]['keyspace_count'], 0,
                             "QUERY_METADATA should not exist after failed mode update under RAM exhaustion")
        finally:
            self._cleanup_plan_stability_state()
            if filler_bucket_created:
                self.run_cbq_query(query=f'DROP BUCKET IF EXISTS {filler_bucket}')
                self.with_retry(lambda: self.run_cbq_query(
                    query=f'SELECT COUNT(*) AS keyspace_count FROM system:keyspaces WHERE name = {filler_bucket}')[
                    'results'][0]['keyspace_count'], eval=0, delay=1, tries=20)
                

    def test_plan_stability_prepared_only(self):
        initial_prepare_count = 4
        additional_prepare_count = 3
        try:
            self._cleanup_plan_stability_state()
            self._prepare_named_statements("po_existing", initial_prepare_count)

            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "prepared_only"')
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = false') == initial_prepare_count,
                            eval=True, delay=1, tries=20)

            existing_prepared_doc_count = self._query_metadata_doc_count(where_clause='ad_hoc = false')
            self._prepare_named_statements("po_new", additional_prepare_count)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = false') == existing_prepared_doc_count + additional_prepare_count,
                            eval=True, delay=1, tries=20)

            prepared_docs_before_ad_hoc_queries = self._query_metadata_doc_count(where_clause='ad_hoc = false')
            self._run_plan_stability_ad_hoc_queries()
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = false') == prepared_docs_before_ad_hoc_queries,
                            eval=True, delay=1, tries=20)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = true'),
                            eval=0, delay=1, tries=20)
        finally:
            self._cleanup_plan_stability_state()

    def test_plan_stability_ad_hoc(self):
        initial_prepare_count = 4
        try:
            self._cleanup_plan_stability_state()
            self._prepare_named_statements("ah_existing", initial_prepare_count)

            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "ad_hoc"')
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = false') == initial_prepare_count,
                            eval=True, delay=1, tries=20)

            prepared_doc_count_before_ad_hoc_queries = self._query_metadata_doc_count(where_clause='ad_hoc = false')
            ad_hoc_doc_count_before_ad_hoc_queries = self._query_metadata_doc_count(where_clause='ad_hoc = true')
            self._run_plan_stability_ad_hoc_queries()

            # TODO: Validate individual QUERY_METADATA._system._query document contents for ad_hoc entries.
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = true') == ad_hoc_doc_count_before_ad_hoc_queries + 3,
                            eval=True, delay=1, tries=20)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = false') == prepared_doc_count_before_ad_hoc_queries,
                            eval=True, delay=1, tries=20)
        finally:
            self._cleanup_plan_stability_state()

    def test_plan_stability_ad_hoc_to_prepared_only(self):
        ad_hoc_filter = "ad_hoc = true"
        ad_hoc_queries = [
            "SELECT * FROM {0} LIMIT 1".format(self.query_bucket),
            "SELECT * FROM {0} WHERE join_day = 10 LIMIT 2".format(self.query_bucket),
            "SELECT META().id FROM {0} WHERE join_mo = 10 LIMIT 3".format(self.query_bucket)]
        try:
            self._cleanup_plan_stability_state()
            self._prepare_named_statements("ah_to_po", 3)

            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "ad_hoc"')
            for query in ad_hoc_queries:
                self.run_cbq_query(query=query)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause=ad_hoc_filter) > 0,
                            eval=True, delay=1, tries=20)

            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "prepared_only"')
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause=ad_hoc_filter),
                            eval=0, delay=1, tries=20)
            self.with_retry(lambda: self._query_metadata_doc_count(),eval=3, delay=1, tries=20)
        finally:
            self._cleanup_plan_stability_state()

    def test_plan_stability_ad_hoc_to_off(self):
        ad_hoc_queries = [
            "SELECT * FROM {0} LIMIT 1".format(self.query_bucket),
            "SELECT * FROM {0} WHERE join_day = 10 LIMIT 2".format(self.query_bucket),
            "SELECT META().id FROM {0} WHERE join_mo = 10 LIMIT 3".format(self.query_bucket)]
        try:
            self._cleanup_plan_stability_state()
            self._prepare_named_statements("ah_to_off", 3)

            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "ad_hoc"')
            for query in ad_hoc_queries:
                self.run_cbq_query(query=query)
            self.with_retry(lambda: self._query_metadata_doc_count() > 0,
                            eval=True, delay=1, tries=20)

            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "off"')
            self.with_retry(lambda: self._query_metadata_doc_count(), eval=0, delay=1, tries=20)
        finally:
            self._cleanup_plan_stability_state()

    def test_plan_stability_storage_ad_hoc_read_only(self):
        expected_prepared_docs = 2
        try:
            self._cleanup_plan_stability_state()
            self._prepare_named_statements("storage_ah_ro", expected_prepared_docs)

            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "ad_hoc_read_only"')
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = false') == expected_prepared_docs,
                            eval=True, delay=1, tries=20)

            before_stats = self._query_metadata_stats()
            self._run_plan_stability_ad_hoc_queries()
            self.with_retry(lambda: self._query_metadata_doc_count(), eval=before_stats['total'], delay=1, tries=20)

            after_stats = self._query_metadata_stats()
            self.assertEqual(after_stats['total'], before_stats['total'])
            self.assertEqual(after_stats['ad_hoc_docs'], before_stats['ad_hoc_docs'])
            self.assertEqual(after_stats['prepared_docs'], before_stats['prepared_docs'])
        finally:
            self._cleanup_plan_stability_state()

    def test_plan_stability_management_off_drops_query_metadata(self):
        try:
            self._cleanup_plan_stability_state()

            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "prepared_only"')
            self.with_retry(lambda: self._query_metadata_keyspace_count() > 0, eval=True, delay=1, tries=20)
            self._prepare_named_statements("mgmt_off", 2)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = false') == 2,
                            eval=True, delay=1, tries=20)
            self.with_retry(lambda: self._query_metadata_doc_count() > 0, eval=True, delay=1, tries=20)

            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "off"')
            self.with_retry(lambda: self._query_metadata_doc_count(), eval=0, delay=1, tries=20)
        finally:
            self._cleanup_plan_stability_state()

    def test_ps_auto_ah_exact_dedup(self):
        statement = "SELECT * FROM {0} WHERE join_day = 10 LIMIT 2".format(self.query_bucket)
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "ad_hoc"')
            self.sleep(1)
            ad_hoc_before = self._query_metadata_doc_count(where_clause='ad_hoc = true')
            for _ in range(3):
                self.run_cbq_query(query=statement)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = true'), eval=ad_hoc_before + 1, delay=1, tries=20)
        finally:
            self._cleanup_plan_stability_state()

    def test_ps_auto_ah_ws_nl_distinct(self):
        statement_one = "SELECT * FROM {0} WHERE join_day = 11 LIMIT 2".format(self.query_bucket)
        statement_two = "SELECT * FROM {0}\nWHERE join_day = 11 LIMIT 2".format(self.query_bucket)
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "ad_hoc"')
            ad_hoc_before = self._query_metadata_doc_count(where_clause='ad_hoc = true')
            self.run_cbq_query(query=statement_one)
            self.run_cbq_query(query=statement_two)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = true'), eval=ad_hoc_before + 2, delay=1, tries=20)
        finally:
            self._cleanup_plan_stability_state()

    def test_ps_auto_ah_skip_dml(self):
        doc_id = "ps_ad_hoc_dml_doc"
        dml_query = 'UPSERT INTO {0} (KEY, VALUE) VALUES ("{1}", {{"name":"ps_dml","join_day":99}})'.format(self.query_bucket, doc_id)
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "ad_hoc"')
            ad_hoc_before = self._query_metadata_doc_count(where_clause='ad_hoc = true')
            self.run_cbq_query(query=dml_query)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = true'), eval=ad_hoc_before, delay=1, tries=20)
        finally:
            try:
                self.run_cbq_query(query='DELETE FROM {0} USE KEYS "{1}"'.format(self.query_bucket, doc_id))
            except CBQError:
                pass
            self._cleanup_plan_stability_state()

    def test_ps_auto_ah_skip_ddl(self):
        index_name = "idx_ps_ad_hoc_skip_ddl"
        create_index_query = "CREATE INDEX {0} ON {1}(join_day)".format(index_name, self.query_bucket)
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "ad_hoc"')
            self.run_cbq_query(query=create_index_query)
            self.with_retry(
                lambda: self.run_cbq_query(
                    query='SELECT COUNT(*) AS create_doc_count FROM `QUERY_METADATA`.`_system`.`_query` '
                          'WHERE UPPER(text) LIKE "%CREATE%"')['results'][0]['create_doc_count'],
                eval=1, delay=1, tries=20)
        finally:
            self.run_cbq_query(query="DROP INDEX {0} IF EXISTS ON {1}".format(index_name, self.query_bucket))
            self._cleanup_plan_stability_state()

    def test_ps_auto_ah_skip_params(self):
        parameterized_query = "SELECT * FROM {0} WHERE join_day = $day LIMIT 2".format(self.query_bucket)
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "ad_hoc"')
            ad_hoc_before = self._query_metadata_doc_count(where_clause='ad_hoc = true')
            self.run_cbq_query(query=parameterized_query, query_params={'$day': 10})
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = true'), eval=ad_hoc_before, delay=1, tries=20)
        finally:
            self._cleanup_plan_stability_state()

    def test_ps_auto_off_no_new_persist(self):
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "ad_hoc"')
            self._run_plan_stability_ad_hoc_queries()
            self.with_retry(lambda: self._query_metadata_doc_count() > 0, eval=True, delay=1, tries=20)
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "off"')
            self.with_retry(lambda: self._query_metadata_doc_count(), eval=0, delay=1, tries=20)
            self._run_plan_stability_ad_hoc_queries()
            self.with_retry(lambda: self._query_metadata_doc_count(), eval=0, delay=1, tries=20)
        finally:
            self._cleanup_plan_stability_state()

    def test_ps_auto_off_to_auto_resume(self):
        seed_name = "off_resume_seed"
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "off"')
            self._prepare_plan_statement(seed_name, self._plan_stability_statement_for_day(12), save=True)
            self.with_retry(lambda: self._query_metadata_keyspace_count() > 0, eval=True, delay=1, tries=20)
            prepared_before = self._query_metadata_doc_count(where_clause='ad_hoc = false')
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "prepared_only"')
            self._prepare_named_statements("off_resume", 2)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = false') == prepared_before + 2, eval=True, delay=1, tries=20)
        finally:
            self._cleanup_plan_stability_state()

    def test_ps_auto_off_to_auto_recreate(self):
        seed_name = "off_recreate_seed"
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "off"')
            self.with_retry(lambda: self._query_metadata_keyspace_count(), eval=0, delay=1, tries=20)
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "prepared_only"')
            self._prepare_named_statements("off_recreate", 2)
            self.with_retry(lambda: self._query_metadata_keyspace_count() > 0, eval=True, delay=1, tries=20)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = false') == 2, eval=True, delay=1, tries=20)
        finally:
            self._cleanup_plan_stability_state()

    def test_ps_off_keeps_save_only(self):
        mode_prepared_count = 2
        saved_statement_count = 2
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "ad_hoc"')

            self._prepare_named_statements("off_keep_save", mode_prepared_count)
            self._run_plan_stability_ad_hoc_queries()
            for i in range(saved_statement_count):
                self._prepare_plan_statement("off_keep_save_manual_{0}".format(i),
                                             self._plan_stability_statement_for_day(30 + i), save=True)

            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = true') == 4,
                            eval=True, delay=1, tries=20)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = false') == mode_prepared_count +saved_statement_count,
                            eval=True, delay=1, tries=20)

            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "off"')

            self.with_retry(lambda: self._query_metadata_keyspace_count() > 0, eval=True, delay=1, tries=20)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = true'),
                            eval=0, delay=1, tries=20)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = false'),
                            eval=saved_statement_count, delay=1, tries=20)
            self.with_retry(lambda: self._query_metadata_doc_count(),
                            eval=saved_statement_count, delay=1, tries=20)
        finally:
            self._cleanup_plan_stability_state()

    def test_ps_manual_save_creates_bucket(self):
        saved_statement_count = 1
        plan_name = "manual_save_create_bucket"
        try:
            self._cleanup_plan_stability_state()
            self._prepare_plan_statement(plan_name, self._plan_stability_statement_for_day(14), save=True)
            self.with_retry(lambda: self._query_metadata_keyspace_count() > 0, eval=True, delay=1, tries=20)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = false') == saved_statement_count,
                            eval=True, delay=1, tries=20)
            self.with_retry(lambda: self._query_metadata_doc_count() == saved_statement_count,
                            eval=True, delay=1, tries=20)
        finally:
            self._cleanup_plan_stability_state()

    def test_ps_manual_save_mode_off(self):
        plan_name = "manual_save_mode_off"
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "off"')
            prepared_before = self._query_metadata_doc_count(where_clause='ad_hoc = false')
            self._prepare_plan_statement(plan_name, self._plan_stability_statement_for_day(15), save=True)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = false') == prepared_before + 1, eval=True, delay=1, tries=20)
        finally:
            self._cleanup_plan_stability_state()

    def test_ps_manual_save_mode_on(self):
        plan_name = "manual_save_mode_on"
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "prepared_only"')
            self._prepare_named_statements("manual_on_seed", 1)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = false') == 1, eval=True, delay=1, tries=20)
            prepared_before = self._query_metadata_doc_count(where_clause='ad_hoc = false')
            self._prepare_plan_statement(plan_name, self._plan_stability_statement_for_day(16), save=True)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = false') == prepared_before + 1, eval=True, delay=1, tries=20)
        finally:
            self._cleanup_plan_stability_state()

    def test_ps_manual_save_dup_name(self):
        plan_name = "manual_save_duplicate"
        try:
            self._cleanup_plan_stability_state()
            self._prepare_plan_statement(plan_name, self._plan_stability_statement_for_day(17), save=True)
            try:
                self._prepare_plan_statement(plan_name, self._plan_stability_statement_for_day(18), save=True)
                self.fail("Expected duplicate PREPARE SAVE name to fail")
            except CBQError as ex:
                error_message = str(ex).lower()
                self.assertTrue("duplicate" in error_message or "already" in error_message or "unable to add name" in error_message,
                                "Unexpected duplicate SAVE error: {0}".format(str(ex)))
        finally:
            self._cleanup_plan_stability_state()

    def test_ps_manual_force_no_save_no_persist(self):
        plan_name = "manual_force_without_save"
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "off"')
            self._prepare_plan_statement(plan_name, self._plan_stability_statement_for_day(19), force=True)
            self.assertEqual(self._query_metadata_keyspace_count(), 0,
                             "PREPARE FORCE without SAVE should not create QUERY_METADATA bucket")
            self.assertTrue(self._prepared_statement_text(plan_name) is not None,
                            "PREPARE FORCE should create/update an in-memory prepared statement")
        finally:
            self._cleanup_plan_stability_state()

    def test_ps_manual_save_force_overwrite(self):
        plan_name = "manual_save_force_overwrite"
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "off"')
            self._prepare_plan_statement(plan_name, self._plan_stability_statement_for_day(1), save=True)
            first_statement = self.run_cbq_query(query='SELECT encoded_plan FROM QUERY_METADATA._system._query WHERE name = "{0}"'.format(plan_name))['results'][0]
            self.assertTrue(first_statement is not None,
                            "Initial SAVE statement text not found in prepared cache")
            self.run_cbq_query(query="Create index idx_join_day on {0}(join_day)".format(self.query_bucket))
            self.wait_for_all_indexes_online()
            self._prepare_plan_statement(plan_name, self._plan_stability_statement_for_day(1), save=True, force=True)
            second_statement = self.run_cbq_query(query='SELECT encoded_plan FROM QUERY_METADATA._system._query WHERE name = "{0}"'.format(plan_name))['results'][0]
            self.assertTrue(second_statement is not None and second_statement != first_statement,
                            f"SAVE FORCE did not update prepared statement text, please look original statement: {first_statement} and  updated_statement: {second_statement} for details")
            execution_result = self.run_cbq_query(query='EXECUTE {0}'.format(plan_name))
            self.assertTrue(execution_result['metrics']['resultCount'] > 0)
            self.assertTrue(all(value == 1 for value in execution_result['results']),
                            "Expected EXECUTE to use overwritten statement for join_day = 2")
        finally:
            self.run_cbq_query(query="DROP INDEX idx_join_day IF EXISTS ON {0}".format(self.query_bucket))
            self._cleanup_plan_stability_state()

    def test_ps_manual_force_on_saved_no_save(self):
        plan_name = "manual_force_on_saved"
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "off"')
            self._prepare_plan_statement(plan_name, self._plan_stability_statement_for_day(1), save=True)
            first_check = self.run_cbq_query(query='SELECT encoded_plan FROM QUERY_METADATA._system._query WHERE name = "{0}"'.format(plan_name))['results'][0]
            self.run_cbq_query(query="Create index idx_join_day on {0}(join_day)".format(self.query_bucket))
            self._prepare_plan_statement(plan_name, self._plan_stability_statement_for_day(1), force=True)
            second_check = self.run_cbq_query(query='SELECT encoded_plan FROM QUERY_METADATA._system._query WHERE name = "{0}"'.format(plan_name))['results'][0]
            self.assertTrue(second_check is not None and second_check == first_check,
                            f"SAVE FORCE should not have updated the encoded_plan, please look original statement: {first_check} and  updated_statement: {second_check} for details")
            execution_result = self.run_cbq_query(query='EXECUTE {0}'.format(plan_name))
            self.assertTrue(execution_result['metrics']['resultCount'] > 0)
            self.assertTrue(all(value == 1 for value in execution_result['results']),
                            "Expected in-memory FORCE statement to be used for EXECUTE")
        finally:
            self.run_cbq_query(query="DROP INDEX idx_join_day IF EXISTS ON {0}".format(self.query_bucket))
            self._cleanup_plan_stability_state()

    def test_ps_rbac_ro_cannot_update(self):
        user_id = "plan_stability_reader"
        user = None
        try:
            self._cleanup_plan_stability_state()
            user = self._create_plan_stability_user(user_id=user_id, roles='query_system_catalog')

            settings_result = self.run_cbq_query(
                query='SELECT plan_stability.mode AS mode FROM system:settings',
                username=user['id'], password=user['password'])
            self.assertTrue(settings_result['metrics']['resultCount'] > 0,
                            "query_system_catalog user should be able to read system:settings")

            try:
                self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "prepared_only"',
                                   username=user['id'], password=user['password'])
                self.fail("Expected non-admin user update on system:settings to fail")
            except CBQError as ex:
                error = self.process_CBQE(ex)
                self.assertTrue(error['code'] in [13014, 12003, 10000],
                                "Unexpected error code for unauthorized plan_stability.mode update: {0}".format(error['code']))
                self.assertTrue(any(token in error['msg'].lower() for token in ["credential", "permission", "authorized"]),
                                "Unexpected error message for unauthorized update: {0}".format(error['msg']))

            actual_mode = self.run_cbq_query(query='SELECT plan_stability.mode AS mode FROM system:settings')['results'][0]['mode']
            self.assertEqual(actual_mode, "off")
        finally:
            if user:
                self._drop_plan_stability_user(user['id'])
            self._cleanup_plan_stability_state()

    def test_ps_rbac_admin_can_update(self):
        user_id = "plan_stability_admin"
        user = None
        try:
            self._cleanup_plan_stability_state()
            user = self._create_plan_stability_user(user_id=user_id, roles='admin')

            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "prepared_only"',
                               username=user['id'], password=user['password'])

            self.with_retry(lambda: self.run_cbq_query(
                query='SELECT plan_stability.mode AS mode FROM system:settings')['results'][0]['mode'] == "prepared_only",
                eval=True, delay=1, tries=20)
            self.with_retry(lambda: self._query_metadata_keyspace_count() > 0, eval=True, delay=1, tries=20)
        finally:
            if user:
                self._drop_plan_stability_user(user['id'])
            self._cleanup_plan_stability_state()

    def test_ps_rbac_ro_cannot_drop_query_metadata_bucket(self):
        user_id = "plan_stability_drop_reader"
        user = None
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "prepared_only"')
            self._prepare_named_statements("rbac_drop_query_metadata", 2)
            self.with_retry(lambda: self._query_metadata_keyspace_count() > 0, eval=True, delay=1, tries=20)

            user = self._create_plan_stability_user(user_id=user_id, roles='query_system_catalog')
            self._assert_non_admin_query_denied(
                user=user,
                query='DROP BUCKET QUERY_METADATA',
                operation='drop QUERY_METADATA bucket')

            self.with_retry(lambda: self._query_metadata_keyspace_count() > 0, eval=True, delay=1, tries=20)
        finally:
            if user:
                self._drop_plan_stability_user(user['id'])
            self._cleanup_plan_stability_state()

    def test_ps_rbac_ro_cannot_delete_query_metadata_docs(self):
        user_id = "plan_stability_delete_reader"
        user = None
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "prepared_only"')
            self._prepare_named_statements("rbac_delete_query_metadata", 2)
            self.with_retry(lambda: self._query_metadata_doc_count() > 0, eval=True, delay=1, tries=20)

            persisted_before = self._query_metadata_doc_count()
            user = self._create_plan_stability_user(user_id=user_id, roles='query_system_catalog')
            self._assert_non_admin_query_denied(
                user=user,
                query='DELETE FROM `QUERY_METADATA`.`_system`.`_query` LIMIT 1',
                operation='delete QUERY_METADATA documents')

            self.with_retry(lambda: self._query_metadata_doc_count(), eval=persisted_before, delay=1, tries=20)
        finally:
            if user:
                self._drop_plan_stability_user(user['id'])
            self._cleanup_plan_stability_state()

    def test_set_error_policy_modes(self):
        """Accept strict/moderate/flexible (and case variants) and store the normalized value."""
        mode_variants = [("strict", "strict"), ("STRICT", "strict"), ("Strict", "strict"),
                         ("moderate", "moderate"), ("MODERATE", "moderate"), ("Moderate", "moderate"),
                         ("flexible", "flexible"), ("FLEXIBLE", "flexible"), ("Flexible", "flexible")]
        try:
            self._cleanup_plan_stability_state()
            for value, expected in mode_variants:
                self.log.info("Setting plan_stability.error_policy = {0}".format(value))
                self._set_error_policy(value)
                self.assertEqual(self._get_error_policy(), expected,
                                 "Expected error_policy {0} but got {1} for input {2}".format(expected, self._get_error_policy(), value))
        finally:
            self._cleanup_plan_stability_state()

    def test_set_error_policy_negative(self):
        """Reject invalid error_policy values; current value remains unchanged."""
        invalid_values = ['"aggressive"', '"strict.mode"', '"   "', '""', "null"]
        try:
            self._cleanup_plan_stability_state()
            baseline = self._get_error_policy()
            for value_expression in invalid_values:
                try:
                    self.run_cbq_query(query='UPDATE system:settings SET plan_stability.error_policy = {0}'.format(value_expression))
                    self.fail("Expected invalid error_policy update to fail for {0}".format(value_expression))
                except CBQError as ex:
                    self.log.info("Received expected error for invalid error_policy {0}: {1}".format(value_expression, ex))
                self.assertEqual(self._get_error_policy(), baseline,
                                 "error_policy changed unexpectedly after invalid update {0}".format(value_expression))
        finally:
            self._cleanup_plan_stability_state()

    def test_error_policy_default_is_moderate(self):
        """Default plan_stability.error_policy is 'moderate' on a clean cluster."""
        try:
            self._cleanup_plan_stability_state()
            self.assertEqual(self._get_error_policy(), "moderate",
                             "Default error_policy should be 'moderate'")
        finally:
            self._cleanup_plan_stability_state()

    def test_error_policy_strict_drop_index_prepared(self):
        """strict + saved prepared: EXECUTE raises after the used index is dropped."""
        plan_name, idx = "ep_strict_drop_idx_prep", "idx_ep_strict_drop_prep"
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "prepared_only"')
            self._set_error_policy("strict")
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            self._prepare_plan_statement(plan_name, self._plan_stability_statement_for_day(10), save=True)
            self.assertIsNotNone(self._prepared_statement_text(plan_name))
            self._drop_index_for_plan(idx, self.query_bucket, "join_day")
            try:
                self.run_cbq_query(query="EXECUTE {0}".format(plan_name))
                self.fail("strict policy should raise on invalid saved prepared plan")
            except CBQError as ex:
                self.assertTrue("Reprepare error - cause: Plan Stability error policy STRICT prevents reprepare" in str(ex),
                                "Unexpected error message for strict policy reprepare failure: {0}".format(str(ex)))
                self.log.info("strict raised as expected: {0}".format(ex))
        finally:
            self._cleanup_plan_stability_state()

    def test_error_policy_strict_drop_recreate_collection_prepared(self):
        """strict + saved prepared: dropping/recreating the referenced collection raises on EXECUTE."""
        plan_name, scope, coll = "ep_strict_drop_coll_prep", "test2", self.collections[1]
        stmt = "SELECT name FROM default:default.{0}.{1} WHERE name = 'new hotel'".format(scope, coll)
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "prepared_only"')
            self._set_error_policy("strict")
            self._prepare_plan_statement(plan_name, stmt, save=True)
            self._drop_recreate_collection(scope, coll)
            try:
                self.run_cbq_query(query="EXECUTE {0}".format(plan_name))
                self.fail("strict policy should raise after collection drop/recreate")
            except CBQError as ex:
                self.assertTrue("Reprepare error - cause: Plan Stability error policy STRICT prevents reprepare" in str(ex),
                                "Unexpected error message for strict policy reprepare failure: {0}".format(str(ex)))
                self.log.info("strict raised as expected: {0}".format(ex))
        finally:
            self._cleanup_plan_stability_state()

    def test_error_policy_strict_drop_recreate_bucket_prepared(self):
        """strict + saved prepared: dropping/recreating the bucket raises on EXECUTE."""
        plan_name, idx = "ep_strict_drop_bucket_prep", "idx_ep_strict_bucket_prep"
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "prepared_only"')
            self._set_error_policy("strict")
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            self._prepare_plan_statement(plan_name, self._plan_stability_statement_for_day(10), save=True)
            self._drop_recreate_bucket(self.default_bucket_name)
            try:
                self.run_cbq_query(query="EXECUTE {0}".format(plan_name))
                self.fail("strict policy should raise after bucket drop/recreate")
            except CBQError as ex:
                self.assertTrue("Reprepare error - cause: Plan Stability error policy STRICT prevents reprepare" in str(ex),
                                "Unexpected error message for strict policy reprepare failure: {0}".format(str(ex)))
                self.log.info("strict raised as expected: {0}".format(ex))
        finally:
            self.run_cbq_query(query="DROP INDEX {0} IF EXISTS ON {1}".format(idx, self.query_bucket))
            self._cleanup_plan_stability_state()

    def test_error_policy_strict_index_unavailable_prepared(self):
        """strict + saved prepared: stopping the indexer that hosts the only index replica raises on EXECUTE."""
        index_nodes = self._get_index_nodes()
        if len(index_nodes) < 2:
            self.skipTest("Index unavailable scenario requires >= 2 index-service nodes")
        plan_name, idx = "ep_strict_idx_unavail_prep", "idx_ep_strict_unavail_prep"
        host = index_nodes[0]
        remote = None
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "prepared_only"')
            self._set_error_policy("strict")
            self._create_index_for_plan(idx, self.query_bucket, "join_day", num_replica=0,
                                        nodes=["{0}:{1}".format(host.ip, host.port)])
            self._prepare_plan_statement(plan_name, self._plan_stability_statement_for_day(10), save=True)
            remote = self._make_index_unavailable_via_node_down(host)
            try:
                self.run_cbq_query(query="EXECUTE {0}".format(plan_name))
                self.fail("strict policy should raise while indexer is down")
            except CBQError as ex:
                self.assertTrue("Reprepare error - cause: Plan Stability error policy STRICT prevents reprepare" in str(ex),
                                "Unexpected error message for strict policy reprepare failure: {0}".format(str(ex)))
                self.log.info("strict raised as expected: {0}".format(ex))
        finally:
            if remote is not None:
                self._restore_index_node(remote)
            self.run_cbq_query(query="DROP INDEX {0} IF EXISTS ON {1}".format(idx, self.query_bucket))
            self._cleanup_plan_stability_state()

    def test_error_policy_strict_drop_index_adhoc(self):
        """strict + saved ad-hoc: re-running the saved ad-hoc query raises after index drop."""
        idx = "idx_ep_strict_drop_adhoc"
        stmt = self._plan_stability_statement_for_day(11)
        try:
            self._cleanup_plan_stability_state()
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "ad_hoc"')
            self.run_cbq_query(query=stmt)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = true') >= 1,
                            eval=True, delay=1, tries=20)
            prepared_name = self._persisted_adhoc_name(stmt)
            self.assertIsNotNone(prepared_name, "Expected saved ad-hoc plan entry in QUERY_METADATA")
            self._set_error_policy("strict")
            self._drop_index_for_plan(idx, self.query_bucket, "join_day")
            try:
                self.run_cbq_query(query="EXECUTE '{0}'".format(prepared_name))
                self.fail("strict policy should raise on invalid saved ad-hoc plan")
            except CBQError as ex:
                self.assertTrue("Reprepare error - cause: Plan Stability error policy STRICT prevents reprepare" in str(ex),
                                "Unexpected error message for strict policy reprepare failure: {0}".format(str(ex)))
                self.log.info("strict raised as expected: {0}".format(ex))
        finally:
            self._cleanup_plan_stability_state()

    def test_error_policy_strict_drop_recreate_collection_adhoc(self):
        """strict + saved ad-hoc: collection drop/recreate raises on rerun."""
        scope, coll = "test2", self.collections[1]
        stmt = "SELECT name FROM default:default.{0}.{1} WHERE name = 'new hotel'".format(scope, coll)
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "ad_hoc"')
            self.run_cbq_query(query=stmt)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = true') >= 1,
                            eval=True, delay=1, tries=20)
            prepared_name = self._persisted_adhoc_name(stmt)
            self.assertIsNotNone(prepared_name, "Expected saved ad-hoc plan entry in QUERY_METADATA")
            self._set_error_policy("strict")
            self._drop_recreate_collection(scope, coll)
            try:
                self.run_cbq_query(query="EXECUTE '{0}'".format(prepared_name))
                self.fail("strict policy should raise after collection drop/recreate")
            except CBQError as ex:
                self.assertTrue("Reprepare error - cause: Plan Stability error policy STRICT prevents reprepare" in str(ex),
                                "Unexpected error message for strict policy reprepare failure: {0}".format(str(ex)))
                self.log.info("strict raised as expected: {0}".format(ex))
        finally:
            self._cleanup_plan_stability_state()

    def test_error_policy_strict_drop_recreate_bucket_adhoc(self):
        """strict + saved ad-hoc: bucket drop/recreate raises on rerun."""
        idx = "idx_ep_strict_bucket_adhoc"
        stmt = self._plan_stability_statement_for_day(13)
        try:
            self._cleanup_plan_stability_state()
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "ad_hoc"')
            self.run_cbq_query(query=stmt)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = true') >= 1,
                            eval=True, delay=1, tries=20)
            prepared_name = self._persisted_adhoc_name(stmt)
            self.assertIsNotNone(prepared_name, "Expected saved ad-hoc plan entry in QUERY_METADATA")
            self._set_error_policy("strict")
            self._drop_recreate_bucket(self.default_bucket_name)
            try:
                self.run_cbq_query(query="EXECUTE '{0}'".format(prepared_name))
                self.fail("strict policy should raise after bucket drop/recreate")
            except CBQError as ex:
                self.assertTrue("Reprepare error - cause: Plan Stability error policy STRICT prevents reprepare" in str(ex),
                                "Unexpected error message for strict policy reprepare failure: {0}".format(str(ex)))
                self.log.info("strict raised as expected: {0}".format(ex))
        finally:
            self.run_cbq_query(query="DROP INDEX {0} IF EXISTS ON {1}".format(idx, self.query_bucket))
            self._cleanup_plan_stability_state()

    def test_error_policy_strict_index_unavailable_adhoc(self):
        """strict + saved ad-hoc: indexer down raises on rerun."""
        index_nodes = self._get_index_nodes()
        if len(index_nodes) < 2:
            self.skipTest("Index unavailable scenario requires >= 2 index-service nodes")
        idx = "idx_ep_strict_unavail_adhoc"
        stmt = self._plan_stability_statement_for_day(14)
        host = index_nodes[0]
        remote = None
        try:
            self._cleanup_plan_stability_state()
            self._create_index_for_plan(idx, self.query_bucket, "join_day", num_replica=0,
                                        nodes=["{0}:{1}".format(host.ip, host.port)])
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "ad_hoc"')
            self.run_cbq_query(query=stmt)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = true') >= 1,
                            eval=True, delay=1, tries=20)
            prepared_name = self._persisted_adhoc_name(stmt)
            self.assertIsNotNone(prepared_name, "Expected saved ad-hoc plan entry in QUERY_METADATA")
            self._set_error_policy("strict")
            remote = self._make_index_unavailable_via_node_down(host)
            try:
                self.run_cbq_query(query="EXECUTE '{0}'".format(prepared_name))
                self.fail("strict policy should raise while indexer is down")
            except CBQError as ex:
                self.assertTrue("Reprepare error - cause: Plan Stability error policy STRICT prevents reprepare" in str(ex),
                                "Unexpected error message for strict policy reprepare failure: {0}".format(str(ex)))
                self.log.info("strict raised as expected: {0}".format(ex))
        finally:
            if remote is not None:
                self._restore_index_node(remote)
            self.run_cbq_query(query="DROP INDEX {0} IF EXISTS ON {1}".format(idx, self.query_bucket))
            self._cleanup_plan_stability_state()

    def test_error_policy_moderate_drop_index_prepared(self):
        """moderate + saved prepared: EXECUTE re-prepares ad-hoc; saved plan unchanged."""
        plan_name, idx = "ep_mod_drop_idx_prep", "idx_ep_mod_drop_prep"
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "prepared_only"')
            self._set_error_policy("moderate")
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            self._prepare_plan_statement(plan_name, self._plan_stability_statement_for_day(12), save=True)
            prior_in_memory = self._prepared_statement_text(plan_name)
            prior_persisted = self._persisted_prepared_doc(plan_name)
            self._drop_index_for_plan(idx, self.query_bucket, "join_day")
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            self.run_cbq_query(query="EXECUTE {0}".format(plan_name))
            self.assertEqual(self._prepared_statement_text(plan_name), prior_in_memory,
                             "moderate must not overwrite saved prepared plan in system:prepareds")
            self.assertEqual(self._persisted_prepared_doc(plan_name), prior_persisted,
                             "moderate must not overwrite persisted prepared plan in QUERY_METADATA")
            # TODO: when "ad-hoc execution window" setting exists, assert plan replacement after threshold.
        finally:
            self._cleanup_plan_stability_state()

    def test_error_policy_moderate_drop_recreate_collection_prepared(self):
        """moderate + saved prepared: collection drop/recreate, EXECUTE succeeds, saved plan unchanged."""
        plan_name, scope, coll = "ep_mod_drop_coll_prep", "test2", self.collections[1]
        stmt = "SELECT name FROM default:default.{0}.{1} WHERE name = 'new hotel'".format(scope, coll)
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "prepared_only"')
            self._set_error_policy("moderate")
            self._prepare_plan_statement(plan_name, stmt, save=True)
            prior_persisted = self._persisted_prepared_doc(plan_name)
            self._drop_recreate_collection(scope, coll)
            self.run_cbq_query(query="EXECUTE {0}".format(plan_name))
            self.assertEqual(self._persisted_prepared_doc(plan_name), prior_persisted,
                             "moderate must not overwrite persisted prepared plan after collection recreate")
        finally:
            self._cleanup_plan_stability_state()

    def test_error_policy_moderate_drop_recreate_bucket_prepared(self):
        """moderate + saved prepared: bucket drop/recreate, EXECUTE succeeds, saved plan unchanged."""
        plan_name, idx = "ep_mod_drop_bucket_prep", "idx_ep_mod_bucket_prep"
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "prepared_only"')
            self._set_error_policy("moderate")
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            self._prepare_plan_statement(plan_name, self._plan_stability_statement_for_day(10), save=True)
            prior_persisted = self._persisted_prepared_doc(plan_name)
            self._drop_recreate_bucket(self.default_bucket_name)
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            self.run_cbq_query(query="EXECUTE {0}".format(plan_name))
            self.assertEqual(self._persisted_prepared_doc(plan_name), prior_persisted,
                             "moderate must not overwrite persisted prepared plan after bucket recreate")
        finally:
            self.run_cbq_query(query="DROP INDEX {0} IF EXISTS ON {1}".format(idx, self.query_bucket))
            self._cleanup_plan_stability_state()

    def test_error_policy_moderate_index_unavailable_prepared(self):
        """moderate + saved prepared: indexer down then up, EXECUTE succeeds, saved plan unchanged."""
        index_nodes = self._get_index_nodes()
        if len(index_nodes) < 2:
            self.skipTest("Index unavailable scenario requires >= 2 index-service nodes")
        plan_name, idx = "ep_mod_idx_unavail_prep", "idx_ep_mod_unavail_prep"
        host = index_nodes[0]
        remote = None
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "prepared_only"')
            self._set_error_policy("moderate")
            self._create_index_for_plan(idx, self.query_bucket, "join_day", num_replica=0,
                                        nodes=["{0}:{1}".format(host.ip, host.port)])
            self._prepare_plan_statement(plan_name, self._plan_stability_statement_for_day(10), save=True)
            prior_persisted = self._persisted_prepared_doc(plan_name)
            remote = self._make_index_unavailable_via_node_down(host)
            self._restore_index_node(remote)
            remote = None
            self.run_cbq_query(query="EXECUTE {0}".format(plan_name))
            self.assertEqual(self._persisted_prepared_doc(plan_name), prior_persisted,
                             "moderate must not overwrite persisted prepared plan after index recovery")
        finally:
            if remote is not None:
                self._restore_index_node(remote)
            self.run_cbq_query(query="DROP INDEX {0} IF EXISTS ON {1}".format(idx, self.query_bucket))
            self._cleanup_plan_stability_state()

    def test_error_policy_moderate_drop_index_adhoc(self):
        """moderate + saved ad-hoc: rerun succeeds; saved ad-hoc plan unchanged."""
        idx = "idx_ep_mod_drop_adhoc"
        stmt = self._plan_stability_statement_for_day(15)
        try:
            self._cleanup_plan_stability_state()
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "ad_hoc"')
            self.run_cbq_query(query=stmt)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = true') >= 1,
                            eval=True, delay=1, tries=20)
            prepared_name = self._persisted_adhoc_name(stmt)
            self.assertIsNotNone(prepared_name, "Expected saved ad-hoc plan entry in QUERY_METADATA")
            prior_persisted = self._persisted_adhoc_doc(prepared_name)
            self._set_error_policy("moderate")
            self._drop_index_for_plan(idx, self.query_bucket, "join_day")
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            self.run_cbq_query(query="EXECUTE '{0}'".format(prepared_name))
            self.assertEqual(self._persisted_adhoc_doc(prepared_name), prior_persisted,
                             "moderate must not overwrite persisted ad-hoc plan")
        finally:
            self._cleanup_plan_stability_state()

    def test_error_policy_moderate_drop_recreate_collection_adhoc(self):
        """moderate + saved ad-hoc: collection drop/recreate, rerun succeeds, saved plan unchanged."""
        scope, coll = "test2", self.collections[1]
        stmt = "SELECT name FROM default:default.{0}.{1} WHERE name = 'new hotel'".format(scope, coll)
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "ad_hoc"')
            self.run_cbq_query(query=stmt)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = true') >= 1,
                            eval=True, delay=1, tries=20)
            prepared_name = self._persisted_adhoc_name(stmt)
            self.assertIsNotNone(prepared_name, "Expected saved ad-hoc plan entry in QUERY_METADATA")
            prior_persisted = self._persisted_adhoc_doc(prepared_name)
            self._set_error_policy("moderate")
            self._drop_recreate_collection(scope, coll)
            self.run_cbq_query(query="EXECUTE '{0}'".format(prepared_name))
            self.assertEqual(self._persisted_adhoc_doc(prepared_name), prior_persisted,
                             "moderate must not overwrite persisted ad-hoc plan after collection recreate")
        finally:
            self._cleanup_plan_stability_state()

    def test_error_policy_moderate_drop_recreate_bucket_adhoc(self):
        """moderate + saved ad-hoc: bucket drop/recreate, rerun succeeds, saved plan unchanged."""
        idx = "idx_ep_mod_bucket_adhoc"
        stmt = self._plan_stability_statement_for_day(16)
        try:
            self._cleanup_plan_stability_state()
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "ad_hoc"')
            self.run_cbq_query(query=stmt)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = true') >= 1,
                            eval=True, delay=1, tries=20)
            prepared_name = self._persisted_adhoc_name(stmt)
            self.assertIsNotNone(prepared_name, "Expected saved ad-hoc plan entry in QUERY_METADATA")
            prior_persisted = self._persisted_adhoc_doc(prepared_name)
            self._set_error_policy("moderate")
            self._drop_recreate_bucket(self.default_bucket_name)
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            self.run_cbq_query(query="EXECUTE '{0}'".format(prepared_name))
            self.assertEqual(self._persisted_adhoc_doc(prepared_name), prior_persisted,
                             "moderate must not overwrite persisted ad-hoc plan after bucket recreate")
        finally:
            self.run_cbq_query(query="DROP INDEX {0} IF EXISTS ON {1}".format(idx, self.query_bucket))
            self._cleanup_plan_stability_state()

    def test_error_policy_moderate_index_unavailable_adhoc(self):
        """moderate + saved ad-hoc: indexer down then up, rerun succeeds, saved plan unchanged."""
        index_nodes = self._get_index_nodes()
        if len(index_nodes) < 2:
            self.skipTest("Index unavailable scenario requires >= 2 index-service nodes")
        idx = "idx_ep_mod_unavail_adhoc"
        stmt = self._plan_stability_statement_for_day(17)
        host = index_nodes[0]
        remote = None
        try:
            self._cleanup_plan_stability_state()
            self._create_index_for_plan(idx, self.query_bucket, "join_day", num_replica=0,
                                        nodes=["{0}:{1}".format(host.ip, host.port)])
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "ad_hoc"')
            self.run_cbq_query(query=stmt)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = true') >= 1,
                            eval=True, delay=1, tries=20)
            prepared_name = self._persisted_adhoc_name(stmt)
            self.assertIsNotNone(prepared_name, "Expected saved ad-hoc plan entry in QUERY_METADATA")
            prior_persisted = self._persisted_adhoc_doc(prepared_name)
            self._set_error_policy("moderate")
            remote = self._make_index_unavailable_via_node_down(host)
            self._restore_index_node(remote)
            remote = None
            self.run_cbq_query(query="EXECUTE '{0}'".format(prepared_name))
            self.assertEqual(self._persisted_adhoc_doc(prepared_name), prior_persisted,
                             "moderate must not overwrite persisted ad-hoc plan after index recovery")
        finally:
            if remote is not None:
                self._restore_index_node(remote)
            self.run_cbq_query(query="DROP INDEX {0} IF EXISTS ON {1}".format(idx, self.query_bucket))
            self._cleanup_plan_stability_state()

    def test_error_policy_flexible_drop_index_prepared(self):
        """flexible + saved prepared: EXECUTE overwrites saved plan after index drop/recreate."""
        plan_name, idx = "ep_flex_drop_idx_prep", "idx_ep_flex_drop_prep"
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "prepared_only"')
            self._set_error_policy("flexible")
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            self._prepare_plan_statement(plan_name, self._plan_stability_statement_for_day(18), save=True)
            prior_persisted = self._persisted_prepared_doc(plan_name)
            self._drop_index_for_plan(idx, self.query_bucket, "join_day")
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            self.run_cbq_query(query="EXECUTE {0}".format(plan_name))
            self.with_retry(lambda: self._persisted_prepared_doc(plan_name) != prior_persisted,
                            eval=True, delay=1, tries=20)
            self.assertNotEqual(self._persisted_prepared_doc(plan_name), prior_persisted,
                                "flexible must overwrite persisted prepared plan")
        finally:
            self._cleanup_plan_stability_state()

    def test_error_policy_flexible_drop_recreate_collection_prepared(self):
        """flexible + saved prepared: collection drop/recreate then EXECUTE overwrites saved plan."""
        plan_name, scope, coll = "ep_flex_drop_coll_prep", "test2", self.collections[1]
        stmt = "SELECT name FROM default:default.{0}.{1} WHERE name = 'new hotel'".format(scope, coll)
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "prepared_only"')
            self._set_error_policy("flexible")
            self._prepare_plan_statement(plan_name, stmt, save=True)
            prior_persisted = self._persisted_prepared_doc(plan_name)
            self._drop_recreate_collection(scope, coll)
            self.run_cbq_query(query="EXECUTE {0}".format(plan_name))
            self.with_retry(lambda: self._persisted_prepared_doc(plan_name) != prior_persisted,
                            eval=True, delay=1, tries=20)
            self.assertNotEqual(self._persisted_prepared_doc(plan_name), prior_persisted,
                                "flexible must overwrite persisted prepared plan after collection recreate")
        finally:
            self._cleanup_plan_stability_state()

    def test_error_policy_flexible_drop_recreate_bucket_prepared(self):
        """flexible + saved prepared: bucket drop/recreate then EXECUTE overwrites saved plan."""
        plan_name, idx = "ep_flex_drop_bucket_prep", "idx_ep_flex_bucket_prep"
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "prepared_only"')
            self._set_error_policy("flexible")
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            self._prepare_plan_statement(plan_name, self._plan_stability_statement_for_day(19), save=True)
            prior_persisted = self._persisted_prepared_doc(plan_name)
            self._drop_recreate_bucket(self.default_bucket_name)
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            self.run_cbq_query(query="EXECUTE {0}".format(plan_name))
            self.with_retry(lambda: self._persisted_prepared_doc(plan_name) != prior_persisted,
                            eval=True, delay=1, tries=20)
            self.assertNotEqual(self._persisted_prepared_doc(plan_name), prior_persisted,
                                "flexible must overwrite persisted prepared plan after bucket recreate")
        finally:
            self.run_cbq_query(query="DROP INDEX {0} IF EXISTS ON {1}".format(idx, self.query_bucket))
            self._cleanup_plan_stability_state()

    def test_error_policy_flexible_index_unavailable_prepared(self):
        """flexible + saved prepared: indexer down then up; EXECUTE overwrites saved plan."""
        index_nodes = self._get_index_nodes()
        if len(index_nodes) < 2:
            self.skipTest("Index unavailable scenario requires >= 2 index-service nodes")
        plan_name, idx = "ep_flex_idx_unavail_prep", "idx_ep_flex_unavail_prep"
        host = index_nodes[0]
        remote = None
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "prepared_only"')
            self._set_error_policy("flexible")
            self._create_index_for_plan(idx, self.query_bucket, "join_day", num_replica=0,
                                        nodes=["{0}:{1}".format(host.ip, host.port)])
            self._prepare_plan_statement(plan_name, self._plan_stability_statement_for_day(20), save=True)
            prior_persisted = self._persisted_prepared_doc(plan_name)
            remote = self._make_index_unavailable_via_node_down(host)
            self.run_cbq_query(query="EXECUTE {0}".format(plan_name))
            self._restore_index_node(remote)
            remote = None
            self.with_retry(lambda: self._persisted_prepared_doc(plan_name) != prior_persisted,
                            eval=True, delay=1, tries=20)
            self.assertNotEqual(self._persisted_prepared_doc(plan_name), prior_persisted,
                                "flexible must overwrite persisted prepared plan after index recovery")
        finally:
            if remote is not None:
                self._restore_index_node(remote)
            self.run_cbq_query(query="DROP INDEX {0} IF EXISTS ON {1}".format(idx, self.query_bucket))
            self._cleanup_plan_stability_state()

    def test_error_policy_flexible_drop_index_adhoc(self):
        """flexible + saved ad-hoc: rerun overwrites saved ad-hoc plan after index drop/recreate."""
        idx = "idx_ep_flex_drop_adhoc"
        stmt = self._plan_stability_statement_for_day(21)
        try:
            self._cleanup_plan_stability_state()
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "ad_hoc"')
            self.run_cbq_query(query=stmt)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = true') >= 1,
                            eval=True, delay=1, tries=20)
            prepared_name = self._persisted_adhoc_name(stmt)
            self.assertIsNotNone(prepared_name, "Expected saved ad-hoc plan entry in QUERY_METADATA")
            prior_persisted = self._persisted_adhoc_doc(prepared_name)
            self._set_error_policy("flexible")
            self._drop_index_for_plan(idx, self.query_bucket, "join_day")
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            self.run_cbq_query(query="EXECUTE '{0}'".format(prepared_name))
            self.with_retry(lambda: self._persisted_adhoc_doc(prepared_name) != prior_persisted,
                            eval=True, delay=1, tries=20)
            self.assertNotEqual(self._persisted_adhoc_doc(prepared_name), prior_persisted,
                                "flexible must overwrite persisted ad-hoc plan")
        finally:
            self._cleanup_plan_stability_state()

    def test_error_policy_flexible_drop_recreate_collection_adhoc(self):
        """flexible + saved ad-hoc: collection drop/recreate, rerun overwrites saved plan."""
        scope, coll = "test2", self.collections[1]
        stmt = "SELECT name FROM default:default.{0}.{1} WHERE name = 'new hotel'".format(scope, coll)
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "ad_hoc"')
            self.run_cbq_query(query=stmt)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = true') >= 1,
                            eval=True, delay=1, tries=20)
            prepared_name = self._persisted_adhoc_name(stmt)
            self.assertIsNotNone(prepared_name, "Expected saved ad-hoc plan entry in QUERY_METADATA")
            prior_persisted = self._persisted_adhoc_doc(prepared_name)
            self._set_error_policy("flexible")
            self._drop_recreate_collection(scope, coll)
            self.run_cbq_query(query="EXECUTE '{0}'".format(prepared_name))
            self.with_retry(lambda: self._persisted_adhoc_doc(prepared_name) != prior_persisted,
                            eval=True, delay=1, tries=20)
            self.assertNotEqual(self._persisted_adhoc_doc(prepared_name), prior_persisted,
                                "flexible must overwrite persisted ad-hoc plan after collection recreate")
        finally:
            self._cleanup_plan_stability_state()

    def test_error_policy_flexible_drop_recreate_bucket_adhoc(self):
        """flexible + saved ad-hoc: bucket drop/recreate, rerun overwrites saved plan."""
        idx = "idx_ep_flex_bucket_adhoc"
        stmt = self._plan_stability_statement_for_day(22)
        try:
            self._cleanup_plan_stability_state()
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "ad_hoc"')
            self.run_cbq_query(query=stmt)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = true') >= 1,
                            eval=True, delay=1, tries=20)
            prepared_name = self._persisted_adhoc_name(stmt)
            self.assertIsNotNone(prepared_name, "Expected saved ad-hoc plan entry in QUERY_METADATA")
            prior_persisted = self._persisted_adhoc_doc(prepared_name)
            self._set_error_policy("flexible")
            self._drop_recreate_bucket(self.default_bucket_name)
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            self.run_cbq_query(query="EXECUTE '{0}'".format(prepared_name))
            self.with_retry(lambda: self._persisted_adhoc_doc(prepared_name) != prior_persisted,
                            eval=True, delay=1, tries=20)
            self.assertNotEqual(self._persisted_adhoc_doc(prepared_name), prior_persisted,
                                "flexible must overwrite persisted ad-hoc plan after bucket recreate")
        finally:
            self.run_cbq_query(query="DROP INDEX {0} IF EXISTS ON {1}".format(idx, self.query_bucket))
            self._cleanup_plan_stability_state()

    def test_error_policy_flexible_index_unavailable_adhoc(self):
        """flexible + saved ad-hoc: indexer down then up, rerun overwrites saved plan."""
        index_nodes = self._get_index_nodes()
        if len(index_nodes) < 2:
            self.skipTest("Index unavailable scenario requires >= 2 index-service nodes")
        idx = "idx_ep_flex_unavail_adhoc"
        stmt = self._plan_stability_statement_for_day(23)
        host = index_nodes[0]
        remote = None
        try:
            self._cleanup_plan_stability_state()
            self._create_index_for_plan(idx, self.query_bucket, "join_day", num_replica=0,
                                        nodes=["{0}:{1}".format(host.ip, host.port)])
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "ad_hoc"')
            self.run_cbq_query(query=stmt)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = true') >= 1,
                            eval=True, delay=1, tries=20)
            prepared_name = self._persisted_adhoc_name(stmt)
            self.assertIsNotNone(prepared_name, "Expected saved ad-hoc plan entry in QUERY_METADATA")
            prior_persisted = self._persisted_adhoc_doc(prepared_name)
            self._set_error_policy("flexible")
            remote = self._make_index_unavailable_via_node_down(host)
            self._restore_index_node(remote)
            remote = None
            self.run_cbq_query(query="EXECUTE '{0}'".format(prepared_name))
            self.with_retry(lambda: self._persisted_adhoc_doc(prepared_name) != prior_persisted,
                            eval=True, delay=1, tries=20)
            self.assertNotEqual(self._persisted_adhoc_doc(prepared_name), prior_persisted,
                                "flexible must overwrite persisted ad-hoc plan after index recovery")
        finally:
            if remote is not None:
                self._restore_index_node(remote)
            self.run_cbq_query(query="DROP INDEX {0} IF EXISTS ON {1}".format(idx, self.query_bucket))
            self._cleanup_plan_stability_state()

    def test_error_policy_strict_mixed_saved_plans(self):
        """strict + mixed: saved prepared and saved ad-hoc both raise after their shared index is dropped."""
        plan_name, idx = "ep_strict_mixed_prep", "idx_ep_strict_mixed"
        adhoc_stmt = self._plan_stability_statement_for_day(24)
        prepared_stmt = self._plan_stability_statement_for_day(25)
        try:
            self._cleanup_plan_stability_state()
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "ad_hoc"')
            self.run_cbq_query(query=adhoc_stmt)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = true') >= 1,
                            eval=True, delay=1, tries=20)
            adhoc_prepared_name = self._persisted_adhoc_name(adhoc_stmt)
            self.assertIsNotNone(adhoc_prepared_name, "Expected saved ad-hoc plan entry in QUERY_METADATA")
            self._prepare_plan_statement(plan_name, prepared_stmt, save=True)
            self._set_error_policy("strict")
            self._drop_index_for_plan(idx, self.query_bucket, "join_day")
            try:
                self.run_cbq_query(query="EXECUTE {0}".format(plan_name))
                self.fail("strict must raise on saved prepared rerun")
            except CBQError as ex:
                self.assertTrue("Reprepare error - cause: Plan Stability error policy STRICT prevents reprepare" in str(ex),
                                "Unexpected error message for strict policy reprepare failure: {0}".format(str(ex)))
                self.log.info("strict prepared raised as expected: {0}".format(ex))
            try:
                self.run_cbq_query(query="EXECUTE '{0}'".format(adhoc_prepared_name))
                self.fail("strict must raise on saved ad-hoc rerun")
            except CBQError as ex:
                self.assertTrue("Reprepare error - cause: Plan Stability error policy STRICT prevents reprepare" in str(ex),
                                "Unexpected error message for strict policy reprepare failure: {0}".format(str(ex)))
                self.log.info("strict ad-hoc raised as expected: {0}".format(ex))
        finally:
            self._cleanup_plan_stability_state()

    def test_error_policy_moderate_mixed_saved_plans(self):
        """moderate + mixed: both saved prepared and saved ad-hoc succeed; both saved plans unchanged."""
        plan_name, idx = "ep_mod_mixed_prep", "idx_ep_mod_mixed"
        adhoc_stmt = self._plan_stability_statement_for_day(26)
        prepared_stmt = self._plan_stability_statement_for_day(27)
        try:
            self._cleanup_plan_stability_state()
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "ad_hoc"')
            self.run_cbq_query(query=adhoc_stmt)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = true') >= 1,
                            eval=True, delay=1, tries=20)
            adhoc_prepared_name = self._persisted_adhoc_name(adhoc_stmt)
            self.assertIsNotNone(adhoc_prepared_name, "Expected saved ad-hoc plan entry in QUERY_METADATA")
            self._prepare_plan_statement(plan_name, prepared_stmt, save=True)
            prior_prep = self._persisted_prepared_doc(plan_name)
            prior_adhoc = self._persisted_adhoc_doc(adhoc_prepared_name)
            self._set_error_policy("moderate")
            self._drop_index_for_plan(idx, self.query_bucket, "join_day")
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            self.run_cbq_query(query="EXECUTE {0}".format(plan_name))
            self.run_cbq_query(query="EXECUTE '{0}'".format(adhoc_prepared_name))
            self.assertEqual(self._persisted_prepared_doc(plan_name), prior_prep,
                             "moderate must not overwrite persisted prepared plan in mixed scenario")
            self.assertEqual(self._persisted_adhoc_doc(adhoc_prepared_name), prior_adhoc,
                             "moderate must not overwrite persisted ad-hoc plan in mixed scenario")
        finally:
            self._cleanup_plan_stability_state()

    def test_error_policy_flexible_mixed_saved_plans(self):
        """flexible + mixed: both saved prepared and saved ad-hoc succeed and both saved plans are replaced."""
        plan_name, idx = "ep_flex_mixed_prep", "idx_ep_flex_mixed"
        adhoc_stmt = self._plan_stability_statement_for_day(28)
        prepared_stmt = self._plan_stability_statement_for_day(29)
        try:
            self._cleanup_plan_stability_state()
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "ad_hoc"')
            self.run_cbq_query(query=adhoc_stmt)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = true') >= 1,
                            eval=True, delay=1, tries=20)
            adhoc_prepared_name = self._persisted_adhoc_name(adhoc_stmt)
            self.assertIsNotNone(adhoc_prepared_name, "Expected saved ad-hoc plan entry in QUERY_METADATA")
            self._prepare_plan_statement(plan_name, prepared_stmt, save=True)
            prior_prep = self._persisted_prepared_doc(plan_name)
            prior_adhoc = self._persisted_adhoc_doc(adhoc_prepared_name)
            self._set_error_policy("flexible")
            self._drop_index_for_plan(idx, self.query_bucket, "join_day")
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            self.run_cbq_query(query="EXECUTE {0}".format(plan_name))
            self.run_cbq_query(query="EXECUTE '{0}'".format(adhoc_prepared_name))
            self.with_retry(lambda: self._persisted_prepared_doc(plan_name) != prior_prep,
                            eval=True, delay=1, tries=20)
            self.with_retry(lambda: self._persisted_adhoc_doc(adhoc_prepared_name) != prior_adhoc,
                            eval=True, delay=1, tries=20)
            self.assertNotEqual(self._persisted_prepared_doc(plan_name), prior_prep,
                                "flexible must overwrite persisted prepared plan in mixed scenario")
            self.assertNotEqual(self._persisted_adhoc_doc(adhoc_prepared_name), prior_adhoc,
                                "flexible must overwrite persisted ad-hoc plan in mixed scenario")
        finally:
            self._cleanup_plan_stability_state()

    def test_error_policy_strict_to_flexible_recovers_prepared(self):
        """saved prepared: strict raises; switching to flexible lets EXECUTE succeed and overwrite saved plan."""
        plan_name, idx = "ep_trans_prep", "idx_ep_trans_prep"
        try:
            self._cleanup_plan_stability_state()
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "prepared_only"')
            self._set_error_policy("strict")
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            self._prepare_plan_statement(plan_name, self._plan_stability_statement_for_day(30), save=True)
            prior_persisted = self._persisted_prepared_doc(plan_name)
            self._drop_index_for_plan(idx, self.query_bucket, "join_day")
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            try:
                self.run_cbq_query(query="EXECUTE {0}".format(plan_name))
                self.fail("strict policy should raise on invalid saved plan")
            except CBQError as ex:
                self.assertTrue("Reprepare error - cause: Plan Stability error policy STRICT prevents reprepare" in str(ex),
                                "Unexpected error message for strict policy reprepare failure: {0}".format(str(ex)))
                self.log.info("strict raised as expected: {0}".format(ex))
            self._set_error_policy("flexible")
            self.run_cbq_query(query="EXECUTE {0}".format(plan_name))
            self.with_retry(lambda: self._persisted_prepared_doc(plan_name) != prior_persisted,
                            eval=True, delay=1, tries=20)
            self.assertNotEqual(self._persisted_prepared_doc(plan_name), prior_persisted,
                                "flexible must overwrite persisted prepared plan after switching from strict")
        finally:
            self._cleanup_plan_stability_state()

    def test_error_policy_strict_to_flexible_recovers_adhoc(self):
        """saved ad-hoc: strict raises; switching to flexible lets the rerun succeed and overwrite saved plan."""
        idx = "idx_ep_trans_adhoc"
        stmt = self._plan_stability_statement_for_day(31)
        try:
            self._cleanup_plan_stability_state()
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "ad_hoc"')
            self.run_cbq_query(query=stmt)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = true') >= 1,
                            eval=True, delay=1, tries=20)
            prepared_name = self._persisted_adhoc_name(stmt)
            self.assertIsNotNone(prepared_name, "Expected saved ad-hoc plan entry in QUERY_METADATA")
            prior_persisted = self._persisted_adhoc_doc(prepared_name)
            self._set_error_policy("strict")
            self._drop_index_for_plan(idx, self.query_bucket, "join_day")
            self._create_index_for_plan(idx, self.query_bucket, "join_day")
            try:
                self.run_cbq_query(query="EXECUTE '{0}'".format(prepared_name))
                self.fail("strict policy should raise on invalid saved ad-hoc plan")
            except CBQError as ex:
                self.assertTrue("Reprepare error - cause: Plan Stability error policy STRICT prevents reprepare" in str(ex),
                                "Unexpected error message for strict policy reprepare failure: {0}".format(str(ex)))
                self.log.info("strict raised as expected: {0}".format(ex))
            self._set_error_policy("flexible")
            self.run_cbq_query(query="EXECUTE '{0}'".format(prepared_name))
            self.with_retry(lambda: self._persisted_adhoc_doc(prepared_name) != prior_persisted,
                            eval=True, delay=1, tries=20)
            self.assertNotEqual(self._persisted_adhoc_doc(prepared_name), prior_persisted,
                                "flexible must overwrite persisted ad-hoc plan after switching from strict")
        finally:
            self._cleanup_plan_stability_state()

    def test_saved_plans_survive_all_query_nodes_restart(self):
        """Killing cbq-engine on every query node clears system:prepareds; saved plans in QUERY_METADATA still execute."""
        query_nodes = self._get_query_nodes()
        if not query_nodes:
            self.skipTest("No query-service nodes available")
        prepared_saved, prepared_volatile = "ps_restart_saved", "ps_restart_volatile"
        adhoc_stmt = self._plan_stability_statement_for_day(32)
        try:
            self._cleanup_plan_stability_state()
            self._set_error_policy("moderate")
            self._prepare_plan_statement(prepared_saved, self._plan_stability_statement_for_day(33), save=True)
            self.run_cbq_query(query="PREPARE {0} FROM SELECT * FROM {1} LIMIT 5".format(prepared_volatile, self.query_bucket))
            self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "ad_hoc"')
            self.run_cbq_query(query=adhoc_stmt)
            self.with_retry(lambda: self._query_metadata_doc_count(where_clause='ad_hoc = true') >= 1,
                            eval=True, delay=1, tries=20)
            self._restart_all_query_services(query_nodes)
            self.with_retry(lambda: self._cluster_prepareds_count() == 0,
                            eval=True, delay=2, tries=20)
            execute_saved = self.run_cbq_query(query="EXECUTE {0}".format(prepared_saved))
            self.assertTrue('results' in execute_saved, "Saved prepared should execute after restart")
            execute_adhoc = self.run_cbq_query(query=adhoc_stmt)
            self.assertTrue('results' in execute_adhoc, "Saved ad-hoc statement should execute after restart")
            try:
                self.run_cbq_query(query="EXECUTE {0}".format(prepared_volatile))
                self.fail("Volatile prepared should be gone after query restart")
            except CBQError as ex:
                self.assertTrue("No such prepared statement" in str(ex),
                                "Unexpected error for volatile prepared after restart: {0}".format(ex))
        finally:
            self._cleanup_plan_stability_state()

    def _prepare_plan_statement(self, name, statement, save=False, force=False):
        prepare_tokens = ['PREPARE']
        if save:
            prepare_tokens.append('SAVE')
        if force:
            prepare_tokens.append('FORCE')
        prepare_tokens.extend([name, 'AS', statement])
        self.run_cbq_query(query=' '.join(prepare_tokens))

    def _plan_stability_statement_for_day(self, day):
        return 'SELECT RAW join_day FROM {0} WHERE join_day = {1} LIMIT 2'.format(self.query_bucket, day)

    def _prepared_statement_text(self, name):
        result = self.run_cbq_query(query='SELECT prepareds.statement AS statement FROM system:prepareds WHERE prepareds.name = "{0}" LIMIT 1'.format(name))
        if result['metrics']['resultCount'] == 0:
            return None
        return result['results'][0]['statement']

    def _run_plan_stability_ad_hoc_queries(self):
        ad_hoc_queries = [
            "SELECT * FROM {0} LIMIT 1".format(self.query_bucket),
            "SELECT * FROM {0} WHERE join_day = 10 LIMIT 2".format(self.query_bucket),
            "SELECT META().id FROM {0} WHERE join_mo = 10 LIMIT 3".format(self.query_bucket)]
        for query in ad_hoc_queries:
            self.run_cbq_query(query=query)

    def _query_metadata_keyspace_count(self):
        result = self.run_cbq_query(query='SELECT COUNT(*) AS keyspace_count FROM system:keyspaces WHERE name = "QUERY_METADATA"')
        return result['results'][0]['keyspace_count']

    def _query_metadata_stats(self):
        return {
            'total': self._query_metadata_doc_count(),
            'ad_hoc_docs': self._query_metadata_doc_count(where_clause='ad_hoc = true'),
            'prepared_docs': self._query_metadata_doc_count(where_clause='ad_hoc = false')
        }

    def _create_plan_stability_user(self, user_id, roles, password='password1'):
        self._drop_plan_stability_user(user_id)
        user = {'id': user_id, 'name': user_id, 'password': password}
        self.create_users(users=[user])
        self.assign_role(roles=[{'id': user_id, 'name': user_id, 'roles': roles}])
        return user

    def _drop_plan_stability_user(self, user_id):
        try:
            self.run_cbq_query(query='DROP USER {0}'.format(user_id))
        except CBQError as ex:
            error_message = str(ex).lower()
            if "not found" in error_message or "does not exist" in error_message:
                return
            self.log.info("Ignoring user cleanup error for {0}: {1}".format(user_id, str(ex)))

    def _assert_non_admin_query_denied(self, user, query, operation):
        try:
            self.run_cbq_query(query=query, username=user['id'], password=user['password'])
            self.fail("Expected non-admin user to fail while attempting to {0}".format(operation))
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertTrue(error['code'] in [13014, 12003, 10000],
                            "Unexpected error code for unauthorized {0}: {1}".format(operation, error['code']))
            self.assertTrue(any(token in error['msg'].lower() for token in ["credential", "permission", "authorized", "authorization", "access"]),
                            "Unexpected error message for unauthorized {0}: {1}".format(operation, error['msg']))

    def _cleanup_plan_stability_state(self):
        self.run_cbq_query(query='UPDATE system:settings SET plan_stability.mode = "off"')
        self.sleep(10)
        self.run_cbq_query(query='DELETE FROM system:prepareds')
        self.sleep(10)
        self.run_cbq_query(query='DROP BUCKET IF EXISTS QUERY_METADATA')
        self.sleep(30)
        self.with_retry(lambda: self.run_cbq_query(
            query='SELECT COUNT(*) AS keyspace_count FROM system:keyspaces WHERE name = "QUERY_METADATA"')[
            'results'][0]['keyspace_count'], eval=0, delay=1, tries=20)

    def _prepare_named_statements(self, name_prefix, count):
        for i in range(count):
            self.run_cbq_query(query='PREPARE {0}_{1} AS SELECT * FROM {2} WHERE join_day = {3} LIMIT 5'.format(
                name_prefix, i, self.query_bucket, i + 1))

    def _query_metadata_doc_count(self, where_clause=''):
        query = 'SELECT COUNT(*) AS doc_count FROM `QUERY_METADATA`.`_system`.`_query`'
        if where_clause:
            query = '{0} WHERE {1}'.format(query, where_clause)
        try:
            results = self.run_cbq_query(query=query)
            return results['results'][0]['doc_count']
        except CBQError as ex:
            error_message = str(ex)
            if "QUERY_METADATA" in error_message and ("No bucket named" in error_message or
                                                        "not found" in error_message):
                return 0
            raise

    def _set_error_policy(self, policy):
        self.run_cbq_query(query='UPDATE system:settings SET plan_stability.error_policy = "{0}"'.format(policy))

    def _get_error_policy(self):
        result = self.run_cbq_query(query='SELECT * FROM system:settings')
        plan_stability = result['results'][0]['settings'].get('plan_stability', {})
        return plan_stability.get('error_policy')

    def _create_index_for_plan(self, name, keyspace, field, num_replica=None, nodes=None):
        with_clauses = []
        if num_replica is not None:
            with_clauses.append('"num_replica": {0}'.format(num_replica))
        if nodes:
            with_clauses.append('"nodes": [{0}]'.format(', '.join('"{0}"'.format(n) for n in nodes)))
        query = "CREATE INDEX {0} ON {1}({2})".format(name, keyspace, field)
        if with_clauses:
            query = "{0} WITH {{{1}}}".format(query, ', '.join(with_clauses))
        self.run_cbq_query(query=query)
        self._wait_for_index_online(self.default_bucket_name, name)

    def _drop_index_for_plan(self, name, keyspace, field):
        self.run_cbq_query(query="DROP INDEX {0} IF EXISTS ON {1}".format(name, keyspace))
        try:
            self.wait_for_index_drop(self.default_bucket_name, name, [(field, 0)], self.index_type.lower())
        except Exception as ex:
            self.log.info("wait_for_index_drop ignored: {0}".format(ex))

    def _drop_recreate_collection(self, scope, collection):
        self.run_cbq_query(query='DROP COLLECTION `{0}`.`{1}`.`{2}`'.format(self.default_bucket_name, scope, collection))
        self.sleep(30)
        self.run_cbq_query(query='CREATE COLLECTION `{0}`.`{1}`.`{2}`'.format(self.default_bucket_name, scope, collection))
        self.sleep(30)

    def _drop_recreate_bucket(self, bucket):
        self.ensure_bucket_does_not_exist(bucket, using_rest=True)
        self.rest.create_bucket(bucket=bucket, ramQuotaMB=self.bucket_size)
        self.wait_for_buckets_status({bucket: "healthy"}, 5, 120)
        self.sleep(30)

    def _persisted_prepared_doc(self, name):
        try:
            results = self.run_cbq_query(query='SELECT META().id, * FROM `QUERY_METADATA`.`_system`.`_query` WHERE name = "{0}" LIMIT 1'.format(name))
            if results['metrics']['resultCount'] == 0:
                return None
            return str(results['results'][0])
        except CBQError as ex:
            if "QUERY_METADATA" in str(ex) and ("No bucket named" in str(ex) or "not found" in str(ex)):
                return None
            raise

    def _persisted_adhoc_doc(self, name):
        try:
            results = self.run_cbq_query(query='SELECT META().id, * FROM `QUERY_METADATA`.`_system`.`_query` WHERE name = "{0}" LIMIT 1'.format(name))
            if results['metrics']['resultCount'] == 0:
                return None
            return str(results['results'][0])
        except CBQError as ex:
            if "QUERY_METADATA" in str(ex) and ("No bucket named" in str(ex) or "not found" in str(ex)):
                return None
            raise

    def _persisted_adhoc_name(self, statement):
        statement_prefix = statement[:45]
        escaped_statement = statement_prefix.replace('\\', '\\\\').replace('"', '\\"')
        try:
            results = self.run_cbq_query(
                query='SELECT name FROM `QUERY_METADATA`.`_system`.`_query` WHERE ad_hoc = true AND text LIKE "{0}%" LIMIT 1'.format(
                    escaped_statement))
            if results['metrics']['resultCount'] == 0:
                return None
            return results['results'][0]['name']
        except CBQError as ex:
            if "QUERY_METADATA" in str(ex) and ("No bucket named" in str(ex) or "not found" in str(ex)):
                return None
            raise

    def _get_index_nodes(self):
        try:
            return self.get_nodes_from_services_map(service_type="index", get_all_nodes=True) or []
        except Exception as ex:
            self.log.info("Falling back to self.servers for index nodes: {0}".format(ex))
            return list(self.servers[:self.nodes_init])

    def _get_query_nodes(self):
        try:
            return self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True) or []
        except Exception as ex:
            self.log.info("Falling back to self.servers for query nodes: {0}".format(ex))
            return list(self.servers[:self.nodes_init])

    def _make_index_unavailable_via_node_down(self, host_node):
        self.log.info("Stopping indexer node {0}:{1} to force index unavailability".format(host_node.ip, host_node.port))
        remote = RemoteMachineShellConnection(host_node)
        remote.stop_server()
        self.sleep(20)
        return remote

    def _restore_index_node(self, remote):
        self.log.info("Starting back the indexer node")
        remote.start_server()
        self.sleep(30)

    def _restart_all_query_services(self, query_nodes):
        for node in query_nodes:
            try:
                shell = RemoteMachineShellConnection(node)
                shell.execute_command("killall -9 cbq-engine")
                shell.disconnect()
                self.log.info("Killed cbq-engine on {0}".format(node.ip))
            except Exception as ex:
                self.log.info("Failed to kill cbq-engine on {0}: {1}".format(node.ip, ex))
        self.sleep(30)

    def _cluster_prepareds_count(self):
        try:
            results = self.run_cbq_query(query="SELECT COUNT(*) AS prepared_count FROM system:prepareds")
            return results['results'][0]['prepared_count']
        except Exception as ex:
            self.log.info("system:prepareds unavailable, treating as 0: {0}".format(ex))
            return 0