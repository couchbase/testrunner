
from remote.remote_util import RemoteMachineShellConnection
from .tuq import QueryTests
import time
from deepdiff import DeepDiff

class QueryAliasTests(QueryTests):

    def setUp(self):
        super(QueryAliasTests, self).setUp()
        self.log.info("==============  QuerySanityTests setup has started ==============")
        self.explicit_full_path = self.input.param("explicit_full_path", False)
        self.explicit_bucket_name = self.input.param("explicit_bucket_name", False)
        self.implicit_full_path = self.input.param("implicit_full_path", False)
        self.implicit_bucket_name = self.input.param("implicit_bucket_name", False)
        self.nested_field = self.input.param("nested_field", False)
        self.join = self.input.param("join", False)
        self.prepared = self.input.param("prepared", False)
        self.nested_field = self.input.param("nested_field", False)
        self.array = self.input.param("array", False)
        self.subquery = self.input.param("subquery", False)
        self.params = self.input.param("params", False)
        self.query_context = self.input.param("query_context", False)
        self.cbqpath = '{0}cbq -quiet -u {1} -p {2} -e=localhost:8093 '.format(self.path, self.username, self.password)
        self.scope = self.input.param("scope", "test")
        self.collections = self.input.param("collections", ["test1", "test2"])
        self.run_cbq_query(query='CREATE SCOPE default.{0} IF NOT EXISTS'.format(self.scope))
        time.sleep(5)
        self.run_cbq_query(query='CREATE COLLECTION default.{0}.{1} IF NOT EXISTS'.format(self.scope, self.collections[0]))
        time.sleep(5)
        self.run_cbq_query(query='CREATE COLLECTION default.{0}.{1} IF NOT EXISTS'.format(self.scope, self.collections[1]))
        time.sleep(5)
        self.run_cbq_query(
            query=('UPSERT INTO default:default.{0}.{1}'.format(self.scope, self.collections[0]) + '(KEY, VALUE) VALUES ("key2", { "type" : "hotel", "name" : "new hotel" })'))
        self.run_cbq_query(
            query=('UPSERT INTO default:default.{0}.{1}'.format(self.scope, self.collections[0]) + '(KEY, VALUE) VALUES ("key1", { "type" : "hotel", "name" : "old hotel" })'))
        self.run_cbq_query(
            query=('UPSERT INTO default:default.{0}.{1}'.format(self.scope, self.collections[1]) + '(KEY, VALUE) VALUES ("key1", { "type" : "hotel", "name" : "new hotel" })'))
        self.run_cbq_query(
            query=('UPSERT INTO default:default.{0}.{1}'.format(self.scope, self.collections[0]) + ' (KEY, VALUE) VALUES ("key3", { "nested" : {"fields": "fake"}, "name" : "old hotel" })'))
        self.run_cbq_query(
            query=('UPSERT INTO default:default.{0}.{1}'.format(self.scope, self.collections[0]) + ' (KEY, VALUE) VALUES ("key4", { "numbers": [1,2,3,4] , "name" : "old hotel" })'))
        time.sleep(20)


        self.log.info("==============  QuerySanityTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryAliasTests, self).suite_setUp()
        self.log.info("==============  QuerySanityTests suite_setup has started ==============")
        self.log.info("==============  QuerySanityTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QuerySanityTests tearDown has started ==============")
        self.log.info("==============  QuerySanityTests tearDown has completed ==============")
        super(QueryAliasTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QuerySanityTests suite_tearDown has started ==============")
        self.log.info("==============  QuerySanityTests suite_tearDown has completed ==============")
        super(QueryAliasTests, self).suite_tearDown()

    def test_basic_aliasing(self):
        if self.explicit_full_path:
            if self.prepared:
                self.run_cbq_query(query="PREPARE p1 AS SELECT * FROM default:default.test.test1 b WHERE b.name = 'new hotel'")
                results = self.run_cbq_query(query="EXECUTE p1")
                self.assertEqual(results['results'][0]['b'], {'name': 'new hotel', 'type': 'hotel'})
            elif self.join:
                results = self.run_cbq_query(query='SELECT * FROM default:default.test.test1 b1  JOIN default:default.test.test2 b2 ON b2.name = b1.name WHERE b1.type = "hotel"')
                self.assertEqual(results['results'][0], {'b1': {'name': 'new hotel', 'type': 'hotel'}, 'b2': {'name': 'new hotel', 'type': 'hotel'}})
            elif self.nested_field:
                results = self.run_cbq_query(query="SELECT * FROM default:default.test.test1 b WHERE b.nested.fields = 'fake'")
                self.assertEqual(results['results'][0]['b'], {'name': 'old hotel', 'nested': {'fields': 'fake'}})
            elif self.array:
                results = self.run_cbq_query(query="SELECT * FROM default:default.test.test1 b WHERE ANY v in b.numbers SATISFIES v = 1 END")
                self.assertEqual(results['results'][0]['b'], {'name': 'old hotel', 'numbers': [1, 2, 3, 4]})
            elif self.subquery:
                results = self.run_cbq_query(query="select * FROM default:default.test.test1 b WHERE name in (select raw b2.name FROM default:default.test.test2 b2 where b2.name = 'new hotel')")
                self.assertEqual(results['results'][0]['b'], {"name": "new hotel","type": "hotel"})
            elif self.params:
                queries = ['\SET -$name old hotel;',
                               'select name from default:default.test.test1 b where b.name=$name']
                o = self.execute_commands_inside(self.cbqpath, '', queries, '', '', 'default', '')
                self.assertTrue('{"name": "old hotel"},{"name": "old hotel"},{"name": "old hotel"}' in o, "Results are incorrect : {0}".format(o))
            elif self.query_context:
                queries = ['\SET -query_context default.test;',
                               'select name from test1 b where b.name="old hotel"']
                o = self.execute_commands_inside(self.cbqpath, '', queries, '', '', 'default', '')
                self.assertTrue('{"name": "old hotel"},{"name": "old hotel"},{"name": "old hotel"}' in o, "Results are incorrect : {0}".format(o))
            else:
                results = self.run_cbq_query(query="SELECT * FROM default:default.test.test1 b WHERE b.name = 'new hotel'")
                self.assertEqual(results['results'][0]['b'], {'name': 'new hotel', 'type': 'hotel'})
        elif self.explicit_bucket_name:
            if self.prepared:
                self.run_cbq_query(query="PREPARE p2 AS SELECT * FROM default b WHERE b.name = 'employee-9'")
                results = self.run_cbq_query(query="EXECUTE p2")
                self.assertEqual(results['metrics']['resultCount'], 72*self.docs_per_day)
            else:
                results = self.run_cbq_query(query="SELECT * FROM default b WHERE b.name = 'employee-9'")
                self.assertEqual(results['metrics']['resultCount'], 72*self.docs_per_day)
        elif self.implicit_full_path:
            if self.prepared:
                self.run_cbq_query(query="PREPARE p3 AS SELECT * FROM default:default.test.test1  WHERE test1.name = 'new hotel'")
                results = self.run_cbq_query(query="EXECUTE p3")
                self.assertEqual(results['results'][0]['test1'], {'name': 'new hotel', 'type': 'hotel'})
            elif self.join:
                results = self.run_cbq_query(query='SELECT * FROM default:default.test.test1 JOIN default:default.test.test2 ON test2.name = test1.name WHERE test1.type = "hotel"')
                self.assertEqual(results['results'][0], {'test1': {'name': 'new hotel', 'type': 'hotel'}, 'test2': {'name': 'new hotel', 'type': 'hotel'}})
            elif self.nested_field:
                results = self.run_cbq_query(query="SELECT * FROM default:default.test.test1  WHERE test1.nested.fields = 'fake'")
                self.assertEqual(results['results'][0]['test1'], {'name': 'old hotel', 'nested': {'fields': 'fake'}})
            elif self.array:
                results = self.run_cbq_query(query="SELECT * FROM default:default.test.test1  WHERE ANY v in test1.numbers SATISFIES v = 1 END")
                self.assertEqual(results['results'][0]['test1'], {'name': 'old hotel', 'numbers': [1, 2, 3, 4]})
            elif self.subquery:
                results = self.run_cbq_query(query="select * FROM default:default.test.test1  WHERE test1.name in (select raw test2.name FROM default:default.test.test2 where test2.name = 'new hotel')")
                self.assertEqual(results['results'][0]['test1'], {"name": "new hotel","type": "hotel"})
            elif self.params:
                queries = ['\SET -$name old hotel;',
                               'select name from default:default.test.test1 where test1.name=$name']
                o = self.execute_commands_inside(self.cbqpath, '', queries, '', '', 'default', '')
                self.assertTrue('{"name": "old hotel"},{"name": "old hotel"},{"name": "old hotel"}' in o, "Results are incorrect : {0}".format(o))
            elif self.query_context:
                queries = ['\SET -query_context default.test;',
                               'select name from test1  where test1.name="old hotel"']
                o = self.execute_commands_inside(self.cbqpath, '', queries, '', '', 'default', '')
                self.assertTrue('{"name": "old hotel"},{"name": "old hotel"},{"name": "old hotel"}' in o, "Results are incorrect : {0}".format(o))
            else:
                results = self.run_cbq_query(query="SELECT * FROM default:default.test.test1  WHERE test1.name = 'new hotel'")
                self.assertEqual(results['results'][0]['test1'], {'name': 'new hotel', 'type': 'hotel'})
        elif self.implicit_bucket_name:
            if self.prepared:
                self.run_cbq_query(query="PREPARE p4 AS SELECT * FROM default  WHERE default.name = 'employee-9'")
                results = self.run_cbq_query(query="EXECUTE p4")
                self.assertEqual(results['metrics']['resultCount'], 72*self.docs_per_day)
            else:
                results = self.run_cbq_query(query="SELECT * FROM default  WHERE default.name = 'employee-9'")
                self.assertEqual(results['metrics']['resultCount'], 72*self.docs_per_day)

