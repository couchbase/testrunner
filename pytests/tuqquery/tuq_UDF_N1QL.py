from .tuq import QueryTests
from deepdiff import DeepDiff
import requests
from membase.api.exception import CBQError
from collection.collections_n1ql_client import CollectionsN1QL
import json

class QueryUDFN1QLTests(QueryTests):
    ddls = {
        'create_index': {
            'pre': 'DROP INDEX udf_ix IF EXISTS on default',
            'query': 'CREATE INDEX udf_ix ON default(a)',
            'function_expected': {'name': 'udf_ix', 'state': 'online'},
            'post': 'SELECT name FROM system:indexes WHERE name = "udf_ix"',
            'post_expected': [{"name": "udf_ix"}]
        },
        'create_scope': {
            'pre': 'DROP SCOPE default.scope1 IF EXISTS',
            'query': 'CREATE SCOPE default.scope1',
            'function_expected': [[]],
            'post': 'SELECT name FROM system:scopes WHERE `bucket` = "default" AND name = "scope1"',
            'post_expected': [{"name": "scope1"}]
        },
        'create_collection': {
            'pre': 'DROP COLLECTION default._default.collection1 IF EXISTS',
            'query': 'CREATE COLLECTION default._default.collection1',
            'function_expected': [[]],
            'post': 'SELECT name FROM system:keyspaces WHERE `bucket` = "default" AND `scope` = "_default" AND name = "collection1"',
            'post_expected': [{"name": "collection1"}]
        },
        'drop_index': {
            'pre': 'CREATE INDEX udf_ix IF NOT EXISTS ON default(a)',
            'query': 'DROP INDEX udf_ix on default',
            'function_expected': [[]],
            'post': 'SELECT name FROM system:indexes WHERE name = "udf_ix"',
            'post_expected': []
        },
        'drop_scope': {
            'pre': 'CREATE SCOPE default.scope1 IF NOT EXISTS',
            'query': 'DROP SCOPE default.scope1',
            'function_expected': [[]],
            'post': 'SELECT name FROM system:scopes WHERE `bucket` = "default" AND name = "scope1"',
            'post_expected': []
        },
        'drop_collection': {
            'pre': 'CREATE COLLECTION default._default.collection1 IF NOT EXISTS',
            'query': 'DROP COLLECTION default._default.collection1',
            'function_expected': [[]],
            'post': 'SELECT name FROM system:keyspaces WHERE `bucket` = "default" AND `scope` = "_default" AND name = "collection1"',
            'post_expected': []
        },
        'create_inline_function': {
            'pre': 'SELECT "noop"',
            'query': 'CREATE OR REPLACE FUNCTION add(a,b) {a+b}',
            'function_expected': [[]],
            'post': 'SELECT identity.name FROM system:functions WHERE definition.`#language` = "inline" AND identity.name = "add"',
            'post_expected': [{"name": "add"}]
        },
        'drop_inline_function': {
            'pre': 'CREATE OR REPLACE FUNCTION add(a,b) {a+b}',
            'query': 'DROP FUNCTION add',
            'function_expected': [[]],
            'post': 'SELECT identity.name FROM system:functions WHERE definition.`#language` = "inline" AND identity.name = "add"',
            'post_expected': []
        },
        'execute_inline_function': {
            'pre': 'CREATE OR REPLACE FUNCTION add(a,b) {a+b}',
            'query': 'EXECUTE FUNCTION add(3,7)',
            'function_expected': [[10]],
            'post': 'EXECUTE FUNCTION add(3,7)',
            'post_expected': [10]
        },
        'create_js_function': {
            'pre': 'SELECT "noop"',
            'query': 'CREATE OR REPLACE FUNCTION add(a,b) LANGUAGE JAVASCRIPT AS "add" AT "math"',
            'function_expected': [[]],
            'post': 'SELECT identity.name FROM system:functions WHERE definition.`#language` = "javascript" AND identity.name = "add"',
            'post_expected': [{"name": "add"}]
        },
        'drop_js_function': {
            'pre': 'CREATE OR REPLACE FUNCTION add(a,b) LANGUAGE JAVASCRIPT AS "add" AT "math"',
            'query': 'DROP FUNCTION add',
            'function_expected': [[]],
            'post': 'SELECT identity.name FROM system:functions WHERE definition.`#language` = "javascript" AND identity.name = "add"',
            'post_expected': []
        },
        'execute_js_function': {
            'pre': 'CREATE OR REPLACE FUNCTION add(a,b) LANGUAGE JAVASCRIPT AS "add" AT "math"',
            'query': 'EXECUTE FUNCTION add(3,7)',
            'function_expected': [[10]],
            'post': 'EXECUTE FUNCTION add(3,7)',
            'post_expected': [10]
        },
        'update_statistics': {
            'pre': 'UPDATE STATISTICS FOR default DELETE ALL',
            'query': 'UPDATE STATISTICS FOR default(job_title)',
            'function_expected': [[]],

            'post': 'select `bucket`, `scope`, `collection`, `histogramKey` from `default`.`_system`.`_query` data WHERE type = "histogram" and `scope` = "_default" and `collection` = "_default"',
            'post_expected' : {'scope': '_default', 'collection': '_default', 'histogramKey': 'job_title'}
        },
        'analyze': {
            'pre': 'UPDATE STATISTICS FOR default DELETE ALL',
            'query': 'ANALYZE default(job_title)',
            'function_expected': [[]],
            'post': 'select `bucket`, `scope`, `collection`, `histogramKey` from `default`.`_system`.`_query` data WHERE type = "histogram" and `scope` = "_default" and `collection` = "_default"',
            'post_expected' : {'scope': '_default', 'collection': '_default', 'histogramKey': 'job_title'}
        },
        'grant': {
            'pre': 'REVOKE query_select on default from jackdoe',
            'query': 'GRANT query_select on default to jackdoe',
            'function_expected': [[]],
            'post': 'select roles from system:user_info where id = "jackdoe"',
            'post_expected': [{'roles': [{'bucket_name': 'default', 'collection_name': '*', 'origins': [{'type': 'user'}], 'role': 'select', 'scope_name': '*'}]}]
        },
        'revoke': {
            'pre': 'GRANT query_select on default to jackdoe',
            'query': 'REVOKE query_select on default from jackdoe',
            'function_expected': [[]],
            'post': 'select roles from system:user_info where id = "jackdoe"',
            'post_expected': [{'roles': []}]
        }
    }
    dmls = {
        'select': 'SELECT d.* FROM default d ORDER BY META(d).id LIMIT 1',
        'select_with_comment': 'SELECT d.* FROM default d ORDER BY META(d).id LIMIT 1 -- some comment',
        'select_with_comment2': 'SELECT d.* FROM default d /* some comment */ ORDER BY META(d).id LIMIT 1',
        'update': 'UPDATE default SET job_title = "ENGINEER" WHERE join_yr = 2011 AND join_mo = 10 AND lower(job_title) = "engineer" RETURNING name, job_title',
        'insert': 'INSERT INTO default (KEY, VALUE) VALUES ("key1", { "type" : "hotel", "name" : "new hotel" }) RETURNING *',
        'upsert': 'UPSERT INTO default (KEY, VALUE) VALUES ("key1", { "type" : "hotel", "name" : "new hotel" }) RETURNING *',
        'delete': 'DELETE FROM default WHERE job_title = "Engineer" AND join_yr = 2011 AND join_mo = 12 RETURNING *',
        'merge': 'MERGE INTO default t USING [{"job_title":"Engineer"}] source ON t.job_title = source.job_title ' \
            'WHEN MATCHED THEN UPDATE SET t.old_tile = "Engineer", t.job_title = "Ingenieur" ' \
            'WHERE t.join_yr = 2011 AND t.join_mo = 11 LIMIT 2 RETURNING *',
        'insert_from_select': 'INSERT INTO default._default.tmp (KEY UUID(), VALUE _employee) SELECT _employee FROM default _employee WHERE job_title = "Engineer" AND join_yr = 2011 AND join_mo = 10 RETURNING *',
        'cte': 'WITH cte as (SELECT d.* FROM default d ORDER BY META(d).id LIMIT 1) SELECT * FROM cte',
        'search': 'SELECT SEARCH_META() AS meta FROM default AS t1 WHERE SEARCH(t1, {"query": {"match": "ubuntu", "fields": "VMs.os", "analyzer": "standard"}, "includeLocations": true }) LIMIT 3'
    }

    def setUp(self):
        super(QueryUDFN1QLTests, self).setUp()
        self.log.info("==============  QueryUDFN1QLTests setup has started ==============")
        self.statement = self.input.param("statement", "statement")
        self.params = self.input.param("params", "named")
        self.inline_func = self.input.param("inline_func", False)
        self.use_select = self.input.param("use_select", False)
        self.test_sideeffect = self.input.param("test_sideeffect", False)
        self.start_txn = self.input.param("start_txn", "BEGIN WORK")
        self.end_txn = self.input.param("end_txn", "COMMIT WORK")
        self.within_txn = self.input.param("within_txn", False)
        self.explicit_close = self.input.param("explicit_close", False)
        self.library_name = 'n1ql'
        self.log.info("==============  QueryUDFN1QLTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryUDFN1QLTests, self).suite_setUp()
        self.log.info("==============  QueryUDFN1QLTests suite_setup has started ==============")
        functions = 'function add(a, b) { return a + b; }'
        self.create_library('math', functions, 'add')
        self.users = [{"id": "jackdoe", "name": "Jack Downing", "password": "password"}]
        self.create_users()
        self.collections_helper = CollectionsN1QL(self.master)
        self.collections_helper.create_collection(bucket_name="default", scope_name="_default", collection_name="txn_scope")
        self.sleep(10)
        self.run_cbq_query("CREATE primary INDEX on default.`_default`.txn_scope")
        self.log.info("==============  QueryUDFN1QLTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueryUDFN1QLTests tearDown has started ==============")
        self.log.info("==============  QueryUDFN1QLTests tearDown has completed ==============")
        super(QueryUDFN1QLTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QueryUDFN1QLTests suite_tearDown has started ==============")
        self.log.info("==============  QueryUDFN1QLTests suite_tearDown has completed ==============")
        super(QueryUDFN1QLTests, self).suite_tearDown()

    ##############################################################################################
    #
    #   Explain Tests
    ##############################################################################################

    def test_explain_inline(self):
        try:
            self.run_cbq_query("CREATE OR REPLACE FUNCTION func1(nameval) {{ (select * from default:default.{0}.{1} where name = nameval) }}".format(self.scope,self.collections[0]))
            explain_udf = self.run_cbq_query("explain function func1".format(self.scope,self.collections[0]))
            expected_spans = self.run_cbq_query('explain select * from default:default.{0}.{1} where name = "old hotel" '.format(self.scope,self.collections[0]))
            self.assertTrue('plans' in explain_udf['results'][0].keys(), f"The explain should have a query plan in it, please check {explain_udf}")
            diffs = DeepDiff(explain_udf['results'][0]['plans'][0]['plan']['~children'][0]['spans'], expected_spans['results'][0]['plan']['~children'][0]['spans'], ignore_order=True)
            if diffs:
                values = diffs['values_changed'].keys()
                for value in values:
                    if not(diffs['values_changed'][value]['new_value'] == '"old hotel"' and diffs['values_changed'][value]['old_value'] == '`nameval`'):
                        self.assertTrue(False, diffs)
            self.assertTrue('IndexScan3' in str(explain_udf), f"The wrong scan is being used in the explain, please check {explain_udf}")
            self.assertTrue('idx1' in str(explain_udf), f"The wrong index is being used in the explain, please check {explain_udf}")
            self.assertTrue(self.collections[0] in str(explain_udf), f"The wrong keyspace is being used in the explain, please check {explain_udf}")
        except Exception as e:
            self.log.error(str(e))
            self.fail()
        finally:
            try:
                self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

    def test_explain_inline_no_query(self):
        try:
            self.run_cbq_query("CREATE FUNCTION celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9")
            explain_udf = self.run_cbq_query("explain function celsius".format(self.scope,self.collections[0]))
            self.assertTrue('plans' not in explain_udf['results'][0].keys(), f"The explain should not have a query plan in it, please check {explain_udf}")
            self.assertEqual(explain_udf['results'],[{'function': 'default:celsius'}], f"The explain plan is incorrect please check it {explain_udf}")
        except Exception as e:
            self.log.error(str(e))
            self.fail()
        finally:
            try:
                self.run_cbq_query("DROP FUNCTION celsius")
            except Exception as e:
                self.log.error(str(e))

    def test_explain_js_embedded(self):
        function_name = 'execute_udf'
        functions = f'function {function_name}(job, year, month) {{\
            var query = SELECT name FROM _default WHERE job_title = $job AND join_yr = $year AND join_mo = $month ORDER by name LIMIT 3;\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library("library", functions, [function_name])
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}(j, y, m) LANGUAGE JAVASCRIPT AS "{function_name}" AT "library"', query_context='default:default._default')

        # call functions inside of a select statement w/query contexts passed
        # Execute function w/ correct query context
        explain_udf = self.run_cbq_query(f"explain function {function_name}", query_context='default:default._default')
        self.assertTrue('plans' in explain_udf['results'][0].keys(),
                        f"The explain should have a query plan in it, please check {explain_udf}")
        self.assertTrue('((((`_default`.`job_title`) = $job) and ((`_default`.`join_yr`) = $year)) and ((`_default`.`join_mo`) = $month))' in str(explain_udf),
                        f"The wrong filter condition is being used in the explain, please check {explain_udf}")

    def test_explain_js_dynamic(self):
        function_name = 'execute_udf'
        functions = f'function {function_name}(job, year, month) {{\
            var query = N1QL("SELECT name FROM _default WHERE join_yr = 2011 and join_mo = 10 ORDER by name LIMIT 3");\
            var query2 = N1QL("SELECT name FROM _default WHERE join_yr = 2011 and join_mo = 10 ORDER by name LIMIT 3");\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library("library", functions, [function_name])
        self.run_cbq_query(
            f'CREATE OR REPLACE FUNCTION {function_name}(j, y, m) LANGUAGE JAVASCRIPT AS "{function_name}" AT "library"',
            query_context='default:default._default')

        # call functions inside of a select statement w/query contexts passed
        # Execute function w/ correct query context
        explain_udf = self.run_cbq_query(f"explain function {function_name}", query_context='default:default._default')
        self.assertEqual(explain_udf['results'][0]['line_numbers'], [1, 1],
                        f"The amount of lines are wrong, there should be two dynamic n1ql statements in this function, please check {explain_udf}")

    def test_explain_js_mixed(self):
        function_name = 'execute_udf'
        functions = f'function {function_name}(job, year, month) {{\
            var query = N1QL("SELECT name FROM _default WHERE join_yr = 2011 and join_mo = 10 ORDER by name LIMIT 3");\
            var query2 = SELECT name FROM _default WHERE job_title = $job AND join_yr = $year AND join_mo = $month ORDER by name LIMIT 3;\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library("library", functions, [function_name])
        self.run_cbq_query(
            f'CREATE OR REPLACE FUNCTION {function_name}(j, y, m) LANGUAGE JAVASCRIPT AS "{function_name}" AT "library"',
            query_context='default:default._default')

        # call functions inside of a select statement w/query contexts passed
        # Execute function w/ correct query context
        explain_udf = self.run_cbq_query(f"explain function {function_name}", query_context='default:default._default')
        self.assertEqual(explain_udf['results'][0]['line_numbers'], [1],
                        f"The amount of lines are wrong, there should be two dynamic n1ql statements in this function, please check {explain_udf}")
        self.assertTrue('plans' in explain_udf['results'][0].keys(),
                        f"The explain should have a query plan in it, please check {explain_udf}")
        self.assertTrue('((((`_default`.`job_title`) = $job) and ((`_default`.`join_yr`) = $year)) and ((`_default`.`join_mo`) = $month))' in str(explain_udf),
                        f"The wrong filter condition is being used in the explain, please check {explain_udf}")

    def test_explain_js_nested_inline(self):
        self.run_cbq_query(
            "CREATE OR REPLACE FUNCTION func1(nameval) {{ (select * from default:default.{0}.{1} where name = nameval) }}".format(
                self.scope, self.collections[0]))
        function_name = 'execute_udf'
        functions = f'function {function_name}(job, year, month) {{\
            var query = execute function func1("old hotel");\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library("library", functions, [function_name])
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}(j, y, m) LANGUAGE JAVASCRIPT AS "{function_name}" AT "library"', query_context='default:default._default')

        # call functions inside of a select statement w/query contexts passed
        # Execute function w/ correct query context
        explain_udf = self.run_cbq_query(f"explain function {function_name}", query_context='default:default._default')
        self.assertTrue('plans' in explain_udf['results'][0].keys(),
                        f"The explain should have a query plan in it, please check {explain_udf}")
        self.assertTrue('execute function func1("old hotel")' in str(explain_udf),
                        f"The statement is being used in the explain, please check {explain_udf}")

    def test_profiling_js_udf_queries(self):
        string_functions = 'function concater(a,b) { var text = ""; var x; for (x in a) {if (x = b) { return x; }} return "n"; } function comparator(a, b) {if (a > b) { return "old hotel"; } else { return "new hotel" }}'
        function_names2 = ["concater","comparator"]
        created2 = self.create_library("strings",string_functions,function_names2)
        self.run_cbq_query(query='CREATE OR REPLACE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "comparator" AT "strings"')
        self.run_cbq_query("CREATE OR REPLACE FUNCTION func2(degrees) LANGUAGE INLINE AS (degrees - 32) ")
        self.run_cbq_query(query='CREATE OR REPLACE FUNCTION func3(a,b) LANGUAGE JAVASCRIPT AS "concater" AT "strings"')

        function_name = 'execute_udf'
        functions = f'function {function_name}() {{\
            var query = SELECT name FROM default:default.test.test1 LET maximum_no = func2(36) WHERE ANY v in test1.numbers SATISFIES v = maximum_no END GROUP BY name LETTING letter = func3("old hotel","o") HAVING name > letter;\
            var query2 = INSERT INTO default (KEY, VALUE) VALUES ("k004", {{"col1": 10 }});\
            var query3 = SELECT * from default limit 100;\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library("library", functions, [function_name])
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "library"')

        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()',query_params={"profile":"timings"})
        self.assertTrue("~udfStatements" in str(function_result), f"We should see a profile entry for n1ql statements executed inside the udf, please check {function_result}")
        self.assertTrue('SELECT name FROM default:default.test.test1 LET maximum_no = func2(36) WHERE ANY v in test1.numbers SATISFIES v = maximum_no END GROUP BY name LETTING letter = func3("old hotel","o") HAVING name > letter' in str(function_result), f"One of the queries is not in the profile, please check {function_result}" )
        self.assertTrue('INSERT INTO default (KEY, VALUE) VALUES ("k004", {"col1": 10 })' in str(function_result), f"One of the queries is not in the profile, please check {function_result}" )
        self.assertTrue('SELECT * from default limit 100' in str(function_result), f"One of the queries is not in the profile, please check {function_result}" )

    def create_n1ql_function(self, function_name, query):
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            var query = {query};\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            query.close();\
            return acc;}}'
        self.create_library(self.library_name, functions, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')

    def test_dml(self):
        self.run_cbq_query("CREATE INDEX adv_job_title IF NOT EXISTS ON `default`(`job_title`)")
        self.run_cbq_query("CREATE COLLECTION default._default.tmp IF NOT EXISTS")
        function_name = f"{self.statement}_default"
        query = self.dmls[self.statement]
        self.create_n1ql_function(function_name, query)
        # Run query in transaction for comparison
        results = self.run_cbq_query(query='BEGIN WORK')
        txid = results['results'][0]['txid']
        query_result = self.run_cbq_query(query, txnid=txid)
        self.run_cbq_query('ROLLBACK', txnid=txid)
        # Execute function and check
        if self.test_sideeffect:
            try:
                function_result = self.run_cbq_query(f'select {function_name}() from default')
                self.fail("Query should have CBQ error'd")
            except CBQError as ex:
                error = self.process_CBQE(ex)
                self.assertEqual(error['code'], 5010)
                self.assertTrue('not a readonly request' in str(error))
        else:
            function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
            self.assertEqual(function_result['results'][0], query_result['results'])

    def test_explain(self):
        function_name = "explain_default"
        query = 'EXPLAIN SELECT * FROM default WHERE join_yr = 2011 AND join_mo = 10 AND lower(job_title) = "engineer";'
        self.create_n1ql_function(function_name, query)
        # Run query for comparison
        query_result = self.run_cbq_query(query)
        # Execute function and check
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
        diffs = DeepDiff(function_result['results'][0], query_result['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    def test_prepare(self):
        self.run_cbq_query('DELETE FROM system:prepareds WHERE name = "engineer_count(default:)"')
        function_name = 'prepare_default'
        query = 'PREPARE engineer_count as SELECT COUNT(*) as count_engineer FROM default WHERE job_title = "Engineer"'
        self.create_n1ql_function(function_name, query)
        # Execute function and check
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
        self.assertEqual(function_result['results'][0][0]['text'], f'{query};')
        # Execute prepared statement outside of UDF
        query_result = self.run_cbq_query('EXECUTE engineer_count', query_context='default:')
        self.assertEqual(query_result['results'], [{'count_engineer': 672}])

    def test_execute_prepared(self):
        self.run_cbq_query('DELETE FROM system:prepareds WHERE name LIKE "engineer%"')
        function_name = 'execute_prepare_default'
        query = 'EXECUTE engineer_count'
        self.create_n1ql_function(function_name, query)
        # Prepare statement
        self.run_cbq_query('PREPARE engineer_count as SELECT COUNT(*) as count_engineer FROM default WHERE job_title = "Engineer"', query_context="default:")
        # Execute function and check
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
        self.assertEqual(function_result['results'][0], [{'count_engineer': 672}])

    def test_execute_prepared_using(self):
        self.run_cbq_query('DELETE FROM system:prepareds WHERE name LIKE "engineer%"')
        function_name = 'execute_prepare_using_default'
        query = 'EXECUTE engineer_count using ["Engineer"]'
        self.create_n1ql_function(function_name, query)
        # Prepare statement
        self.run_cbq_query('PREPARE engineer_count as SELECT COUNT(*) as count_engineer FROM default WHERE job_title = $1', query_context="default:")
        # Execute function and check
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
        self.assertEqual(function_result['results'][0], [{'count_engineer': 672}])

    def test_infer(self):
        function_name = "infer_default"
        query = 'INFER default'
        self.create_n1ql_function(function_name, query)
        # Run query for comparison
        query_result = self.run_cbq_query(query)
        # Execute function and check
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
        self.assertEqual(function_result['results'][0][0][0]['#docs'], query_result['results'][0][0]['#docs'])

    def test_advise(self):
        function_name = "advise_default"
        query = 'ADVISE SELECT * FROM default WHERE lower(job_title) = "engineer"'
        self.create_n1ql_function(function_name, query)
        # Run query for comparison
        query_result = self.run_cbq_query(query)
        # Execute function and check
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
        function_recommended_index = function_result['results'][0][0]['advice']['adviseinfo']['recommended_indexes']['indexes'][0]['index_statement']
        query_recommended_index = query_result['results'][0]['advice']['adviseinfo']['recommended_indexes']['indexes'][0]['index_statement']
        self.assertEqual(function_recommended_index, query_recommended_index)

    def test_ddl(self):
        function_name = f"{self.statement}_default"
        query = self.ddls[self.statement]['query']
        self.create_n1ql_function(function_name, query)
        # Run pre-req prior to DDL
        pre_query = self.ddls[self.statement]['pre']
        self.run_cbq_query(pre_query)
        self.sleep(5)
        # Execute function
        if self.test_sideeffect:
            try:
                function_result = self.run_cbq_query(f'select {function_name}() from default')
                self.fail("Query should have CBQ error'd")
            except CBQError as ex:
                error = self.process_CBQE(ex)
                self.assertEqual(error['code'], 5010)
                self.assertTrue('not a readonly request' in str(error))
        else:
            function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
            if self.statement == "create_index":
                self.assertTrue(function_result['results'][0][0]['state'] == 'online', f"Index should be online please check {function_result}")
                self.assertTrue(function_result['results'][0][0]['name'] == "udf_ix", f"Index name is incorrect please check {function_result}")
            else:
                self.assertEqual(function_result['results'], self.ddls[self.statement]['function_expected'])
            self.sleep(30)
            # Run post check
            post_query = self.ddls[self.statement]['post']
            post_expected = self.ddls[self.statement]['post_expected']
            post_actual = self.run_cbq_query(post_query)
            if self.statement == "update_statistics" or self.statement == "analyze":
                self.assertTrue(post_expected in post_actual['results'], f"We expect the histogram key to be in the actual results, please check them {post_actual}")
            else:
                self.assertEqual(post_expected, post_actual['results'])

    def test_curl(self):
        url = "https://jsonplaceholder.typicode.com/todos"
        self.rest.create_whitelist(self.master, {"all_access": True})
        function_name = 'curl_default'
        query = f'SELECT CURL("{url}")'
        self.create_n1ql_function(function_name, query)
        # Get expected from curl
        response = requests.get(url)
        expected_curl = response.json()
        # Execute function
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
        actual_result = function_result['results'][0][0]['$1']
        self.assertEqual(actual_result, expected_curl)

    def test_flush_collection(self):
        function_name = 'flush_default'
        query = f'{self.statement} COLLECTION default'
        self.create_n1ql_function(function_name, query)
        # Execute function
        try:
            function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
            self.log.info(function_result)
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 10109)
            self.assertTrue('Requested resource not found' in str(error))

    def test_param_function_param(self):
        functions = 'function add(a, b) { return a + b; }'
        self.create_library('math', functions, 'add')
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION add(a, b) LANGUAGE JAVASCRIPT AS "add" AT "math"')

        function_name = 'param_function_param_default'
        functions = f'function {function_name}() {{\
            var number = 10;\
            var query = EXECUTE FUNCTION add(5, $number);\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library(self.library_name, functions, [function_name])
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}(j, y, m) LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        # Execute function
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}("Engineer", 2011, 10)')
        expected_result = [15]
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, expected_result)

    def test_parameter_from_function(self):
        function_name = 'param_from_function_default'
        functions = f'function {function_name}(job, year, month) {{\
            var query = SELECT name FROM default WHERE job_title = $job AND join_yr = $year AND join_mo = $month ORDER by name LIMIT 3;\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library(self.library_name, functions, [function_name])
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}(j, y, m) LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        # Execute function
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}("Engineer", 2011, 10)')
        expected_result = [{'name': 'employee-1'}, {'name': 'employee-10'}, {'name': 'employee-11'}]
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, expected_result)

    def test_parameter_from_var(self):
        function_name = 'param_from_var_default'
        functions = f'function {function_name}() {{\
            var job = "Engineer";\
            var year = 2011;\
            var month = 10;\
            var query = SELECT name FROM default WHERE job_title = $job AND join_yr = $year AND join_mo = $month ORDER by name LIMIT 3;\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library(self.library_name, functions, [function_name])
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        # Execute function
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
        expected_result = [{'name': 'employee-1'}, {'name': 'employee-10'}, {'name': 'employee-11'}]
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, expected_result)

    def test_execute_prepared_with_param(self):
        self.run_cbq_query('DELETE FROM system:prepareds WHERE name LIKE "engineer%"')
        function_name = 'execute_prepare_default'
        if self.params == 'named':
            param_val = '{"job": "Engineer"}'
            param_var = "$job"
        elif self.params == 'positional':
            param_val = '["Engineer"]'
            param_var = "$1"
        functions = f'function {function_name}() {{\
            var params = {param_val};\
            var query = N1QL("EXECUTE engineer_count", params);\
            var acc = [];\
            for (const row of query) {{ acc.push(row); }}\
            return acc;\
        }}'
        self.create_library(self.library_name, functions, [function_name])
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        # Prepare statement with named or postional parameter
        self.run_cbq_query(f'PREPARE engineer_count as SELECT COUNT(*) as count_engineer FROM default WHERE job_title = {param_var}', query_context="default:")
        # Execute function and check
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
        self.assertEqual(function_result['results'][0], [{'count_engineer': 672}])

    def test_rbac(self):
        self.run_cbq_query("CREATE COLLECTION default._default.tmp IF NOT EXISTS")
        self.run_cbq_query("CREATE INDEX adv_job_title IF NOT EXISTS ON `default`(`job_title`)")
        # Create user with execute external function role only
        self.users = [{"id": "jackDoe", "name": "Jack Downing", "password": "password1"}]
        self.create_users()
        role = 'query_execute_global_external_functions'
        user_id = self.users[0]['id']
        user_pwd = self.users[0]['password']
        self.run_cbq_query(query=f"GRANT {role} to {user_id}")
        # Create function
        function_name = 'rbac_default'
        if self.statement in self.dmls:
            query = self.dmls[self.statement]
        elif self.statement in self.ddls:
            query = self.ddls[self.statement]['query']
            pre_query = self.ddls[self.statement]['pre']
            self.run_cbq_query(pre_query)
        else:
            self.log.error(f'Unknown statement: {self.statement}')
        self.create_n1ql_function(function_name, query)
        # Execute function as user
        try:
            self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()', username=user_id, password=user_pwd)
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 10109)
            self.assertTrue('User does not have credentials to run' in str(error) or 'datastore.couchbase.insufficient_credentials' in str(error))

    def test_explain_rbac(self):
        self.run_cbq_query("CREATE COLLECTION default._default.tmp IF NOT EXISTS")
        self.run_cbq_query("CREATE INDEX adv_job_title IF NOT EXISTS ON `default`(`job_title`)")
        # Create user with execute external function role only
        self.users = [{"id": "jackDoe", "name": "Jack Downing", "password": "password1"}]
        self.create_users()
        role = 'query_execute_global_external_functions'
        user_id = self.users[0]['id']
        user_pwd = self.users[0]['password']
        self.run_cbq_query(query=f"GRANT {role} to {user_id}")
        # Create function
        function_name = 'rbac_default'
        if self.statement in self.dmls:
            query = self.dmls[self.statement]
        elif self.statement in self.ddls:
            query = self.ddls[self.statement]['query']
            pre_query = self.ddls[self.statement]['pre']
            self.run_cbq_query(pre_query)
        else:
            self.log.error(f'Unknown statement: {self.statement}')
        self.create_n1ql_function(function_name, query)
        # Execute function as user
        try:
            self.run_cbq_query(f'EXPLAIN FUNCTION {function_name}', username=user_id, password=user_pwd)
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 13014)
            self.assertTrue('User does not have credentials to run' in str(error))

    def test_circumvent_udf_rbac(self):
        # Create user with execute external function role only
        self.users = [{"id": "scope1", "name": "user1", "password": "password1"}]
        self.create_users()
        user_roles = ['query_manage_external_functions','query_execute_external_functions'
            ,'query_manage_functions','query_execute_functions']

        user_id = self.users[0]['id']
        user_pwd = self.users[0]['password']
        for role in user_roles:
            self.run_cbq_query(query=f"GRANT {role} ON default:default.test to {user_id}")

        if self.inline_func:
            # Create an inline function on scope that user does not have perms on
            self.run_cbq_query(
                "CREATE OR REPLACE FUNCTION default:default._default.celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9")
        else:
            # Create a js function on the scope that the user does not have perms on
            functions = 'function adder(a, b) { return a + b; } function multiplier(a, b) { return a * b; }'
            function_names = ["adder", "multiplier"]
            created = self.create_library("math2", functions, function_names)
            self.run_cbq_query(query='CREATE OR REPLACE FUNCTION default:default._default.func1(a,b) LANGUAGE JAVASCRIPT AS "adder" AT "math2"')

        # Create function
        function_name = 'rbac_default'
        if self.inline_func:
            query = "EXECUTE FUNCTION default:default._default.celsius(10)"
        else:
            query = "EXECUTE FUNCTION default:default._default.func1(1,4)"
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            var query = {query};\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library(self.library_name, functions, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION default:default.test.{function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')        # Execute function as user
        try:
            self.run_cbq_query(f'EXECUTE FUNCTION default:default.test.{function_name}()', username=user_id, password=user_pwd)
            self.fail("Query should have CBQ error'd")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 10109)
            self.assertTrue('User does not have credentials to run' in str(error) or 'datastore.couchbase.insufficient_credentials' in str(error), f"Error is not what we expected {str(ex)}")

    def test_datetime_value(self):
        function_name = 'datetime_value'
        functions = f'function {function_name}() {{\
            const date1 = new Date(Date.UTC(2018, 11, 24, 10, 33, 30, 3));\
            const date2 = new Date(Date.UTC(2018, 11, 24, 10, 33, 30));\
            const date3 = new Date(Date.UTC(2018, 11, 24, 10, 33));\
            const date4 = new Date(Date.UTC(2018, 11, 24, 10));\
            const date5 = new Date(2018, 11, 24);\
            const date6 = new Date(2018, 11);\
            const date7 = new Date(100000000000);\
            const date8 = new Date("December 24, 2018 10:33:30");\
            var query = SELECT $date1 as date_2018_12_24_10_33_30_3,\
                                $date2 as date_2018_12_24_10_33_30,\
                                $date3 as date_2018_12_24_10_33,\
                                $date4 as date_2018_12_24_10,\
                                $date5 as date_2018_12_24,\
                                $date6 as date_2018_12,\
                                $date7 as date_millis,\
                                $date8 as date_str;\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;\
            }}'
        self.create_library(self.library_name, functions, [function_name])
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        # Execute function
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
        expected_result = [ {
            "date_2018_12": "2018-12-01T08:00:00.000Z",
            "date_2018_12_24": "2018-12-24T08:00:00.000Z",
            "date_2018_12_24_10": "2018-12-24T10:00:00.000Z",
            "date_2018_12_24_10_33": "2018-12-24T10:33:00.000Z",
            "date_2018_12_24_10_33_30": "2018-12-24T10:33:30.000Z",
            "date_2018_12_24_10_33_30_3": "2018-12-24T10:33:30.003Z",
            "date_millis": "1973-03-03T09:46:40.000Z",
            "date_str": "2018-12-24T18:33:30.000Z"
            }
        ]
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, expected_result)

    def test_datetime_function(self):
        function_name = 'datetime_function'
        functions = f'function {function_name}() {{\
            const date1 = new Date(Date.UTC(2018, 11, 24, 10, 33, 30, 3));\
            date_short = "2021-05-15";\
            date_time = "01:15:45";\
            var query = SELECT DATE_FORMAT_STR($date1, "1111-11-11") as full_to_short,\
                     DATE_FORMAT_STR($date_short, "1111-11-11T00:00:00+00:00") as short_to_full,\
                     DATE_FORMAT_STR($date_time, "1111-11-11T01:01:01Z") as time_to_full,\
                     DATE_PART_STR($date1, "day") as day_24,\
                     DATE_PART_STR($date1, "millisecond") as millisecond_3,\
                     DATE_PART_STR($date1, "second") as second_30,\
                     DATE_PART_STR($date1, "minute") as minute_33,\
                     DATE_PART_STR($date1, "hour") as hour_10,\
                     DATE_PART_STR($date1, "month") as month_12,\
                     DATE_PART_STR($date1, "week") as week_52,\
                     DATE_PART_STR($date1, "year") as year_2018;\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;\
            }}'
        self.create_library(self.library_name, functions, [function_name])
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        # Execute function
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
        expected_result = [ {
            'day_24': 24,
            'full_to_short': '2018-12-24',
            'hour_10': 10,
            'millisecond_3': 3,
            'minute_33': 33,
            'month_12': 12,
            'second_30': 30,
            'short_to_full': '2021-05-15T00:00:00-07:00',
            'time_to_full': '0000-01-01T01:15:45-07',
            'week_52': 52,
            'year_2018': 2018
            }
        ]

    def test_udf_args(self):
        function_name = 'param_from_var_default'
        functions = f'function {function_name}(job, year, month) {{\
            var job = job;\
            var year = year;\
            var month = month;\
            var query = SELECT name FROM default WHERE job_title = $job AND join_yr = $year AND join_mo = $month ORDER by name LIMIT 3;\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library(self.library_name, functions, [function_name])
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}(...) LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')

        # Execute function
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}("Engineer",2011,10)')
        expected_result = [{'name': 'employee-1'}, {'name': 'employee-10'}, {'name': 'employee-11'}]
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, expected_result)

        # Execute function w/ extra params, should get ignored
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}("Engineer", 2011, 10, "random", "extra")')
        expected_result = [{'name': 'employee-1'}, {'name': 'employee-10'}, {'name': 'employee-11'}]
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, expected_result)

        try:
            # Execute function w/ not enough params should pass none to the last param required
            self.run_cbq_query(f'EXECUTE FUNCTION {function_name}("Engineer", 2011)')
            self.fail("Query should have CBQ error'd")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 10109)
            self.assertTrue('Invalid data type for named parameters' in str(error), f"Error is not what we expected {str(ex)}")

    def test_looped_calls(self):
        # Create a js function that executes the js function that calls it
        function_name = 'call_func2'
        query = "EXECUTE FUNCTION call_func1()"
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            var query = {query};\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library("n1ql", functions, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "n1ql"')
        self.create_n1ql_function(function_name, query)

        # Create function that executes a function that will loop back and call it
        function_name = 'call_func1'
        query = "EXECUTE FUNCTION call_func2()"
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            var query = {query};\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library("n1ql2", functions, function_names)
        self.run_cbq_query(
            f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "n1ql2"')
        # Execute function as user
        try:
            self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
            self.fail("Query should have CBQ error'd")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 10109)
            self.assertTrue('nested javascript calls' in str(error), f"Error is not what we expected {str(ex)}")

    def test_nested_loop_timeout(self):
        # create a function that will just sleep so that we can hit the timeout instead of the nested loop limit
        function_name = 'sleep'
        function_names = [function_name]
        function_sleep = 'function sleep(delay) { var start = new Date().getTime(); while (new Date().getTime() < start + delay); return delay; }'
        self.create_library("sleep", function_sleep, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}(t) LANGUAGE JAVASCRIPT AS "{function_name}" AT "sleep"')
        sleep_query = f"EXECUTE FUNCTION {function_name}(30000)"

        # Create a js function that executes the js function that calls it
        function_name = 'call_func2'
        query = "EXECUTE FUNCTION call_func1()"
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            var sleep_query = {sleep_query};\
            var query = {query};\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library("n1ql", functions, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "n1ql"')
        self.create_n1ql_function(function_name, query)

        # Create function that executes a function that will loop back and call it
        function_name = 'call_func1'
        query = "EXECUTE FUNCTION call_func2()"
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            var sleep_query = {sleep_query};\
            var query = {query};\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library("n1ql2", functions, function_names)
        self.run_cbq_query(
            f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "n1ql2"')
        # Execute function as user
        try:
            self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()', query_params={'timeout': "2m"})
            self.fail("Query should have CBQ error'd")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 1080)
            self.assertTrue('Timeout 2m0s exceeded' in str(error), f"Error is not what we expected {str(ex)}")

    def test_nested_udf_inline(self):
        # Create an inline function on scope that user does not have perms on
        self.run_cbq_query(
            "CREATE OR REPLACE FUNCTION default:default._default.celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9")

        # Create function
        function_name = 'rbac_default'
        query = "EXECUTE FUNCTION default:default._default.celsius(10)"
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            var query = {query};\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library(self.library_name, functions, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION default:default.test.{function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        # Execute function as user
        results = self.run_cbq_query(f'EXECUTE FUNCTION default:default.test.{function_name}()')
        self.assertEqual(results['results'], [[-12.222222222222221]], f"results mismatch {results}")

    def test_nested_udf_recursion(self):
        # Create function that executes a function that will loop back and call it
        function_name = 'call_func1'
        query = "EXECUTE FUNCTION call_func1($x)"
        function_names = [function_name]
        functions = f'function {function_name}(x) {{\
            var levels = 1;\
            if (x > 5){{ x = x - 1;\
            levels = levels + 1; \
            var query = {query};}}\
            else{{ return levels; }}}}'
        self.create_library("n1ql2", functions, function_names)
        self.run_cbq_query(
            f'CREATE OR REPLACE FUNCTION {function_name}(x) LANGUAGE JAVASCRIPT AS "{function_name}" AT "n1ql2"')
        # Execute function as user
        results = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}(6)')

    def test_query_context(self):
        # Create an inline function on scope that user does not have perms on
        self.run_cbq_query(
            "CREATE OR REPLACE FUNCTION default:default.test.celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9")

        # Create function
        function_name = 'execute_udf'
        query = "EXECUTE FUNCTION default:default._default.celsius(10)"
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            var query = {query};\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library(self.library_name, functions, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"', query_context="default:default.test")

        function_name = 'execute_udf'
        functions = f'function {function_name}(job, year, month) {{\
            var query = SELECT name FROM _default WHERE job_title = $job AND join_yr = $year AND join_mo = $month ORDER by name LIMIT 3;\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library("library", functions, [function_name])
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}(j, y, m) LANGUAGE JAVASCRIPT AS "{function_name}" AT "library"', query_context='default:default._default')

        if self.use_select:
            # call functions inside of a select statement w/query contexts passed
            # Execute function w/ correct query context
            function_result = self.run_cbq_query(f'select {function_name}("Engineer", 2011, 10)',
                                                 query_context='default:default._default')
            expected_result = {'$1': [{'name': 'employee-1'}, {'name': 'employee-10'}, {'name': 'employee-11'}]}
            actual_result = function_result['results'][0]
            self.assertEqual(actual_result, expected_result, f"The results are wrong, actual_result : {actual_result} , expected: {expected_result}")

            # Execute function relative path should be picked up from the context of the udf
            function_result = self.run_cbq_query(
                f'select default:default._default.{function_name}("Engineer", 2011, 10)')
            expected_result = {'$1': [{'name': 'employee-1'}, {'name': 'employee-10'}, {'name': 'employee-11'}]}
            actual_result = function_result['results'][0]
            self.assertEqual(actual_result, expected_result, f"The results are wrong, actual_result : {actual_result} , expected: {expected_result}")
            try:
                # Execute function w/ incorrect query context, incorrect function should be used
                function_result = self.run_cbq_query(f'select {function_name}("Engineer", 2011, 10)',
                                                     query_context='default:default.test')
                self.fail("Query should have CBQ error'd")
            except CBQError as ex:
                error = self.process_CBQE(ex)
                self.assertTrue("10104" in str(error), f'Error code is wrong please check the error: {error}')
                self.assertTrue('Incorrect number of arguments supplied' in str(error),
                                f"Error is not what we expected {str(ex)}")
        else:
            # Execute function w/ correct query context
            function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}("Engineer", 2011, 10)', query_context='default:default._default')
            expected_result = [{'name': 'employee-1'}, {'name': 'employee-10'}, {'name': 'employee-11'}]
            actual_result = function_result['results'][0]
            self.assertEqual(actual_result, expected_result, f"The results are wrong, actual_result : {actual_result} , expected: {expected_result}")

            # Execute function relative path should be picked up from the context of the udf
            function_result = self.run_cbq_query(f'EXECUTE FUNCTION default:default._default.{function_name}("Engineer", 2011, 10)')
            expected_result = [{'name': 'employee-1'}, {'name': 'employee-10'}, {'name': 'employee-11'}]
            actual_result = function_result['results'][0]
            self.assertEqual(actual_result, expected_result, f"The results are wrong, actual_result : {actual_result} , expected: {expected_result}")
            try:
                # Execute function w/ incorrect query context, incorrect function should be used
                function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}("Engineer", 2011, 10)', query_context='default:default.test')
                self.fail("Query should have CBQ error'd")
            except CBQError as ex:
                error = self.process_CBQE(ex)
                self.assertTrue("10104" in str(error), f'Error code is wrong please check the error: {error}')
                self.assertTrue('Incorrect number of arguments supplied' in str(error), f"Error is not what we expected {str(ex)}")

    def test_letting_having_groupby(self):
        string_functions = 'function concater(a,b) { var text = ""; var x; for (x in a) {if (x = b) { return x; }} return "n"; } function comparator(a, b) {if (a > b) { return "old hotel"; } else { return "new hotel" }}'
        function_names2 = ["concater","comparator"]
        created2 = self.create_library("strings",string_functions,function_names2)
        self.run_cbq_query(query='CREATE OR REPLACE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "comparator" AT "strings"')
        self.run_cbq_query("CREATE OR REPLACE FUNCTION func2(degrees) LANGUAGE INLINE AS (degrees - 32) ")
        self.run_cbq_query(query='CREATE OR REPLACE FUNCTION func3(a,b) LANGUAGE JAVASCRIPT AS "concater" AT "strings"')

        function_name = 'execute_udf'
        functions = f'function {function_name}() {{\
            var query = SELECT name FROM default:default.test.test1 LET maximum_no = func2(36) WHERE ANY v in test1.numbers SATISFIES v = maximum_no END GROUP BY name LETTING letter = func3("old hotel","o") HAVING name > letter;\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library("library", functions, [function_name])
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "library"')

        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
        self.assertEqual(function_result['results'], [[{'name': 'old hotel'}]])

    def test_cancel_n1ql_udf(self):
        self.run_cbq_query('CREATE INDEX ix1 ON default(c1)')
        self.run_cbq_query('UPSERT INTO default  (KEY _k, VALUE _v) '
                           'SELECT  "k0"||TO_STR(d) AS _k , {"c1":d, "c2":2*d, "c3":3*d} AS _v '
                           'FROM ARRAY_RANGE(1,1000) AS d')
        function_name = 'execute_udf'
        functions = f'function {function_name}() {{\
            var query = WITH aa AS (WITH a1 AS (SELECT RAW COUNT(l.c2) \
                    FROM default AS l JOIN default AS r ON l.c1 < r.c1 \
                    JOIN default r1 ON r.c1 < r1.c1 WHERE l.c1 >= 0) SELECT a1) SELECT aa;\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library("library", functions, [function_name])
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "library"')

        try:
            self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()', query_params={'timeout': "5s"})
        except CBQError as e:
            self.assertTrue('Timeout 5s exceeded' in str(e))
        self.sleep(1)
        num_requests = self.get_num_requests("default:ix1",self.nodes_init)
        if num_requests == 0:
            self.fail("Requests should be non zero as a query has run against this index")
        self.log.info(f"Number of requests before sleep: {num_requests}")
        self.sleep(10)
        new_num_requests = self.get_num_requests("default:ix1",self.nodes_init)
        self.log.info(f"Number of requests after sleep: {new_num_requests}")
        self.assertEqual(num_requests, new_num_requests,
                         "The number of requests should be the same, it is not, please check")

    def test_cancel_udf_nested_inline(self):
        self.run_cbq_query('CREATE INDEX ix10 ON default(c1)')
        self.run_cbq_query('UPSERT INTO default  (KEY _k, VALUE _v) '
                           'SELECT  "k0"||TO_STR(d) AS _k , {"c1":d, "c2":2*d, "c3":3*d} AS _v '
                           'FROM ARRAY_RANGE(1,1000) AS d')
        function_name = 'execute_udf'
        functions = f'function {function_name}() {{\
            var query = WITH aa AS (WITH a1 AS (SELECT RAW COUNT(l.c2) \
                    FROM default AS l JOIN default AS r ON l.c1 < r.c1 \
                    JOIN default r1 ON r.c1 < r1.c1 WHERE l.c1 >= 0) SELECT a1) SELECT aa;\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library("library", functions, [function_name])
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "library"')
        # Create an inline function that executes the js udf that runs a n1ql statement
        self.run_cbq_query(
            f"CREATE OR REPLACE FUNCTION udf_nested() {{(SELECT {function_name}())}}")

        try:
            self.run_cbq_query(f'EXECUTE FUNCTION udf_nested()', query_params={'timeout': "10s"})
        except CBQError as e:
            self.assertTrue('Timeout 10s exceeded' in str(e))
        self.sleep(1)
        num_requests = self.get_num_requests("default:ix10",self.nodes_init)
        if num_requests == 0:
            self.fail("Requests should be non zero as a query has run against this index")
        self.log.info(f"Number of requests before sleep: {num_requests}")
        self.sleep(10)
        new_num_requests = self.get_num_requests("default:ix10",self.nodes_init)
        self.log.info(f"Number of requests after sleep: {new_num_requests}")
        self.assertEqual(num_requests, new_num_requests,
                         "The number of requests should be the same, it is not, please check")

    def test_global_query_context(self):
        self.run_cbq_query(
            "CREATE OR REPLACE FUNCTION default:default.test.celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9")

        function_name = 'execute_udf'
        functions = f'function {function_name}(job, year, month) {{\
            var query = SELECT name FROM _default WHERE job_title = $job AND join_yr = $year AND join_mo = $month ORDER by name LIMIT 3;\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library("library", functions, [function_name])
        # Create a global udf so that the relative path call fails, and create the proper scope udf
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}(j, y, m) LANGUAGE JAVASCRIPT AS "{function_name}" AT "library"', query_context='default:default._default')
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}(j, y, m) LANGUAGE JAVASCRIPT AS "{function_name}" AT "library"')

        # Execute function w/ correct query context
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}("Engineer", 2011, 10)', query_context='default:default._default')
        expected_result = [{'name': 'employee-1'}, {'name': 'employee-10'}, {'name': 'employee-11'}]
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, expected_result)

        try:
            # Execute global function, relative path query should fail
            function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}("Engineer", 2011, 10)')
            self.fail("Query should have CBQ error'd")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 10109)
            self.assertTrue('No bucket named _default' in str(error), f"Error is not what we expected {str(ex)}")

    def test_query_context_cross_scope(self):
        # Create an inline function on scope that user does not have perms on
        self.run_cbq_query(
            "CREATE OR REPLACE FUNCTION default:default.test.celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9")

        # Create function
        function_name = 'execute_udf'
        query = "EXECUTE FUNCTION default:default.test.celsius(10)"
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            var query = {query};\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library(self.library_name, functions, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"', query_context="default:default.test")

        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()', query_context='default:default.test')
        self.assertEqual(function_result['results'], [[-12.222222222222221]], f"results mismatch {function_result}")

    def test_query_context_absolute_path(self):
        # Create a scope function, call that scope function w/absolute path and pass in an incorrect query context, absolute path should be used
        self.run_cbq_query(
            "CREATE OR REPLACE FUNCTION default:default.test.celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9")

        # Create function
        function_name = 'execute_udf'
        query = "EXECUTE FUNCTION celsius(10)"
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            var query = {query};\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library(self.library_name, functions, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"', query_context="default:default.test")
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}(a,b) LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"', query_context="default:default._default")


        function_result = self.run_cbq_query(f'EXECUTE FUNCTION default:default.test.{function_name}()', query_context='default:default._default')
        self.assertEqual(function_result['results'], [[-12.222222222222221]], f"results mismatch {function_result}")

    def test_query_context_negative(self):
        function_name = 'execute_default'
        query_context = '{"query_context": "default:default._default"}'
        functions = f'function {function_name}() {{\
            var params = {query_context};\
            var query = N1QL("SELECT name FROM _default WHERE job_title = $job AND join_yr = $year AND join_mo = $month ORDER by name LIMIT 3", params);\
            var acc = [];\
            for (const row of query) {{ acc.push(row); }}\
            return acc;\
        }}'
        self.create_library(self.library_name, functions, [function_name])
        self.run_cbq_query(
            f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        try:
            # Execute function, you should not be able to pass query context to N1QL function
            function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
            self.fail("Query should have CBQ error'd")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 10109)
            self.assertTrue('No bucket named _default' in str(error), f"Error is not what we expected {str(ex)}")

    def test_query_context_prepared(self):
        self.run_cbq_query('DELETE FROM system:prepareds WHERE name LIKE "engineer%"')
        function_name = 'execute_prepare_default'
        param_val = '["Engineer"]'
        param_var = "$1"
        functions = f'function {function_name}() {{\
            var params = {param_val};\
            var query = N1QL("EXECUTE engineer_count", params);\
            var acc = [];\
            for (const row of query) {{ acc.push(row); }}\
            return acc;\
        }}'
        self.create_library(self.library_name, functions, [function_name])
        self.run_cbq_query(
            f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        self.run_cbq_query(
            f'CREATE OR REPLACE FUNCTION default:default._default.{function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        self.run_cbq_query(
            f'CREATE OR REPLACE FUNCTION default:default.test.{function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        # Prepare statement with named or postional parameter
        self.run_cbq_query(
            f'PREPARE engineer_count as SELECT COUNT(*) as count_engineer FROM _default WHERE job_title = {param_var}',
            query_context="default:default._default")
        # Execute function and check
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()', query_context="default:default._default")
        self.assertEqual(function_result['results'][0], [{'count_engineer': 672}])

        # Execute global function should error
        try:
            # Execute function, you should not be able to pass query context to N1QL function
            function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
            self.fail("Query should have CBQ error'd")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 10109)
            self.assertTrue('No such prepared statement: engineer_count, context: default:' in str(error), f"Error is not what we expected {str(ex)}")

        try:
            #execute function in wrong scope should error
            function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()', query_context="default:default.test")
            self.fail("Query should have CBQ error'd")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 10109)
            self.assertTrue('No such prepared statement: engineer_count, context: default:default.test' in str(error), f"Error is not what we expected {str(ex)}")

    def test_udf_prepare_query_context(self):
        self.run_cbq_query('DELETE FROM system:prepareds WHERE name LIKE "engineer%"')
        function_name = 'execute_prepare_default'
        param_val = '["Engineer"]'
        param_var = "$1"
        functions = f'function {function_name}() {{\
            var params = {param_val};\
            var query = PREPARE engineer_count as SELECT COUNT(*) as count_engineer FROM _default WHERE job_title = {param_var}; \
            var query1 = N1QL("EXECUTE engineer_count", params);\
            var acc = [];\
            for (const row of query1) {{ acc.push(row); }}\
            return acc;\
        }}'
        self.create_library(self.library_name, functions, [function_name])
        self.run_cbq_query(
            f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        self.run_cbq_query(
            f'CREATE OR REPLACE FUNCTION default:default._default.{function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        self.run_cbq_query(
            f'CREATE OR REPLACE FUNCTION default:default.test.{function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')

        # Execute function and check
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()', query_context="default:default._default")
        self.assertEqual(function_result['results'][0], [{'count_engineer': 672}])

        # Execute global function should error
        try:
            # Execute function, you should not be able to pass query context to N1QL function
            function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
            self.fail("Query should have CBQ error'd")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 10109)
            self.assertTrue('Keyspace not found in CB datastore: default:_default' in str(error), f"Error is not what we expected {str(ex)}")

        try:
            #execute function in wrong scope should error
            function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()', query_context="default:default.test")
            self.fail("Query should have CBQ error'd")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 10109)
            self.assertTrue('Keyspace not found in CB datastore: default:default.test._default' in str(error), f"Error is not what we expected {str(ex)}")

        prepared_results = self.run_cbq_query(query="select * from system:prepareds")
        self.assertEqual(prepared_results['results'][0]['prepareds']['name'],'engineer_count(default:default._default)', f"the prepared name is wrong please check prepareds {prepared_results}")

    def test_global_udf_prepare_query_context(self):
        self.run_cbq_query('DELETE FROM system:prepareds WHERE name LIKE "engineer%"')
        function_name = 'execute_prepare_default'
        param_val = '["Engineer"]'
        param_var = "$1"
        functions = f'function {function_name}() {{\
            var params = {param_val};\
            var query = PREPARE engineer_count as SELECT COUNT(*) as count_engineer FROM default WHERE job_title = {param_var}; \
            var query1 = N1QL("EXECUTE engineer_count", params);\
            var acc = [];\
            for (const row of query1) {{ acc.push(row); }}\
            return acc;\
        }}'
        self.create_library(self.library_name, functions, [function_name])
        self.run_cbq_query(
            f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        self.run_cbq_query(
            f'CREATE OR REPLACE FUNCTION default:default._default.{function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        self.run_cbq_query(
            f'CREATE OR REPLACE FUNCTION default:default.test.{function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')

        # Execute function, you should not be able to pass query context to N1QL function
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
        self.assertEqual(function_result['results'][0], [{'count_engineer': 672}])

        try:
            #execute function in wrong scope should error
            function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()', query_context="default:default._default")
            self.fail("Query should have CBQ error'd")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 10109)
            self.assertTrue('Keyspace not found in CB datastore: default:default._default.default' in str(error), f"Error is not what we expected {str(ex)}")

        try:
            #execute function in wrong scope should error
            function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()', query_context="default:default.test")
            self.fail("Query should have CBQ error'd")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 10109)
            self.assertTrue('Keyspace not found in CB datastore: default:default.test.default' in str(error), f"Error is not what we expected {str(ex)}")

        prepared_results = self.run_cbq_query(query="select * from system:prepareds")
        self.assertEqual(prepared_results['results'][0]['prepareds']['name'],'engineer_count', f"the prepared name is wrong please check prepareds {prepared_results}")

    def test_nested_udf_prepare_query_context(self):
        self.run_cbq_query('DELETE FROM system:prepareds WHERE name LIKE "engineer%"')
        self.run_cbq_query("CREATE PRIMARY INDEX ON default:default.test.test1")
        function_name = 'execute_default'
        param_val = '["Engineer"]'
        param_var = "$1"
        # Create a correct function to call for each scope, based on what scope will call it
        functions = f'function {function_name}() {{\
            var params = {param_val};\
            var query = N1QL("SELECT COUNT(*) as count_engineer FROM _default WHERE job_title = {param_var}",params); \
            var acc = [];\
            for (const row of query) {{ acc.push(row); }}\
            return acc;\
        }}'
        self.create_library(self.library_name, functions, [function_name])

        functions = f'function {function_name}() {{\
            var params = {param_val};\
            var query = N1QL("SELECT COUNT(*) as count_engineer FROM default WHERE job_title = {param_var}",params); \
            var acc = [];\
            for (const row of query) {{ acc.push(row); }}\
            return acc;\
        }}'
        self.create_library("global_library", functions, [function_name])

        functions = f'function {function_name}() {{\
            var query = SELECT * FROM test1; \
            var acc = [];\
            for (const row of query) {{ acc.push(row); }}\
            return acc;\
        }}'
        self.create_library("test_library", functions, [function_name])

        self.run_cbq_query(
            f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "global_library"')
        self.run_cbq_query(
            f'CREATE OR REPLACE FUNCTION default:default._default.{function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        self.run_cbq_query(
            f'CREATE OR REPLACE FUNCTION default:default.test.{function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "test_library"')

        self.run_cbq_query(f'PREPARE engineer_count as SELECT {function_name}()', query_context="default:default._default")
        self.run_cbq_query(f'PREPARE engineer_count as SELECT {function_name}()', query_context="default:default.test")
        self.run_cbq_query(f'PREPARE engineer_count as SELECT {function_name}()', query_context="default:")

        prepared_results = self.run_cbq_query(query="EXECUTE engineer_count", query_context="default:default._default")
        self.assertEqual(prepared_results['results'], [{'$1': [{'count_engineer': 672}]}], f"Results are wrong {prepared_results}")
        prepared_results = self.run_cbq_query(query="EXECUTE engineer_count", query_context="default:default.test")
        self.assertEqual(prepared_results['results'], [{'$1': [{'test1': {'name': 'old hotel', 'type': 'hotel'}},
                                                               {'test1': {'name': 'new hotel', 'type': 'hotel'}},
                                                               {'test1': {'name': 'old hotel', 'nested': {'fields': 'fake'}}},
                                                               {'test1': {'name': 'old hotel', 'numbers': [1, 2, 3, 4]}}]}],
                         f"Results are wrong {prepared_results}")
        prepared_results = self.run_cbq_query(query="EXECUTE engineer_count", query_context="default:")
        self.assertEqual(prepared_results['results'], [{'$1': [{'count_engineer': 672}]}], f"Results are wrong {prepared_results}")

    def test_nested_udf_prepare_query_context_scope_negative(self):
        self.run_cbq_query('DELETE FROM system:prepareds WHERE name LIKE "engineer%"')
        function_name = 'execute_default'
        param_val = '["Engineer"]'
        param_var = "$1"
        functions = f'function {function_name}() {{\
            var params = {param_val};\
            var query = SELECT COUNT(*) as count_engineer FROM _default WHERE job_title = {param_var}; \
            var query1 = N1QL("EXECUTE engineer_count", params);\
            var acc = [];\
            for (const row of query1) {{ acc.push(row); }}\
            return acc;\
        }}'
        self.create_library(self.library_name, functions, [function_name])

        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION default:default._default.{function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        self.run_cbq_query(f'PREPARE engineer_count as SELECT {function_name}()', query_context="default:default._default")

        try:
            prepared_results = self.run_cbq_query(query="EXECUTE engineer_count", query_context="default:default.test")
            self.fail("Query should have CBQ error'd")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 4040)
            self.assertTrue('No such prepared statement: engineer_count, context: default:default.test' in str(error), f"Error is not what we expected {str(ex)}")

        try:
            prepared_results = self.run_cbq_query(query="EXECUTE engineer_count", query_context="default:")
            self.fail("Query should have CBQ error'd")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 4040)
            self.assertTrue('No such prepared statement: engineer_count, context: default:' in str(error), f"Error is not what we expected {str(ex)}")

    def test_nested_udf_prepare_query_context_global_negative(self):
        self.run_cbq_query('DELETE FROM system:prepareds WHERE name LIKE "engineer%"')
        function_name = 'execute_default'
        functions = f'function {function_name}() {{\
            var params = ["Engineer"];\
            var query = SELECT COUNT(*) as count_engineer FROM default WHERE job_title = $1; \
            var acc = [];\
            for (const row of query) {{ acc.push(row); }}\
            return acc;\
        }}'
        self.create_library(self.library_name, functions, [function_name])

        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        self.run_cbq_query(f'PREPARE engineer_count as SELECT {function_name}()', query_context="default:default._default")

        try:
            prepared_results = self.run_cbq_query(query="EXECUTE engineer_count", query_context="default:default._default")
            self.fail("Query should have CBQ error'd")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 5010)
            self.assertTrue('Keyspace not found in CB datastore: default:default._default.default' in str(error), f"Error is not what we expected {str(ex)}")

        try:
            prepared_results = self.run_cbq_query(query="EXECUTE engineer_count", query_context="default:default.test")
            self.fail("Query should have CBQ error'd")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 4040)
            self.assertTrue('No such prepared statement: engineer_count, context: default:' in str(error), f"Error is not what we expected {str(ex)}")

    def test_parameter_values(self):
        function_name = 'param_values_default'
        functions = f'function {function_name}() {{\
            var num = 10;\
            var str = "Hello World!";\
            var obj = {{"Name": "Grogu", Age: 50}};\
            var arr1 = ["a", "b", "c"];\
            var arr2 = [1, 2, 3];\
            var tru = true;\
            var fal = false;\
            var nul = null;\
            var nan = NaN;\
            var inf = Infinity;\
            var query = SELECT $num as value_number, $str as value_string,$obj as value_object,\
                    $arr1 as value_array_1, $arr2 as value_array_2, $tru as value_true, $fal as value_false,\
                    $nan as value_nan, $inf as value_infinity, $nul as value_null;\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library(self.library_name, functions, [function_name])
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        # Execute function
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
        expected_result = [
            {'value_array_1': ['a', 'b', 'c'],
            'value_array_2': [1, 2, 3],
            'value_false': False,
            'value_infinity': None, 'value_nan': None, 'value_null': None,
            'value_number': 10,
            'value_object': {'Age': 50, 'Name': 'Grogu'},
            'value_string': 'Hello World!',
            'value_true': True}
        ]
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, expected_result)

    def test_comment(self):
        function_name = 'success'
        functions = f"""function {function_name}() {{
            // some comment
            query = SELECT "success" as res; // some other comment
            var acc = [];
            for (const row of query) {{
                acc.push(row);
            }}
            return acc;
        }}
        """
        self.create_library(self.library_name, functions, [function_name])
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        # Execute function
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, [{'res': 'success'}])

    def test_comment2(self):
        function_name = 'success'
        functions = f"""function {function_name}() {{
            // some comment
            query = SELECT * FROM [1,2,3] as t // can i put a comment here?
                    WHERE t > 1; // and another one here?
            var acc = [];
            for (const row of query) {{
                acc.push(row);
            }}
            return acc;
        }}
        """
        self.create_library(self.library_name, functions, [function_name])
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        # Execute function
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, [{'t': 2}, {'t': 3}])

    def test_make_statement(self):
        function_name = 'param_from_var_default'
        functions = f'function {function_name}(j, y, m) {{\
            var job = j;\
            var year = y;\
            var month = m;\
            var query = SELECT name FROM default WHERE job_title = $job AND join_yr = $year AND join_mo = $month ORDER by name LIMIT 3;\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library(self.library_name, functions, [function_name])
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}(...) LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')

        # Execute function
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}("Engineer",2011,10)')
        expected_result = [{'name': 'employee-1'}, {'name': 'employee-10'}, {'name': 'employee-11'}]
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, expected_result)

    def test_build_statement_strings(self):
        function_name = 'param_from_var_default'
        functions = f'function {function_name}(selector, j, y, m) {{\
            var projection = "";\
            if (selector){{ projection = "name";}} else {{ projection = "job_title"; }}\
            var job = j;\
            var year = y;\
            var month = m;\
            var query_string = "SELECT " + projection + " FROM default WHERE job_title = $job AND join_yr = $year AND join_mo = $month ORDER by name LIMIT 3";\
            var query = N1QL(query_string, {{"job":job, "month": month, "year": year}});\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library(self.library_name, functions, [function_name])
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}(...) LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')

        # Execute function
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}(1,"Engineer",2011,10)')
        expected_result = self.run_cbq_query("SELECT name FROM default use index(`#primary`) WHERE job_title = 'Engineer' AND join_yr = 2011 AND join_mo = 10 ORDER by name LIMIT 3")
        expected_result = expected_result['results']
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, expected_result)

        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}(0,"Engineer",2011,10)')
        expected_result = self.run_cbq_query("SELECT job_title FROM default use index(`#primary`) WHERE job_title = 'Engineer' AND join_yr = 2011 AND join_mo = 10 ORDER by name LIMIT 3")
        expected_result = expected_result['results']
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, expected_result)

    def test_iterator(self):
        function_name = 'param_from_var_default'
        # once iterator is closed iter.next will no longer return a value
        functions = f'function {function_name}(selector, j, y, m) {{\
            var projection = "";\
            if (selector){{ projection = "name";}} else {{ projection = "job_title"; }}\
            var job = j;\
            var year = y;\
            var month = m;\
            var query_string = "SELECT " + projection + " FROM default WHERE job_title = $job AND join_yr = $year AND join_mo = $month ORDER by name LIMIT 3";\
            var query = N1QL(query_string, {{"job":job, "month": month, "year": year}});\
            let iter = query[Symbol.iterator]();\
            var firstrow = iter.next();\
            query.close();\
            var secondrow = iter.next();\
            var thirdrow = iter.next();\
            return {{"first_row": firstrow, "secondrow": secondrow, "thirdrow": thirdrow}};}}'
        self.create_library(self.library_name, functions, [function_name])
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}(...) LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')

        # Execute function
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}(1,"Engineer",2011,10)')
        expected_result = {'first_row': {'done': False, 'value': {'name': 'employee-1'}}, 'secondrow': {'done': True, 'value': None}, 'thirdrow': {'done': True, 'value': None}}
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, expected_result)

        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}(0,"Engineer",2011,10)')
        expected_result = {'first_row': {'done': False, 'value': {'job_title': 'Engineer'}}, 'secondrow': {'done': True, 'value': None}, 'thirdrow': {'done': True, 'value': None}}
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, expected_result)

        #if we do not close iterator we can get a subset of values
        functions = f'function {function_name}(selector, j, y, m) {{\
            var projection = "";\
            if (selector){{ projection = "name";}} else {{ projection = "job_title"; }}\
            var job = j;\
            var year = y;\
            var month = m;\
            var query_string = "SELECT " + projection + " FROM default WHERE job_title = $job AND join_yr = $year AND join_mo = $month ORDER by name LIMIT 3";\
            var query = N1QL(query_string, {{"job":job, "month": month, "year": year}});\
            let iter = query[Symbol.iterator]();\
            var firstrow = iter.next();\
            var secondrow = iter.next();\
            return {{"first_row": firstrow, "secondrow": secondrow}};}}'
        self.create_library(self.library_name, functions, [function_name])

        # Execute function
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}(1,"Engineer",2011,10)')
        expected_result = {'first_row': {'done': False, 'value': {'name': 'employee-1'}}, 'secondrow': {'done': False, 'value': {'name': 'employee-10'}}}
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, expected_result)

        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}(0,"Engineer",2011,10)')
        expected_result = {'first_row': {'done': False, 'value': {'job_title': 'Engineer'}}, 'secondrow': {'done': False, 'value': {'job_title': 'Engineer'}}}
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, expected_result)

        #if we do not close iterator we can get a subset of values
        functions = f'function {function_name}(selector, j, y, m) {{\
            var projection = "";\
            if (selector){{ projection = "name";}} else {{ projection = "job_title"; }}\
            var job = j;\
            var year = y;\
            var month = m;\
            var query_string = "SELECT " + projection + " FROM default WHERE job_title = $job AND join_yr = $year AND join_mo = $month ORDER by name LIMIT 3";\
            var query = N1QL(query_string, {{"job":job, "month": month, "year": year}});\
            let iter = query[Symbol.iterator]();\
            var firstrow = iter.next();\
            var secondrow = iter.next();\
            var thirdrow = iter.next();\
            return {{"first_row": firstrow, "secondrow": secondrow, "thirdrow" : thirdrow}};}}'
        self.create_library(self.library_name, functions, [function_name])

        # Execute function
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}(1,"Engineer",2011,10)')
        expected_result = {'first_row': {'done': False, 'value': {'name': 'employee-1'}}, 'secondrow': {'done': False, 'value': {'name': 'employee-10'}}, 'thirdrow': {'done': False, 'value': {'name': 'employee-11'}}}
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, expected_result)

        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}(0,"Engineer",2011,10)')
        expected_result = {'first_row': {'done': False, 'value': {'job_title': 'Engineer'}}, 'secondrow': {'done': False, 'value': {'job_title': 'Engineer'}}, 'thirdrow': {'done': False, 'value': {'job_title': 'Engineer'}}}
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, expected_result)

        # We can iterate through the iterator with a for loop for as long as we have values to use
        functions = f'function {function_name}(selector, j, y, m) {{\
            var projection = "";\
            if (selector){{ projection = "name";}} else {{ projection = "job_title"; }}\
            var job = j;\
            var year = y;\
            var month = m;\
            var query_string = "SELECT " + projection + " FROM default WHERE job_title = $job AND join_yr = $year AND join_mo = $month ORDER by name LIMIT 10";\
            var query = N1QL(query_string, {{"job":job, "month": month, "year": year}});\
            let iter = query[Symbol.iterator]();\
            var acc = [];\
            for ( let i = 0; i < 5; i++) {{acc.push(iter.next());}}\
            return acc;}}'
        self.create_library(self.library_name, functions, [function_name])

        # Execute function
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}(1,"Engineer",2011,10)')
        expected_result = [{'done': False, 'value': {'name': 'employee-1'}}, {'done': False, 'value': {'name': 'employee-10'}}, {'done': False, 'value': {'name': 'employee-11'}}, {'done': False, 'value': {'name': 'employee-12'}}, {'done': False, 'value': {'name': 'employee-13'}}]
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, expected_result)

        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}(0,"Engineer",2011,10)')
        expected_result = [{'done': False, 'value': {'job_title': 'Engineer'}}, {'done': False, 'value': {'job_title': 'Engineer'}}, {'done': False, 'value': {'job_title': 'Engineer'}}, {'done': False, 'value': {'job_title': 'Engineer'}}, {'done': False, 'value': {'job_title': 'Engineer'}}]
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, expected_result)

    def test_multiple_inner(self):
        function_name = 'multiple_inner'
        functions = f'function {function_name}() {{\
            var res=[];\
            var q1 = SELECT * FROM [1,2,3,4,5] AS t ORDER BY t;\
            for (const doc of q1) {{\
                res.push(doc);\
                var q2 = SELECT COUNT(*) FROM [1,2,3] AS s;\
                q2.close();\
            }}\
            q1.close();\
            return res;\
        }}'
        self.create_library(self.library_name, functions, [function_name])
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')

        # Execute function
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
        expected_result = [{"t": 1}, {"t": 2}, {"t": 3}, {"t": 4}, {"t": 5}]
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, expected_result)

    def test_inner_insert(self):
        self.run_cbq_query("DELETE FROM default WHERE implicit_close = True")
        function_name = 'implicit_close'
        # Function will iterate over 11 rows from select, we should not hit default limit (10)
        # since insert iterator does not return row and should will be implicitly close
        functions = f'function {function_name}() {{\
            var res=[];\
            var q1 = SELECT * FROM [1,2,3,4,5,6,7,8,9,10,11] AS a;\
            for (const doc of q1) {{\
                var q2 = INSERT INTO default VALUES (UUID(), {{"implicit_close": True}});\
            }}\
            return "Success";\
        }}'
        self.create_library(self.library_name, functions, [function_name])
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')

        # Execute function
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
        expected_result = "Success"
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, expected_result)

    def test_inner_select(self):
        function_name = 'implicit_close'
        # Function need to iterate over less than 10 rows given there is no explicit close
        # and it will open 10 iterarors which is default max.
        functions = f'function {function_name}() {{\
            var res=[];\
            var q1 = SELECT * FROM [1,2,3,4,5,6,7,8,9] AS a;\
            for (const doc of q1) {{\
                var q2 = SELECT COUNT(*) FROM [1,2,3] AS b;\
            }}\
            return "Success";\
        }}'
        self.create_library(self.library_name, functions, [function_name])
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')

        # Execute function
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
        expected_result = "Success"
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, expected_result)

    def test_inner_select_exception(self):
        function_name = 'implicit_close'
        # Function will terate over more than 10 rows. Given there is no explicit close
        # it will open 10+ iterarors and reach max of 10.
        functions = f'function {function_name}() {{\
            var res=[];\
            var q1 = SELECT * FROM [1,2,3,4,5,6,7,8,9,10,11] AS a;\
            for (const doc of q1) {{\
                var q2 = SELECT COUNT(*) FROM [1,2,3] AS b;\
            }}\
            return "Success";\
        }}'
        self.create_library(self.library_name, functions, [function_name])
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')

        # Execute function
        try:
            self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
            self.log.fail("We should have hit max ")
        except CBQError as ex:
            error = self.process_CBQE(ex, 0)
            self.assertEqual(error['code'], 10109)
            self.assertEqual(error['reason']['details']['Exception'], " Active iterator limit of 10 reached. Close unused iterators and retry")

    def test_multiple_iterator(self):
        function_name = 'param_from_var_default'
        functions = f'function {function_name}(j, y, m) {{\
            var job = j;\
            var year = y;\
            var month = m;\
            var inset = INSERT INTO default (KEY, VALUE) VALUES ("key1000", {{ "type" : "hotel", "name" : "new hotel" }}) RETURNING *;\
            var query = SELECT name FROM default WHERE job_title = $job AND join_yr = $year AND join_mo = $month ORDER by name LIMIT 3;\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            query.close();\
            var query2 = SELECT name FROM default WHERE job_title = $job AND join_yr = $year AND join_mo = $month ORDER by name LIMIT 8;\
            var acc2 = [];\
            for (const row of query2) {{acc2.push(row);}}\
            query2.close();\
            return {{"query1": acc, "query2": acc2}};}}'
        self.create_library(self.library_name, functions, [function_name])
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}(...) LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')

        # Execute function
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}("Engineer",2011,10)')
        expected_result = {'query1': [{'name': 'employee-1'}, {'name': 'employee-10'}, {'name': 'employee-11'}],
                           'query2': [{'name': 'employee-1'}, {'name': 'employee-10'}, {'name': 'employee-11'},
                                      {'name': 'employee-12'}, {'name': 'employee-13'}, {'name': 'employee-14'},
                                      {'name': 'employee-15'}, {'name': 'employee-16'}]}
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, expected_result)

    def test_create_library_error(self):
        function_name = 'param_function_param_default'
        functions = f'function {function_name}() {{\
            var number = 10;\
            var query = EXECUTE FUNCTION add(5, $number);\
            var acc = []\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library(self.library_name, functions, [function_name], error=True)

    def test_syntax_error(self):
        function_name = "syntax_error"
        query = 'SELEC * FROM default WHERE lower(job_title) = "engineer"'
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            var query = {query};\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library(self.library_name, functions, function_names, error=True)

    def test_timeout(self):
        function_name = 'sleep'
        function_names = [function_name]
        function_sleep = 'function sleep(delay) { var start = new Date().getTime(); while (new Date().getTime() < start + delay); return delay; }'
        self.create_library("sleep", function_sleep, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}(t) LANGUAGE JAVASCRIPT AS "{function_name}" AT "sleep"')
        sleep_query = f"EXECUTE FUNCTION {function_name}(300000)"

        try:
            self.run_cbq_query(sleep_query, query_params={'timeout':'10s'})
            self.fail("Query should have CBQ error'd")
        except CBQError as ex:
            error = self.process_CBQE(ex, 0)
            self.assertEqual(error['code'], 1080)
            self.assertTrue('Timeout 10s exceeded' in str(error), f"Error is not what we expected {str(ex)}")

        try:
            self.run_cbq_query(sleep_query, query_params={'timeout':'600s'})
            self.fail("Query should have CBQ error'd")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 10109)
            self.assertTrue('stopped after running beyond 120000 ms' in str(error), f"Error is not what we expected {str(ex)}")

    def test_try_catch(self):
        function_name = "syntax_error"
        query = 'SELECT * FROM fake_bucket WHERE lower(job_title) = "engineer"'
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            try{{\
            var query = {query};\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}}}catch(err){{throw "bucket does not exist"}}\
            return acc;}}'
        self.create_library(self.library_name, functions, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        try:
            self.run_cbq_query(f"EXECUTE FUNCTION {function_name}()")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 10109)
            self.assertTrue('bucket does not exist' in str(error), f"Error is not what we expected {str(ex)}")

    def test_transaction_commit(self):
        self.run_cbq_query("DELETE FROM default.`_default`.txn_scope")
        function_name = 'commit_default'
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            var start_txn = {self.start_txn};\
            var query1 = INSERT INTO default.`_default`.txn_scope(key, value) VALUES (UUID(), {{"a":1, "b":2}}) ;\
            var query2 = SELECT * FROM default.`_default`.txn_scope;\
            var acc = [];\
            for (const row of query2) {{\
                acc.push(row);\
            }}\
            var end_txn = {self.end_txn};\
            return acc;\
        }}'
        self.create_library(self.library_name, functions, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        udf = self.run_cbq_query(f"EXECUTE FUNCTION {function_name}()")
        self.sleep(10)
        self.assertEqual(udf['results'][0], [{'txn_scope': {'a': 1, 'b': 2}}])
        result = self.run_cbq_query("SELECT * FROM default.`_default`.txn_scope")
        self.assertEqual(result['results'], [{'txn_scope': {'a': 1, 'b': 2}}])

    def test_transaction_rollback(self):
        self.run_cbq_query("DELETE FROM default.`_default`.txn_scope")
        self.run_cbq_query('INSERT INTO default.`_default`.txn_scope(key, value) VALUES (UUID(), {"a":1, "b":2})')
        function_name = 'rollback_default'
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            var start_txn = {self.start_txn};\
            var query1 = DELETE FROM default.`_default`.txn_scope;\
            var query2 = SELECT * FROM default.`_default`.txn_scope;\
            var acc = [];\
            for (const row of query2) {{\
                acc.push(row);\
            }}\
            var end_txn = {self.end_txn};\
            return acc;\
        }}'
        self.create_library(self.library_name, functions, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        udf = self.run_cbq_query(f"EXECUTE FUNCTION {function_name}()")
        self.assertEqual(udf['results'][0], [])
        result = self.run_cbq_query("SELECT * FROM default.`_default`.txn_scope")
        self.assertEqual(result['results'], [{'txn_scope': {'a': 1, 'b': 2}}])

    def test_transaction_savepoint(self):
        self.run_cbq_query("DELETE FROM default.`_default`.txn_scope")
        function_name = 'savepoint_default'
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            var start_txn = {self.start_txn};\
            var query1 = INSERT INTO default.`_default`.txn_scope(key, value) VALUES (UUID(), {{"a":1, "b":2}}) ;\
            var query2 = SAVEPOINT S1;\
            var query3 = INSERT INTO default.`_default`.txn_scope(key, value) VALUES (UUID(), {{"a":2, "b":2}}) ;\
            var query4 = SAVEPOINT S2;\
            var query5 = INSERT INTO default.`_default`.txn_scope(key, value) VALUES (UUID(), {{"a":3, "b":2}}) ;\
            var query6 = SELECT * FROM default.`_default`.txn_scope ORDER BY a;\
            var acc = [];\
            for (const row of query6) {{\
                acc.push(row);\
            }}\
            var query7 = ROLLBACK TO SAVEPOINT S1;\
            var end_txn = {self.end_txn};\
            return acc;\
        }}'
        self.create_library(self.library_name, functions, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        udf = self.run_cbq_query(f"EXECUTE FUNCTION {function_name}()")
        self.assertEqual(udf['results'][0], [{'txn_scope': {'a': 1, 'b': 2}}, {'txn_scope': {'a': 2, 'b': 2}}, {'txn_scope': {'a': 3, 'b': 2}}])
        result = self.run_cbq_query("SELECT * FROM default.`_default`.txn_scope")
        self.assertEqual(result['results'], [{'txn_scope': {'a': 1, 'b': 2}}])

    def test_transaction_error_nest(self):
        function_name = 'nested_default'
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            var start_txn = BEGIN WORK;\
            var query1 = INSERT INTO default.`_default`.txn_scope(key, value) VALUES (UUID(), {{"a":1, "b":2}}) ;\
            var query2 = BEGIN WORK;\
            var query3 = COMMIT;\
        }}'
        self.create_library(self.library_name, functions, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        try:
            udf = self.run_cbq_query(f"EXECUTE FUNCTION {function_name}()")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 10109)
            self.assertTrue('START_TRANSACTION statement is not supported within the transaction' in str(error), f"Error is not what we expected {str(ex)}")

    def test_transaction_error_nostart(self):
        function_name = 'nostart_default'
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            var query1 = SELECT * FROM default.`_default`.txn_scope;\
            var query3 = {self.end_txn};\
        }}'
        self.create_library(self.library_name, functions, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        try:
            udf = self.run_cbq_query(f"EXECUTE FUNCTION {function_name}()")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 10109)
            self.assertTrue(f'{self.end_txn} statement is not supported outside the transaction' in str(error), f"Error is not what we expected {str(ex)}")

    def test_transaction_dml(self):
        self.run_cbq_query("CREATE INDEX adv_job_title IF NOT EXISTS ON `default`(`job_title`)")
        function_name = 'transaction_dml'
        function_names = [function_name]
        query = self.dmls[self.statement]
        functions = f'function {function_name}() {{\
            var query1 = {self.start_txn};\
            var query2 = {query};\
            var query3 = ROLLBACK;\
            return "Success";\
        }}'
        self.create_library(self.library_name, functions, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        udf = self.run_cbq_query(f"EXECUTE FUNCTION {function_name}()")
        self.assertEqual(udf['results'], ["Success"])

    def test_transaction_active_transaction(self):
        self.run_cbq_query("DELETE FROM default.`_default`.txn_scope")
        function_name = 'nested_txn'
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            var start_txn = BEGIN WORK;\
            var query1 = INSERT INTO default.`_default`.txn_scope(key, value) VALUES (UUID(), {{"a":1, "b":2}}) ;\
            var query3 = COMMIT;\
        }}'
        self.create_library(self.library_name, functions, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        try:
            results = self.run_cbq_query(query="START TRANSACTION", txtimeout="2m",server=self.master)
            txid = results['results'][0]['txid']
            if self.within_txn:
                udf = self.run_cbq_query(f"EXECUTE FUNCTION {function_name}()", txnid=txid,server=self.master)
                self.fail("Query should have CBQ error'd")
            else:
                udf = self.run_cbq_query(f"EXECUTE FUNCTION {function_name}()",server=self.master)
                results = self.run_cbq_query("select * from default.`_default`.txn_scope")
                expected_result = [{"txn_scope": {"a": 1, "b": 2}}]
                self.assertEqual(results['results'], expected_result,
                                 f"Results are not as expected, expected: {expected_result} ,  actual: {results}")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 10109)
            self.assertTrue('START_TRANSACTION statement is not supported within the transaction' in str(error), f"Error is not what we expected {str(ex)}")

    def test_transaction_timedout_transaction(self):
        self.run_cbq_query("DELETE FROM default.`_default`.txn_scope")
        function_name = 'nested_txn'
        function_names = [function_name]
        functions = f'function {function_name}() {{\
                    var start_txn = BEGIN WORK;\
                    var query1 = INSERT INTO default.`_default`.txn_scope(key, value) VALUES (UUID(), {{"a":1, "b":2}}) ;\
                    var query3 = COMMIT;\
                }}'
        self.create_library(self.library_name, functions, function_names)
        self.run_cbq_query(
            f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        results = self.run_cbq_query(query="START TRANSACTION", txtimeout="1m")
        txid = results['results'][0]['txid']
        self.sleep(60)
        try:
            udf = self.run_cbq_query(f"EXECUTE FUNCTION {function_name}()", txnid=txid)
            self.fail("Query should have CBQ error'd")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 17010)
            self.assertTrue('Transaction timeout' in str(error), f"Error is not what we expected {str(ex)}")
        try:
            udf = self.run_cbq_query(f"EXECUTE FUNCTION {function_name}()")
            results = self.run_cbq_query("select * from default.`_default`.txn_scope")
            expected_result = [{"txn_scope": {"a": 1,"b": 2}}]
            self.assertEqual(results['results'], expected_result,
                             f"Results are not as expected, expected: {expected_result} ,  actual: {results}")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.fail("Query should not have CBQ error'd")

    def test_transaction_side_effect(self):
        function_name = 'nested_txn'
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            var start_txn = BEGIN WORK;\
            var query1 = INSERT INTO default.`_default`.txn_scope(key, value) VALUES (UUID(), {{"a":1, "b":2}}) ;\
            var query3 = COMMIT;\
        }}'
        self.create_library(self.library_name, functions, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        try:
            udf = self.run_cbq_query(f"select {function_name}() from default")
            self.fail("Query should have CBQ error'd")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 5010)
            self.assertTrue('not a readonly request' in str(error), f"Error is not what we expected {str(ex)}")

    def test_transaction_nested_no_side_effect(self):
        function_name = 'nested_txn'
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            var start_txn = BEGIN WORK;\
            var query1 = SELECT * FROM default limit 10 ;\
            var query3 = COMMIT;\
        }}'
        self.create_library(self.library_name, functions, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        try:
            udf = self.run_cbq_query(f"select {function_name}() from default")
            self.fail("Query should have CBQ error'd")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 5010)
            self.assertTrue('not a readonly request' in str(error), f"Error is not what we expected {str(ex)}")

    def test_multiple_transactions(self):
        self.run_cbq_query("DELETE FROM default.`_default`.txn_scope")
        self.run_cbq_query("CREATE PRIMARY INDEX ON `default`:`default`.`test`.`test2`")
        function_name = 'nested_txn'
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            var start_txn2 = START TRANSACTION;\
            var query1 = DELETE FROM default.test.test2;\
            var query2 = SELECT * FROM default.test.test2;\
            var query3 = COMMIT;\
            var start_txn = BEGIN WORK;\
            var query1 = INSERT INTO default.`_default`.txn_scope(key, value) VALUES (UUID(), {{"a":1, "b":2}}) ;\
            var query3 = COMMIT;\
        }}'
        self.create_library(self.library_name, functions, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        udf = self.run_cbq_query(f"EXECUTE FUNCTION {function_name}()")
        results = self.run_cbq_query("SELECT * from default.test.test2;")
        self.assertEqual(results['results'], [],
                         f"Results are not as expected, expected: [] ,  actual: {results}")
        results = self.run_cbq_query("SELECT * FROM default.`_default`.txn_scope")
        expected_result = [{"txn_scope": {"a": 1, "b": 2}}]
        self.assertEqual(results['results'], expected_result,
                         f"Results are not as expected, expected: {expected_result} ,  actual: {results}")

    def test_transaction_udf_timeout(self):
        function_name = 'sleep'
        function_names = [function_name]
        function_sleep = 'function sleep(delay) { var start = new Date().getTime(); while (new Date().getTime() < start + delay); return delay; }'
        self.create_library("sleep", function_sleep, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}(t) LANGUAGE JAVASCRIPT AS "{function_name}" AT "sleep"')
        sleep_query = f"EXECUTE FUNCTION {function_name}(300000)"

        function_name = 'nested_txn'
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            var start_txn = BEGIN WORK;\
            var query1 = {sleep_query};\
            var query3 = COMMIT;\
        }}'
        self.create_library(self.library_name, functions, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        try:
            udf = self.run_cbq_query(f"EXECUTE FUNCTION {function_name}()")
            self.fail("Query should have CBQ error'd")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 10109)
            self.assertTrue('Function timed out' in str(error), f"Error is not what we expected {str(ex)}")

    def test_transaction_with_udf(self):
        function_name = 'param_from_var_default'
        functions = f'function {function_name}(j, y, m) {{\
            var job = j;\
            var year = y;\
            var month = m;\
            var inset = INSERT INTO default (KEY, VALUE) VALUES ("key001", {{ "type" : "hotel", "name" : "new hotel" }}) RETURNING *;\
            var query = SELECT name FROM default WHERE job_title = $job AND join_yr = $year AND join_mo = $month ORDER by name LIMIT 3;\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library(self.library_name, functions, [function_name])
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}(...) LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')

        # Execute function
        results = self.run_cbq_query(query="START TRANSACTION", txtimeout="2m")
        txid = results['results'][0]['txid']
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}("Engineer",2011,10)',txnid=txid)
        expected_result = [{'name': 'employee-1'}, {'name': 'employee-10'}, {'name': 'employee-11'}]
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, expected_result)
        self.run_cbq_query(query="COMMIT TRANSACTION", txtimeout="2m",txnid=txid)
        results = self.run_cbq_query("SELECT * FROM default where type = 'hotel' and name = 'new hotel'")
        expected_result = [{'default': {'name': 'new hotel', 'type': 'hotel'}}]
        self.assertEqual(results['results'], expected_result, f"Results are not as expected, expected: {expected_result} ,  actual: {results}")

    def test_transaction_with_multiple_dml(self):
        self.run_cbq_query(query="DELETE from system:prepareds")
        self.run_cbq_query("DELETE FROM default.`_default`.txn_scope")
        function_name = 'savepoint_default'
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            var start_txn = BEGIN WORK;\
            var query1 = INSERT INTO default.`_default`.txn_scope(key, value) VALUES (UUID(), {{"a":1, "b":2}}) ;\
            query1.close();\
            var query2 = SAVEPOINT S1;\
            query2.close();\
            var query3 = INSERT INTO default.`_default`.txn_scope(key, value) VALUES (UUID(), {{"a":2, "b":2}}) ;\
            query3.close();\
            var query4 = SAVEPOINT S2; \
            query4.close(); \
            var query5 = INSERT INTO default.`_default`.txn_scope(key, value) VALUES (UUID(), {{"a":3, "b":2}}) ; \
            query5.close(); \
            var query6 = SAVEPOINT S3;\
            query6.close();\
            var query7 = UPDATE default.`_default`.txn_scope SET c = 1 where b = 2; \
            query7.close(); \
            var query8 = SAVEPOINT S4;\
            query8.close();\
            var query9 = DELETE from default.`_default`.txn_scope where a = 1; \
            query9.close(); \
            var query10 = SAVEPOINT S5; \
            query10.close(); \
            var query11 = SELECT * FROM default.`_default`.txn_scope ORDER BY a;\
            var acc = [];\
            for (const row of query11) {{\
                acc.push(row);\
            }} \
            query11.close(); \
            var query12 = ROLLBACK TO SAVEPOINT S4;\
            var end_txn = COMMIT WORK;\
            return acc;\
        }}'
        self.create_library(self.library_name, functions, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        udf = self.run_cbq_query(f"EXECUTE FUNCTION {function_name}()")
        self.assertEqual(udf['results'][0], [{'txn_scope': {'a': 2, 'b': 2, 'c': 1}}, {'txn_scope': {'a': 3, 'b': 2, 'c':1}}])
        result = self.run_cbq_query("SELECT * FROM default.`_default`.txn_scope ORDER BY a")
        self.assertEqual(result['results'], [{'txn_scope': {'a': 1, 'b': 2, 'c': 1}},{'txn_scope': {'a': 2, 'b': 2, 'c': 1}}, {'txn_scope': {'a': 3, 'b': 2, 'c':1}}])

    def test_tximplicit_with_prepareds(self):
        self.run_cbq_query(query="DELETE from system:prepareds")
        prepare_beginwork = "PREPARE PAYMENT_beginWork as BEGIN WORK"
        prepare_commitWork = "PREPARE PAYMENT_commitWork AS COMMIT"
        prepare_getWarehouse = "PREPARE PAYMENT_getWarehouse AS SELECT * FROM default LIMIT 100"
        self.run_cbq_query(prepare_beginwork)
        self.run_cbq_query(prepare_commitWork)
        self.run_cbq_query(prepare_getWarehouse)
        function_name = 'doPayment'
        functions = f'function doPayment(w_id,d_id,h_amount,c_w_id,c_d_id,c_id,c_last,h_date){{\
            var query1 = N1QL("EXECUTE PAYMENT_beginWork");\
            query1.close();\
            params = [w_id];\
            query1 = N1QL("EXECUTE PAYMENT_getWarehouse",params);\
            var warehouse = [];\
            for (const row of query1) {{warehouse.push(row);}}\
            query1.close();\
            var query1 = N1QL("EXECUTE PAYMENT_commitWork");\
            query1.close();\
            return [ warehouse];}}'
        self.create_library(self.library_name, functions, [function_name])
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}(...) LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        udf = self.run_cbq_query('EXECUTE FUNCTION doPayment(1,6,2873.55,1,6,"501","None","2022-01-10 11:07:34.823485")', query_params={"tximplicit":True})
        self.assertEqual(udf['metrics']['resultCount'], 1)

    def test_prepared_begin_commit_rollback(self):
        self.run_cbq_query(query="DELETE from system:prepareds")
        self.run_cbq_query("DELETE FROM default.`_default`.txn_scope")
        prepare_beginwork = "PREPARE beginWork as BEGIN WORK"
        prepare_commitwork = "PREPARE commitWork as COMMIT"
        prepare_rollback_savepoint = "PREPARE rollbackWork AS ROLLBACK TRANSACTION TO SAVEPOINT S2"
        self.run_cbq_query(prepare_beginwork)
        self.run_cbq_query(prepare_commitwork)
        self.run_cbq_query(prepare_rollback_savepoint)
        function_name = 'savepoint_default'
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            var start_txn = EXECUTE beginWork;\
            start_txn.close();\
            var query1 = INSERT INTO default.`_default`.txn_scope(key, value) VALUES (UUID(), {{"a":1, "b":2}}) ;\
            query1.close();\
            var query2 = SAVEPOINT S1;\
            query2.close();\
            var query3 = INSERT INTO default.`_default`.txn_scope(key, value) VALUES (UUID(), {{"a":2, "b":2}}) ;\
            query3.close();\
            var query4 = SAVEPOINT S2; \
            query4.close(); \
            var query5 = INSERT INTO default.`_default`.txn_scope(key, value) VALUES (UUID(), {{"a":3, "b":2}}) ; \
            query5.close(); \
            var query6 = SELECT * FROM default.`_default`.txn_scope ORDER BY a;\
            var acc = [];\
            for (const row of query6) {{\
                acc.push(row);\
            }} \
            query6.close(); \
            var query7 = EXECUTE rollbackWork; \
            query7.close(); \
            var end_txn = EXECUTE commitWork;\
            end_txn.close();\
            return acc;}}'
        self.create_library(self.library_name, functions, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        udf = self.run_cbq_query(f"EXECUTE FUNCTION {function_name}()")
        self.assertEqual(udf['results'][0], [{'txn_scope': {'a': 1, 'b': 2}}, {'txn_scope': {'a': 2, 'b': 2}}, {'txn_scope': {'a': 3, 'b': 2}}])
        expected_result = [{'txn_scope': {'a': 1, 'b': 2}},{'txn_scope': {'a': 2, 'b': 2}}]
        result = self.run_cbq_query("SELECT * FROM default.`_default`.txn_scope order by a")
        self.assertEqual(result['results'], expected_result, f"Results are not as expected, expected: {expected_result}, actual: {result}")

        self.run_cbq_query("DELETE FROM system:prepareds where name = 'rollbackWork'")
        self.run_cbq_query("DELETE FROM default.`_default`.txn_scope")
        prepare_rollback_savepoint = "PREPARE rollbackWork AS ROLLBACK TRANSACTION TO SAVEPOINT S1"
        self.run_cbq_query(prepare_rollback_savepoint)
        udf = self.run_cbq_query(f"EXECUTE FUNCTION {function_name}()")
        self.assertEqual(udf['results'][0], [{'txn_scope': {'a': 1, 'b': 2}}, {'txn_scope': {'a': 2, 'b': 2}}, {'txn_scope': {'a': 3, 'b': 2}}])
        expected_result = [{'txn_scope': {'a': 1, 'b': 2}}]
        result = self.run_cbq_query("SELECT * FROM default.`_default`.txn_scope order by a")
        self.assertEqual(result['results'], expected_result, f"Results are not as expected, expected: {expected_result}, actual: {result}")

    def test_transaction_metrics(self):
        self.run_cbq_query("DELETE FROM default.`_default`.txn_scope")
        function_name = 'txn'
        function_names = [function_name]
        functions = f'function {function_name}() {{\
                    var start_txn = BEGIN WORK;\
                    var query1 = INSERT INTO default.`_default`.txn_scope(key, value) VALUES (UUID(), {{"a":1, "b":2}}) ;\
                    var query3 = COMMIT;\
                }}'
        self.create_library(self.library_name, functions, function_names)
        self.run_cbq_query(
            f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        try:
            udf = self.run_cbq_query(f"EXECUTE FUNCTION {function_name}()")
            self.assertTrue("transactionRemainingTime" not in str(udf), f"the field transactionRemainingTime should not appear since the transaction is now done {udf}")
            results = self.run_cbq_query("select * from default.`_default`.txn_scope")
            expected_result = [{"txn_scope": {"a": 1,"b": 2}}]
            self.assertEqual(results['results'], expected_result,
                             f"Results are not as expected, expected: {expected_result} ,  actual: {results}")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.fail("Query should not have CBQ error'd")

    def test_try_catch_rollback(self):
        self.run_cbq_query("DELETE FROM default.`_default`.txn_scope")
        function_name = "syntax_error"
        query = 'SELECT * FROM fake_bucket WHERE lower(job_title) = "engineer"'
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            try{{\
            var beginWork = BEGIN WORK;\
            var savepoint1 = SAVEPOINT S1;\
            var query1 = INSERT INTO default.`_default`.txn_scope(key, value) VALUES (UUID(), {{"a":1, "b":2}}) ;\
            var query2 = SELECT * FROM default.`_default`.txn_scope ORDER BY a;\
            var acc = [];\
            for (const row of query2) {{acc.push(row);}}\
            var query = {query};\
            }}catch(err){{ var query3 = ROLLBACK WORK; throw "bucket does not exist";}}\
            return acc;}}'
        self.create_library(self.library_name, functions, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        try:
            self.run_cbq_query(f"EXECUTE FUNCTION {function_name}()")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 10109)
            self.assertTrue('bucket does not exist' in str(error), f"Error is not what we expected {str(ex)}")
        results = self.run_cbq_query("select * from default.`_default`.txn_scope")
        self.assertEqual(results['results'], [],
                         f"Results are not as expected, expected: [] ,  actual: {results}")

    def test_implicitly_pass_transaction_time(self):
        function_name = 'sleep'
        function_names = [function_name]
        function_sleep = 'function sleep(delay) { var start = new Date().getTime(); while (new Date().getTime() < start + delay); return delay; }'
        self.create_library("sleep", function_sleep, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}(t) LANGUAGE JAVASCRIPT AS "{function_name}" AT "sleep"')
        sleep_query = f"EXECUTE FUNCTION {function_name}(300000)"

        function_name = 'nested_txn'
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            var start_txn = BEGIN WORK;\
            var query1 = {sleep_query};\
            var query3 = COMMIT;\
        }}'
        self.create_library(self.library_name, functions, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        try:
            udf = self.run_cbq_query(f"EXECUTE FUNCTION {function_name}()", txtimeout="1m")
            self.fail("Query should have CBQ error'd")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 10109)
            self.assertTrue('nested_txn stopped after running beyond 120000 ms' in str(error), f"Error is not what we expected {str(ex)}")

    def test_transaction_multiple_node(self):
        self.run_cbq_query("DELETE FROM default.`_default`.txn_scope")
        function_name = 'nested_txn'
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            var start_txn = BEGIN WORK;\
            var query1 = INSERT INTO default.`_default`.txn_scope(key, value) VALUES (UUID(), {{"a":1, "b":2}}) ;\
            var query3 = COMMIT;\
        }}'
        self.create_library(self.library_name, functions, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        try:
            results = self.run_cbq_query(query="START TRANSACTION", txtimeout="2m", server=self.master)
            txid = results['results'][0]['txid']
            udf = self.run_cbq_query(f"EXECUTE FUNCTION {function_name}()", server=self.servers[1])
            results = self.run_cbq_query("select * from default.`_default`.txn_scope")
            expected_result = [{"txn_scope": {"a": 1,"b": 2}}]
            self.assertEqual(results['results'], expected_result,
                             f"Results are not as expected, expected: {expected_result} ,  actual: {results}")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.fail("Query should not have CBQ error'd")

    def test_dml_consume(self):
        function_name = 'consume_dml'
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            var query = UPDATE default SET a = "foo" WHERE job_title = "Engineer" RETURNING job_title;\
        }}'
        if self.explicit_close:
            functions = f'function {function_name}() {{\
                var query = UPDATE default SET a = "foo" WHERE job_title = "Engineer" RETURNING job_title;\
                query.close();\
            }}'
        self.run_cbq_query('UPDATE default SET a = "" WHERE job_title = "Engineer"')
        result = self.run_cbq_query('SELECT count(*) as count FROM default WHERE job_title = "Engineer"')
        expected_count = result['results'][0]['count']
        self.create_library(self.library_name, functions, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
        result = self.run_cbq_query('SELECT count(*) as count FROM default WHERE a = "foo"')
        actual_count = result['results'][0]['count']
        self.assertEqual(expected_count, actual_count)

    def test_error_handling(self):
        function_name = "error_handling"
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            try {{\
                var query1 = INSERT INTO default (KEY, VALUE) VALUES ("k004", {{"col1": 10 }});\
                var query2 = INSERT INTO default (KEY, VALUE) VALUES ("k004", {{"col1": 10 }});\
                return "Success!";\
            }} catch(error) {{\
                n1ql_error = JSON.parse(error.message);\
                return {{\
                    "caller": n1ql_error.caller,\
                    "code": n1ql_error.code,\
                    "reason": n1ql_error.cause,\
                    "icode": n1ql_error.icause,\
                    "key": n1ql_error.key,\
                    "message": n1ql_error.message,\
                    "retry": n1ql_error.retry,\
                    "stack": error.stack\
                }};\
            }}\
        }}'
        self.create_library(self.library_name, functions, function_names)
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}() LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')
        result = self.run_cbq_query(f"EXECUTE FUNCTION {function_name}()")
        expected_result = [
            {
                'caller': 'couchbase:2673', 'code': 12009, 'icode': 'Duplicate Key: k004',
                'key': 'datastore.couchbase.DML_error',
                'message': 'DML Error, possible causes include concurrent modification. Failed to perform INSERT on key k004',
                'reason': {'_level':'exception', 'caller': 'couchbase:2571', 'code': 17012, 'key': 'dml.statement.duplicatekey', 'message': 'Duplicate Key: k004'},
                'retry': False,
                'stack': 'Error\n    at error_handling (functions/n1ql.js:1:190)'
            }
        ]
        self.assertEqual(result['results'], expected_result)