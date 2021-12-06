from .tuq import QueryTests
from deepdiff import DeepDiff
import requests
from membase.api.exception import CBQError

class QueryUDFN1QLTests(QueryTests):
    ddls = {
        'create_index': {
            'pre': 'DROP INDEX udf_ix IF EXISTS on default',
            'query': 'CREATE INDEX udf_ix ON default(a)',
            'function_expected': [[]],
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
            'post': 'select `bucket`, `scope`, `collection`, `histogramKey` from `N1QL_SYSTEM_BUCKET`.`N1QL_SYSTEM_SCOPE`.`N1QL_CBO_STATS` data WHERE type = "histogram"',
            'post_expected' : [{"bucket": "default", "collection": "_default", "histogramKey": "job_title", "scope": "_default"}]
        },
        'analyze': {
            'pre': 'UPDATE STATISTICS FOR default DELETE ALL',
            'query': 'ANALYZE default(job_title)',
            'function_expected': [[]],
            'post': 'select `bucket`, `scope`, `collection`, `histogramKey` from `N1QL_SYSTEM_BUCKET`.`N1QL_SYSTEM_SCOPE`.`N1QL_CBO_STATS` data WHERE type = "histogram"',
            'post_expected' : [{"bucket": "default", "collection": "_default", "histogramKey": "job_title", "scope": "_default"}]
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
        'cte': 'WITH cte as (SELECT d.* FROM default d ORDER BY META(d).id LIMIT 1) SELECT * FROM cte'
    }

    def setUp(self):
        super(QueryUDFN1QLTests, self).setUp()
        self.log.info("==============  QueryUDFN1QLTests setup has started ==============")
        self.statement = self.input.param("statement", "statement")
        self.params = self.input.param("params", "named")
        self.inline_func = self.input.param("inline_func", False)
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
        self.log.info("==============  QueryUDFN1QLTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueryUDFN1QLTests tearDown has started ==============")
        self.log.info("==============  QueryUDFN1QLTests tearDown has completed ==============")
        super(QueryUDFN1QLTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QueryUDFN1QLTests suite_tearDown has started ==============")
        self.log.info("==============  QueryUDFN1QLTests suite_tearDown has completed ==============")
        super(QueryUDFN1QLTests, self).suite_tearDown()

    def create_n1ql_function(self, function_name, query):
        function_names = [function_name]
        functions = f'function {function_name}() {{\
            var query = {query};\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
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
        # Execute function
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
        self.assertEqual(function_result['results'], self.ddls[self.statement]['function_expected'])
        # Run post check
        post_query = self.ddls[self.statement]['post']
        post_expected = self.ddls[self.statement]['post_expected']
        post_actual = self.run_cbq_query(post_query)
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
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
        self.log.info(function_result)

    def test_param_function_param(self):
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
            self.assertTrue('User does not have credentials to run' in error['msg'])

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
            self.assertTrue('User does not have credentials to run' in error['msg'], f"Error is not what we expected {str(ex)}")

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
            self.assertTrue('Invalid data type for named parameters: "undefined" is not a valid type' in error['msg'], f"Error is not what we expected {str(ex)}")

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
            self.assertTrue('nested javascript calls' in error['msg'], f"Error is not what we expected {str(ex)}")

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
            self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
            self.fail("Query should have CBQ error'd")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 10109)
            self.assertTrue('Evaluator error Function timed out' in error['msg'], f"Error is not what we expected {str(ex)}")

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
            var acc = [];\
            if (x > 5){{ x = x - 1;\
            var query = {query};\
            for (const row of query){{\
                acc.push(row);}}}}\
            else{{ return acc; }}}}'
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

        # Execute function w/ correct query context
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}("Engineer", 2011, 10)', query_context='default:default._default')
        expected_result = [{'name': 'employee-1'}, {'name': 'employee-10'}, {'name': 'employee-11'}]
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, expected_result)

        # Execute function relative path should be picked up from the context of the udf
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION default:default._default.{function_name}("Engineer", 2011, 10)')
        expected_result = [{'name': 'employee-1'}, {'name': 'employee-10'}, {'name': 'employee-11'}]
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, expected_result)
        try:
            # Execute function w/ incorrect query context, incorrect function should be used
            function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}("Engineer", 2011, 10)', query_context='default:default.test')
            self.fail("Query should have CBQ error'd")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 10104)
            self.assertTrue('Incorrect number of arguments supplied' in error['msg'], f"Error is not what we expected {str(ex)}")

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
            self.assertTrue('No bucket named _default' in error['msg'], f"Error is not what we expected {str(ex)}")

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
            self.assertTrue('No bucket named _default' in error['msg'], f"Error is not what we expected {str(ex)}")

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
            var year = y.toString();\
            var month = m.toString();\
            var query_string = "SELECT " + projection + " FROM default WHERE job_title = " + job + "AND join_yr = " + year "AND join_mo = " + month + "ORDER by name LIMIT 3";\
            var query = N1QL(query_string);\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library(self.library_name, functions, [function_name])
        self.run_cbq_query(f'CREATE OR REPLACE FUNCTION {function_name}(...) LANGUAGE JAVASCRIPT AS "{function_name}" AT "{self.library_name}"')

        # Execute function
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}(1,"Engineer",2011,10)')
        expected_result = [{'name': 'employee-1'}, {'name': 'employee-10'}, {'name': 'employee-11'}]
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, expected_result)

        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}(0,"Engineer",2011,10)')
        expected_result = [{'job_title': 'Engineer'}, {'job_title': 'Engineer'}, {'job_title': 'Engineer'}]
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
        sleep_query = f"EXECUTE FUNCTION {function_name}(30000)"

        try:
            self.run_cbq_query(sleep_query, query_params={'timeout':'10s'})
            self.fail("Query should have CBQ error'd")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 10109)
            self.assertTrue('sleep stopped after running beyond 10000 ms' in error['msg'], f"Error is not what we expected {str(ex)}")

        try:
            self.run_cbq_query(sleep_query, query_params={'timeout':'600s'})
            self.fail("Query should have CBQ error'd")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 10109)
            self.assertTrue('sleep stopped after running beyond 120000 ms' in error['msg'], f"Error is not what we expected {str(ex)}")

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
            self.assertTrue('bucket does not exist' in error['msg'], f"Error is not what we expected {str(ex)}")