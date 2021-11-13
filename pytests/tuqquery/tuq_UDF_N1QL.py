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
        }
    }
    dmls = {
        'select': 'SELECT d.* FROM default d ORDER BY META(d).id LIMIT 1',
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
        self.library_name = 'n1ql'
        self.log.info("==============  QueryUDFN1QLTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryUDFN1QLTests, self).suite_setUp()
        self.log.info("==============  QueryUDFN1QLTests suite_setup has started ==============")
        functions = 'function add(a, b) { return a + b; }'
        self.create_library('math', functions, 'add')
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
        breakpoint()
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
        actual_result = function_result['results'][0]
        self.assertEqual(actual_result, expected_curl)

    def test_flush_collection(self):
        function_name = 'flush_default'
        query = f'{self.statement} COLLECTION default'
        self.create_n1ql_function(function_name, query)
        # Execute function
        function_result = self.run_cbq_query(f'EXECUTE FUNCTION {function_name}()')
        self.log.info(function_result)

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
            param_val = {"job": "Engineer"}
            param_var = "$job"
        elif self.params == 'positional':
            param_val = ["Engineer"]
            param_var = "$1"
        functions = f'function {function_name}() {{\
            var query = EXECUTE engineer_count USING {param_val};\
            var acc = [];\
            for (const row of query) {{\
                acc.push(row);\
            }}\
            return acc;}}'
        self.create_library(self.library_name, functions, [function_name])
        # Prepare statement wit named parameter $job
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