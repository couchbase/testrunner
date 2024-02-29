from serverless.serverless_basetestcase import ServerlessBaseTestCase
from TestInput import TestInputSingleton
from membase.api.exception import CBQError
from lib.Cb_constants.CBServer import CbServer
import requests

class QuerySecurityTests(ServerlessBaseTestCase):
    def setUp(self):
        self.input = TestInputSingleton.input
        self.scope = '_default'
        self.collection = '_default'
        self.query = self.input.param("query", 'select')
        CbServer.capella_run = True
        return super().setUp()

    def tearDown(self):
        return super().tearDown()

    def test_query_authorization(self):
        self.provision_databases(2)
        keys = self.databases.keys()
        database1 = self.databases[list(keys)[0]]
        database2 = self.databases[list(keys)[1]]

        queries_database2 = {
            'select': f'SELECT * FROM `{database2.id}`.{self.scope}.{self.collection} LIMIT 1',
            'update': f'UPDATE `{database2.id}`.{self.scope}.{self.collection} SET name = "Portland"',
            'delete': f'DELETE FROM `{database2.id}`.{self.scope}.{self.collection}',
            'insert': f'INSERT INTO `{database2.id}`.{self.scope}.{self.collection} (key k, value v) SELECT uuid() as k , {{"name": "Portland"}} as v FROM array_range(0,10) d',
            'udf': f'EXECUTE FUNCTION `{database2.id}`.{self.scope}.f2()',
            'ddl': f'CREATE SCOPE `{database2.id}`.scope2',
            'prepare': f'EXECUTE p2'
        }
        insert1 = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {{"name": "San Francisco"}} as v FROM array_range(0,10) d'
        insert2 = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {{"name": "Paris"}} as v FROM array_range(0,10) d'
        result1 = self.run_query(database1, insert1)
        result2 = self.run_query(database2, insert2)
        udf2 = self.run_query(database2, f'CREATE or REPLACE FUNCTION {self.scope}.f2() {{(SELECT name FROM {self.collection})}}')
        prepare2 = self.run_query(database2, f'PREPARE p2 AS SELECT name FROM {self.collection}')
        try:
            result = self.run_query(database1, queries_database2[self.query])
            self.log.info(result['results'])
            self.fail("user1 should not be able to run query in database2")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 12037)
            self.assertEqual(error['msg'], f'User does not have access to {database2.id}')

    def test_query_context(self):
        self.provision_databases(2)
        keys = self.databases.keys()
        database1 = self.databases[list(keys)[0]]
        database2 = self.databases[list(keys)[1]]
        scope1 = self.run_query(database1, 'CREATE SCOPE scope1')
        scope2 = self.run_query(database2, 'CREATE SCOPE scope2')

        queries = ['SELECT * FROM system:scopes', 'SELECT 10+10']
        for query in queries:
            try:
                result = self.run_query(database1, query, query_params={'query_context': f'default:{database2.id}'})
                self.log.info(result['results'])
                self.fail('We should have got an HTTP exception 401')
            except requests.exceptions.HTTPError as ex:
                self.log.info('We got exception as expected')
                self.assertTrue('401 Client Error' in str(ex))

    def test_admin(self):
        self.provision_databases(1)
        keys = self.databases.keys()
        database1 = self.databases[list(keys)[0]]
        endpoints = {
            'vitals',
            'stats',
            'settings',
            'ping',
            'clusters',
            'config',
            'prepareds',
            'indexes/prepareds',
            'active_requests',
            'indexes/active_requests',
            'completed_requests',
            'indexes/completed_requests'
        }
        for endpoint in endpoints:
            with self.subTest(endpoint):
                self.log.info("="*40)
                self.log.info(f"===== SUBTEST: {endpoint}")
                try:
                    result = self.get_admin(database1, endpoint)
                    self.log.info(result)
                    if endpoint == 'ping':
                        self.assertEqual(result, {'status': 'OK'})
                    else:
                        self.fail('We should have got an HTTP exception 404')
                except requests.exceptions.HTTPError as ex:
                    if endpoint == 'ping':
                        self.fail(f'We should not get expection for ping: {str(ex)}')
                    else:
                        self.log.info('We got 404 as expected')
                        self.assertTrue('404 Client Error' in str(ex))

    def test_system_catalog(self):
        self.provision_databases(2)
        keys = self.databases.keys()
        database1 = self.databases[list(keys)[0]]
        database2 = self.databases[list(keys)[1]]

        # Create scope, index, prepared, function in each database
        scope1 = self.run_query(database1, 'CREATE SCOPE scope1')
        scope2 = self.run_query(database2, 'CREATE SCOPE scope2')
        index1 = self.run_query(database1, f'CREATE INDEX ix1 ON {self.collection}(name)')
        index2 = self.run_query(database2, f'CREATE INDEX ix2 ON {self.collection}(name)')
        prepare1 = self.run_query(database1, f'PREPARE p1 AS SELECT name FROM {self.collection}')
        prepare2 = self.run_query(database2, f'PREPARE p2 AS SELECT name FROM {self.collection}')
        udf1 = self.run_query(database1, f'CREATE or REPLACE FUNCTION {self.scope}.f1() {{(SELECT name FROM {self.collection})}}')
        udf2 = self.run_query(database2, f'CREATE or REPLACE FUNCTION {self.scope}.f2() {{(SELECT name FROM {self.collection})}}')
        js1 = self.run_query(database1, "CREATE or REPLACE FUNCTION _default.sleep1(delay) LANGUAGE JAVASCRIPT as 'function sleep1(delay) { var start = new Date().getTime(); while (new Date().getTime() < start + delay); return delay; }'")
        js2 = self.run_query(database2, "CREATE or REPLACE FUNCTION _default.sleep2(delay) LANGUAGE JAVASCRIPT as 'function sleep2(delay) { var start = new Date().getTime(); while (new Date().getTime() < start + delay); return delay; }'")
        sleep1 = self.run_query(database1, 'EXECUTE FUNCTION _default.sleep1(6000)')
        sleep2 = self.run_query(database2, 'EXECUTE FUNCTION _default.sleep2(6000)')

        all_keyspaces_1 = [
            {'path': f'default:{database1.id}'}, 
            {'path': f'default:{database1.id}._default._default'}, 
            {'path': f'default:{database1.id}._system._eventing'}, 
            {'path': f'default:{database1.id}._system._mobile'}, 
            {'path': f'default:{database1.id}._system._query'}, 
            {'path': f'default:{database1.id}._system._transactions'}, 
            {'path': 'system:active_requests'}, {'path': 'system:all_indexes'}, {'path': 'system:all_keyspaces'}, 
            {'path': 'system:all_keyspaces_info'}, {'path': 'system:all_scopes'}, {'path': 'system:buckets'}, 
            {'path': 'system:completed_requests'}, {'path': 'system:datastores'}, {'path': 'system:dual'}, 
            {'path': 'system:functions'}, {'path': 'system:indexes'}, {'path': 'system:keyspaces'}, 
            {'path': 'system:keyspaces_info'}, {'path': 'system:my_user_info'}, {'path': 'system:namespaces'}, 
            {'path': 'system:prepareds'}, {'path': 'system:scopes'}]
        all_keyspaces_2 = [
            {'path': f'default:{database2.id}'}, 
            {'path': f'default:{database2.id}._default._default'}, 
            {'path': f'default:{database2.id}._system._eventing'}, 
            {'path': f'default:{database2.id}._system._mobile'}, 
            {'path': f'default:{database2.id}._system._query'}, 
            {'path': f'default:{database2.id}._system._transactions'}, 
            {'path': 'system:active_requests'}, {'path': 'system:all_indexes'}, {'path': 'system:all_keyspaces'}, 
            {'path': 'system:all_keyspaces_info'}, {'path': 'system:all_scopes'}, {'path': 'system:buckets'}, 
            {'path': 'system:completed_requests'}, {'path': 'system:datastores'}, {'path': 'system:dual'}, 
            {'path': 'system:functions'}, {'path': 'system:indexes'}, {'path': 'system:keyspaces'}, 
            {'path': 'system:keyspaces_info'}, {'path': 'system:my_user_info'}, {'path': 'system:namespaces'}, 
            {'path': 'system:prepareds'}, {'path': 'system:scopes'}]

        system_catalog = {
            'completed_requests':  {'statement': 'SELECT `queryContext`, statement FROM system:completed_requests WHERE statement like "EXECUTE%"', 'expected1': [{'queryContext': f'default:{database1.id}', 'statement': 'EXECUTE FUNCTION _default.sleep1(6000)'}], 'expected2': [{'queryContext': f'default:{database2.id}', 'statement': 'EXECUTE FUNCTION _default.sleep2(6000)'}]},
            'active_requests': {'statement': 'SELECT `queryContext` FROM system:active_requests', 'expected1': [{'queryContext': f'default:{database1.id}'}], 'expected2': [{'queryContext': f'default:{database2.id}'}]},
            'all_indexes':  {'statement': 'SELECT DISTINCT name FROM system:all_indexes WHERE name not like "#%"', 'expected1': [{'name': 'ix1'}], 'expected2': [{'name': 'ix2'}]},
            'all_keyspaces':  {'statement': 'SELECT `path` FROM system:all_keyspaces ORDER BY `path`', 'expected1': all_keyspaces_1, 'expected2': all_keyspaces_2},
            'all_keyspaces_info':  {'statement': 'SELECT `path` FROM system:all_keyspaces_info ORDER BY `path`', 'expected1': all_keyspaces_1, 'expected2': all_keyspaces_2},
            'all_scopes':  {'statement': 'SELECT name FROM system:all_scopes ORDER BY name', 'expected1': [{'name': '_default'}, {'name': '_system'}, {'name': 'scope1'}], 'expected2': [{'name': '_default'}, {'name': '_system'}, {'name': 'scope2'}]},
            'buckets':  {'statement': 'SELECT name FROM system:buckets', 'expected1': [{'name': f'{database1.id}'}], 'expected2': [{'name': f'{database2.id}'}]},
            'datastores':  {'statement': 'SELECT * FROM system:datastores', 'expected1': [{'datastores': {'id': 'http://127.0.0.1:8091', 'url': 'http://127.0.0.1:8091'}}], 'expected2': [{'datastores': {'id': 'http://127.0.0.1:8091', 'url': 'http://127.0.0.1:8091'}}]},
            'dual':  {'statement': 'SELECT * FROM system:dual', 'expected1': [{'dual': None}], 'expected2': [{'dual': None}]},
            'functions':  {'statement': 'SELECT identity.name FROM system:functions', 'expected1': [{'name': 'f1'}, {'name': 'sleep1'}], 'expected2': [{'name': 'f2'}, {'name': 'sleep2'}]},
            'indexes':  {'statement': 'SELECT name FROM system:indexes', 'expected1': [{'name': 'ix1'}], 'expected2': [{'name': 'ix2'}]},
            'keyspaces':  {'statement': 'SELECT `path` FROM system:keyspaces', 'expected1': [{'path': f'default:{database1.id}'}], 'expected2': [{'path': f'default:{database2.id}'}]},
            'keyspaces_info':  {'statement': 'SELECT `path` FROM system:keyspaces_info', 'expected1': [{'path': f'default:{database1.id}'}], 'expected2': [{'path': f'default:{database2.id}'}]},
            'my_user_info':  {'statement': 'SELECT id FROM system:my_user_info', 'expected1': [{'id': f'{database1.access_key}'}], 'expected2': [{'id': f'{database2.access_key}'}]},
            'namespaces':  {'statement': 'SELECT * FROM system:namespaces', 'expected1': [{'namespaces': {'datastore_id': 'http://127.0.0.1:8091', 'id': 'default', 'name': 'default'}}], 'expected2': [{'namespaces': {'datastore_id': 'http://127.0.0.1:8091', 'id': 'default', 'name': 'default'}}]}, 
            'prepareds':  {'statement': 'SELECT DISTINCT name FROM system:prepareds WHERE name like "p%"', 'expected1': [{'name': f'p1(default:{database1.id})'}], 'expected2': [{'name': f'p2(default:{database2.id})'}]},
            'scopes':  {'statement': 'SELECT name FROM system:scopes', 'expected1': [{'name': 'scope1'}], 'expected2': [{'name': 'scope2'}]}
        }

        for catalog, query in system_catalog.items():
            with self.subTest(catalog):
                self.log.info("="*40)
                self.log.info(f"===== SUBTEST: {catalog}")
                result1 = self.run_query(database1, query['statement'])
                result2 = self.run_query(database2, query['statement'])
                self.log.info(f"{catalog}: {result1['results']}")
                self.log.info(f"{catalog}: {result2['results']}")
                self.assertEqual(result1['results'], query['expected1'])
                self.assertEqual(result2['results'], query['expected2'])
