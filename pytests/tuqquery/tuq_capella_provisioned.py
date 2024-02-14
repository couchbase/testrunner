from pytests.tuqquery.tuq import QueryTests
import time
import math
import random

class QueryProvisionedTests(QueryTests):
    def setUp(self):
        super(QueryProvisionedTests, self).setUp()
        self.doc_count = 1000
        self.scope = 'scope1'
        self.collection = 'collection1'
        self.bucket = 'default'
        self.run_cbq_query(f'DELETE FROM {self.bucket}')
        self.run_cbq_query(f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {{"name": "San Francisco"}} as v FROM array_range(0,{self.doc_count}) d')

    def tearDown(self):
        pass

    def suite_setUp(self):
        pass

    def suite_tearDown(self):
        super(QueryProvisionedTests, self).tearDown()

    def check_index_online(self, name, desired_state='online', timeout=30):
        current_state = 'offline'
        stop = time.time() + timeout
        while (current_state != desired_state and time.time() < stop):
            result = self.run_cbq_query(f'SELECT raw state FROM system:indexes WHERE name = "{name}"')
            current_state = result['results'][0]
            self.log.info(f'INDEX {name} state: {current_state}')
        if current_state != desired_state:
            self.fail(f'INDEX {name} state: {current_state}, fail to reach state: {desired_state} within timeout: {timeout}')

    def test_create_scope(self):
        result = self.run_cbq_query(f'CREATE SCOPE {self.bucket}.{self.scope}')
        result = self.run_cbq_query(f'CREATE COLLECTION {self.bucket}.{self.scope}.{self.collection}')
        result = self.run_cbq_query(f'SELECT * FROM system:keyspaces WHERE name = "{self.collection}"')
        self.assertEqual(result['results'], [{'keyspaces': {'bucket': f'{self.bucket}', 'datastore_id': 'http://127.0.0.1:8091', 'id': f'{self.collection}', 'name': f'{self.collection}', 'namespace': 'default', 'namespace_id': 'default', 'path': f'default:{self.bucket}.{self.scope}.{self.collection}', 'scope': f'{self.scope}'}}])

    def test_insert(self):
        result = self.run_cbq_query(f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {{"name": "San Francisco"}} as v FROM array_range(0,{self.doc_count}) d')
        self.assertEqual(result['metrics']['mutationCount'], self.doc_count)

    def test_primary_scan(self):
        explain = self.run_cbq_query(f'EXPLAIN SELECT * FROM {self.bucket}')
        self.assertEqual(explain['results'][0]['plan']['~children'][0]['index'], '#primary')
        result = self.run_cbq_query(f'SELECT * FROM {self.bucket}')
        self.assertEqual(result['metrics']['resultCount'], self.doc_count)

    def test_prepare_statement(self):
        prepare = self.run_cbq_query(f'PREPARE p1 AS SELECT name FROM {self.bucket}')
        result = self.run_cbq_query(f'EXECUTE p1')
        self.assertEqual(result['metrics']['resultCount'], self.doc_count)

    def test_inline_udf(self):
        udf = self.run_cbq_query(f'CREATE or REPLACE FUNCTION f1() {{(SELECT name FROM {self.bucket})}}')
        result = self.run_cbq_query(f'EXECUTE FUNCTION f1()')
        self.assertEqual(result['metrics']['resultCount'], 1)
        self.assertEqual(len(result['results'][0]), self.doc_count)

    def test_transaction(self):
        begin = self.run_cbq_query('BEGIN WORK')
        txid = begin['results'][0]['txid']
        result = self.run_cbq_query(f'UPDATE {self.bucket} SET name = "Paris"', txnid=txid)
        self.assertEqual(result['metrics']['mutationCount'], self.doc_count)
        rollback = self.run_cbq_query('ROLLBACK', txnid=txid)
        result = self.run_cbq_query(f'SELECT count(*) as Paris FROM {self.bucket} WHERE name = "Paris"')
        self.assertEqual(result['results'], [{'Paris': 0}])

    def test_aggregate(self):
        result = self.run_cbq_query(f'SELECT name, count(*) as count FROM {self.bucket} GROUP BY name')
        self.assertEqual(result['results'], [{'count': self.doc_count, 'name': 'San Francisco'}])

    def test_join(self):
        self.run_cbq_query(f'CREATE INDEX idx_name ON {self.bucket}(name)')
        self.check_index_online('idx_name')
        result = self.run_cbq_query(f'SELECT count(*) as count FROM {self.bucket} a JOIN {self.bucket} b ON a.name = b.name')
        self.assertEqual(result['results'], [{"count": self.doc_count * self.doc_count}])
        self.run_cbq_query(f'DROP INDEX idx_name ON {self.bucket}')

    def test_update_statistics(self):
        result = self.run_cbq_query(f'UPDATE STATISTICS FOR {self.bucket}(name)')
        result = self.run_cbq_query(f'SELECT `scope`, `collection`, `histogramKey` FROM `{self.bucket}`.`_system`.`_query` WHERE `scope` = "_default" AND `collection` = "_default" AND `histogramKey` = "name"')
        self.log.info(f"results: {result['results']}")
        self.assertEqual(result['results'], [{'collection': '_default', 'histogramKey': 'name', 'scope': '_default'}])
        explain = self.run_cbq_query(f'EXPLAIN SELECT * FROM {self.bucket} WHERE name is not null')
        self.assertTrue(explain['results'][0]['cost'] > 0)

    def test_cte(self):
        result = self.run_cbq_query(f'WITH city as (SELECT t.* FROM {self.bucket} t) SELECT city.name FROM city')
        self.assertEqual(result['metrics']['resultCount'], self.doc_count)
        self.assertEqual(result['results'][0], {'name': 'San Francisco'})

    def test_advise(self):
        advise1 = self.run_cbq_query(f'ADVISE SELECT * FROM {self.bucket} WHERE name is not null')
        expected_index = f'CREATE INDEX adv_name ON `{self.bucket}`(`name`)'
        self.assertEqual(advise1['results'][0]['advice']['adviseinfo']['recommended_indexes']['indexes'][0]['index_statement'], expected_index)

    def test_advisor(self):
        query_lyon=f'SELECT airportname FROM `{self.bucket_name}` WHERE lower(city) = "lyon" AND country = "France"'
        query_grenoble=f'SELECT airportname FROM `{self.bucket_name}` WHERE lower(city) = "grenoble" AND country = "France"'
        query_nice=f'SELECT airportname FROM `{self.bucket_name}` WHERE lower(city) = "nice" AND country = "France"'

        start = self.run_cbq_query("SELECT ADVISOR({'action': 'start', 'duration': '15m', 'query_count': 6})")
        session = start['results'][0]['$1']['session']
        # Run 9 queries 
        results = self.run_cbq_query(query_lyon)
        results = self.run_cbq_query(query_grenoble)
        results = self.run_cbq_query(query_nice)
        results = self.run_cbq_query(query_lyon)
        results = self.run_cbq_query(query_grenoble)
        results = self.run_cbq_query(query_lyon)
        results = self.run_cbq_query(query_nice)
        results = self.run_cbq_query(query_grenoble)
        results = self.run_cbq_query(query_nice)
        # Stop and check session advise. We should only see 6 queries count = 3*lyon + 2*grenoble + 1*nice
        stop = self.run_cbq_query(f"SELECT ADVISOR({{'action':'stop', 'session':'{session}'}}) as Stop")
        get = self.run_cbq_query(f"SELECT ADVISOR({{'action':'get', 'session':'{session}'}}) as Get")
        queries_count = dict()
        for index in get['results'][0]['Get'][0][0]['recommended_indexes']:
            for query in index['statements']:
                queries_count[query['statement']] = query['run_count']
        self.assertEqual(queries_count[query_lyon], 3)
        self.assertEqual(queries_count[query_grenoble], 2)
        self.assertEqual(queries_count[query_nice], 1)        

    def test_index_scan(self):
        self.run_cbq_query(f'CREATE INDEX idx_name ON {self.bucket}(name)')
        self.check_index_online('idx_name')
        result = self.run_cbq_query(f'SELECT * FROM system:indexes WHERE name = "idx_name"')
        explain = self.run_cbq_query(f'EXPLAIN SELECT * FROM {self.bucket} WHERE name is not null')
        self.assertEqual(explain['results'][0]['plan']['~children'][0]['index'], 'idx_name')
        result = self.run_cbq_query(f'SELECT * FROM {self.bucket} WHERE name is not null')
        self.assertEqual(result['metrics']['resultCount'], self.doc_count)

    def test_index_scan_covered(self):
        self.run_cbq_query(f'CREATE INDEX idx_name IF NOT EXISTS ON {self.bucket}(name)')
        self.check_index_online('idx_name')
        explain = self.run_cbq_query(f'EXPLAIN SELECT name FROM {self.bucket} WHERE name is not null')
        self.assertEqual(explain['results'][0]['plan']['~children'][0]['covers'], [f'cover ((`{self.bucket}`.`name`))', f'cover ((meta(`{self.bucket}`).`id`))'])
        result = self.run_cbq_query(f'SELECT name FROM {self.bucket} WHERE name is not null')
        self.assertEqual(result['metrics']['resultCount'], self.doc_count)

    def test_array_index(self):
        fruits = [
            "apple", "banana", "pear", "peach", "apricot",
            "date", "grape", "plum", "kiwi", "mango", "avocado",
            "strawberry", "raspberry", "blueberry", "gooseberry",
            "currant", "watermelon", "melon", "cherry", "tomato"
        ]
        for n in range(0,10):
            insert = self.run_cbq_query(f'INSERT INTO {self.bucket} (key, value) values (uuid(), {{ "store": "Store-{n}", "fruits": {random.sample(fruits, 5)} }} )')
        index = self.run_cbq_query(f'CREATE INDEX index_arr ON {self.bucket}(ALL ARRAY f FOR f IN fruits END)')
        self.check_index_online('index_arr')
        explain = self.run_cbq_query(f'EXPLAIN SELECT s.store FROM {self.bucket} s WHERE ANY f IN s.fruits SATISFIES f = "apple" END')
        self.assertEqual(explain['results'][0]['plan']['~children'][0]['scan']['index'], 'index_arr')
        result = self.run_cbq_query(f'SELECT * FROM {self.bucket} s WHERE ANY f IN s.fruits SATISFIES f = "apple" END')
        doc_count = result['metrics']['resultCount']
        self.log.info(f"Fruit result: {result['results']}")

    def test_sanity(self):
            with self.subTest('UDF javascript'):
                self.log.info("="*40)
                self.log.info("===== SUBTEST: udf javascript")
                udf = self.run_cbq_query(f"CREATE or REPLACE FUNCTION {self.scope}.plus(a,b) LANGUAGE JAVASCRIPT as 'function plus(a,b) {{return a+b}}'")
                result = self.run_cbq_query(f'EXECUTE FUNCTION {self.scope}.plus(32,68)')
                self.assertEqual(result['results'], [100])
            with self.subTest('UDF javascript n1ql'):
                self.log.info("="*40)
                self.log.info("===== SUBTEST: udf javascript n1ql")
                udf = self.run_cbq_query(f"CREATE or REPLACE FUNCTION {self.scope}.select_js(city, num) LANGUAGE JAVASCRIPT as 'function select_js(city, num) {{\
                    var query = SELECT name FROM {self.collection} WHERE name = $city LIMIT $num;\
                    var acc = [];\
                    for (const row of query) {{ acc.push(row); }}\
                    return acc;}}'")
                result = self.run_cbq_query(f'EXECUTE FUNCTION {self.scope}.select_js("San Francisco", 5)')
                self.assertEqual(result['results'], [[{'name': 'San Francisco'}, {'name': 'San Francisco'}, {'name': 'San Francisco'}, {'name': 'San Francisco'}, {'name': 'San Francisco'}]])
