from serverless.serverless_basetestcase import ServerlessBaseTestCase
import time
import math
import random
from lib.Cb_constants.CBServer import CbServer

class QuerySanity(ServerlessBaseTestCase):
    def setUp(self):
        self.doc_count = 1000
        self.scope = 'scope1'
        self.collection = 'collection1'
        self.index_ru = 256
        CbServer.capella_run = True
        return super().setUp()

    def tearDown(self):
        return super().tearDown()

    def check_index_online(self, database, name, timeout = 30, desired_state='online'):
        current_state = 'offline'
        stop = time.time() + timeout
        while (current_state != desired_state and time.time() < stop):
            result = self.run_query(database, f'SELECT raw state FROM system:indexes WHERE name = "{name}"')
            current_state = result['results'][0]
            self.log.info(f'INDEX {name} state: {current_state}')
        if current_state != desired_state:
            self.fail(f'INDEX {name} state: {current_state}, fail to reach state: {desired_state} within timeout: {timeout}')

    def test_sanity(self):
        self.provision_databases()
        for database in self.databases.values():
            with self.subTest('create scope/collection'):
                self.log.info("="*40)
                self.log.info("===== SUBTEST: create scope/collection")
                result = self.run_query(database, f'CREATE SCOPE {self.scope}')
                result = self.run_query(database, f'CREATE COLLECTION {self.scope}.{self.collection}')
                result = self.run_query(database, f'SELECT * FROM system:keyspaces WHERE name = "{self.collection}"')
                self.assertEqual(result['results'], [{'keyspaces': {'bucket': f'{database.id}', 'datastore_id': 'http://127.0.0.1:8091', 'id': f'{self.collection}', 'name': f'{self.collection}', 'namespace': 'default', 'namespace_id': 'default', 'path': f'default:{database.id}.{self.scope}.{self.collection}', 'scope': f'{self.scope}'}}])
            with self.subTest('insert'):
                self.log.info("="*40)
                self.log.info("===== SUBTEST: insert")
                result = self.run_query(database, f'INSERT INTO {self.scope}.{self.collection} (key k, value v) SELECT uuid() as k , {{"name": "San Francisco"}} as v FROM array_range(0,{self.doc_count}) d')
                self.assertEqual(result['billingUnits'], {'wu': {'kv': self.doc_count}})
                self.assertEqual(result['metrics']['mutationCount'], self.doc_count)
            with self.subTest('sequential scan'):
                self.log.info("="*40)
                self.log.info("===== SUBTEST: sequential scan")
                explain = self.run_query(database, f'EXPLAIN SELECT * FROM {self.scope}.{self.collection}')
                self.assertEqual(explain['results'][0]['plan']['~children'][0]['index'], '#sequentialscan')
                result = self.run_query(database, f'SELECT * FROM {self.scope}.{self.collection}')
                self.assertEqual(result['metrics']['resultCount'], self.doc_count)
                self.assertEqual(result['billingUnits'], {'ru': {'kv': self.doc_count+64*2}})
            with self.subTest('prepare statement'):
                self.log.info("="*40)
                self.log.info("===== SUBTEST: prepare statement")
                prepare = self.run_query(database, f'PREPARE p1 AS SELECT name FROM {self.scope}.{self.collection}')
                result = self.run_query(database, f'EXECUTE p1')
                self.assertEqual(result['metrics']['resultCount'], self.doc_count)
                self.assertEqual(result['billingUnits'], {'ru': {'kv': self.doc_count+64*2}})
            with self.subTest('UDF inline'):
                self.log.info("="*40)
                self.log.info("===== SUBTEST: udf inline")
                udf = self.run_query(database, f'CREATE or REPLACE FUNCTION {self.scope}.f1() {{(SELECT name FROM {self.collection})}}')
                result = self.run_query(database, f'EXECUTE FUNCTION {self.scope}.f1()')
                self.assertEqual(result['metrics']['resultCount'], 1)
                self.assertEqual(len(result['results'][0]), self.doc_count)
                self.assertEqual(result['billingUnits'], {'ru': {'kv': self.doc_count+64*2}})
            with self.subTest('UDF javascript'):
                self.log.info("="*40)
                self.log.info("===== SUBTEST: udf javascript")
                udf = self.run_query(database, f"CREATE or REPLACE FUNCTION {self.scope}.plus(a,b) LANGUAGE JAVASCRIPT as 'function plus(a,b) {{return a+b}}'")
                result = self.run_query(database, f'EXECUTE FUNCTION {self.scope}.plus(32,68)')
                self.assertEqual(result['results'], [100])
            with self.subTest('UDF javascript n1ql'):
                self.log.info("="*40)
                self.log.info("===== SUBTEST: udf javascript n1ql")
                udf = self.run_query(database, f"CREATE or REPLACE FUNCTION {self.scope}.select_js(city, num) LANGUAGE JAVASCRIPT as 'function select_js(city, num) {{\
                    var query = SELECT name FROM {self.collection} WHERE name = $city LIMIT $num;\
                    var acc = [];\
                    for (const row of query) {{ acc.push(row); }}\
                    return acc;}}'")
                result = self.run_query(database, f'EXECUTE FUNCTION {self.scope}.select_js("San Francisco", 5)')
                self.assertEqual(result['results'], [[{'name': 'San Francisco'}, {'name': 'San Francisco'}, {'name': 'San Francisco'}, {'name': 'San Francisco'}, {'name': 'San Francisco'}]])
            with self.subTest('transaction'):
                self.log.info("="*40)
                self.log.info("===== SUBTEST: transaction")
                begin = self.run_query(database, 'BEGIN WORK')
                txid = begin['results'][0]['txid']
                result = self.run_query(database, f'UPDATE {self.scope}.{self.collection} SET name = "Paris"', txnid=txid)
                self.assertEqual(result['metrics']['mutationCount'], self.doc_count)
                rollback = self.run_query(database, 'ROLLBACK', txnid=txid)
                result = self.run_query(database, f'SELECT count(*) as Paris FROM {self.scope}.{self.collection} WHERE name = "Paris"')
                self.assertEqual(result['results'], [{'Paris': 0}])
            with self.subTest('Aggregate'):
                self.log.info("="*40)
                self.log.info("===== SUBTEST: aggregate")
                result = self.run_query(database, f'SELECT name, count(*) as count FROM {self.scope}.{self.collection} GROUP BY name')
                self.assertEqual(result['results'], [{'count': self.doc_count, 'name': 'San Francisco'}])
                self.assertEqual(result['billingUnits'], {'ru': {'kv': self.doc_count+64*2}})
            with self.subTest('Join'):
                self.log.info("="*40)
                self.log.info("===== SUBTEST: join")
                result = self.run_query(database, f'SELECT count(*) as count FROM {self.scope}.{self.collection} a JOIN {self.scope}.{self.collection} b ON a.name = b.name')
                # self.assertEqual(result['billingUnits'], {'ru': {'kv': (self.doc_count+64*2)*2}})
                self.assertEqual(result['results'], [{"count": self.doc_count * self.doc_count}])
            with self.subTest('Update Statistics'):
                self.log.info("="*40)
                self.log.info("===== SUBTEST: update statistics")
                result = self.run_query(database, f'UPDATE STATISTICS FOR {self.scope}.{self.collection}(name)')
                result = self.run_query(database, f'SELECT `scope`, `collection`, `histogramKey` FROM _system._query WHERE `scope` = "{self.scope}" AND `collection` = "{self.collection}" AND `histogramKey` = "name"')
                self.assertEqual(result['results'], [{'collection': self.collection, 'histogramKey': 'name', 'scope': self.scope}])
            with self.subTest('CTE'):
                self.log.info("="*40)
                self.log.info("===== SUBTEST: CTE")
                result = self.run_query(database, f'WITH city as (SELECT t.* FROM {self.scope}.{self.collection} t) SELECT city.name FROM city')
                self.assertEqual(result['metrics']['resultCount'], self.doc_count)
                self.assertEqual(result['results'][0], {'name': 'San Francisco'})
                self.assertEqual(result['billingUnits'], {'ru': {'kv': self.doc_count+64*2}})
            with self.subTest('Advise'):
                self.log.info("="*40)
                self.log.info("===== SUBTEST: advise")
                advise1 = self.run_query(database, f'ADVISE SELECT * FROM {self.scope}.{self.collection} WHERE name is not null')
                advise2 = self.run_query(database, f'ADVISE SELECT * FROM {self.scope}.{self.collection} WHERE name is not null')
                expected_index = f'CREATE INDEX adv_name ON `default`:`{database.id}`.`{self.scope}`.`{self.collection}`(`name`)'
                self.assertEqual(advise1['results'][0]['advice']['adviseinfo']['recommended_indexes']['indexes'][0]['index_statement'], expected_index)
            with self.subTest('Index Scan (not covered)'):
                self.log.info("="*40)
                self.log.info("===== SUBTEST: index scan (not covered)")
                self.run_query(database, f'CREATE INDEX idx_name ON {self.scope}.{self.collection}(name)')
                self.check_index_online(database, 'idx_name')
                result = self.run_query(database, f'SELECT * FROM system:indexes WHERE name = "idx_name"')
                explain = self.run_query(database, f'EXPLAIN SELECT * FROM {self.scope}.{self.collection} WHERE name is not null')
                self.assertEqual(explain['results'][0]['plan']['~children'][0]['index'], 'idx_name')
                result = self.run_query(database, f'SELECT * FROM {self.scope}.{self.collection} WHERE name is not null')
                self.assertEqual(result['metrics']['resultCount'], self.doc_count)
                index_ru = math.ceil(self.doc_count * (36 + len('San Francisco')) / self.index_ru * 1.1)
                self.assertEqual(result['billingUnits'], {'ru': {'index': index_ru, 'kv': self.doc_count}})
            with self.subTest('Index Scan (covered)'):
                self.log.info("="*40)
                self.log.info("===== SUBTEST: index scan (covered)")
                explain = self.run_query(database, f'EXPLAIN SELECT name FROM {self.scope}.{self.collection} WHERE name is not null')
                self.assertEqual(explain['results'][0]['plan']['~children'][0]['covers'], [f'cover ((`{self.collection}`.`name`))', f'cover ((meta(`{self.collection}`).`id`))'])
                result = self.run_query(database, f'SELECT name FROM {self.scope}.{self.collection} WHERE name is not null')
                self.assertEqual(result['metrics']['resultCount'], self.doc_count)
                self.assertEqual(result['billingUnits'], {'ru': {'index': index_ru}})
            with self.subTest('Array Index'):
                self.log.info("="*40)
                self.log.info("===== SUBTEST: Array index scan")
                fruits = ["apple", "banana", "pear", "peach", "apricot", "date", "grape", "plum", "kiwi", "mango", "avocado", "strawberry", "raspberry", "blueberry", "gooseberry", "currant", "watermelon", "melon", "cherry", "tomato"]
                for n in range(0,10):
                    insert = self.run_query(database, f'INSERT INTO {self.scope}.{self.collection} (key, value) values (uuid(), {{ "store": "Store-{n}", "fruits": {random.sample(fruits, 5)} }} )')
                index = self.run_query(database, f'CREATE INDEX index_arr ON {self.scope}.{self.collection}(ALL ARRAY f FOR f IN fruits END)')
                self.check_index_online(database, 'index_arr')
                result = self.run_query(database, f'SELECT raw fruits[0] FROM {self.scope}.{self.collection} WHERE fruits is not missing LIMIT 1')
                fruit = result['results'][0]
                explain = self.run_query(database, f'EXPLAIN SELECT s.store FROM {self.scope}.{self.collection} s WHERE ANY f IN s.fruits SATISFIES f = "{fruit}" END')
                self.assertEqual(explain['results'][0]['plan']['~children'][0]['scan']['index'], 'index_arr')
                result = self.run_query(database, f'SELECT * FROM {self.scope}.{self.collection} s WHERE ANY f IN s.fruits SATISFIES f = "{fruit}" END')
                doc_count = result['metrics']['resultCount']
                index_ru = math.ceil(doc_count * (36 + len(fruit)) / 256)
                self.assertEqual(result['billingUnits'], {'ru': {'index': index_ru, 'kv': doc_count}} )
