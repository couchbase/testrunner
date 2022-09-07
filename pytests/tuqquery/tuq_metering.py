from .tuq import QueryTests
import re
import requests
import json
import math
from lib.metering_throttling import metering

class QueryMeteringTests(QueryTests):
    def setUp(self):
        super(QueryMeteringTests, self).setUp()
        self.bucket = "default"
        self.doc_count = self.input.param("doc_count", 10)
        self.value_size = self.input.param("value_size", 256)
        self.with_returning = self.input.param("with_returning", False)
        self.statement = self.input.param("statement", "explain")
        self.kv_wu, self.kv_ru = 1024, 4096
        self.index_wu, self.index_ru = 1024, 256
        self.doc = {"name": "a"*self.value_size}
        self.index_key_size = len ("a"*self.value_size) # index is on name
        self.doc_key_size = 36 # use uuid()
        self.doc_size= len(json.dumps(self.doc))
        self.log.info(f'Doc count: {self.doc_count} - Doc key size is: {self.doc_key_size} - Doc size is: {self.doc_size}')
        self.meter = metering(self.server.ip, self.rest.username, self.rest.password)

    def suite_setUp(self):
        super(QueryMeteringTests, self).suite_setUp()
        self.use_cbo = self.input.param("use_cbo", False)
        if not self.use_cbo:
            api = f"http://{self.server.ip}:8091/settings/querySettings"
            data = {"queryUseCBO": "false"}
            response = requests.post(api, data=data, auth=(self.rest.username,self.rest.password), verify=False)
            self.log.info(f"Response: {response}")

    def tearDown(self):
        super(QueryMeteringTests, self).tearDown()

    def suite_tearDown(self):
        super(QueryMeteringTests, self).suite_tearDown()

    def test_kv_insert(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        # MB-53214
        # self.run_cbq_query(f'DELETE from {self.bucket}')

        expected_wu = self.doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_wu)
        if self.with_returning:
            insert_query = insert_query + " returning name"

        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)
        result = self.run_cbq_query(insert_query)
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)

        assert_query, msg = self.meter.assert_query_billing_unit(result, expected_wu, unit='wu', service='kv')
        self.assertTrue(assert_query, msg)
        self.assertTrue("ru" not in result['billingUnits'])
        self.assertEqual(before_kv_ru, after_kv_ru, f"KV RU before: {before_kv_ru} and after: {after_kv_ru} are different") # expect no KV reads

    def test_kv_upsert(self):
        upsert_doc_count = math.ceil(self.doc_count/5)
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        # MB-53214
        # self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(insert_query)
        result = self.run_cbq_query(f'SELECT raw meta().id FROM {self.bucket} LIMIT {upsert_doc_count}')
        meta_id = result['results']
        upsert_query = f'UPSERT INTO {self.bucket} (key k, value v) SELECT k , repeat("b", {self.value_size}) as v FROM {meta_id} k'

        expected_wu = upsert_doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_wu)
        if self.with_returning:
            upsert_query = upsert_query + " returning name"

        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)
        result = self.run_cbq_query(upsert_query)
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)

        assert_query, msg = self.meter.assert_query_billing_unit(result, expected_wu, unit='wu', service='kv')
        self.assertTrue(assert_query, msg)
        self.assertTrue("ru" not in result['billingUnits'])
        self.assertEqual(before_kv_ru, after_kv_ru, f"KV RU before: {before_kv_ru} and after: {after_kv_ru} are different") # expect no KV reads


    def test_kv_delete(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(insert_query)

        expected_wu = self.doc_count
        result = self.run_cbq_query(f'DELETE from {self.bucket}')
        assert_query, msg = self.meter.assert_query_billing_unit(result, expected_wu, unit='wu', service='kv')
        self.assertTrue(assert_query, msg)

    def test_kv_update(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {{"name": "San Francisco"}} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(insert_query)

        # Get sequential scan read unit
        result = self.run_cbq_query(f'SELECT meta().id FROM {self.bucket}')
        sc_ru = result['billingUnits']['ru']['kv']

        doc_read_size = self.doc_key_size + len(json.dumps({"name": "San Francisco"}))
        doc_write_size = self.doc_key_size + len(json.dumps({"name": "a"*self.value_size}))

        expected_wu = self.doc_count * math.ceil( doc_write_size / self.kv_wu)
        expected_ru = self.doc_count * math.ceil( doc_read_size / self.kv_ru)
        result = self.run_cbq_query(f'UPDATE {self.bucket} SET name = repeat("a", {self.value_size})')
        
        assert_query, msg = self.meter.assert_query_billing_unit(result, expected_wu, unit='wu', service='kv')
        self.assertTrue(assert_query, msg)
        assert_query, msg = self.meter.assert_query_billing_unit(result, expected_ru + sc_ru, unit='ru', service='kv')
        self.assertTrue(assert_query, msg)

    def test_kv_merge(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {{"name": "San Francisco"}} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(insert_query)

        doc_read_size = self.doc_key_size + len(json.dumps({"name": "San Francisco"}))
        doc_write_size = self.doc_key_size + len(json.dumps({"name": "San Francisco","country": "a"*self.value_size}))

        expected_wu = self.doc_count * math.ceil( doc_write_size / self.kv_wu)
        expected_ru = self.doc_count * math.ceil( doc_read_size / self.kv_ru)
        
        merge_query = f'MERGE INTO {self.bucket} t \
            USING [{{"name": "San Francisco", "country": "{"a"*self.value_size}"}}] source \
            ON t.name = source.name WHEN MATCHED \
            THEN UPDATE SET t.country = source.country'
        result = self.run_cbq_query(merge_query)
        self.meter.assert_query_billing_unit(result, expected_wu, unit='wu', service='kv')
        # Ru will include sequential scan ru which depends on vbucket placement
        # self.meter.assert_query_billing_unit(result, expected_ru, unit='ru', service='kv')
        actual_ru = result['billingUnits']['ru']['kv']
        self.assertTrue(actual_ru >= expected_ru)
        self.assertTrue(actual_ru <= expected_ru+self.doc_count*2)

    def test_kv_select(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(insert_query)

        # Get sequential scan read unit
        result = self.run_cbq_query(f'SELECT meta().id FROM {self.bucket}')
        sc_ru = result['billingUnits']['ru']['kv']

        expected_ru = self.doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_ru)
        result = self.run_cbq_query(f'SELECT * FROM {self.bucket}')
        assert_query, msg = self.meter.assert_query_billing_unit(result, expected_ru + sc_ru, unit='ru', service='kv')
        self.assertTrue(assert_query, msg)

    def test_kv_select_count(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(insert_query)

        # for count(*) we should get this from meta and no need to read from KV
        result = self.run_cbq_query(f'SELECT count(*) as count FROM {self.bucket}')
        self.assertTrue('billingUnits' not in result.keys() or 'ru' not in result['billingUnits'].keys(), f'We should not have got billingUnits[ru] from query')
        self.assertEqual(result['results'][0]['count'], self.doc_count, f'We expected {self.doc_count} but for diff result: {result}')

        # Get sequential scan read unit
        result = self.run_cbq_query(f'SELECT meta().id FROM {self.bucket}')
        sc_ru = result['billingUnits']['ru']['kv']

        # for count(field) we need to scan all KV doc
        expected_ru = self.doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_ru)
        result = self.run_cbq_query(f'SELECT count(name) as count FROM {self.bucket}')
        self.assertEqual(result['results'][0]['count'], self.doc_count, f'We expected {self.doc_count} but for diff result: {result}')
        assert_query, msg = self.meter.assert_query_billing_unit(result, expected_ru + sc_ru, unit='ru', service='kv')
        self.assertTrue(assert_query, msg)

    def test_kv_select_meta(self):
        options = {"expiration": 123456}
        self.doc["country"] = "France"
        self.doc_size = len(json.dumps(self.doc))
        insert_query = f'INSERT INTO {self.bucket} (key k, value v, options o) SELECT uuid() as k , {self.doc} as v, {options} o FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(insert_query)

        # Get sequential scan read unit
        result = self.run_cbq_query(f'SELECT meta().id FROM {self.bucket}')
        sc_ru = result['billingUnits']['ru']['kv']

        expected_ru = self.doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_ru)
        result = self.run_cbq_query(f'SELECT meta().expiration, country FROM {self.bucket}')
        assert_query, msg = self.meter.assert_query_billing_unit(result, expected_ru + sc_ru, unit='ru', service='kv')
        self.assertTrue(assert_query, msg)

    def test_kv_prepare(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        # MB-53214
        # self.run_cbq_query(f'DELETE from {self.bucket}')

        # Prepare statement should shows no bilingUnits
        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)
        result = self.run_cbq_query(f"PREPARE prepared_insert AS {insert_query}")
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)
        self.assertEqual(before_kv_ru, after_kv_ru)
        self.assertEqual(before_kv_wu, after_kv_wu)
        self.assertTrue('billingUnits' not in result)

        # Get sequential scan read unit
        result = self.run_cbq_query(f'SELECT meta().id FROM {self.bucket}')
        sc_ru = result['billingUnits']['ru']['kv']

        expected_wu = self.doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_wu)

        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)
        result = self.run_cbq_query(f"EXECUTE prepared_insert")
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)

        assert_query, msg = self.meter.assert_query_billing_unit(result, expected_wu + sc_ru, unit='wu', service='kv')
        self.assertTrue(assert_query, msg)
        self.assertTrue("ru" not in result['billingUnits'])
        self.assertEqual(before_kv_ru, after_kv_ru, f"KV RU before: {before_kv_ru} and after: {after_kv_ru} are different") # expect no KV reads

    def test_kv_inline_udf(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(insert_query)

        # Create function should shows no bilingUnits
        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)
        result = self.run_cbq_query(f"CREATE or REPLACE FUNCTION select_func() {{(SELECT * FROM {self.bucket})}}")
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)
        self.assertEqual(before_kv_ru, after_kv_ru)
        self.assertEqual(before_kv_wu, after_kv_wu)
        self.assertTrue('billingUnits' not in result)

        # Get sequential scan read unit
        result = self.run_cbq_query(f'SELECT meta().id FROM {self.bucket}')
        sc_ru = result['billingUnits']['ru']['kv']

        expected_ru = self.doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_ru)
        result = self.run_cbq_query(f'EXECUTE FUNCTION select_func()')
        assert_query, msg = self.meter.assert_query_billing_unit(result, expected_ru + sc_ru, unit='ru', service='kv')
        self.assertTrue(assert_query, msg)

    def test_gsi_select(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(insert_query)

        self.run_cbq_query(f'CREATE INDEX idx_name IF NOT EXISTS on {self.bucket}(name)')
        self.wait_for_all_indexes_online()

        expected_kv_ru = self.doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_ru)
        expected_index_ru = math.ceil(self.doc_count * (self.doc_key_size + self.index_key_size) / self.index_ru)
        result = self.run_cbq_query(f'SELECT name, city FROM {self.bucket} WHERE name = repeat("a", {self.value_size})')
        assert_query, msg = self.meter.assert_query_billing_unit(result, expected_kv_ru, unit='ru', service = 'kv')
        self.assertTrue(assert_query, msg)
        assert_query, msg = self.meter.assert_query_billing_unit(result, expected_index_ru, unit='ru', service = 'index')
        self.assertTrue(assert_query, msg)

    def test_gsi_create(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'DROP INDEX idx_name IF EXISTS on {self.bucket}')
        self.run_cbq_query(insert_query)

        before_index_ru, before_index_wu = self.meter.get_index_rwu(self.bucket)
        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)
        self.log.info(f"before_index_ru:{before_index_ru}, before_index_wu:{before_index_wu}, before_kv_ru:{before_kv_ru}, before_kv_wu:{before_kv_wu}")
        self.run_cbq_query(f'CREATE INDEX idx_name on {self.bucket}(name)')
        self.wait_for_all_indexes_online()
        after_index_ru, after_index_wu = self.meter.get_index_rwu(self.bucket)
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)
        self.log.info(f"after_index_ru:{after_index_ru}, after_index_wu:{after_index_wu}, after_kv_ru:{after_kv_ru}, after_kv_wu:{after_kv_wu}")

        expected_index_wu = self.doc_count * math.ceil((self.doc_key_size + self.index_key_size) / self.index_wu)
        expected_kv_ru = self.doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_ru)

        self.assertEqual(before_index_ru, after_index_ru) # no index ru
        self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
        self.assertEqual(before_kv_wu, after_kv_wu) # no kv wu
        self.assertEqual(expected_kv_ru, after_kv_ru - before_kv_ru)

    def test_no_rw_unit(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        # self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(insert_query)
        self.run_cbq_query(f'CREATE INDEX idx_name IF NOT EXISTS on {self.bucket}(name)')
        self.wait_for_all_indexes_online()
        queries = {
            'explain': f'EXPLAIN SELECT name, city FROM {self.bucket} WHERE name = repeat("a", {self.value_size})',
            'advise': f'ADVISE SELECT name, city FROM {self.bucket} WHERE name = repeat("a", {self.value_size})',
            'catalog': 'SELECT * FROM system:keyspaces',
            'query1': 'SELECT * FROM array_repeat("a", 10) as t'
        }
        before_index_ru, before_index_wu = self.meter.get_index_rwu(self.bucket)
        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)
        result = self.run_cbq_query(queries[self.statement])
        after_index_ru, after_index_wu = self.meter.get_index_rwu(self.bucket)
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)

        # No kv or index RWU
        self.assertTrue('billingUnits' not in result)
        self.assertEqual(before_index_ru, after_index_ru)
        self.assertEqual(before_index_wu, after_index_wu)
        self.assertEqual(before_kv_wu, after_kv_wu)
        self.assertEqual(before_kv_ru, after_kv_ru)

    def test_no_rw_scope(self):
        insert_query = f'INSERT INTO {self.bucket}.s1.c1 (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DROP SCOPE {self.bucket}.s1 IF EXISTS')

        # Create scope
        before_index_ru, before_index_wu = self.meter.get_index_rwu(self.bucket)
        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)
        result = self.run_cbq_query(f'CREATE SCOPE {self.bucket}.s1')
        after_index_ru, after_index_wu = self.meter.get_index_rwu(self.bucket)
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)

        # No kv or index RWU
        self.assertTrue('billingUnits' not in result)
        self.assertEqual(before_index_ru, after_index_ru)
        self.assertEqual(before_index_wu, after_index_wu)
        self.assertEqual(before_kv_wu, after_kv_wu)
        self.assertEqual(before_kv_ru, after_kv_ru)

        self.run_cbq_query(f'CREATE COLLECTION {self.bucket}.s1.c1')
        self.sleep(2)
        self.run_cbq_query(insert_query)
        self.run_cbq_query(f'CREATE INDEX idx_name on {self.bucket}.s1.c1(name)')
        self.wait_for_all_indexes_online()

        # Drop scope
        before_index_ru, before_index_wu = self.meter.get_index_rwu(self.bucket)
        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)
        result = self.run_cbq_query(f'DROP SCOPE {self.bucket}.s1')
        after_index_ru, after_index_wu = self.meter.get_index_rwu(self.bucket)
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)

        # No kv or index RWU
        self.assertTrue('billingUnits' not in result)
        self.assertEqual(before_index_ru, after_index_ru)
        self.assertEqual(before_index_wu, after_index_wu)
        self.assertEqual(before_kv_wu, after_kv_wu)
        self.assertEqual(before_kv_ru, after_kv_ru)

    def test_no_rw_collection(self):
        insert_query = f'INSERT INTO {self.bucket}.s1.c1 (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DROP SCOPE {self.bucket}.s1 IF EXISTS')
        self.run_cbq_query(f'CREATE SCOPE {self.bucket}.s1')

        # Create collection
        before_index_ru, before_index_wu = self.meter.get_index_rwu(self.bucket)
        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)
        result = self.run_cbq_query(f'CREATE COLLECTION {self.bucket}.s1.c1')
        after_index_ru, after_index_wu = self.meter.get_index_rwu(self.bucket)
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)

        self.sleep(2)
        self.run_cbq_query(insert_query)
        self.run_cbq_query(f'CREATE INDEX idx_name on {self.bucket}.s1.c1(name)')
        self.wait_for_all_indexes_online()

        # No kv or index RWU
        self.assertTrue('billingUnits' not in result)
        self.assertEqual(before_index_ru, after_index_ru)
        self.assertEqual(before_index_wu, after_index_wu)
        self.assertEqual(before_kv_wu, after_kv_wu)
        self.assertEqual(before_kv_ru, after_kv_ru)

        # Drop collection
        before_index_ru, before_index_wu = self.meter.get_index_rwu(self.bucket)
        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)
        result = self.run_cbq_query(f'DROP COLLECTION {self.bucket}.s1.c1')
        after_index_ru, after_index_wu = self.meter.get_index_rwu(self.bucket)
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)

        # No kv or index RWU
        self.assertTrue('billingUnits' not in result)
        self.assertEqual(before_index_ru, after_index_ru)
        self.assertEqual(before_index_wu, after_index_wu)
        self.assertEqual(before_kv_wu, after_kv_wu)
        self.assertEqual(before_kv_ru, after_kv_ru)

    def test_eval_cu(self):
        self.users = [{"id": "johnDoe", "name": "Jonathan Downing", "password": "password1"}]
        user_id = self.users[0]['id']
        user_pwd = self.users[0]['password']
        self.create_users()
        self.run_cbq_query(f"GRANT query_select ON {self.bucket} to {user_id}")

        # simple eval, we expect at least 1
        expected_cu = 1
        before_cu = self.meter.get_query_cu(self.bucket, variant="eval")
        result = self.run_cbq_query('SELECT 10+10', username=user_id, password=user_pwd, query_context=self.bucket)
        after_cu = self.meter.get_query_cu(self.bucket, variant="eval")
        self.assertEqual(expected_cu, after_cu - before_cu)

        before_cu = self.meter.get_query_cu(self.bucket, variant="eval")
        result = self.run_cbq_query('SELECT array_repeat(array_repeat(repeat("a",100), 500), 1000)', username=user_id, password=user_pwd, query_context=self.bucket )
        after_cu = self.meter.get_query_cu(self.bucket, variant="eval")
        # 1 compute unit is 32MB per second
        expected_cu = math.ceil(int(result['metrics']['usedMemory']) * float(result['metrics']['executionTime'][:-1]) / (32*1024*1024))
        self.assertEqual (expected_cu, after_cu - before_cu)

    def test_seqscan_multiple_vb(self):
        insert_query = f'INSERT INTO {self.bucket}(key k, value v) with dockeys as (\
            ["k000000156", "k000000082", "k000000056", "k000000012", "k000000013", "k000000057", "k000000083", "k000000157", "k000000081", "k000000155"]\
            ) SELECT k, {self.doc} as v FROM dockeys as k'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        # insert 10 docs, based on keys they should end up in (10) different vbucket
        self.run_cbq_query(insert_query)
        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)
        result = self.run_cbq_query(f'SELECT meta().id FROM {self.bucket}')
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)

        expected_sc_ru = 2 * 10 # 2 per vbucket
        self.assertTrue(after_kv_wu, before_kv_wu)
        assert_query, msg = self.meter.assert_query_billing_unit(result, expected_sc_ru, unit='ru', service='kv')
        self.assertTrue(assert_query, msg)
        self.assertTrue(expected_sc_ru, after_kv_ru - before_kv_ru)

    def test_seqscan_single_vb(self):
        insert_query = f'INSERT INTO {self.bucket}(key k, value v) with dockeys as (\
            ["k000000156", "k000000162", "k000000213", "k000000227", "k000000383", "k000000555", "k000000561", "k000000579", "k000000608", "k000000610"]\
            ) SELECT k, {self.doc} as v FROM dockeys as k'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        # insert 10 docs, based on keys they should end up in same vbucket
        self.run_cbq_query(insert_query)
        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)
        result = self.run_cbq_query(f'SELECT meta().id FROM {self.bucket}')
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)

        expected_sc_ru = 2 * 1 # 2 per vbucket
        self.assertTrue(after_kv_wu, before_kv_wu)
        assert_query, msg = self.meter.assert_query_billing_unit(result, expected_sc_ru, unit='ru', service='kv')
        self.assertTrue(assert_query, msg)
        self.assertTrue(expected_sc_ru, after_kv_ru - before_kv_ru)
