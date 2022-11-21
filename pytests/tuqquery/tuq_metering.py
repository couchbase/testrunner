from .tuq import QueryTests
import requests
import json
import ast
import math
from lib.metering_throttling import metering, throttling
from membase.api.exception import CBQError

class QueryMeteringTests(QueryTests):
    def setUp(self):
        super(QueryMeteringTests, self).setUp()
        self.bucket = "default"
        self.doc_count = self.input.param("doc_count", 10)
        self.value_size = self.input.param("value_size", 256)
        self.with_returning = self.input.param("with_returning", False)
        self.statement = self.input.param("statement", "explain")
        self.create_distinct = self.input.param("create_distinct", False)
        self.kv_wu, self.kv_ru = 1024, 4096
        self.index_wu, self.index_ru = 1024, 256
        self.doc = {"name": "a"*self.value_size}
        self.composite_doc = {"fname": "a"*self.value_size, "lname":"b"*self.value_size}
        self.array_doc = {"names": [{"fname":"a"*(int(self.value_size/6)),"lname":"b"*(int(self.value_size/6))},{"fname":"a"*(int(self.value_size/6)),"lname":"d"*(int(self.value_size/6))},{"fname":"e"*(int(self.value_size/6)),"lname":"f"*(int(self.value_size/6))}]}
        self.index_key_size = len ("a"*self.value_size) # index is on name
        self.array_index_key_size = len("a"*(int(self.value_size/6))) + len("a"*(int(self.value_size/6))) + len("e"*(int(self.value_size/6)))
        self.distinct_array_index_key_size = len("a"*(int(self.value_size/6))) + len("e"*(int(self.value_size/6)))
        self.composite_index_key_size = len ("a"*self.value_size) + len("b"*self.value_size)
        self.doc_key_size = 36 # use uuid()
        self.doc_size= len(json.dumps(self.doc))
        self.composite_doc_size = len(json.dumps(self.composite_doc))
        self.log.info(f'Doc count: {self.doc_count} - Doc key size is: {self.doc_key_size} - Doc size is: {self.doc_size}')
        self.meter = metering(self.server.ip, self.rest.username, self.rest.password)
        self.throttle = throttling(self.server.ip, self.rest.username, self.rest.password)

    def suite_setUp(self):
        super(QueryMeteringTests, self).suite_setUp()
        self.use_cbo = self.input.param("use_cbo", True)
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

    def test_node_quota(self):
        insert = f'insert into {self.bucket} (key, value) values("1", {{ "abcd": {{ "def": repeat("abcd", 29990), "arr": array_repeat(repeat("efgh", 1000), 5000)}}}})'
        self.run_cbq_query(insert)
        node_quota = self.throttle.get_query_settings('node-quota')
        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)
        before_credit = self.meter.get_credit_unit(self.bucket, 'ru', 'kv')
        try:
            self.throttle.set_query_settings('node-quota', 10)
            result = self.run_cbq_query(f'SELECT * FROM _default', query_context=self.bucket)
        except CBQError as ex:
            after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)
            content = ast.literal_eval(str(ex).split("ERROR:")[1])
            result = json.loads(json.dumps(content))
            self.assertEqual(result['refundedUnits'], result['billingUnits'])
            self.assertEqual(result['errors'], [{'code': 5600, 'msg': 'Query node has run out of memory'}])
            after_credit = self.meter.get_credit_unit(self.bucket, 'ru', 'kv')
            self.assertEqual(after_credit - before_credit, after_kv_ru - before_kv_ru)
        finally:
            self.throttle.set_query_settings('node-quota', 0)

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

    def test_inline_udf(self):
        result = self.run_cbq_query(f"CREATE or REPLACE FUNCTION default:{self.bucket}._default.plusone(a) {{ a + 1 }}")
        self.assertTrue('billingUnits' not in result)
        result = self.run_cbq_query(f"EXECUTE FUNCTION default:{self.bucket}._default.plusone(10)")
        self.assertTrue('billingUnits' not in result)
        self.assertTrue(result['results'], [11])

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
        expected_index_ru = math.ceil(self.doc_count * 1.1 * (self.doc_key_size + self.index_key_size) / self.index_ru)
        result = self.run_cbq_query(f'SELECT name, city FROM {self.bucket} WHERE name = repeat("a", {self.value_size})')
        assert_query, msg = self.meter.assert_query_billing_unit(result, expected_kv_ru, unit='ru', service = 'kv')
        self.assertTrue(assert_query, msg)
        assert_query, msg = self.meter.assert_query_billing_unit(result, expected_index_ru, unit='ru', service = 'index')
        self.assertTrue(assert_query, msg)

    def test_select_primary(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'DROP PRIMARY INDEX IF EXISTS on {self.bucket}')
        self.run_cbq_query(insert_query)

        self.run_cbq_query(f'CREATE PRIMARY INDEX ON default')
        self.wait_for_all_indexes_online()

        expected_kv_ru = self.doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_ru)
        expected_index_ru = math.ceil(self.doc_count * 1.1 * (self.doc_key_size) / self.index_ru)
        result = self.run_cbq_query(f'SELECT name, city FROM {self.bucket} WHERE name = repeat("a", {self.value_size})')
        assert_query, msg = self.meter.assert_query_billing_unit(result, expected_kv_ru, unit='ru', service = 'kv')
        self.assertTrue(assert_query, msg)
        assert_query, msg = self.meter.assert_query_billing_unit(result, expected_index_ru, unit='ru', service = 'index')
        self.assertTrue(assert_query, msg)

    def test_select_non_covering(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.composite_doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(insert_query)

        self.run_cbq_query(f'CREATE INDEX idx_name IF NOT EXISTS on {self.bucket}(fname)')
        self.wait_for_all_indexes_online()

        expected_kv_ru = self.doc_count * math.ceil( (self.composite_doc_size + self.doc_key_size) / self.kv_ru)
        expected_index_ru = math.ceil(self.doc_count * 1.1 * (self.doc_key_size + self.index_key_size) / self.index_ru)
        result = self.run_cbq_query(f'SELECT fname, lname FROM {self.bucket} WHERE fname = repeat("a", {self.value_size}) and lname = repeat("b", {self.value_size})')
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
        self.run_cbq_query(f'CREATE INDEX idx_name on {self.bucket}(name)')
        self.wait_for_all_indexes_online()
        after_index_ru, after_index_wu = self.meter.get_index_rwu(self.bucket)
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)

        expected_index_wu = self.doc_count * math.ceil((self.doc_key_size + self.index_key_size) / self.index_wu)

        self.assertEqual(before_index_ru, after_index_ru) # no index ru
        self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
        self.assertEqual(before_kv_wu, after_kv_wu) # no kv wu
        self.assertEqual(after_kv_ru, before_kv_ru)

    def test_gsi_create_deferred(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'DROP INDEX idx_name IF EXISTS on {self.bucket}')
        self.run_cbq_query(insert_query)

        before_index_ru, before_index_wu = self.meter.get_index_rwu(self.bucket)
        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)
        self.run_cbq_query(f'CREATE INDEX idx_name on {self.bucket}(name) WITH {{"defer_build":true}}')
        self.sleep(30)
        after_index_ru, after_index_wu = self.meter.get_index_rwu(self.bucket)
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)

        expected_index_wu = 0

        self.assertEqual(before_index_ru, after_index_ru) # no index ru
        self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
        self.assertEqual(before_kv_wu, after_kv_wu) # no kv wu
        self.assertEqual(after_kv_ru, before_kv_ru)

        self.run_cbq_query(f'BUILD INDEX ON {self.bucket}(idx_name)')
        self.wait_for_all_indexes_online()
        after_index_ru, after_index_wu = self.meter.get_index_rwu(self.bucket)
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)

        expected_index_wu = self.doc_count * math.ceil((self.doc_key_size + self.index_key_size) / self.index_wu)

        self.assertEqual(before_index_ru, after_index_ru) # no index ru
        self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
        self.assertEqual(before_kv_wu, after_kv_wu) # no kv wu
        self.assertEqual(after_kv_ru, before_kv_ru)

    def test_create_composite_diff_values(self):
        doc1 = {"name": "San Francisco", "type": "city"}
        doc2 = {"name": "Oakland", "type": "city"}
        doc3 = {"name": "USA", "type": "country", "extra": "yes"}
        doc_size1 = len(json.dumps(doc1))
        doc_size2 = len(json.dumps(doc2))
        doc_size3 = len(json.dumps({"name": "USA", "type": "country"}))

        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {doc1} as v FROM array_range(0,5) d'
        insert_query2 = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {doc2} as v FROM array_range(0,5) d'
        insert_query3 = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {doc3} as v FROM array_range(0,5) d'

        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'DROP INDEX idx_name IF EXISTS on {self.bucket}')
        self.run_cbq_query(insert_query)
        self.run_cbq_query(insert_query2)
        self.run_cbq_query(insert_query3)

        before_index_ru, before_index_wu = self.meter.get_index_rwu(self.bucket)
        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)
        self.run_cbq_query(f'CREATE INDEX idx_name on {self.bucket}(name,type)')
        self.wait_for_all_indexes_online()
        after_index_ru, after_index_wu = self.meter.get_index_rwu(self.bucket)
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)

        expected_index_wu = 5 * math.ceil((doc_size1 + self.index_key_size) / self.index_wu) + 5 * math.ceil((doc_size2 + self.index_key_size) / self.index_wu) + 5 * math.ceil((doc_size3 + self.index_key_size) / self.index_wu)
        self.assertEqual(before_index_ru, after_index_ru) # no index ru
        self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
        self.assertEqual(before_kv_wu, after_kv_wu) # no kv wu
        self.assertEqual(after_kv_ru, before_kv_ru)

    def test_gsi_create_partial(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'DROP INDEX idx_name IF EXISTS on {self.bucket}')
        self.run_cbq_query(insert_query)
        doc1 = {"name":"hotel"}
        insert_query2 = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {doc1} as v FROM array_range(0,4) d'
        self.run_cbq_query(insert_query2)

        name = "a"*self.value_size
        before_index_ru, before_index_wu = self.meter.get_index_rwu(self.bucket)
        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)
        self.run_cbq_query(f'CREATE INDEX idx_name on {self.bucket}(name) where name = "{name}"')
        self.wait_for_all_indexes_online()
        after_index_ru, after_index_wu = self.meter.get_index_rwu(self.bucket)
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)

        expected_index_wu = self.doc_count * math.ceil((self.doc_key_size + self.index_key_size) / self.index_wu)

        self.assertEqual(before_index_ru, after_index_ru) # no index ru
        self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
        self.assertEqual(before_kv_wu, after_kv_wu) # no kv wu
        self.assertEqual(after_kv_ru,before_kv_ru)

    def test_gsi_create_partial_diff_values(self):
        doc1 = {"name":"hotel"}
        doc2 = {"name":"airport"}
        doc3 = {"name":"airline"}
        self.doc_size = len(json.dumps(doc1))
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {doc1} as v FROM array_range(0,4) d'
        insert_query2 = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {doc2} as v FROM array_range(0,4) d'
        insert_query3 = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {doc3} as v FROM array_range(0,4) d'


        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'DROP INDEX idx_name IF EXISTS on {self.bucket}')
        self.run_cbq_query(insert_query)
        self.run_cbq_query(insert_query2)
        self.run_cbq_query(insert_query3)


        before_index_ru, before_index_wu = self.meter.get_index_rwu(self.bucket)
        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)
        self.run_cbq_query(f'CREATE INDEX idx_name on {self.bucket}(name) where name = "hotel"')
        self.wait_for_all_indexes_online()
        after_index_ru, after_index_wu = self.meter.get_index_rwu(self.bucket)
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)

        expected_index_wu = 4 * math.ceil((self.doc_key_size + self.index_key_size) / self.index_wu)

        self.assertEqual(before_index_ru, after_index_ru) # no index ru
        self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
        self.assertEqual(before_kv_wu, after_kv_wu) # no kv wu
        self.assertEqual(after_kv_ru, before_kv_ru)

    def test_create_primary(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'DROP PRIMARY INDEX IF EXISTS on {self.bucket}')
        self.run_cbq_query(insert_query)

        before_index_ru, before_index_wu = self.meter.get_index_rwu(self.bucket)
        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)
        self.run_cbq_query(f'CREATE PRIMARY INDEX ON default')
        self.wait_for_all_indexes_online()
        after_index_ru, after_index_wu = self.meter.get_index_rwu(self.bucket)
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)

        expected_index_wu = self.doc_count * math.ceil((self.doc_key_size) / self.index_wu)

        self.assertEqual(before_index_ru, after_index_ru)  # no index ru
        self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
        self.assertEqual(before_kv_wu, after_kv_wu)  # no kv wu
        self.assertEqual(after_kv_ru, before_kv_ru)

    def test_create_missing(self):
        doc2 = {"type": "airline"}
        doc_size2 = len(json.dumps(doc2))
        index_key_size2 = len("airline")

        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        insert_query2 = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {doc2} as v FROM array_range(0,4) d'

        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'DROP INDEX idx_name IF EXISTS on {self.bucket}')
        self.run_cbq_query(insert_query)
        self.run_cbq_query(insert_query2)


        before_index_ru, before_index_wu = self.meter.get_index_rwu(self.bucket)
        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)
        self.run_cbq_query(f'CREATE INDEX idx_name on {self.bucket}(name INCLUDE MISSING)')
        self.wait_for_all_indexes_online()
        after_index_ru, after_index_wu = self.meter.get_index_rwu(self.bucket)
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)

        expected_index_wu = self.doc_count * math.ceil((self.doc_key_size + self.index_key_size) / self.index_wu) + 4 * math.ceil((doc_size2 + index_key_size2) / self.index_wu)

        self.assertEqual(before_index_ru, after_index_ru) # no index ru
        self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
        self.assertEqual(before_kv_wu, after_kv_wu) # no kv wu
        self.assertEqual(after_kv_ru, before_kv_ru)

    def test_create_missing_diff_values(self):
        doc1 = {"name": "hotel"}
        doc2 = {"name": "airport"}
        doc3 = {"type": "airline"}
        doc_size1 = len(json.dumps(doc1))
        doc_size2 = len(json.dumps(doc2))
        doc_size3 = len(json.dumps(doc3))

        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {doc1} as v FROM array_range(0,4) d'
        insert_query2 = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {doc2} as v FROM array_range(0,4) d'
        insert_query3 = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {doc3} as v FROM array_range(0,4) d'

        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'DROP INDEX idx_name IF EXISTS on {self.bucket}')
        self.run_cbq_query(insert_query)
        self.run_cbq_query(insert_query2)
        self.run_cbq_query(insert_query3)

        before_index_ru, before_index_wu = self.meter.get_index_rwu(self.bucket)
        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)
        self.run_cbq_query(f'CREATE INDEX idx_name on {self.bucket}(name INCLUDE MISSING)')
        self.wait_for_all_indexes_online()
        after_index_ru, after_index_wu = self.meter.get_index_rwu(self.bucket)
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)

        expected_index_wu = 4 * math.ceil((doc_size1 + self.index_key_size) / self.index_wu) + 4 * math.ceil((doc_size2 + self.index_key_size) / self.index_wu) + 4 * math.ceil((doc_size3 + self.index_key_size) / self.index_wu)

        self.assertEqual(before_index_ru, after_index_ru)  # no index ru
        self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
        self.assertEqual(before_kv_wu, after_kv_wu)  # no kv wu
        self.assertEqual(after_kv_ru, before_kv_ru)

    def test_create_index_non_covering(self):
        doc1 = {"name": "hotel"}
        doc2 = {"name": "airport"}
        doc3 = {"type": "airline"}
        doc_size1 = len(json.dumps(doc1))
        doc_size2 = len(json.dumps(doc2))
        doc_size3 = len(json.dumps(doc3))

        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {doc1} as v FROM array_range(0,4) d'
        insert_query2 = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {doc2} as v FROM array_range(0,4) d'
        insert_query3 = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {doc3} as v FROM array_range(0,4) d'

        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'DROP INDEX idx_name IF EXISTS on {self.bucket}')
        self.run_cbq_query(insert_query)
        self.run_cbq_query(insert_query2)
        self.run_cbq_query(insert_query3)

        before_index_ru, before_index_wu = self.meter.get_index_rwu(self.bucket)
        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)
        self.run_cbq_query(f'CREATE INDEX idx_name on {self.bucket}(name)')
        self.wait_for_all_indexes_online()
        after_index_ru, after_index_wu = self.meter.get_index_rwu(self.bucket)
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)

        expected_index_wu = 4 * math.ceil((doc_size1 + self.index_key_size) / self.index_wu) + 4 * math.ceil((doc_size2 + self.index_key_size) / self.index_wu)

        self.assertEqual(before_index_ru, after_index_ru)  # no index ru
        self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
        self.assertEqual(before_kv_wu, after_kv_wu)  # no kv wu
        self.assertEqual(after_kv_ru, before_kv_ru)

    def test_create_equivalent(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'DROP INDEX idx_name IF EXISTS on {self.bucket}')
        self.run_cbq_query(insert_query)

        before_index_ru, before_index_wu = self.meter.get_index_rwu(self.bucket)
        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)
        self.run_cbq_query(f'CREATE INDEX idx_name on {self.bucket}(name)')
        self.run_cbq_query(f'CREATE INDEX idx_name2 on {self.bucket}(name)')
        self.wait_for_all_indexes_online()
        after_index_ru, after_index_wu = self.meter.get_index_rwu(self.bucket)
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)

        expected_index_wu = self.doc_count * math.ceil((self.doc_key_size + self.index_key_size) / self.index_wu) * 2

        self.assertEqual(before_index_ru, after_index_ru)  # no index ru
        self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
        self.assertEqual(before_kv_wu, after_kv_wu)  # no kv wu
        self.assertEqual(after_kv_ru, before_kv_ru)

    def test_create_composite(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.composite_doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'DROP INDEX idx_name IF EXISTS on {self.bucket}')
        self.run_cbq_query(insert_query)

        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'DROP INDEX idx_name IF EXISTS on {self.bucket}')
        self.run_cbq_query(insert_query)

        before_index_ru, before_index_wu = self.meter.get_index_rwu(self.bucket)
        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)
        self.run_cbq_query(f'CREATE INDEX idx_name on {self.bucket}(fname,lname)')
        self.wait_for_all_indexes_online()
        after_index_ru, after_index_wu = self.meter.get_index_rwu(self.bucket)
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)

        expected_index_wu = self.doc_count * math.ceil((self.doc_key_size + self.composite_index_key_size) / self.index_wu)

        self.assertEqual(before_index_ru, after_index_ru) # no index ru
        self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
        self.assertEqual(before_kv_wu, after_kv_wu) # no kv wu
        self.assertEqual(after_kv_ru, before_kv_ru)

    def test_create_composite_diff_values(self):
        doc1 = {"name": "San Francisco", "type": "city"}
        doc2 = {"name": "Oakland", "type": "city"}
        doc3 = {"name": "USA", "type": "country", "extra": "yes"}
        doc_size1 = len(json.dumps(doc1))
        doc_size2 = len(json.dumps(doc2))
        doc_size3 = len(json.dumps({"name": "USA", "type": "country"}))

        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {doc1} as v FROM array_range(0,5) d'
        insert_query2 = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {doc2} as v FROM array_range(0,5) d'
        insert_query3 = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {doc3} as v FROM array_range(0,5) d'

        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'DROP INDEX idx_name IF EXISTS on {self.bucket}')
        self.run_cbq_query(insert_query)
        self.run_cbq_query(insert_query2)
        self.run_cbq_query(insert_query3)

        before_index_ru, before_index_wu = self.meter.get_index_rwu(self.bucket)
        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)
        self.run_cbq_query(f'CREATE INDEX idx_name on {self.bucket}(name,type)')
        self.wait_for_all_indexes_online()
        after_index_ru, after_index_wu = self.meter.get_index_rwu(self.bucket)
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)

        expected_index_wu = 5 * math.ceil((doc_size1 + self.index_key_size) / self.index_wu) + 5 * math.ceil((doc_size2 + self.index_key_size) / self.index_wu) + 5 * math.ceil((doc_size3 + self.index_key_size) / self.index_wu)
        self.assertEqual(before_index_ru, after_index_ru) # no index ru
        self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
        self.assertEqual(before_kv_wu, after_kv_wu) # no kv wu
        self.assertEqual(after_kv_ru, before_kv_ru)

    def test_create_array_index(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.array_doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'DROP INDEX idx_names IF EXISTS on {self.bucket}')
        self.run_cbq_query(insert_query)

        before_index_ru, before_index_wu = self.meter.get_index_rwu(self.bucket)
        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)
        if self.create_distinct:
            self.run_cbq_query(f'CREATE INDEX idx_names on {self.bucket}(DISTINCT ARRAY x.fname for x in names END)')
        else:
            self.run_cbq_query(f'CREATE INDEX idx_names on {self.bucket}(ALL ARRAY x.fname for x in names END)')
        self.wait_for_all_indexes_online()
        after_index_ru, after_index_wu = self.meter.get_index_rwu(self.bucket)
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)

        if self.create_distinct:
            expected_index_wu = self.doc_count * math.ceil((self.doc_key_size + self.distinct_array_index_key_size) / self.index_wu)
        else:
            expected_index_wu = self.doc_count * math.ceil((self.doc_key_size + self.array_index_key_size) / self.index_wu)

        self.assertEqual(before_index_ru, after_index_ru) # no index ru
        self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
        self.assertEqual(before_kv_wu, after_kv_wu) # no kv wu
        self.assertEqual(after_kv_ru, before_kv_ru)

    def test_select_array_index(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.array_doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'DROP INDEX idx_names IF EXISTS on {self.bucket}')
        self.run_cbq_query(insert_query)

        if self.create_distinct:
            self.run_cbq_query(f'CREATE INDEX idx_names on {self.bucket}(DISTINCT ARRAY x.fname for x in names END)')
        else:
            self.run_cbq_query(f'CREATE INDEX idx_names on {self.bucket}(ALL ARRAY x.fname for x in names END)')
        self.wait_for_all_indexes_online()

        self.array_index_key_size = len("a"*(int(self.value_size/6)))

        expected_kv_ru = self.doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_ru)
        expected_index_ru = math.ceil(self.doc_count * 1.1 * (self.doc_key_size + self.array_index_key_size) / self.index_ru)
        result = self.run_cbq_query(query=f"select names from {self.bucket} where ANY x in names SATISFIES x.fname = repeat('a', {int(self.value_size/6)} ) END")
        assert_query, msg = self.meter.assert_query_billing_unit(result, expected_kv_ru, unit='ru', service = 'kv')
        self.assertTrue(assert_query, msg)
        assert_query, msg = self.meter.assert_query_billing_unit(result, expected_index_ru, unit='ru', service = 'index')
        self.assertTrue(assert_query, msg)

    def test_select_partial(self):
        doc1 = {"name":"hotel"}
        doc2 = {"name":"airport"}
        doc3 = {"name":"airline"}
        self.doc_size = len(json.dumps(doc1))
        self.index_key_size = 5 # value of hotel
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {doc1} as v FROM array_range(0,4) d'
        insert_query2 = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {doc2} as v FROM array_range(0,4) d'
        insert_query3 = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {doc3} as v FROM array_range(0,4) d'

        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'DROP INDEX idx_name IF EXISTS on {self.bucket}')
        self.run_cbq_query(insert_query)
        self.run_cbq_query(insert_query2)
        self.run_cbq_query(insert_query3)

        self.run_cbq_query(f'CREATE INDEX idx_name on {self.bucket}(name) where name = "hotel"')
        self.wait_for_all_indexes_online()

        expected_index_ru = math.ceil(4 * 1.1 * (self.doc_key_size + self.index_key_size) / self.index_ru)
        result = self.run_cbq_query(f'SELECT name FROM {self.bucket} WHERE name = "hotel"')
        assert_query, msg = self.meter.assert_query_billing_unit(result, expected_index_ru, unit='ru', service = 'index')
        self.assertTrue(assert_query, msg)

    def test_select_composite(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.composite_doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'DROP INDEX idx_name IF EXISTS on {self.bucket}')
        self.run_cbq_query(insert_query)

        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'DROP INDEX idx_name IF EXISTS on {self.bucket}')
        self.run_cbq_query(insert_query)

        self.run_cbq_query(f'CREATE INDEX idx_name on {self.bucket}(fname,lname)')
        self.wait_for_all_indexes_online()

        expected_index_ru = math.ceil(self.doc_count * 1.1 * (self.doc_key_size + self.composite_index_key_size) / self.index_ru)
        result = self.run_cbq_query(f'SELECT fname, lname FROM {self.bucket} WHERE fname = repeat("a", {self.value_size}) and lname = repeat("b", {self.value_size})')
        self.assertTrue("kv" not in str(result['billingUnits']), f"Index is covering, there should be no kv units : {result}")
        assert_query, msg = self.meter.assert_query_billing_unit(result, expected_index_ru, unit='ru', service = 'index')
        self.assertTrue(assert_query, msg)

    def test_select_composite_diff_values(self):
        doc1 = {"name": "San Francisco", "type": "city"}
        doc2 = {"name": "Oakland", "type": "city"}
        doc3 = {"name": "USA", "type": "country", "extra": "yes"}
        self.composite_index_key_size = 10

        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {doc1} as v FROM array_range(0,5) d'
        insert_query2 = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {doc2} as v FROM array_range(0,5) d'
        insert_query3 = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {doc3} as v FROM array_range(0,5) d'

        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'DROP INDEX idx_name IF EXISTS on {self.bucket}')
        self.run_cbq_query(insert_query)
        self.run_cbq_query(insert_query2)
        self.run_cbq_query(insert_query3)
        self.run_cbq_query(f'CREATE INDEX idx_name on {self.bucket}(name,type)')
        self.wait_for_all_indexes_online()

        expected_index_ru = math.ceil(5 * 1.1 * (self.doc_key_size + self.composite_index_key_size) / self.index_ru)

        result = self.run_cbq_query(f'SELECT name FROM {self.bucket} WHERE name = "USA" and type = "country"')
        self.assertTrue("kv" not in str(result['billingUnits']), f"Index is covering, there should be no kv units : {result}")
        assert_query, msg = self.meter.assert_query_billing_unit(result, expected_index_ru, unit='ru', service = 'index')
        self.assertTrue(assert_query, msg)

    def test_index_insert(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'DROP INDEX idx_name IF EXISTS on {self.bucket}')
        self.run_cbq_query(insert_query)


        self.run_cbq_query(f'CREATE INDEX idx_name on {self.bucket}(name)')
        self.wait_for_all_indexes_online()
        before_index_ru, before_index_wu = self.meter.get_index_rwu(self.bucket)
        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)

        self.new_doc = {"name": "b" * self.value_size}
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(insert_query)
        self.sleep(30)
        after_index_ru, after_index_wu = self.meter.get_index_rwu(self.bucket)
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)

        expected_index_wu = self.doc_count * math.ceil((self.doc_key_size + self.index_key_size) / self.index_wu)

        self.assertEqual(before_index_ru, after_index_ru)  # no index ru
        self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
        self.assertEqual(after_kv_ru, before_kv_ru)

    def test_index_update(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'DROP INDEX idx_name IF EXISTS on {self.bucket}')
        self.run_cbq_query(insert_query)

        self.run_cbq_query(f'CREATE INDEX idx_name on {self.bucket}(name)')
        self.wait_for_all_indexes_online()
        before_index_ru, before_index_wu = self.meter.get_index_rwu(self.bucket)
        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)

        update_query = f'UPDATE {self.bucket} SET name = repeat("b", {self.value_size})'
        self.run_cbq_query(update_query)
        self.sleep(30)
        after_index_ru, after_index_wu = self.meter.get_index_rwu(self.bucket)
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)

        result = self.run_cbq_query(f'SELECT meta().id FROM {self.bucket}')
        sc_ru = result['billingUnits']['ru']['kv']

        expected_index_wu = self.doc_count * math.ceil((self.doc_key_size + self.index_key_size) / self.index_wu)
        expected_kv_ru = self.doc_count * math.ceil((self.doc_key_size + self.doc_size)/ self.kv_ru) + sc_ru

        self.assertEqual(before_index_ru, after_index_ru)  # no index ru
        self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
        self.assertEqual(expected_kv_ru, after_kv_ru - before_kv_ru)

    def test_index_delete(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'DROP INDEX idx_name IF EXISTS on {self.bucket}')
        self.run_cbq_query(insert_query)

        self.run_cbq_query(f'CREATE INDEX idx_name on {self.bucket}(name)')
        self.wait_for_all_indexes_online()
        result = self.run_cbq_query(f'SELECT meta().id FROM {self.bucket}')
        before_index_ru, before_index_wu = self.meter.get_index_rwu(self.bucket)
        before_kv_ru, before_kv_wu = self.meter.get_kv_rwu(self.bucket)

        delete_query = f'DELETE FROM {self.bucket}'
        self.run_cbq_query(delete_query)
        self.sleep(30)
        after_index_ru, after_index_wu = self.meter.get_index_rwu(self.bucket)
        after_kv_ru, after_kv_wu = self.meter.get_kv_rwu(self.bucket)

        expected_index_wu = self.doc_count * math.ceil((self.doc_key_size + self.index_key_size) / self.index_wu)
        expected_kv_ru = result['billingUnits']['ru']['kv']

        self.assertEqual(before_index_ru, after_index_ru)  # no index ru
        self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
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
            'query1': 'SELECT * FROM array_repeat("a", 10) as t',
            'system_scope_query': f'SELECT * FROM {self.bucket}.`_system`.`_query`',
            'system_scope_mobile': f'SELECT * FROM {self.bucket}.`_system`.`_mobile`',
            'system_scope_eventing': f'SELECT * FROM {self.bucket}.`_system`.`_eventing`',
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
