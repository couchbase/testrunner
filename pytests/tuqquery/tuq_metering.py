from .tuq import QueryTests
import re
import requests
import json
import math

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

    def suite_setUp(self):
        super(QueryMeteringTests, self).suite_setUp()

    def tearDown(self):
        super(QueryMeteringTests, self).tearDown()

    def suite_tearDown(self):
        super(QueryMeteringTests, self).suite_tearDown()

    def get_metering_index(self, bucket='default'):
        ru, wu = 0, 0
        api = f"https://{self.server.ip}:19102/_metering"
        idx_ru_pattern = re.compile(f'meter_ru_total{{bucket="{bucket}",for="index",unbilled="",variant=""}} (\d+)')
        idx_wu_pattern = re.compile(f'meter_wu_total{{bucket="{bucket}",for="index",unbilled="",variant=""}} (\d+)')
        response = requests.get(api, verify=False)
        if idx_ru_pattern.search(response.text):
            ru = int(idx_ru_pattern.findall(response.text)[0])
        if idx_wu_pattern.search(response.text):
            wu = int(idx_wu_pattern.findall(response.text)[0])
        return ru, wu

    def get_metering_kv(self, bucket='default'):
        ru, wu = 0, 0
        api = f"https://{self.server.ip}:18091/metrics"
        kv_ru_pattern = re.compile(f'meter_ru_total{{bucket="{bucket}"}} (\d+)')
        kv_wu_pattern = re.compile(f'meter_wu_total{{bucket="{bucket}"}} (\d+)')
        response = requests.get(api, auth=requests.auth.HTTPBasicAuth(self.rest.username, self.rest.password), verify=False)
        if kv_ru_pattern.search(response.text):
            ru = int(kv_ru_pattern.findall(response.text)[0])
        if kv_wu_pattern.search(response.text):
            wu = int(kv_wu_pattern.findall(response.text)[0])
        return ru, wu

    def assert_billing_unit(self, result, expected, unit="ru", service="kv"):
        if 'billingUnits' in result.keys():
            if unit in result['billingUnits'].keys():
                if service in result['billingUnits'][unit].keys():
                    actual = result['billingUnits'][unit][service]
                    self.assertEqual(actual, expected, f'Expected {expected} {service} {unit} unit but got {actual}')
                else:
                    self.fail(f"result['billingUnits'][{unit}] does not contain {service}, result['billingUnits'][{unit}] is: {result['billingUnits'][unit]}")
            else:
                self.fail(f"result['billingUnits'] does not contain {unit}, result['billingUnits'] is: {result['billingUnits']}")
        else:
            self.fail(f'result does not contain billingUnits, result is: {result}')

    def test_kv_insert(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        # MB-53214
        # self.run_cbq_query(f'DELETE from {self.bucket}')

        expected_wu = self.doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_wu)
        if self.with_returning:
            insert_query = insert_query + " returning name"

        before_kv_ru, before_kv_wu = self.get_metering_kv(self.bucket)
        result = self.run_cbq_query(insert_query)
        after_kv_ru, after_kv_wu = self.get_metering_kv(self.bucket)

        self.assert_billing_unit(result, expected_wu, unit='wu', service='kv')
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

        before_kv_ru, before_kv_wu = self.get_metering_kv(self.bucket)
        result = self.run_cbq_query(upsert_query)
        after_kv_ru, after_kv_wu = self.get_metering_kv(self.bucket)

        self.assert_billing_unit(result, expected_wu, unit='wu', service='kv')
        self.assertTrue("ru" not in result['billingUnits'])
        self.assertEqual(before_kv_ru, after_kv_ru, f"KV RU before: {before_kv_ru} and after: {after_kv_ru} are different") # expect no KV reads


    def test_kv_delete(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(insert_query)

        expected_wu = self.doc_count
        result = self.run_cbq_query(f'DELETE from {self.bucket}')
        self.assert_billing_unit(result, expected_wu, unit='wu', service='kv')

    def test_kv_update(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {{"name": "San Francisco"}} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(insert_query)

        doc_read_size = self.doc_key_size + len(json.dumps({"name": "San Francisco"}))
        doc_write_size = self.doc_key_size + len(json.dumps({"name": "a"*self.value_size}))

        expected_wu = self.doc_count * math.ceil( doc_write_size / self.kv_wu)
        expected_ru = self.doc_count * math.ceil( doc_read_size / self.kv_ru)
        result = self.run_cbq_query(f'UPDATE {self.bucket} SET name = repeat("a", {self.value_size})')
        self.assert_billing_unit(result, expected_wu, unit='wu', service='kv')
        self.assert_billing_unit(result, expected_ru, unit='ru', service='kv')

    def test_kv_select(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(insert_query)

        expected_ru = self.doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_ru)
        result = self.run_cbq_query(f'SELECT * FROM {self.bucket}')
        self.assert_billing_unit(result, expected_ru, unit='ru', service='kv')

    def test_kv_select_count(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(insert_query)

        # for count(*) we should get this from meta and no need to read from KV
        result = self.run_cbq_query(f'SELECT count(*) as count FROM {self.bucket}')
        self.assertTrue('billingUnits' not in result.keys() or 'ru' not in result['billingUnits'].keys(), f'We should not have got billingUnits[ru] from query')
        self.assertEqual(result['results'][0]['count'], self.doc_count, f'We expected {self.doc_count} but for diff result: {result}')

        # for count(field) we need to scan all KV doc
        expected_ru = self.doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_ru)
        result = self.run_cbq_query(f'SELECT count(name) as count FROM {self.bucket}')
        self.assert_billing_unit(result, expected_ru, unit='ru', service='kv')
        self.assertEqual(result['results'][0]['count'], self.doc_count, f'We expected {self.doc_count} but for diff result: {result}')

    def test_kv_select_meta(self):
        options = {"expiration": 123456}
        self.doc["country"] = "France"
        self.doc_size = len(json.dumps(self.doc))
        insert_query = f'INSERT INTO {self.bucket} (key k, value v, options o) SELECT uuid() as k , {self.doc} as v, {options} o FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(insert_query)

        expected_ru = self.doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_ru)
        result = self.run_cbq_query(f'SELECT meta().expiration, country FROM {self.bucket}')
        self.assert_billing_unit(result, expected_ru, unit='ru', service='kv')

    def test_kv_prepare(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        # MB-53214
        # self.run_cbq_query(f'DELETE from {self.bucket}')

        # Prepare statement should shows no bilingUnits
        before_kv_ru, before_kv_wu = self.get_metering_kv(self.bucket)
        result = self.run_cbq_query(f"PREPARE prepared_insert AS {insert_query}")
        after_kv_ru, after_kv_wu = self.get_metering_kv(self.bucket)
        self.assertEqual(before_kv_ru, after_kv_ru)
        self.assertEqual(before_kv_wu, after_kv_wu)
        self.assertTrue('billingUnits' not in result)
        
        expected_wu = self.doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_wu)

        before_kv_ru, before_kv_wu = self.get_metering_kv(self.bucket)
        result = self.run_cbq_query(f"EXECUTE prepared_insert")
        after_kv_ru, after_kv_wu = self.get_metering_kv(self.bucket)

        self.assert_billing_unit(result, expected_wu, unit='wu', service='kv')
        self.assertTrue("ru" not in result['billingUnits'])
        self.assertEqual(before_kv_ru, after_kv_ru, f"KV RU before: {before_kv_ru} and after: {after_kv_ru} are different") # expect no KV reads

    def test_kv_inline_udf(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(insert_query)

        # Create function should shows no bilingUnits
        before_kv_ru, before_kv_wu = self.get_metering_kv(self.bucket)
        result = self.run_cbq_query(f"CREATE or REPLACE FUNCTION select_func() {{(SELECT * FROM {self.bucket})}}")
        after_kv_ru, after_kv_wu = self.get_metering_kv(self.bucket)
        self.assertEqual(before_kv_ru, after_kv_ru)
        self.assertEqual(before_kv_wu, after_kv_wu)
        self.assertTrue('billingUnits' not in result)

        expected_ru = self.doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_ru)
        result = self.run_cbq_query(f'EXECUTE FUNCTION select_func()')
        self.assert_billing_unit(result, expected_ru, unit='ru', service='kv')

    def test_gsi_select(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(insert_query)

        self.run_cbq_query(f'CREATE INDEX idx_name IF NOT EXISTS on {self.bucket}(name)')
        self.wait_for_all_indexes_online()

        expected_kv_ru = self.doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_ru)
        expected_index_ru = math.ceil(self.doc_count * (self.doc_key_size + self.index_key_size) / self.index_ru)
        result = self.run_cbq_query(f'SELECT name, city FROM {self.bucket} WHERE name = repeat("a", {self.value_size})')
        self.assert_billing_unit(result, expected_kv_ru, unit='ru', service = 'kv')
        self.assert_billing_unit(result, expected_index_ru, unit='ru', service = 'index')

    def test_gsi_create(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'DROP INDEX idx_name IF EXISTS on {self.bucket}')
        self.run_cbq_query(insert_query)

        before_index_ru, before_index_wu = self.get_metering_index(self.bucket)
        before_kv_ru, before_kv_wu = self.get_metering_kv(self.bucket)
        self.run_cbq_query(f'CREATE INDEX idx_name on {self.bucket}(name)')
        self.wait_for_all_indexes_online()
        after_index_ru, after_index_wu = self.get_metering_index(self.bucket)
        after_kv_ru, after_kv_wu = self.get_metering_kv(self.bucket)

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
        before_index_ru, before_index_wu = self.get_metering_index(self.bucket)
        before_kv_ru, before_kv_wu = self.get_metering_kv(self.bucket)
        result = self.run_cbq_query(queries[self.statement])
        after_index_ru, after_index_wu = self.get_metering_index(self.bucket)
        after_kv_ru, after_kv_wu = self.get_metering_kv(self.bucket)

        # No kv or index RWU
        self.assertTrue('billingUnits' not in result)
        self.assertEqual(before_index_ru, after_index_ru)
        self.assertEqual(before_index_wu, after_index_wu)
        self.assertEqual(before_kv_wu, after_kv_wu)
        self.assertEqual(before_kv_ru, after_kv_ru)
