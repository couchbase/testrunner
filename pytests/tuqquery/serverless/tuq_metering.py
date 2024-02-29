from serverless.serverless_basetestcase import ServerlessBaseTestCase
from TestInput import TestInputSingleton
import json
import math
from lib.metering_throttling import metering
import random, string
import time
from lib.Cb_constants.CBServer import CbServer

class QueryMeteringTests(ServerlessBaseTestCase):
    def setUp(self):
        self.input = TestInputSingleton.input
        self.scope = '_default'
        self.collection = '_default'
        self.doc_count = self.input.param("doc_count", 10)
        self.value_size = self.input.param("value_size", 256)
        self.with_returning = self.input.param("with_returning", False)
        self.statement = self.input.param("statement", "explain")
        self.kv_wu, self.kv_ru = 1024, 4096
        self.index_wu, self.index_ru = 1024, 256
        self.doc = {"name": ''.join(random.choices(string.ascii_lowercase, k=self.value_size))}
        self.composite_doc = {"fname": "a"*self.value_size, "lname":"b"*self.value_size}
        self.index_key_size = len ("a"*self.value_size) # index is on name
        self.composite_index_key_size = len ("a"*self.value_size) + len("b"*self.value_size)
        self.doc_key_size = 36 # use uuid()
        self.doc_size= len(json.dumps(self.doc))
        self.composite_doc_size = len(json.dumps(self.composite_doc))
        CbServer.capella_run = True
        return super().setUp()

    def tearDown(self):
        return super().tearDown()

    def test_kv_insert(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
            expected_wu = self.doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_wu)
            if self.with_returning:
                insert_query = insert_query + " returning name"

            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            result = self.run_query(database, insert_query)
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            assert_query, msg = meter.assert_query_billing_unit(result, expected_wu, unit='wu', service='kv')
            self.assertTrue(assert_query, msg)
            self.assertTrue("ru" not in result['billingUnits'])
            self.assertEqual(before_kv_ru, after_kv_ru, f"KV RU before: {before_kv_ru} and after: {after_kv_ru} are different") # expect no KV reads
            self.assertEqual(expected_wu, after_kv_wu-before_kv_wu)

    def test_kv_upsert(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            upsert_doc_count = math.ceil(self.doc_count/5)
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
            self.run_query(database, insert_query)
            result = self.run_query(database, f'SELECT raw meta().id FROM {self.collection} LIMIT {upsert_doc_count}')
            meta_id = result['results']
            upsert_query = f'UPSERT INTO {self.collection} (key k, value v) SELECT k , repeat("b", {self.value_size}) as v FROM {meta_id} k'

            expected_wu = upsert_doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_wu)
            if self.with_returning:
                upsert_query = upsert_query + " returning name"

            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            result = self.run_query(database, upsert_query)
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            assert_query, msg = meter.assert_query_billing_unit(result, expected_wu, unit='wu', service='kv')
            self.assertTrue(assert_query, msg)
            self.assertTrue("ru" not in result['billingUnits'])
            self.assertEqual(before_kv_ru, after_kv_ru, f"KV RU before: {before_kv_ru} and after: {after_kv_ru} are different") # expect no KV reads
            self.assertEqual(expected_wu, after_kv_wu-before_kv_wu)

    def test_kv_delete(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
            self.run_query(database, insert_query)

            # Get sequential scan read unit
            result = self.run_query(database, f'SELECT meta().id FROM {self.collection}')
            sc_ru = result['billingUnits']['ru']['kv']

            expected_wu = self.doc_count
            expected_ru = sc_ru
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            result = self.run_query(database, f'DELETE from {self.collection}')
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            assert_query, msg = meter.assert_query_billing_unit(result, expected_wu, unit='wu', service='kv')
            self.assertTrue(assert_query, msg)
            assert_query, msg = meter.assert_query_billing_unit(result, expected_ru, unit='ru', service='kv')
            self.assertTrue(assert_query, msg)
            self.assertEqual(expected_wu, after_kv_wu-before_kv_wu)
            self.assertEqual(expected_ru, after_kv_ru-before_kv_ru)

    def test_kv_update(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {{"name": "San Francisco"}} as v FROM array_range(0,{self.doc_count}) d'
            self.run_query(database, insert_query)

            # Get sequential scan read unit
            result = self.run_query(database, f'SELECT meta().id FROM {self.collection}')
            sc_ru = result['billingUnits']['ru']['kv']

            doc_read_size = self.doc_key_size + len(json.dumps({"name": "San Francisco"}))
            doc_write_value = ''.join(random.choices(string.ascii_lowercase, k=self.value_size))
            doc_write = {"name": doc_write_value}
            doc_write_size = self.doc_key_size + len(json.dumps(doc_write))
            expected_wu = self.doc_count * math.ceil( doc_write_size / self.kv_wu)
            expected_ru = self.doc_count * math.ceil( doc_read_size / self.kv_ru)

            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            result = self.run_query(database, f'UPDATE {self.collection} SET name = "{doc_write_value}"')
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)
            
            assert_query, msg = meter.assert_query_billing_unit(result, expected_wu, unit='wu', service='kv')
            self.assertTrue(assert_query, msg)
            assert_query, msg = meter.assert_query_billing_unit(result, expected_ru + sc_ru, unit='ru', service='kv')
            self.assertTrue(assert_query, msg)
            self.assertEqual(expected_wu, after_kv_wu-before_kv_wu)
            self.assertEqual(expected_ru + sc_ru, after_kv_ru-before_kv_ru)

    def test_kv_merge(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {{"name": "San Francisco"}} as v FROM array_range(0,{self.doc_count}) d'
            self.run_query(database, insert_query)

            doc_read_size = self.doc_key_size + len(json.dumps({"name": "San Francisco"}))
            doc_write_size = self.doc_key_size + len(json.dumps({"name": "San Francisco","country": "a"*self.value_size}))

            # Get sequential scan read unit
            result = self.run_query(database, f'SELECT meta().id FROM {self.collection}')
            sc_ru = result['billingUnits']['ru']['kv']

            expected_wu = self.doc_count * math.ceil( doc_write_size / self.kv_wu)
            expected_ru = self.doc_count * math.ceil( doc_read_size / self.kv_ru)
            
            merge_query = f'MERGE INTO {self.collection} t \
                USING [{{"name": "San Francisco", "country": "{"a"*self.value_size}"}}] source \
                ON t.name = source.name WHEN MATCHED \
                THEN UPDATE SET t.country = source.country'
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            result = self.run_query(database, merge_query)
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            assert_query, msg = meter.assert_query_billing_unit(result, expected_wu, unit='wu', service='kv')
            self.assertTrue(assert_query, msg)
            assert_query, msg = meter.assert_query_billing_unit(result, expected_ru + sc_ru, unit='ru', service='kv')
            self.assertTrue(assert_query, msg)
            self.assertEqual(expected_wu, after_kv_wu-before_kv_wu)
            self.assertEqual(expected_ru + sc_ru, after_kv_ru-before_kv_ru)

    def test_kv_select(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
            self.run_query(database, insert_query)

            # Get sequential scan read unit
            result = self.run_query(database, f'SELECT meta().id FROM {self.collection}')
            sc_ru = result['billingUnits']['ru']['kv']

            expected_ru = self.doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_ru)

            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            result = self.run_query(database, f'SELECT * FROM {self.collection}')
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            assert_query, msg = meter.assert_query_billing_unit(result, expected_ru + sc_ru, unit='ru', service='kv')
            self.assertTrue(assert_query, msg)
            self.assertEqual(after_kv_wu, before_kv_wu) # no KV write
            self.assertEqual(expected_ru + sc_ru, after_kv_ru-before_kv_ru)

    def test_kv_select_count(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
            self.run_query(database, insert_query)

            # for count(*) we should get this from meta and no need to read from KV
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            result = self.run_query(database, f'SELECT count(*) as count FROM {self.collection}')
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            self.assertTrue('billingUnits' not in result.keys() or 'ru' not in result['billingUnits'].keys(), f'We should not have got billingUnits[ru] from query')
            self.assertEqual(result['results'][0]['count'], self.doc_count, f'We expected {self.doc_count} but for diff result: {result}')
            self.assertEqual(after_kv_wu, before_kv_wu) # no KV write
            self.assertEqual(after_kv_ru, before_kv_ru) # no KV read

            # Get sequential scan read unit
            result = self.run_query(database, f'SELECT meta().id FROM {self.collection}')
            sc_ru = result['billingUnits']['ru']['kv']
            expected_ru = self.doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_ru)

            # for count(field) we need to scan all KV doc
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            result = self.run_query(database, f'SELECT count(name) as count FROM {self.collection}')
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            self.assertEqual(result['results'][0]['count'], self.doc_count, f'We expected {self.doc_count} but for diff result: {result}')
            assert_query, msg = meter.assert_query_billing_unit(result, expected_ru + sc_ru, unit='ru', service='kv')
            self.assertTrue(assert_query, msg)
            self.assertEqual(after_kv_wu, before_kv_wu) # no KV write
            self.assertEqual(expected_ru + sc_ru, after_kv_ru-before_kv_ru)

    def test_kv_select_meta(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            options = {"expiration": 123456}
            self.doc["country"] = "France"
            self.doc_size = len(json.dumps(self.doc))
            insert_query = f'INSERT INTO {self.collection} (key k, value v, options o) SELECT uuid() as k , {self.doc} as v, {options} o FROM array_range(0,{self.doc_count}) d'
            self.run_query(database, insert_query)

            # Get sequential scan read unit
            result = self.run_query(database, f'SELECT meta().id FROM {self.collection}')
            sc_ru = result['billingUnits']['ru']['kv']
            expected_ru = self.doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_ru)

            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            result = self.run_query(database, f'SELECT meta().expiration, country FROM {self.collection}')
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            assert_query, msg = meter.assert_query_billing_unit(result, expected_ru + sc_ru, unit='ru', service='kv')
            self.assertTrue(assert_query, msg)
            self.assertEqual(after_kv_wu, before_kv_wu) # no KV write
            self.assertEqual(expected_ru + sc_ru, after_kv_ru-before_kv_ru)

    def test_kv_prepare(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'

            # Prepare statement should shows no bilingUnits
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            result = self.run_query(database, f"PREPARE prepared_insert AS {insert_query}")
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)
            self.assertEqual(before_kv_ru, after_kv_ru)
            self.assertEqual(before_kv_wu, after_kv_wu)
            self.assertTrue('billingUnits' not in result)

            expected_wu = self.doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_wu)

            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            result = self.run_query(database, f"EXECUTE prepared_insert")
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            assert_query, msg = meter.assert_query_billing_unit(result, expected_wu, unit='wu', service='kv')
            self.assertTrue(assert_query, msg)
            self.assertTrue("ru" not in result['billingUnits'])
            self.assertEqual(expected_wu, after_kv_wu-before_kv_wu)
            self.assertEqual(before_kv_ru, after_kv_ru) # no KV reads

    def test_inline_udf(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            result = self.run_query(database, f"CREATE or REPLACE FUNCTION {self.scope}.plusone(a) {{ a + 1 }}")
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)
            self.assertTrue('billingUnits' not in result)
            self.assertEqual(before_kv_ru, after_kv_ru) # no KV read
            self.assertEqual(before_kv_wu, after_kv_wu) # no KV write

            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            before_cu_unbilled = meter.get_query_cu(database.id, unbilled="true",variant="udf")
            before_cu_billed = meter.get_query_cu(database.id, unbilled="false",variant="udf")
            result = self.run_query(database, f"EXECUTE FUNCTION {self.scope}.plusone(10)")
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)
            after_cu_unbilled = meter.get_query_cu(database.id, unbilled="true",variant="udf")
            after_cu_billed = meter.get_query_cu(database.id, unbilled="false",variant="udf")
            self.assertTrue('billingUnits' not in result)
            self.assertTrue(result['results'], [11])
            self.assertEqual(before_kv_ru, after_kv_ru) # no KV read
            self.assertEqual(before_kv_wu, after_kv_wu) # no KV write
            self.assertEqual(before_cu_billed, after_cu_billed) # no billed compute unit
            self.assertEqual(before_cu_unbilled, after_cu_unbilled) # no billed compute unit

    def test_js_udf(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            result = self.run_query(database, f"CREATE or REPLACE FUNCTION {self.scope}.plus(a, b) LANGUAGE JAVASCRIPT as 'function plus(a,b) {{return a+b}}'")
            self.assertTrue('billingUnits' not in result)
            expected_cu = 1
            before_cu_unbilled = meter.get_query_cu(database.id, unbilled="true",variant="udf")
            before_cu_billed = meter.get_query_cu(database.id, unbilled="false",variant="udf")
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            result = self.run_query(database, f"EXECUTE FUNCTION {self.scope}.plus(10,20)")
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)
            after_cu_unbilled = meter.get_query_cu(database.id, unbilled="true",variant="udf")
            after_cu_billed = meter.get_query_cu(database.id, unbilled="false",variant="udf")
            # self.assertTrue('billingUnits' not in result)
            self.assertTrue(result['results'], [30])
            self.assertEqual(before_kv_ru, after_kv_ru) # no kv write
            self.assertEqual(before_kv_wu, after_kv_wu) # no kv read
            self.assertEqual(expected_cu, after_cu_unbilled - before_cu_unbilled)
            self.assertEqual(after_cu_billed, before_cu_billed) # no cu billed unit

    def test_js_udf_n1ql(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
            self.run_query(database, insert_query)

            result = self.run_query(database, f"CREATE or REPLACE FUNCTION {self.scope}.select_js(city) LANGUAGE JAVASCRIPT as 'function select_js(city) {{\
                var query = SELECT name FROM {self.collection} WHERE name = $city;\
                var acc = [];\
                for (const row of query) {{ acc.push(row); }}\
                return acc;}}'")
            self.assertTrue('billingUnits' not in result)

            # Get sequential scan read unit
            result = self.run_query(database, f'SELECT meta().id FROM {self.collection}')
            sc_ru = result['billingUnits']['ru']['kv']

            expected_ru = self.doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_ru)

            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            result = self.run_query(database, f"EXECUTE FUNCTION {self.scope}.select_js('Paris')")
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)
            # assert_query, msg = meter.assert_query_billing_unit(result, expected_ru + sc_ru, unit='ru', service='kv')
            # self.assertTrue(assert_query, msg)
            self.assertEqual(before_kv_wu, after_kv_wu) # no kv write
            self.assertEqual(expected_ru + sc_ru, after_kv_ru-before_kv_ru)


    def test_kv_inline_udf(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
            self.run_query(database, insert_query)

            # Create function should shows no bilingUnits
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            result = self.run_query(database, f"CREATE or REPLACE FUNCTION {self.scope}.select_func() {{(SELECT * FROM {self.collection})}}")
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)
            self.assertEqual(before_kv_ru, after_kv_ru)
            self.assertEqual(before_kv_wu, after_kv_wu)
            self.assertTrue('billingUnits' not in result)

            # Get sequential scan read unit
            result = self.run_query(database, f'SELECT meta().id FROM {self.collection}')
            sc_ru = result['billingUnits']['ru']['kv']

            expected_ru = self.doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_ru)
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            result = self.run_query(database, f'EXECUTE FUNCTION {self.scope}.select_func()')
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)
            assert_query, msg = meter.assert_query_billing_unit(result, expected_ru + sc_ru, unit='ru', service='kv')
            self.assertTrue(assert_query, msg)
            self.assertEqual(after_kv_wu, before_kv_wu) # no KV write
            self.assertEqual(expected_ru + sc_ru, after_kv_ru-before_kv_ru)

    def test_no_rw_unit(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
            self.run_query(database, insert_query)
            self.run_query(database, f'CREATE INDEX idx_name IF NOT EXISTS on {self.collection}(name)')
            self.check_index_status(database, 'idx_name', desired_state='online')
            # Wait another 10s to make sure RU for index are recorded
            time.sleep(10)
            queries = {
                'explain': f'EXPLAIN SELECT name, city FROM {self.collection} WHERE name = repeat("a", {self.value_size})',
                'advise': f'ADVISE SELECT name, city FROM {self.collection} WHERE name = repeat("a", {self.value_size})',
                'catalog': 'SELECT * FROM system:keyspaces',
                'query1': 'SELECT * FROM array_repeat("a", 10) as t',
                'system_scope_query': f'SELECT * FROM `{database.id}`.`_system`.`_query`',
                'system_scope_mobile': f'SELECT * FROM `{database.id}`.`_system`.`_mobile`',
                'system_scope_eventing': f'SELECT * FROM `{database.id}`.`_system`.`_eventing`',
            }
            before_index_ru, before_index_wu = meter.get_index_rwu(database.id)
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            result = self.run_query(database, queries[self.statement])
            after_index_ru, after_index_wu = meter.get_index_rwu(database.id)
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            # No kv or index RWU
            self.assertTrue('billingUnits' not in result)
            self.assertEqual(before_index_ru, after_index_ru)
            self.assertEqual(before_index_wu, after_index_wu)
            self.assertEqual(before_kv_wu, after_kv_wu)
            self.assertEqual(before_kv_ru, after_kv_ru)

    def test_no_rw_scope(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO `{database.id}`.s1.c1 (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'

            # Create scope
            before_index_ru, before_index_wu = meter.get_index_rwu(database.id)
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            result = self.run_query(database, f'CREATE SCOPE `{database.id}`.s1')
            after_index_ru, after_index_wu = meter.get_index_rwu(database.id)
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            # No kv or index RWU
            self.assertTrue('billingUnits' not in result)
            self.assertEqual(before_index_ru, after_index_ru)
            self.assertEqual(before_index_wu, after_index_wu)
            self.assertEqual(before_kv_wu, after_kv_wu)
            self.assertEqual(before_kv_ru, after_kv_ru)

            self.run_query(database, f'CREATE COLLECTION `{database.id}`.s1.c1')
            time.sleep(2)
            self.run_query(database, insert_query)
            self.run_query(database, f'CREATE INDEX idx_name on `{database.id}`.s1.c1(name)')
            time.sleep(10)

            # Drop scope
            before_index_ru, before_index_wu = meter.get_index_rwu(database.id)
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            result = self.run_query(database, f'DROP SCOPE `{database.id}`.s1')
            after_index_ru, after_index_wu = meter.get_index_rwu(database.id)
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            # No kv or index RWU
            self.assertTrue('billingUnits' not in result)
            self.assertEqual(before_index_ru, after_index_ru)
            self.assertEqual(before_index_wu, after_index_wu)
            self.assertEqual(before_kv_wu, after_kv_wu)
            self.assertEqual(before_kv_ru, after_kv_ru)

    def test_no_rw_collection(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO `{database.id}`.s1.c1 (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
            self.run_query(database, f'CREATE SCOPE `{database.id}`.s1')

            # Create collection
            before_index_ru, before_index_wu = meter.get_index_rwu(database.id)
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            result = self.run_query(database, f'CREATE COLLECTION `{database.id}`.s1.c1')
            after_index_ru, after_index_wu = meter.get_index_rwu(database.id)
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            # No kv or index RWU
            self.assertTrue('billingUnits' not in result)
            self.assertEqual(before_index_ru, after_index_ru)
            self.assertEqual(before_index_wu, after_index_wu)
            self.assertEqual(before_kv_wu, after_kv_wu)
            self.assertEqual(before_kv_ru, after_kv_ru)
            
            time.sleep(2)
            self.run_query(database, insert_query)
            self.run_query(database, f'CREATE INDEX idx_name on `{database.id}`.s1.c1(name)')
            time.sleep(10)

            # Drop collection
            before_index_ru, before_index_wu = meter.get_index_rwu(database.id)
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            result = self.run_query(database, f'DROP COLLECTION `{database.id}`.s1.c1')
            after_index_ru, after_index_wu = meter.get_index_rwu(database.id)
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            # No kv or index RWU
            self.assertTrue('billingUnits' not in result)
            self.assertEqual(before_index_ru, after_index_ru)
            self.assertEqual(before_index_wu, after_index_wu)
            self.assertEqual(before_kv_wu, after_kv_wu)
            self.assertEqual(before_kv_ru, after_kv_ru)

    def test_eval_cu(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            # simple eval, we expect at least 1
            expected_cu = 1
            before_cu = meter.get_query_cu(database.id, variant="eval")
            result = self.run_query(database, 'SELECT 10+10')
            after_cu = meter.get_query_cu(database.id, variant="eval")
            self.assertEqual(expected_cu, after_cu - before_cu)

            before_cu = meter.get_query_cu(database.id, variant="eval")
            repeat=100
            result = self.run_query(database, f'SELECT SUM( {"SQRT("*repeat} t {")"*repeat} ) FROM array_concat(array_repeat(pi(), 30000), array_repeat(pi(), 30000), array_repeat(pi(), 30000), array_repeat(pi(), 19000) ) t')
            after_cu = meter.get_query_cu(database.id, variant="eval")
            # 1 compute unit is 32MB per second
            expected_cu = math.ceil(int(result['metrics']['usedMemory']) * float(result['metrics']['executionTime'][:-1]) / (32*1024*1024))
            self.assertEqual (expected_cu, after_cu - before_cu)

    def test_seqscan_multiple_vb(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO {self.collection}(key k, value v) with dockeys as (\
                ["k000000156", "k000000082", "k000000056", "k000000012", "k000000013", "k000000057", "k000000083", "k000000157", "k000000081", "k000000155"]\
                ) SELECT k, {self.doc} as v FROM dockeys as k'
            # insert 10 docs, based on keys they should end up in (10) different vbucket
            self.run_query(database, insert_query)
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            result = self.run_query(database, f'SELECT meta().id FROM {self.collection}')
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            expected_sc_ru = 2 * 10 # 2 per vbucket
            self.assertTrue(after_kv_wu, before_kv_wu)
            assert_query, msg = meter.assert_query_billing_unit(result, expected_sc_ru, unit='ru', service='kv')
            self.assertTrue(assert_query, msg)
            self.assertTrue(expected_sc_ru, after_kv_ru - before_kv_ru)

    def test_seqscan_single_vb(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO {self.collection}(key k, value v) with dockeys as (\
                ["k000000156", "k000000162", "k000000213", "k000000227", "k000000383", "k000000555", "k000000561", "k000000579", "k000000608", "k000000610"]\
                ) SELECT k, {self.doc} as v FROM dockeys as k'
            # insert 10 docs, based on keys they should end up in same vbucket
            self.run_query(database, insert_query)
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            result = self.run_query(database, f'SELECT meta().id FROM {self.collection}')
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            expected_sc_ru = 2 * 1 # 2 per vbucket
            self.assertTrue(after_kv_wu, before_kv_wu)
            assert_query, msg = meter.assert_query_billing_unit(result, expected_sc_ru, unit='ru', service='kv')
            self.assertTrue(assert_query, msg)
            self.assertTrue(expected_sc_ru, after_kv_ru - before_kv_ru)
