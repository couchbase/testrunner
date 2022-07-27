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
        self.kv_wu = 1024
        self.kv_ru = 4096
        self.doc = {"name": "a"*self.value_size}
        self.doc_size= 36 + len(json.dumps(self.doc)) # doc length is key (uuid) size + doc size
        self.log.info(f'Doc count: {self.doc_count} - Doc size is: {self.doc_size}')

    def suite_setUp(self):
        super(QueryMeteringTests, self).suite_setUp()

    def tearDown(self):
        super(QueryMeteringTests, self).tearDown()

    def suite_tearDown(self):
        super(QueryMeteringTests, self).suite_tearDown()

    def get_metering_index(self, bucket='default'):
        ru, wu = 0, 0
        api = f"http://{self.server.ip}:9102/_metering"
        ru_pattern = re.compile(f'meter_ru_total{{bucket="{bucket}",for="index",unbilled="",variant=""}} (\d+)')
        wu_pattern = re.compile(f'meter_wu_total{{bucket="{bucket}",for="index",unbilled="",variant=""}} (\d+)')
        response = requests.get(api)
        if ru_pattern.search(response.text):
            ru = int(ru_pattern.findall(response.text)[0])
        if wu_pattern.search(response.text):
            wu = int(wu_pattern.findall(response.text)[0])
        return ru, wu

    def assert_billing_unit(self, result, expected, unit="ru", service="kv"):
        if 'billingUnits' in result.keys():
            if unit in result['billingUnits'].keys():
                if service in result['billingUnits'][unit].keys():
                    actual = result['billingUnits'][unit][service]
                    self.assertEqual(actual, expected, f'Expected {expected} unit but got {actual}')
                else:
                    self.fail(f"result['billingUnits'][{unit}] does not contain {service}, result['billingUnits'][{unit}] is: {result['billingUnits'][unit]}")
            else:
                self.fail(f"result['billingUnits'] does not contain {unit}, result['billingUnits'] is: {result['billingUnits']}")
        else:
            self.fail(f'result does not contain billingUnits, result is: {result}')

    def test_kv_insert(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')

        expected_wu = self.doc_count * math.ceil( self.doc_size / self.kv_wu)
        result = self.run_cbq_query(insert_query)
        self.assert_billing_unit(result, expected_wu, unit='wu', service='kv')

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

        doc_read_size = 36 + len(json.dumps({"name": "San Francisco"}))
        doc_write_size = 36 + len(json.dumps({"name": "a"*self.value_size}))

        expected_wu = self.doc_count * math.ceil( doc_write_size / self.kv_wu)
        expected_ru = self.doc_count * math.ceil( doc_read_size / self.kv_ru)
        result = self.run_cbq_query(f'UPDATE {self.bucket} SET name = repeat("a", {self.value_size})')
        self.assert_billing_unit(result, expected_wu, unit='wu', service='kv')
        self.assert_billing_unit(result, expected_ru, unit='ru', service='kv')

    def test_kv_select(self):
        insert_query = f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(insert_query)

        expected_ru = self.doc_count * math.ceil( self.doc_size / self.kv_ru)
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
        expected_ru = self.doc_count * math.ceil( self.doc_size / self.kv_ru)
        result = self.run_cbq_query(f'SELECT count(name) as count FROM {self.bucket}')
        self.assert_billing_unit(result, expected_ru, unit='ru', service='kv')
        self.assertEqual(result['results'][0]['count'], self.doc_count, f'We expected {self.doc_count} but for diff result: {result}')

