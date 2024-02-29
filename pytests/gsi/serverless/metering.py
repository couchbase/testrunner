from serverless.serverless_basetestcase import ServerlessBaseTestCase
from TestInput import TestInputSingleton
import json
import math
from lib.metering_throttling import metering
import random, string
import time
from lib.Cb_constants.CBServer import CbServer

class GSIMeter(ServerlessBaseTestCase):
    def setUp(self):
        self.input = TestInputSingleton.input
        self.scope = '_default'
        self.collection = '_default'
        self.doc_count = self.input.param("doc_count", 10)
        self.value_size = self.input.param("value_size", 256)
        self.create_distinct = self.input.param("create_distinct", False)
        self.with_returning = self.input.param("with_returning", False)
        self.statement = self.input.param("statement", "explain")
        self.kv_wu, self.kv_ru = 1024, 4096
        self.index_wu, self.index_ru = 1024, 256
        self.doc_value = ''.join(random.choices(string.ascii_lowercase, k=self.value_size))
        self.doc_value2 = ''.join(random.choices(string.ascii_lowercase, k=self.value_size))
        self.array_doc_value = ''.join(random.choices(string.ascii_lowercase, k=int(self.value_size/6)))
        self.array_doc_value2 = ''.join(random.choices(string.ascii_lowercase, k=int(self.value_size/6)))
        self.array_doc_value3 = ''.join(random.choices(string.ascii_lowercase, k=int(self.value_size/6)))
        self.array_doc_value4 = ''.join(random.choices(string.ascii_lowercase, k=int(self.value_size/6)))
        self.array_doc_value5 = ''.join(random.choices(string.ascii_lowercase, k=int(self.value_size/6)))
        self.doc = {"name": f'{self.doc_value}'}
        self.composite_doc = {"fname": f'{self.doc_value}', "lname": f'{self.doc_value2}'}
        self.array_doc = {"names": [{"fname":f'{self.array_doc_value}',"lname":f'{self.array_doc_value2}'},{"fname":f'{self.array_doc_value}',"lname":f'{self.array_doc_value3}'},{"fname":f'{self.array_doc_value4}',"lname":f'{self.array_doc_value5}'}]}
        self.array_index_key_size = len(self.array_doc_value) + len(self.array_doc_value) + len(self.array_doc_value4)
        self.distinct_array_index_key_size = len(self.array_doc_value) + len(self.array_doc_value4)
        self.index_key_size = len(self.doc_value)  # index is on name
        self.composite_index_key_size = len(self.doc_value) + len(self.doc_value2)
        self.doc_key_size = 36  # use uuid()
        self.doc_size = len(json.dumps(self.doc))
        self.composite_doc_size = len(json.dumps(self.composite_doc))
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

    def test_index_write(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            result = self.run_query(database, f'INSERT INTO {self.collection} (key k, value v) select uuid() as k , {{"name": "San Francisco"}} as v from array_range(0,{self.doc_count}) d')

            before_ix_ru, before_ix_wu = meter.get_index_rwu(database.id, "true", "build")
            result = self.run_query(database, f'CREATE INDEX idx_name on {self.collection}(name)')
            self.check_index_online(database, 'idx_name')
            after_ix_ru, after_ix_wu = meter.get_index_rwu(database.id, "true", "build")

            self.assertTrue('billingUnits' not in result)
            self.assertEqual(after_ix_wu - before_ix_wu, self.doc_count)
            self.assertEqual(after_ix_ru, before_ix_ru)

    def test_index_read(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            result = self.run_query(database, f'INSERT INTO {self.collection} (key k, value v) select uuid() as k , {{"name": "San Francisco"}} as v from array_range(0,{self.doc_count}) d')
            result = self.run_query(database, f'CREATE INDEX idx_name on {self.collection}(name)')
            self.check_index_online(database, 'idx_name')

            before_ix_ru, before_ix_wu = meter.get_index_rwu(database.id, "true", "build")
            result = self.run_query(database, f'SELECT name FROM {self.collection} WHERE name is not null')
            after_ix_ru, after_ix_wu = meter.get_index_rwu(database.id, "true", "build")

            index_ru = math.ceil(self.doc_count * (36 + len('San Francisco')) / self.index_ru * 1.1)
            self.assertEqual(result['billingUnits'], {'ru': {'index': index_ru}})
            self.assertEqual(after_ix_ru - before_ix_ru, index_ru)
            self.assertEqual(after_ix_wu, before_ix_wu)

    def test_gsi_create(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
            self.run_query(database, f'DELETE from {self.collection}')
            self.run_query(database, f'DROP INDEX idx_name IF EXISTS on {self.collection}')
            self.run_query(database, insert_query)

            before_index_ru, before_index_wu = meter.get_index_rwu(database.id, "true", "build")
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            self.run_query(database, f'CREATE INDEX idx_name on {self.collection}(name)')
            self.check_index_online(database, 'idx_name')
            after_index_ru, after_index_wu = meter.get_index_rwu(database.id, "true", "build")
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            expected_index_wu = self.doc_count * math.ceil((self.doc_key_size + self.index_key_size) / self.index_wu)

            self.assertEqual(before_index_ru, after_index_ru) # no index ru
            self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
            self.assertEqual(before_kv_wu, after_kv_wu) # no kv wu

    def test_gsi_create_deferred(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
            self.run_query(database, f'DELETE from {self.collection}')
            self.run_query(database, f'DROP INDEX idx_name IF EXISTS on {self.collection}')
            self.run_query(database, insert_query)

            before_index_ru, before_index_wu = meter.get_index_rwu(database.id, "true", "build")
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            self.run_query(database, f'CREATE INDEX idx_name on {self.collection}(name) WITH {{"defer_build":true}}')
            time.sleep(30)
            after_index_ru, after_index_wu = meter.get_index_rwu(database.id, "true", "build")
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            expected_index_wu = 0

            self.assertEqual(before_index_ru, after_index_ru) # no index ru
            self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
            self.assertEqual(before_kv_wu, after_kv_wu) # no kv wu

            self.run_query(database, f'BUILD INDEX ON {self.collection}(idx_name)')
            self.check_index_online(database, 'idx_name')
            after_index_ru, after_index_wu = meter.get_index_rwu(database.id, "true", "build")
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            expected_index_wu = self.doc_count * math.ceil((self.doc_key_size + self.index_key_size) / self.index_wu)

            self.assertEqual(before_index_ru, after_index_ru) # no index ru
            self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
            self.assertEqual(before_kv_wu, after_kv_wu) # no kv wu

    def test_create_composite(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {self.composite_doc} as v FROM array_range(0,{self.doc_count}) d'
            self.run_query(database,f'DELETE from {self.collection}')
            self.run_query(database,f'DROP INDEX idx_name IF EXISTS on {self.collection}')
            self.run_query(database,insert_query)

            self.run_query(database,f'DELETE from {self.collection}')
            self.run_query(database,f'DROP INDEX idx_name IF EXISTS on {self.collection}')
            self.run_query(database,insert_query)

            before_index_ru, before_index_wu = meter.get_index_rwu(database.id, "true", "build")
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            self.run_query(database,f'CREATE INDEX idx_name on {self.collection}(fname,lname)')
            self.check_index_online(database, 'idx_name')
            after_index_ru, after_index_wu = meter.get_index_rwu(database.id, "true", "build")
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            expected_index_wu = self.doc_count * math.ceil((self.doc_key_size + self.composite_index_key_size) / self.index_wu)

            self.assertEqual(before_index_ru, after_index_ru) # no index ru
            self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
            self.assertEqual(before_kv_wu, after_kv_wu) # no kv wu

    def test_create_composite_diff_values(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            doc1 = {"name": "San Francisco", "type": "city"}
            doc2 = {"name": "Oakland", "type": "city"}
            doc3 = {"name": "USA", "type": "country", "extra": "yes"}
            doc_size1 = len(json.dumps(doc1))
            doc_size2 = len(json.dumps(doc2))
            doc_size3 = len(json.dumps({"name": "USA", "type": "country"}))

            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {doc1} as v FROM array_range(0,5) d'
            insert_query2 = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {doc2} as v FROM array_range(0,5) d'
            insert_query3 = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {doc3} as v FROM array_range(0,5) d'

            self.run_query(database,f'DELETE from {self.collection}')
            self.run_query(database,f'DROP INDEX idx_name IF EXISTS on {self.collection}')
            self.run_query(database,insert_query)
            self.run_query(database,insert_query2)
            self.run_query(database,insert_query3)

            before_index_ru, before_index_wu = meter.get_index_rwu(database.id, "true", "build")
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            self.run_query(database,f'CREATE INDEX idx_name on {self.collection}(name,type)')
            self.check_index_online(database, 'idx_name')
            after_index_ru, after_index_wu = meter.get_index_rwu(database.id, "true", "build")
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            expected_index_wu = 5 * math.ceil((doc_size1 + self.index_key_size) / self.index_wu) + 5 * math.ceil((doc_size2 + self.index_key_size) / self.index_wu) + 5 * math.ceil((doc_size3 + self.index_key_size) / self.index_wu)
            self.assertEqual(before_index_ru, after_index_ru) # no index ru
            self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
            self.assertEqual(before_kv_wu, after_kv_wu) # no kv wu

    def test_gsi_create_partial(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
            self.run_query(database,f'DELETE from {self.collection}')
            self.run_query(database,f'DROP INDEX idx_name IF EXISTS on {self.collection}')
            self.run_query(database,insert_query)
            doc1 = {"name":"hotel"}
            insert_query2 = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {doc1} as v FROM array_range(0,4) d'
            self.run_query(database,insert_query2)

            name = self.doc_value
            before_index_ru, before_index_wu = meter.get_index_rwu(database.id, "true", "build")
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            self.run_query(database,f'CREATE INDEX idx_name on {self.collection}(name) where name = "{name}"')
            self.check_index_online(database, 'idx_name')
            after_index_ru, after_index_wu = meter.get_index_rwu(database.id, "true", "build")
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            expected_index_wu = self.doc_count * math.ceil((self.doc_key_size + self.index_key_size) / self.index_wu)

            self.assertEqual(before_index_ru, after_index_ru) # no index ru
            self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
            self.assertEqual(before_kv_wu, after_kv_wu) # no kv wu

    def test_create_missing(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            doc2 = {"type": "airline"}
            doc_size2 = len(json.dumps(doc2))
            index_key_size2 = len("airline")

            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
            insert_query2 = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {doc2} as v FROM array_range(0,4) d'

            self.run_query(database,f'DELETE from {self.collection}')
            self.run_query(database,f'DROP INDEX idx_name IF EXISTS on {self.collection}')
            self.run_query(database,insert_query)
            self.run_query(database,insert_query2)


            before_index_ru, before_index_wu = meter.get_index_rwu(database.id, "true", "build")
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            self.run_query(database,f'CREATE INDEX idx_name on {self.collection}(name INCLUDE MISSING)')
            self.check_index_online(database, 'idx_name')
            after_index_ru, after_index_wu = meter.get_index_rwu(database.id, "true", "build")
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            expected_index_wu = self.doc_count * math.ceil((self.doc_key_size + self.index_key_size) / self.index_wu) + 4 * math.ceil((doc_size2 + index_key_size2) / self.index_wu)

            self.assertEqual(before_index_ru, after_index_ru) # no index ru
            self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
            self.assertEqual(before_kv_wu, after_kv_wu) # no kv wu

    def test_create_primary(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
            self.run_query(database,f'DELETE from {self.collection}')
            self.run_query(database,f'DROP PRIMARY INDEX IF EXISTS on {self.collection}')
            self.run_query(database,insert_query)

            before_index_ru, before_index_wu = meter.get_index_rwu(database.id, "true", "build")
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            self.run_query(database,f'CREATE PRIMARY INDEX ON {self.collection}')
            self.check_index_online(database, '#primary')
            after_index_ru, after_index_wu = meter.get_index_rwu(database.id, "true", "build")
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            expected_index_wu = self.doc_count * math.ceil((self.doc_key_size) / self.index_wu)

            self.assertEqual(before_index_ru, after_index_ru)  # no index ru
            self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
            self.assertEqual(before_kv_wu, after_kv_wu)  # no kv wu

    def test_create_equivalent(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
            self.run_query(database,f'DELETE from {self.collection}')
            self.run_query(database,f'DROP INDEX idx_name IF EXISTS on {self.collection}')
            self.run_query(database,insert_query)

            before_index_ru, before_index_wu = meter.get_index_rwu(database.id, "true", "build")
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            self.run_query(database,f'CREATE INDEX idx_name on {self.collection}(name)')
            self.run_query(database,f'CREATE INDEX idx_name2 on {self.collection}(name)')
            self.check_index_online(database, 'idx_name')
            self.check_index_online(database, 'idx_name2')

            after_index_ru, after_index_wu = meter.get_index_rwu(database.id, "true", "build")
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            expected_index_wu = self.doc_count * math.ceil((self.doc_key_size + self.index_key_size) / self.index_wu) * 2

            self.assertEqual(before_index_ru, after_index_ru)  # no index ru
            self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
            self.assertEqual(before_kv_wu, after_kv_wu)  # no kv wu

    def test_gsi_create_partial_diff_values(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            doc1 = {"name": "hotel"}
            doc2 = {"name": "airport"}
            doc3 = {"name": "airline"}
            self.doc_size = len(json.dumps(doc1))
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {doc1} as v FROM array_range(0,4) d'
            insert_query2 = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {doc2} as v FROM array_range(0,4) d'
            insert_query3 = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {doc3} as v FROM array_range(0,4) d'

            self.run_query(database,f'DELETE from {self.collection}')
            self.run_query(database,f'DROP INDEX idx_name IF EXISTS on {self.collection}')
            self.run_query(database,insert_query)
            self.run_query(database,insert_query2)
            self.run_query(database,insert_query3)

            before_index_ru, before_index_wu = meter.get_index_rwu(database.id, "true", "build")
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            self.run_query(database,f'CREATE INDEX idx_name on {self.collection}(name) where name = "hotel"')
            self.check_index_online(database, 'idx_name')
            after_index_ru, after_index_wu = meter.get_index_rwu(database.id, "true", "build")
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            expected_index_wu = 4 * math.ceil((self.doc_key_size + self.index_key_size) / self.index_wu)

            self.assertEqual(before_index_ru, after_index_ru)  # no index ru
            self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
            self.assertEqual(before_kv_wu, after_kv_wu)  # no kv wu

    def test_create_index_non_covering(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            doc1 = {"name": "hotel"}
            doc2 = {"name": "airport"}
            doc3 = {"type": "airline"}
            doc_size1 = len(json.dumps(doc1))
            doc_size2 = len(json.dumps(doc2))
            doc_size3 = len(json.dumps(doc3))

            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {doc1} as v FROM array_range(0,4) d'
            insert_query2 = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {doc2} as v FROM array_range(0,4) d'
            insert_query3 = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {doc3} as v FROM array_range(0,4) d'

            self.run_query(database,f'DELETE from {self.collection}')
            self.run_query(database,f'DROP INDEX idx_name IF EXISTS on {self.collection}')
            self.run_query(database,insert_query)
            self.run_query(database,insert_query2)
            self.run_query(database,insert_query3)

            before_index_ru, before_index_wu = meter.get_index_rwu(database.id, "true", "build")
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            self.run_query(database,f'CREATE INDEX idx_name on {self.collection}(name)')
            self.check_index_online(database, 'idx_name')
            after_index_ru, after_index_wu = meter.get_index_rwu(database.id, "true", "build")
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            expected_index_wu = 4 * math.ceil((doc_size1 + self.index_key_size) / self.index_wu) + 4 * math.ceil((doc_size2 + self.index_key_size) / self.index_wu)

            self.assertEqual(before_index_ru, after_index_ru)  # no index ru
            self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
            self.assertEqual(before_kv_wu, after_kv_wu)  # no kv wu

    def test_create_missing_diff_values(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            doc1 = {"name": "hotel"}
            doc2 = {"name": "airport"}
            doc3 = {"type": "airline"}
            doc_size1 = len(json.dumps(doc1))
            doc_size2 = len(json.dumps(doc2))
            doc_size3 = len(json.dumps(doc3))

            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {doc1} as v FROM array_range(0,4) d'
            insert_query2 = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {doc2} as v FROM array_range(0,4) d'
            insert_query3 = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {doc3} as v FROM array_range(0,4) d'

            self.run_query(database,f'DELETE from {self.collection}')
            self.run_query(database,f'DROP INDEX idx_name IF EXISTS on {self.collection}')
            self.run_query(database,insert_query)
            self.run_query(database,insert_query2)
            self.run_query(database,insert_query3)

            before_index_ru, before_index_wu = meter.get_index_rwu(database.id, "true", "build")
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            self.run_query(database,f'CREATE INDEX idx_name on {self.collection}(name INCLUDE MISSING)')
            self.check_index_online(database, 'idx_name')
            after_index_ru, after_index_wu = meter.get_index_rwu(database.id, "true", "build")
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            expected_index_wu = 4 * math.ceil((doc_size1 + self.index_key_size) / self.index_wu) + 4 * math.ceil((doc_size2 + self.index_key_size) / self.index_wu) + 4 * math.ceil((doc_size3 + self.index_key_size) / self.index_wu)

            self.assertEqual(before_index_ru, after_index_ru)  # no index ru
            self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
            self.assertEqual(before_kv_wu, after_kv_wu)  # no kv wu

    def test_create_array_index(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {self.array_doc} as v FROM array_range(0,{self.doc_count}) d'
            self.run_query(database,f'DELETE from {self.collection}')
            self.run_query(database,f'DROP INDEX idx_names IF EXISTS on {self.collection}')
            self.run_query(database,insert_query)

            before_index_ru, before_index_wu = meter.get_index_rwu(database.id, "true", "build")
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            if self.create_distinct:
                self.run_query(database,f'CREATE INDEX idx_names on {self.collection}(DISTINCT ARRAY x.fname for x in names END)')
            else:
                self.run_query(database,f'CREATE INDEX idx_names on {self.collection}(ALL ARRAY x.fname for x in names END)')
            self.check_index_online(database, 'idx_names')
            after_index_ru, after_index_wu = meter.get_index_rwu(database.id, "true", "build")
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            expected_index_wu = self.doc_count * math.ceil((self.doc_key_size + self.distinct_array_index_key_size) / self.index_wu)

            self.assertEqual(before_index_ru, after_index_ru) # no index ru
            self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
            self.assertEqual(before_kv_wu, after_kv_wu) # no kv wu

    def test_gsi_select(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
            self.run_query(database,f'DELETE from {self.collection}')
            self.run_query(database, insert_query)

            self.run_query(database, f'CREATE INDEX idx_name IF NOT EXISTS on {self.collection}(name)')
            self.check_index_online(database, 'idx_name')

            expected_kv_ru = self.doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_ru)
            expected_index_ru = math.ceil(self.doc_count * 1.1 * (self.doc_key_size + self.index_key_size) / self.index_ru)
            result = self.run_query(database, f'SELECT name, city FROM {self.collection} WHERE name = {self.doc_value}')
            assert_query, msg = meter.assert_query_billing_unit(result, expected_kv_ru, unit='ru', service = 'kv')
            self.assertTrue(assert_query, msg)
            assert_query, msg = meter.assert_query_billing_unit(result, expected_index_ru, unit='ru', service = 'index')
            self.assertTrue(assert_query, msg)

    def test_select_primary(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
            self.run_query(database,f'DELETE from {self.collection}')
            self.run_query(database,f'DROP PRIMARY INDEX IF EXISTS on {self.collection}')
            self.run_query(database,insert_query)

            self.run_query(database,f'CREATE PRIMARY INDEX ON {self.collection}')
            self.check_index_online(database, '#primary')

            expected_kv_ru = self.doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_ru)
            expected_index_ru = math.ceil(self.doc_count * 1.1 * (self.doc_key_size) / self.index_ru)
            result = self.run_query(database,f'SELECT name, city FROM {self.collection} WHERE name = {self.doc_value}')
            assert_query, msg = meter.assert_query_billing_unit(result, expected_kv_ru, unit='ru', service = 'kv')
            self.assertTrue(assert_query, msg)
            assert_query, msg = meter.assert_query_billing_unit(result, expected_index_ru, unit='ru', service = 'index')
            self.assertTrue(assert_query, msg)

    def test_select_non_covering(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {self.composite_doc} as v FROM array_range(0,{self.doc_count}) d'
            self.run_query(database,f'DELETE from {self.collection}')
            self.run_query(database,insert_query)

            self.run_query(database,f'CREATE INDEX idx_name IF NOT EXISTS on {self.collection}(fname)')
            self.check_index_online(database, 'idx_name')

            expected_kv_ru = self.doc_count * math.ceil( (self.composite_doc_size + self.doc_key_size) / self.kv_ru)
            expected_index_ru = math.ceil(self.doc_count * 1.1 * (self.doc_key_size + self.index_key_size) / self.index_ru)
            result = self.run_query(database,f'SELECT fname, lname FROM {self.collection} WHERE fname = "{self.doc_value}" and lname = "{self.doc_value2}"')
            assert_query, msg = meter.assert_query_billing_unit(result, expected_kv_ru, unit='ru', service = 'kv')
            self.assertTrue(assert_query, msg)
            assert_query, msg = meter.assert_query_billing_unit(result, expected_index_ru, unit='ru', service = 'index')
            self.assertTrue(assert_query, msg)

    def test_select_partial(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            doc1 = {"name":"hotel"}
            doc2 = {"name":"airport"}
            doc3 = {"name":"airline"}
            self.doc_size = len(json.dumps(doc1))
            self.index_key_size = 5 # value of hotel
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {doc1} as v FROM array_range(0,4) d'
            insert_query2 = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {doc2} as v FROM array_range(0,4) d'
            insert_query3 = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {doc3} as v FROM array_range(0,4) d'

            self.run_query(database,f'DELETE from {self.collection}')
            self.run_query(database,f'DROP INDEX idx_name IF EXISTS on {self.collection}')
            self.run_query(database,insert_query)
            self.run_query(database,insert_query2)
            self.run_query(database,insert_query3)

            self.run_query(database,f'CREATE INDEX idx_name on {self.collection}(name) where name = "hotel"')
            self.check_index_online(database, 'idx_name')

            expected_index_ru = math.ceil(4 * 1.1 * (self.doc_key_size + self.index_key_size) / self.index_ru)
            result = self.run_query(database,f'SELECT name FROM {self.collection} WHERE name = "hotel"')
            assert_query, msg = meter.assert_query_billing_unit(result, expected_index_ru, unit='ru', service = 'index')
            self.assertTrue(assert_query, msg)

    def test_select_composite(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {self.composite_doc} as v FROM array_range(0,{self.doc_count}) d'
            self.run_query(database,f'DELETE from {self.collection}')
            self.run_query(database,f'DROP INDEX idx_name IF EXISTS on {self.collection}')
            self.run_query(database,insert_query)

            self.run_query(database,f'CREATE INDEX idx_name on {self.collection}(fname,lname)')
            self.check_index_online(database, 'idx_name')

            expected_index_ru = math.ceil(self.doc_count * 1.1 * (self.doc_key_size + self.composite_index_key_size) / self.index_ru)
            result = self.run_query(database,f'SELECT fname, lname FROM {self.collection} WHERE fname = "{self.doc_value}" and lname = "{self.doc_value2}"')
            self.assertTrue("kv" not in str(result['billingUnits']), f"Index is covering, there should be no kv units : {result}")
            assert_query, msg = meter.assert_query_billing_unit(result, expected_index_ru, unit='ru', service = 'index')
            self.assertTrue(assert_query, msg)

    def test_select_composite_diff_values(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            doc1 = {"name": "San Francisco", "type": "city"}
            doc2 = {"name": "Oakland", "type": "city"}
            doc3 = {"name": "USA", "type": "country", "extra": "yes"}
            self.composite_index_key_size = 10

            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {doc1} as v FROM array_range(0,5) d'
            insert_query2 = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {doc2} as v FROM array_range(0,5) d'
            insert_query3 = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {doc3} as v FROM array_range(0,5) d'

            self.run_query(database,f'DELETE from {self.collection}')
            self.run_query(database,f'DROP INDEX idx_name IF EXISTS on {self.collection}')
            self.run_query(database,insert_query)
            self.run_query(database,insert_query2)
            self.run_query(database,insert_query3)
            self.run_query(database,f'CREATE INDEX idx_name on {self.collection}(name,type)')
            self.check_index_online(database, 'idx_name')

            expected_index_ru = 2

            result = self.run_query(database,f'SELECT name FROM {self.collection} WHERE name = "USA" and type = "country"')
            self.assertTrue("kv" not in str(result['billingUnits']), f"Index is covering, there should be no kv units : {result}")
            assert_query, msg = meter.assert_query_billing_unit(result, expected_index_ru, unit='ru', service = 'index')
            self.assertTrue(assert_query, msg)

    def test_select_array_index(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {self.array_doc} as v FROM array_range(0,{self.doc_count}) d'
            self.run_query(database,f'DELETE from {self.collection}')
            self.run_query(database,f'DROP INDEX idx_names IF EXISTS on {self.collection}')
            self.run_query(database,insert_query)

            if self.create_distinct:
                self.run_query(database,f'CREATE INDEX idx_names on {self.collection}(DISTINCT ARRAY x.fname for x in names END)')
            else:
                self.run_query(database,f'CREATE INDEX idx_names on {self.collection}(ALL ARRAY x.fname for x in names END)')
            self.check_index_online(database, 'idx_names')

            self.array_index_key_size = len(self.array_doc_value)

            expected_kv_ru = self.doc_count * math.ceil( (self.doc_size + self.doc_key_size) / self.kv_ru)
            expected_index_ru = math.ceil(self.doc_count * 1.1 * (self.doc_key_size + self.array_index_key_size) / self.index_ru)
            result = self.run_query(database,query=f"select names from {self.collection} where ANY x in names SATISFIES x.fname = '{self.array_doc_value}' END")
            assert_query, msg = meter.assert_query_billing_unit(result, expected_kv_ru, unit='ru', service = 'kv')
            self.assertTrue(assert_query, msg)
            assert_query, msg = meter.assert_query_billing_unit(result, expected_index_ru, unit='ru', service = 'index')
            self.assertTrue(assert_query, msg)

    def test_index_insert(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
            self.run_query(database,f'DELETE from {self.collection}')
            self.run_query(database,f'DROP INDEX idx_name IF EXISTS on {self.collection}')
            self.run_query(database,insert_query)


            self.run_query(database,f'CREATE INDEX idx_name on {self.collection}(name)')
            self.check_index_online(database, 'idx_name')
            before_index_ru, before_index_wu = meter.get_index_rwu(database.id, "true", "build")
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)

            self.new_doc = {"name": "b" * self.value_size}
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
            self.run_query(database,insert_query)
            time.sleep(30)
            # three different write units to look for but only one read unit to look for
            after_index_ru, after_index_wu = meter.get_index_rwu(database.id, "true", "build")
            after_index_ru_update_insert, after_index_wu_update_insert = meter.get_index_rwu(database.id, "true", "updateinsertonly")
            after_index_ru_update, after_index_wu_update = meter.get_index_rwu(database.id, "true", "update")
            after_index_wu = after_index_wu + after_index_wu_update_insert + after_index_wu_update
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            expected_index_wu = self.doc_count * math.ceil((self.doc_key_size + self.index_key_size) / self.index_wu)

            self.assertEqual(before_index_ru, after_index_ru)  # no index ru
            self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)

    def test_index_update(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
            self.run_query(database,f'DELETE from {self.collection}')
            self.run_query(database,f'DROP INDEX idx_name IF EXISTS on {self.collection}')
            self.run_query(database,insert_query)

            self.run_query(database,f'CREATE INDEX idx_name on {self.collection}(name)')
            self.check_index_online(database, 'idx_name')
            before_index_ru, before_index_wu = meter.get_index_rwu(database.id, "true", "build")
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)

            update_query = f'UPDATE {self.collection} SET name = {self.doc_value2}'
            self.run_query(database,update_query)
            time.sleep(30)
            after_index_ru, after_index_wu = meter.get_index_rwu(database.id, "true", "build")
            after_index_ru_update_insert, after_index_wu_update_insert = meter.get_index_rwu(database.id, "true",  "updateinsertonly")
            after_index_ru_update, after_index_wu_update = meter.get_index_rwu(database.id, "true", "update")
            after_index_wu = after_index_wu + after_index_wu_update_insert + after_index_wu_update
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            result = self.run_query(database,f'SELECT meta().id FROM {self.collection}')
            sc_ru = result['billingUnits']['ru']['kv']

            expected_index_wu = self.doc_count * math.ceil((self.doc_key_size + self.index_key_size) / self.index_wu)
            expected_kv_ru = self.doc_count * math.ceil((self.doc_key_size + self.doc_size)/ self.kv_ru) + sc_ru

            self.assertEqual(before_index_ru, after_index_ru)  # no index ru
            self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
            self.assertEqual(expected_kv_ru, after_kv_ru - before_kv_ru)

    def test_index_delete(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {self.doc} as v FROM array_range(0,{self.doc_count}) d'
            self.run_query(database,f'DELETE from {self.collection}')
            self.run_query(database,f'DROP INDEX idx_name IF EXISTS on {self.collection}')
            self.run_query(database,insert_query)

            self.run_query(database,f'CREATE INDEX idx_name on {self.collection}(name)')
            self.check_index_online(database, 'idx_name')
            result = self.run_query(database,f'SELECT meta().id FROM {self.collection}')
            before_index_ru, before_index_wu = meter.get_index_rwu(database.id, "true", "build")
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)

            delete_query = f'DELETE FROM {self.collection}'
            self.run_query(database,delete_query)
            time.sleep(30)
            after_index_ru, after_index_wu = meter.get_index_rwu(database.id, "true", "build")
            after_index_ru_update_insert, after_index_wu_update_insert = meter.get_index_rwu(database.id, "true",
                                                                                             "updateinsertonly")
            after_index_ru_update, after_index_wu_update = meter.get_index_rwu(database.id, "true", "update")
            after_index_wu = after_index_wu + after_index_wu_update_insert + after_index_wu_update
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)

            expected_index_wu = self.doc_count * math.ceil((self.doc_key_size + self.index_key_size) / self.index_wu)
            expected_kv_ru = result['billingUnits']['ru']['kv']

            self.assertEqual(before_index_ru, after_index_ru)  # no index ru
            self.assertEqual(expected_index_wu, after_index_wu - before_index_wu)
            self.assertEqual(expected_kv_ru, after_kv_ru - before_kv_ru)
