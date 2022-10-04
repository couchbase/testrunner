from serverless.serverless_basetestcase import ServerlessBaseTestCase
from lib.metering_throttling import metering
import time
import math

class GSIMeterSanity(ServerlessBaseTestCase):
    def setUp(self):
        self.doc_count = 10
        self.scope = '_default'
        self.collection = '_default'
        self.index_ru = 256
        return super().setUp()

    def tearDown(self):
        return super().tearDown()

    def suite_setUp(self):
        pass

    def suite_tearDown(self):
        pass

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

            before_ix_ru, before_ix_wu = meter.get_index_rwu(database.id)
            result = self.run_query(database, f'CREATE INDEX idx_name on {self.collection}(name)')
            self.check_index_online(database, 'idx_name')
            after_ix_ru, after_ix_wu = meter.get_index_rwu(database.id)

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

            before_ix_ru, before_ix_wu = meter.get_index_rwu(database.id)
            result = self.run_query(database, f'SELECT name FROM {self.collection} WHERE name is not null')
            after_ix_ru, after_ix_wu = meter.get_index_rwu(database.id)

            index_ru = math.ceil(self.doc_count * (36 + len('San Francisco')) / self.index_ru * 1.1)
            self.assertEqual(result['billingUnits'], {'ru': {'index': index_ru}})
            self.assertEqual(after_ix_ru - before_ix_ru, index_ru)
            self.assertEqual(after_ix_wu, before_ix_wu)

