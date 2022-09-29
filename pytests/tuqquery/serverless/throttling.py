from serverless.serverless_basetestcase import ServerlessBaseTestCase
from lib.metering_throttling import throttling
import time

class QueryThrottleSanity(ServerlessBaseTestCase):
    def setUp(self):
        self.doc_count = 10
        self.scope = '_default'
        self.collection = '_default'
        return super().setUp()

    def tearDown(self):
        return super().tearDown()

    def suite_setUp(self):
        pass

    def suite_tearDown(self):
        pass

    def test_throttle_kv(self):
        self.provision_databases()
        for database in self.databases.values():
            throttle = throttling(database.rest_host, database.admin_username, database.admin_password)
            kv_limit = throttle.get_bucket_limit(database.id,'dataThrottleLimit')
            before_count, before_seconds = throttle.get_metrics(database.id, service='kv')
            result = self.run_query(database, f'INSERT INTO {self.collection} (key k, value v) select uuid() as k , {{"name": "San Francisco"}} as v from array_range(0,{kv_limit*2}) d')
            result = self.run_query(database, f'UPDATE {self.collection} SET name = "Paris"')
            after_count, after_seconds = throttle.get_metrics(database.id, service='kv')
            self.assertTrue(after_count > before_count)
            # self.assertTrue(after_seconds > before_seconds)

    def test_throttle_index(self):
        self.provision_databases()
        for database in self.databases.values():
            throttle = throttling(database.rest_host, database.admin_username, database.admin_password)
            index_limit = throttle.get_bucket_limit(database.id,'indexThrottleLimit')
            before_count, before_seconds = throttle.get_metrics(database.id, service='index')
            result = self.run_query(database, f'INSERT INTO {self.collection} (key k, value v) select uuid() as k , {{"name": "San Francisco"}} as v from array_range(0,{index_limit*2}) d')
            result = self.run_query(database, f'CREATE INDEX idx_name on {self.collection}(name)')
            after_count, after_seconds = throttle.get_metrics(database.id, service='index')
            self.assertTrue(after_count > before_count)
            self.assertTrue(after_seconds > before_seconds)

    def test_throttle_query(self):
        self.provision_databases()
        for database in self.databases.values():
            throttle = throttling(database.rest_host, database.admin_username, database.admin_password)
            kv_limit = throttle.get_bucket_limit(database.id,'dataThrottleLimit')
            result = self.run_query(database, f'INSERT INTO {self.collection} (key k, value v) select uuid() as k , {{"name": "San Francisco"}} as v from array_range(0,{kv_limit*2}) d')

            _, _ = throttle.get_metrics(database.id, service='kv')
            throttle.set_bucket_limit(database.id, value=int(kv_limit/5), service='dataThrottleLimit')
            time.sleep(2)
            result = self.run_query(database, f'SELECT count(city) FROM {self.collection}')
            execution_time_lower_throttle = self.get_execution_time(result['metrics']['executionTime'])

            _, _ = throttle.get_metrics(database.id, service='kv')
            throttle.set_bucket_limit(database.id, value=int(kv_limit/2), service='dataThrottleLimit')
            time.sleep(2)
            result = self.run_query(database, f'SELECT count(city) FROM {self.collection}')
            execution_time_higher_throttle = self.get_execution_time(result['metrics']['executionTime'])

            _, _ = throttle.get_metrics(database.id, service='kv')
            self.assertTrue(execution_time_lower_throttle > execution_time_higher_throttle*2, f'execution_time_lower_throttle: {execution_time_lower_throttle} and execution_time_higher_throttle: {execution_time_higher_throttle}')

    def get_execution_time(self, time):
        if time[-2:] == 'ms':
            return float(time[:-2])*1000
        if time[-1] == 's':
            return float(time[:-1])