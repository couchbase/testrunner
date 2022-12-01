from serverless.serverless_basetestcase import ServerlessBaseTestCase
from lib.metering_throttling import throttling
import random, string
import threading
import time

class GSIThrottle(ServerlessBaseTestCase):
    def setUp(self):
        self.doc_count = 10
        self.scope = '_default'
        self.collection = '_default'
        self.doc_value = ''.join(random.choices(string.ascii_lowercase, k=5048))
        self.doc_value2 = ''.join(random.choices(string.ascii_lowercase, k=5048))
        self.composite_doc = {"fname": f'{self.doc_value}', "lname": f'{self.doc_value2}'}
        return super().setUp()

    def tearDown(self):
        return super().tearDown()

    def suite_setUp(self):
        pass

    def suite_tearDown(self):
        pass

    def test_throttle_create_index(self):
        self.provision_databases()
        for database in self.databases.values():
            throttle = throttling(database.rest_host, database.admin_username, database.admin_password)
            index_limit = throttle.get_bucket_limit(database.id,'indexThrottleLimit')
            before_count, before_seconds = throttle.get_metrics(database.id, service='index')
            result = self.run_query(database, f'INSERT INTO {self.collection} (key k, value v) select uuid() as k , {{"name": "San Francisco"}} as v from array_range(0,{index_limit*4}) d')
            result = self.run_query(database, f'CREATE INDEX idx_name on {self.collection}(name)')
            after_count, after_seconds = throttle.get_metrics(database.id, service='index')
            self.assertTrue(after_count > before_count)
            self.assertTrue(after_seconds > before_seconds)

    def test_throttle_query_index(self):
        self.provision_databases()
        for database in self.databases.values():
            throttle = throttling(database.rest_host, database.admin_username, database.admin_password)
            kv_limit = throttle.get_bucket_limit(database.id,'indexThrottleLimit')
            result = self.run_query(database, f'INSERT INTO {self.collection} (key k, value v) select uuid() as k , {{"name": "San Francisco"}} as v from array_range(0,{kv_limit*2}) d')

            self.run_query(database, f"CREATE INDEX idx_name on {self.collection}(name)")

            _, _ = throttle.get_metrics(database.id, service='index')
            throttle.set_bucket_limit(database.id, value=int(kv_limit/5), service='indexThrottleLimit')
            time.sleep(3)
            result = self.run_query(database, f'SELECT count(name) FROM {self.collection} where name = "San Francisco"')
            result = self.run_query(database, f'SELECT count(name) FROM {self.collection} where name = "San Francisco"')
            result = self.run_query(database, f'SELECT count(name) FROM {self.collection} where name = "San Francisco"')
            execution_time_lower_throttle = self.get_execution_time(result['metrics']['executionTime'])

            _, _ = throttle.get_metrics(database.id, service='index')
            throttle.set_bucket_limit(database.id, value=int(kv_limit/2), service='indexThrottleLimit')
            time.sleep(3)
            result = self.run_query(database, f'SELECT count(name) FROM {self.collection} where name = "San Francisco"')
            result = self.run_query(database, f'SELECT count(name) FROM {self.collection} where name = "San Francisco"')
            result = self.run_query(database, f'SELECT count(name) FROM {self.collection} where name = "San Francisco"')
            execution_time_higher_throttle = self.get_execution_time(result['metrics']['executionTime'])

            _, _ = throttle.get_metrics(database.id, service='index')
            self.assertTrue(execution_time_lower_throttle > execution_time_higher_throttle, f'execution_time_lower_throttle: {execution_time_lower_throttle} and execution_time_higher_throttle: {execution_time_higher_throttle}')

    def test_throttle_multiple_databases(self):
        self.provision_databases(2)
        keys = self.databases.keys()
        database1 = self.databases[list(keys)[0]]
        database2 = self.databases[list(keys)[1]]
        try:
            throttle = throttling(database1.rest_host, database1.admin_username, database1.admin_password)
        except:
            throttle = throttling(database2.rest_host, database2.admin_username, database2.admin_password)

        index_limit = throttle.get_bucket_limit(database1.id, 'searchThrottleLimit')

        # We want to insert data such that one index hits throttling but the other does not, on the other database, the one that should not hit index throttling should be unaffected by other db
        result = self.run_query(database1,
                                f'INSERT INTO {self.collection} (key k, value v) select uuid() as k , {self.composite_doc} as v from array_range(0,{index_limit*4}) d')
        result2 = self.run_query(database2,
                                f'INSERT INTO {self.collection} (key k, value v) select uuid() as k , {self.composite_doc} as v from array_range(0,{index_limit/10}) d')

        # get throttle count before
        before_count_database1, _ = throttle.get_metrics(database1.id, service='index')
        before_count_database2, _ = throttle.get_metrics(database2.id, service='index')

        # create one thread per database
        t1 = threading.Thread(target=self.run_async_create, args=(database1, "1"))
        t2 = threading.Thread(target=self.run_async_create, args=(database2, "2"))

        # start threads
        t1.start()
        t2.start()

        # wait for thread to finish
        t1.join()
        t2.join()

        # get throttle count after
        after_count_database1, _ = throttle.get_metrics(database1.id, service='index')
        after_count_database2, _ = throttle.get_metrics(database2.id, service='index')
        self.assertTrue(after_count_database1 > before_count_database1)
        self.assertEqual(after_count_database2, before_count_database2)

    def run_async_create(self, database, prefix):
        idx_name = "idx_name" + prefix
        self.run_query(database, f'CREATE INDEX {idx_name} on {self.collection}(fname,lname)')

    def get_execution_time(self,time):
        if time[-2:] == 'ms':
            return float(time[:-2])*1000
        if time[-1] == 's':
            return float(time[:-1])
