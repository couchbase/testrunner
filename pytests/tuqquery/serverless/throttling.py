from serverless.serverless_basetestcase import ServerlessBaseTestCase
from lib.metering_throttling import throttling
import time
import threading
from lib.Cb_constants.CBServer import CbServer

class QueryThrottleSanity(ServerlessBaseTestCase):
    def setUp(self):
        self.doc_count = 10
        self.scope = '_default'
        self.collection = '_default'
        CbServer.capella_run = True
        return super().setUp()

    def tearDown(self):
        return super().tearDown()

    @staticmethod
    def __get_execution_time(time):
        if time[-2:] == 'ms':
            return float(time[:-2])*1000
        if time[-1] == 's':
            return float(time[:-1])

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

    def test_throttle_query(self):
        self.provision_databases()
        for database in self.databases.values():
            throttle = throttling(database.rest_host, database.admin_username, database.admin_password)
            kv_limit = throttle.get_bucket_limit(database.id,'dataThrottleLimit')
            result = self.run_query(database, f'INSERT INTO {self.collection} (key k, value v) select uuid() as k , {{"name": "San Francisco"}} as v from array_range(0,{kv_limit*2}) d')

            _, _ = throttle.get_metrics(database.id, service='kv')
            throttle.set_bucket_limit(database.id, value=int(kv_limit/5), service='dataThrottleLimit')
            time.sleep(3)
            result = self.run_query(database, f'SELECT count(city) FROM {self.collection}')
            result = self.run_query(database, f'SELECT count(city) FROM {self.collection}')
            result = self.run_query(database, f'SELECT count(city) FROM {self.collection}')
            execution_time_lower_throttle = QueryThrottleSanity.__get_execution_time(result['metrics']['executionTime'])

            _, _ = throttle.get_metrics(database.id, service='kv')
            throttle.set_bucket_limit(database.id, value=int(kv_limit/2), service='dataThrottleLimit')
            time.sleep(3)
            result = self.run_query(database, f'SELECT count(city) FROM {self.collection}')
            result = self.run_query(database, f'SELECT count(city) FROM {self.collection}')
            result = self.run_query(database, f'SELECT count(city) FROM {self.collection}')
            execution_time_higher_throttle = QueryThrottleSanity.__get_execution_time(result['metrics']['executionTime'])

            _, _ = throttle.get_metrics(database.id, service='kv')
            self.assertTrue(execution_time_lower_throttle > execution_time_higher_throttle, f'execution_time_lower_throttle: {execution_time_lower_throttle} and execution_time_higher_throttle: {execution_time_higher_throttle}')

    def run_async_query(self, database, count):
        results = self.run_query(database, f"SELECT raw count(name) FROM {self.collection}")
        self.assertEqual(results['results'], [count])

    def test_not_throttle_multi(self):
        self.provision_databases(2)
        keys = self.databases.keys()
        database1 = self.databases[list(keys)[0]]
        database2 = self.databases[list(keys)[1]]
        try:
            throttle = throttling(database1.rest_host, database1.admin_username, database1.admin_password)
        except:
            throttle = throttling(database2.rest_host, database2.admin_username, database2.admin_password)

        # Set doc count as 80% of kv throttle limit
        limit = throttle.get_bucket_limit(database1.id, 'dataThrottleLimit')
        doc_count = int(limit * 80/100)

        insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {{"name": "San Francisco"}} as v FROM array_range(0,{doc_count}) d'
        result1 = self.run_query(database1, insert_query)
        result2 = self.run_query(database2, insert_query)

        # get throttle count before
        before_count_database1, _ = throttle.get_metrics(database1.id, service='kv')
        before_count_database2, _ = throttle.get_metrics(database2.id, service='kv')

        # create one thread per database
        t1 = threading.Thread(target=self.run_async_query,args=(database1, doc_count))
        t2 = threading.Thread(target=self.run_async_query,args=(database2, doc_count))

        # start threads
        t1.start()
        t2.start()

        # wait for thread to finish
        t1.join()
        t2.join()

        # get throttle count after
        after_count_database1, _ = throttle.get_metrics(database1.id, service='kv')
        after_count_database2, _ = throttle.get_metrics(database2.id, service='kv')
        self.assertEqual(after_count_database1, before_count_database1)
        self.assertEqual(after_count_database2, before_count_database2)
