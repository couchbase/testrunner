from serverless.serverless_basetestcase import ServerlessBaseTestCase
from lib.metering_throttling import throttling
from TestInput import TestInputSingleton
import random, string
import threading
from membase.api.exception import CBQError
import time
from lib.Cb_constants.CBServer import CbServer


class GSIThrottle(ServerlessBaseTestCase):
    def setUp(self):
        self.input = TestInputSingleton.input
        self.doc_count = 10
        self.scope = '_default'
        self.collection = '_default'
        self.gsi_limit = 200
        self.doc_value = ''.join(random.choices(string.ascii_lowercase, k=5048))
        self.doc_value2 = ''.join(random.choices(string.ascii_lowercase, k=5048))
        self.test_limit=self.input.param("test_limit", 'indexStorageLimit')
        self.composite_doc = {"fname": f'{self.doc_value}', "lname": f'{self.doc_value2}'}
        CbServer.capella_run = True
        return super().setUp()

    def tearDown(self):
        return super().tearDown()

    def test_throttle_create_index(self):
        self.provision_databases()
        for database in self.databases.values():
            throttle = throttling(database.rest_host, database.admin_username, database.admin_password)
            index_limit = throttle.get_bucket_limit(database.id,'indexThrottleLimit')
            before_count, before_seconds = throttle.get_metrics(database.id, service='index')
            num_docs = index_limit*4
            result = self.run_query(database, f'INSERT INTO {self.collection} (key k, value v) select uuid() as k , {self.composite_doc} as v from array_range(0,{num_docs}) d')
            result = self.run_query(database, f'CREATE INDEX idx_name on {self.collection}(fname,lname)')
            after_count, after_seconds = throttle.get_metrics(database.id, service='index')
            self.assertTrue(after_count > before_count)
            self.assertTrue(after_seconds > before_seconds)

    def test_throttle_query_index(self):
        self.provision_databases()
        for database in self.databases.values():
            throttle = throttling(database.rest_host, database.admin_username, database.admin_password)
            index_limit = throttle.get_bucket_limit(database.id,'indexThrottleLimit')
            result = self.run_query(database, f'INSERT INTO {self.collection} (key k, value v) select uuid() as k , {{"name": "San Francisco"}} as v from array_range(0,{index_limit*4}) d')

            self.run_query(database, f"CREATE INDEX idx_name on {self.collection}(name)")

            _, _ = throttle.get_metrics(database.id, service='index')
            throttle.set_bucket_limit(database.id, value=int(index_limit/5), service='indexThrottleLimit')
            time.sleep(3)
            result = self.run_query(database, f'SELECT count(name) FROM {self.collection} where name = "San Francisco"')
            result = self.run_query(database, f'SELECT count(name) FROM {self.collection} where name = "San Francisco"')
            result = self.run_query(database, f'SELECT count(name) FROM {self.collection} where name = "San Francisco"')
            execution_time_lower_throttle = self.get_execution_time(result['metrics']['executionTime'])

            _, _ = throttle.get_metrics(database.id, service='index')
            throttle.set_bucket_limit(database.id, value=int(index_limit/2), service='indexThrottleLimit')
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

        index_limit = throttle.get_bucket_limit(database1.id, 'indexThrottleLimit')

        # We want to insert data such that one index hits throttling but the other does not, on the other database, the one that should not hit index throttling should be unaffected by other db
        result = self.run_query(database1,
                                f'INSERT INTO {self.collection} (key k, value v) select uuid() as k , {self.composite_doc} as v from array_range(0,{index_limit*4}) d')
        result2 = self.run_query(database2,
                                f'INSERT INTO {self.collection} (key k, value v) select uuid() as k , {self.composite_doc} as v from array_range(0,{index_limit/12}) d')

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

    def test_gsi_limit(self):
        self.provision_databases()
        for database in self.databases.values():
            throttle = throttling(database.rest_host, database.admin_username, database.admin_password)
            indexes = self.run_query(database,'SELECT raw count(*) FROM system:indexes WHERE `using` = "gsi"')
            count = indexes['results'][0]
            for i in range(self.gsi_limit - count):
                self.run_query(database,f'CREATE INDEX idx{i} IF NOT EXISTS ON {self.collection}(a)')
            try:
                self.run_query(database,f'CREATE INDEX idx{self.gsi_limit+1} IF NOT EXISTS ON {self.collection}(a)')
                self.fail(f'We should not be able to create index beyond limit of {self.gsi_limit}')
            except CBQError as ex:
                error = self.process_CBQE(ex)
                self.assertEqual(error['code'], 4350)
                self.assertEqual(error['msg'], f'GSI CreateIndex() - cause: Limit for number of indexes that can be created per bucket has been reached. Limit : {self.gsi_limit + 1}')
            finally:
                for i in range(self.gsi_limit):
                    self.run_query(database,f'DROP INDEX idx{i} IF EXISTS ON {self.collection}')

    def test_limit_storage(self):
        self.provision_databases()
        for database in self.databases.values():

            self.run_query(database, f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {{"city": "Paris"}} as v FROM array_range(0,1000) d')

            # Set stroage limit to 0 to simulate we have reached limit
            throttle = throttling(database.rest_host, database.admin_username, database.admin_password)
            throttle.set_bucket_limit(database.id, 0, service=self.test_limit)
            current_limit = throttle.get_bucket_limit(database.id, self.test_limit)
            self.assertEqual(current_limit, 0)

            limits = {
                'indexStorageLimit': {
                    'statement': f'CREATE INDEX index_name ON {self.collection}(city)',
                    'code': 4350,
                    'msg': 'GSI CreateIndex() - cause: Bucket\'s disk size limit has been reached.'},
                'dataStorageLimit': { # Error code/message might need to be changed based on MB-54113
                    'statement': f'INSERT INTO {self.collection} (key, value) values (uuid(), {{"city": "Lyon"}})',
                    'code': 12009,
                    'msg': 'DML Error, possible causes include concurrent modification. Failed to perform INSERT'}
            }
            try:
                # Repeat operation until we hit limit as setting limit is not immediate
                for i in range(30):
                    self.run_query(database, limits[self.test_limit]['statement'].replace('index_name', f'idx{i}'))
                    time.sleep(i)
                self.fail(f'We should have error out at this point since storage limit should be reached')
            except CBQError as ex:
                error = self.process_CBQE(ex)
                self.assertEqual(error['code'], limits[self.test_limit]['code'])
                self.assertEqual(error['msg'][:84], limits[self.test_limit]['msg'])

    def run_async_create(self, database, prefix):
        idx_name = "idx_name" + prefix
        self.run_query(database, f'CREATE INDEX {idx_name} on {self.collection}(fname)')

    def get_execution_time(self,time):
        if time[-2:] == 'ms':
            return float(time[:-2])*1000
        if time[-1] == 's':
            return float(time[:-1])
