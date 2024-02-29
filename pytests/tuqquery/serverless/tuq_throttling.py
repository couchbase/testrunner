from serverless.serverless_basetestcase import ServerlessBaseTestCase
from TestInput import TestInputSingleton
from lib.metering_throttling import throttling
from membase.api.exception import CBQError
import time, random, time, string
from lib.Cb_constants.CBServer import CbServer

class QueryThrottlingTests(ServerlessBaseTestCase):
    def setUp(self):
        self.input = TestInputSingleton.input
        self.kv_wu, self.kv_ru = 1024, 4096
        self.index_wu, self.index_ru = 1024, 256
        self.doc_count = 5000
        self.scope = '_default'
        self.collection = '_default'
        self.scope_limit, self.collection_limit = 100, 100
        self.gsi_limit = 200
        self.test_limit=self.input.param("test_limit", 'dataStorageLimit')
        self.kvThrottleLimit = 5000
        CbServer.capella_run = True
        return super().setUp()

    def tearDown(self):
        return super().tearDown()

    def test_kv_insert(self):
        self.provision_databases()
        for database in self.databases.values():
            throttle = throttling(database.rest_host, database.admin_username, database.admin_password)
            throttle.set_bucket_limit(database.id, 2500, 'dataThrottleLimit')
            time.sleep(10)
            before_throttle_count_total, before_throttle_seconds_total = throttle.get_metrics(database.id, 'kv')
            self.run_query(database, f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {{"city": repeat("a", {self.kv_wu})}} as v FROM array_range(0,{self.doc_count}) d')
            after_throttle_count_total, after_throttle_seconds_total = throttle.get_metrics(database.id, 'kv')

            before_throttle_count_total, before_throttle_seconds_total = throttle.get_metrics(database.id, 'kv')
            self.run_query(database, f'SELECT count(city) FROM {self.collection}')
            #self.run_query(database, f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {{"city": repeat("a", {self.kv_wu})}} as v FROM array_range(0,{self.doc_count}) d')
            after_throttle_count_total, after_throttle_seconds_total = throttle.get_metrics(database.id, 'kv')

            before_throttle_count_total, before_throttle_seconds_total = throttle.get_metrics(database.id, 'kv')
            self.run_query(database, f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {{"city": repeat("a", {self.kv_wu})}} as v FROM array_range(0,{self.doc_count}) d')
            after_throttle_count_total, after_throttle_seconds_total = throttle.get_metrics(database.id, 'kv')

            self.assertTrue(after_throttle_count_total > before_throttle_count_total)

    def test_kv_select(self):
        self.provision_databases()
        for database in self.databases.values():
            throttle = throttling(database.rest_host, database.admin_username, database.admin_password)
            doc = {"city": ''.join(random.choices(string.ascii_lowercase, k=self.kv_ru))}
            self.run_query(database, f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {doc} as v FROM array_range(0,{self.doc_count}) d')
            throttle.set_bucket_limit(database.id, 2500, 'dataThrottleLimit')
            time.sleep(10)
            self.run_query(database, f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {doc} as v FROM array_range(0,{self.doc_count}) d')
            before_throttle_count_total, before_throttle_seconds_total = throttle.get_metrics()
            result = self.run_query(database, f'SELECT * FROM {self.collection} WHERE city is not missing')
            after_throttle_count_total, after_throttle_seconds_total = throttle.get_metrics()
            self.log.info(f'Throttle after, count: {after_throttle_count_total}, seconds: {after_throttle_seconds_total}')
            self.assertTrue(after_throttle_count_total > before_throttle_count_total)

    def test_kv_below_throttle(self):
        self.provision_databases()
        for database in self.databases.values():
            throttle = throttling(database.rest_host, database.admin_username, database.admin_password)
            kvLimit = throttle.get_bucket_limit(database.id, 'dataThrottleLimit')
            before_throttle_count_total, before_throttle_seconds_total = throttle.get_metrics()
            self.run_query(database, f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {{"city": "Paris"}} as v FROM array_range(0,{kvLimit/2}) d')
            after_throttle_count_total, after_throttle_seconds_total = throttle.get_metrics()
            self.assertEqual(after_throttle_count_total, before_throttle_count_total)
            self.assertEqual(after_throttle_seconds_total, before_throttle_seconds_total)

    def test_kv_set_throttle(self):
        self.provision_databases()
        for database in self.databases.values():
            throttle = throttling(database.rest_host, database.admin_username, database.admin_password)
            kvLimit = throttle.get_bucket_limit(database.id, 'dataThrottleLimit')
            self.run_query(database, f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {{"city": "Paris"}} as v FROM array_range(0,{kvLimit}) d')

            throttle.set_bucket_limit(database.id, int(kvLimit*30/100), 'dataThrottleLimit')
            time.sleep(15)
            result = self.run_query(database, f'SELECT count(city) FROM {self.collection}')
            execution_time_at_30pct = float(result['metrics']['executionTime'][:-2])

            throttle.set_bucket_limit(database.id, kvLimit, 'dataThrottleLimit')
            time.sleep(15)
            result = self.run_query(database, f'SELECT count(city) FROM {self.collection}')
            execution_time_at_100pct = float(result['metrics']['executionTime'][:-2])

            # We expect the execution with higher throttle level (100pct) to be faster than the run at lower
            # throttle level (30pct) by some factor, in this case 1.75, to be within margin as we cannot 
            # garantee expected execution time to be consistant.
            self.assertTrue(execution_time_at_30pct > execution_time_at_100pct * 1.5, f"Expected Ratio of 1.5 or higher but got: {execution_time_at_30pct/execution_time_at_100pct}")

    def test_scope_limit(self):
        self.provision_databases()
        for database in self.databases.values():
            for i in range(self.scope_limit):
                self.run_query(database, f'CREATE SCOPE `{database.id}`.scope{i}')
            try:
                self.run_query(database, f'CREATE SCOPE `{database.id}`.scope{self.scope_limit}')
                self.fail(f'We should not be able to create scope beyond limit of {self.scope_limit}')
            except CBQError as ex:
                error = self.process_CBQE(ex)
                self.assertEqual(error['code'], 12025)
                self.assertTrue(f'_:Maximum number of scopes ({self.scope_limit}) for this bucket has been reached' in error['msg'])

    def test_collection_limit(self):
        self.provision_databases()
        for database in self.databases.values():
            for i in range(self.collection_limit):
                self.run_query(database, f'CREATE COLLECTION `{database.id}`.`_default`.collection{i} IF NOT EXISTS')
            try:
                self.run_query(database, f'CREATE COLLECTION `{database.id}`.`_default`.collection{self.collection_limit}')
                self.fail(f'We should not be able to create scope beyond limit of {self.collection_limit}')
            except CBQError as ex:
                error = self.process_CBQE(ex)
                self.assertEqual(error['code'], 12027)
                self.assertTrue(f'_:Maximum number of collections ({self.collection_limit}) for this bucket has been reached' in error['msg'])

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
                    'statement': f'CREATE INDEX index_name ON {self.collection}(a)',
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
