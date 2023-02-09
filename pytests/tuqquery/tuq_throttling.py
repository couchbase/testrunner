from .tuq import QueryTests
from lib.metering_throttling import throttling
from membase.api.exception import CBQError

class QueryThrottlingTests(QueryTests):
    def setUp(self):
        super(QueryThrottlingTests, self).setUp()
        self.bucket = "default"
        self.kv_wu, self.kv_ru = 1024, 4096
        self.index_wu, self.index_ru = 1024, 256
        self.scope_limit, self.collection_limit = 100, 100
        self.gsi_limit = 200
        self.test_limit=self.input.param("test_limit", 'indexStorageLimit')
        self.throttle = throttling(self.server.ip, self.rest.username, self.rest.password)
        self.kvThrottleLimit = 5000

    def suite_setUp(self):
        super(QueryThrottlingTests, self).suite_setUp()
        if self.use_server_groups:
            self._create_server_groups()

    def tearDown(self):
        super(QueryThrottlingTests, self).tearDown()

    def suite_tearDown(self):
        super(QueryThrottlingTests, self).suite_tearDown()

    def test_kv_insert(self):
        before_throttle_count_total, before_throttle_seconds_total = self.throttle.get_metrics()
        self.log.info(f'Throttle before, count: {before_throttle_count_total}, seconds: {before_throttle_seconds_total}')
        self.run_cbq_query(f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {{"city": repeat("a", {self.kv_wu})}} as v FROM array_range(0,{self.kvThrottleLimit*2}) d')
        after_throttle_count_total, after_throttle_seconds_total = self.throttle.get_metrics()
        self.log.info(f'Throttle after, count: {after_throttle_count_total}, seconds: {after_throttle_seconds_total}')
        self.assertTrue(after_throttle_count_total > before_throttle_count_total)

    def test_kv_select(self):
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {{"city": repeat("a", {self.kv_ru})}} as v FROM array_range(0,{self.kvThrottleLimit}) d')
        before_throttle_count_total, before_throttle_seconds_total = self.throttle.get_metrics()
        self.log.info(f'Throttle before, count: {before_throttle_count_total}, seconds: {before_throttle_seconds_total}')
        self.sleep(3)
        self.run_cbq_query(f'SELECT count(city) FROM {self.bucket}')
        after_throttle_count_total, after_throttle_seconds_total = self.throttle.get_metrics()
        self.log.info(f'Throttle after, count: {after_throttle_count_total}, seconds: {after_throttle_seconds_total}')
        self.assertTrue(after_throttle_count_total > before_throttle_count_total)

    def test_kv_below_throttle(self):
        before_throttle_count_total, before_throttle_seconds_total = self.throttle.get_metrics()
        self.log.info(f'Throttle before, count: {before_throttle_count_total}, seconds: {before_throttle_seconds_total}')
        self.run_cbq_query(f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {{"city": repeat("a", {self.kv_wu})}} as v FROM array_range(0,{self.kvThrottleLimit/10}) d')
        after_throttle_count_total, after_throttle_seconds_total = self.throttle.get_metrics()
        self.log.info(f'Throttle after, count: {after_throttle_count_total}, seconds: {after_throttle_seconds_total}')
        self.assertEqual(after_throttle_count_total, before_throttle_count_total)

    def test_kv_set_throttle(self):
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {{"city": repeat("a", {self.kv_ru})}} as v FROM array_range(0,{self.kvThrottleLimit}) d')

        self.throttle.set_bucket_limit(value=int(self.kvThrottleLimit*30/100), service='kvThrottleLimit')
        result = self.run_cbq_query(f'SELECT count(city) FROM {self.bucket}')
        execution_time_at_30pct = float(result['metrics']['executionTime'][:-1])

        self.throttle.set_bucket_limit(value=self.kvThrottleLimit, service='kvThrottleLimit')
        result = self.run_cbq_query(f'SELECT count(city) FROM {self.bucket}')
        execution_time_at_100pct = float(result['metrics']['executionTime'][:-1])

        # We expect the execution with higher throttle level (100pct) to be faster
        # than the run at lower throttle level (30pct)
        self.assertTrue(execution_time_at_30pct > execution_time_at_100pct)

    def test_gsi_create(self):
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'DROP INDEX idx_name IF EXISTS on {self.bucket}')
        self.run_cbq_query(f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {{"city": repeat("a", {self.index_wu})}} as v FROM array_range(0,{self.kvThrottleLimit}) d')

        before_throttle_count_total, before_throttle_seconds_total = self.throttle.get_metrics(service='index')
        self.log.info(f'Throttle before, count: {before_throttle_count_total}, seconds: {before_throttle_seconds_total}')
        self.run_cbq_query(f'CREATE INDEX idx_name on {self.bucket}(city)')
        self.wait_for_all_indexes_online
        after_throttle_count_total, after_throttle_seconds_total = self.throttle.get_metrics(service='index')
        self.log.info(f'Throttle after, count: {after_throttle_count_total}, seconds: {after_throttle_seconds_total}')
        self.assertTrue(after_throttle_count_total > before_throttle_count_total)

    def test_gsi_below_throttle(self):
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'DROP INDEX idx_name IF EXISTS on {self.bucket}')
        self.run_cbq_query(f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {{"city": repeat("a", {self.kv_wu})}} as v FROM array_range(0,{self.kvThrottleLimit/10}) d')

        before_throttle_count_total, before_throttle_seconds_total = self.throttle.get_metrics()
        self.log.info(f'Throttle before, count: {before_throttle_count_total}, seconds: {before_throttle_seconds_total}')
        self.run_cbq_query(f'CREATE INDEX idx_name on {self.bucket}(city)')
        after_throttle_count_total, after_throttle_seconds_total = self.throttle.get_metrics()
        self.log.info(f'Throttle after, count: {after_throttle_count_total}, seconds: {after_throttle_seconds_total}')
        self.assertEqual(after_throttle_count_total, before_throttle_count_total)

    def test_scope_limit(self):
        for i in range(self.scope_limit):
            self.run_cbq_query(f'CREATE SCOPE {self.bucket}.scope{i} IF NOT EXISTS')
        try:
            self.run_cbq_query(f'CREATE SCOPE {self.bucket}.scope{self.scope_limit+1}')
            self.fail(f'We should not be able to create scope beyond limit of {self.scope_limit}')
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 12025)
            self.assertTrue(f'_:Maximum number of scopes ({self.scope_limit}) for this bucket has been reached' in error['msg'])
        finally:
            for i in range(self.scope_limit):
                self.run_cbq_query(f'DROP SCOPE {self.bucket}.scope{i} IF EXISTS')                

    def test_collection_limit(self):
        for i in range(self.collection_limit):
            self.run_cbq_query(f'CREATE COLLECTION {self.bucket}.`_default`.collection{i} IF NOT EXISTS')
        try:
            self.run_cbq_query(f'CREATE COLLECTION {self.bucket}.`_default`.collection{self.collection_limit+1}')
            self.fail(f'We should not be able to create scope beyond limit of {self.collection_limit}')
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 12027)
            self.assertTrue(f'_:Maximum number of collections ({self.collection_limit}) for this bucket has been reached' in error['msg'])
        finally:
            for i in range(self.collection_limit):
                self.run_cbq_query(f'DROP COLLECTION {self.bucket}.`_default`.collection{i} IF EXISTS')

    def test_gsi_limit(self):
        indexes = self.run_cbq_query('SELECT raw count(*) FROM system:indexes WHERE `using` = "gsi"')
        count = indexes['results'][0]
        for i in range(self.gsi_limit - count):
            self.run_cbq_query(f'CREATE INDEX idx{i} IF NOT EXISTS ON {self.bucket}(a)')
        try:
            self.run_cbq_query(f'CREATE INDEX idx{self.gsi_limit+1} IF NOT EXISTS ON {self.bucket}(a)')
            self.fail(f'We should not be able to create index beyond limit of {self.gsi_limit}')
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 4350)
            self.assertEqual(error['msg'], f'GSI CreateIndex() - cause: Limit for number of indexes that can be created per bucket has been reached. Limit : {self.gsi_limit}')
        finally:
            for i in range(self.gsi_limit):
                self.run_cbq_query(f'DROP INDEX idx{i} IF EXISTS ON {self.bucket}')

    def test_limit_storage(self):
        self.run_cbq_query(f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {{"city": "Paris"}} as v FROM array_range(0,1000) d')

        # Set stroage limit to 0 to simulate we have reached limit
        self.throttle.set_bucket_limit(bucket=self.bucket, value=0, service=self.test_limit)
        current_limit = self.throttle.get_bucket_limit(bucket=self.bucket, service=self.test_limit)
        self.assertEqual(current_limit, 0)

        limits = {
            'indexStorageLimit': {
                'statement': f'CREATE INDEX index_name ON {self.bucket}(a)',
                'code': 4350,
                'msg': 'GSI CreateIndex() - cause: Bucket\'s disk size limit has been reached.'},
            'dataStorageLimit': { # Error code/message might need to be changed based on MB-54113
                'statement': f'INSERT INTO {self.bucket} (key, value) values (uuid(), {{"city": "Lyon"}})',
                'code': 12009,
                'msg': 'DML Error, possible causes include concurrent modification. Failed to perform INSERT'}
        }
        try:
            # Repeat operation until we hit limit as setting limit is not immediate
            for i in range(30):
                self.run_cbq_query(limits[self.test_limit]['statement'].replace('index_name', f'idx{i}'))
                self.sleep(i)
            self.fail(f'We should have error out at this point since storage limit should be reached')
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], limits[self.test_limit]['code'])
            self.assertEqual(error['msg'][:84], limits[self.test_limit]['msg'])
        finally:
            self.throttle.set_bucket_limit(bucket=self.bucket, value=500, service=self.test_limit)
            for i in range(20):
                self.run_cbq_query(f'DROP INDEX idx{i} IF EXISTS ON {self.bucket}')