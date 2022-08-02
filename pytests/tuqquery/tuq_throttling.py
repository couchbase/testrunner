from .tuq import QueryTests
import re
import requests

class QueryThrottlingTests(QueryTests):
    def setUp(self):
        super(QueryThrottlingTests, self).setUp()
        self.bucket = "default"
        self.kv_wu, self.kv_ru = 1024, 4096
        self.index_wu, self.index_ru = 1024, 256
        self.kvThrottleLimit = self.get_throttle_limits(service='kvThrottleLimit')
        self.log.info(f'kvThrottleLimit is {self.kvThrottleLimit}')

    def suite_setUp(self):
        super(QueryThrottlingTests, self).suite_setUp()

    def tearDown(self):
        super(QueryThrottlingTests, self).tearDown()

    def suite_tearDown(self):
        super(QueryThrottlingTests, self).suite_tearDown()

    def get_throttle_limits(self, service='kvThrottleLimit'):
        api = f"https://{self.server.ip}:18091/settings/throttle"
        response = requests.get(api, auth = requests.auth.HTTPBasicAuth(self.rest.username, self.rest.password), verify=False)
        throttle_limits = response.json()
        return throttle_limits[service]

    def set_throttle_limits(self, value=5000, service='kvThrottleLimit'):
        api = f"https://{self.server.ip}:18091/settings/throttle"
        data = {}
        data[service] = value
        response = requests.post(api, data=data, auth = requests.auth.HTTPBasicAuth(self.rest.username, self.rest.password), verify=False)
        if response.status_code not in (200,201):
            self.fail(f'Fail to set throttle limits: {response.text}')

    def get_throttling_metrics(self, bucket='default', service='kv'):
        throttle_count_total, throttle_seconds_total = 0, 0
        api = f"https://{self.server.ip}:18091/metrics"
        throttle_seconds_pattern = re.compile(f'throttle_seconds_total{{bucket="{bucket}",for="{service}".*}} (\d+)')
        throttle_count_pattern = re.compile(f'throttle_count_total{{bucket="{bucket}",for="{service}".*}} (\d+)')
        response = requests.get(api, auth = requests.auth.HTTPBasicAuth(self.rest.username, self.rest.password), verify=False)
        if throttle_seconds_pattern.search(response.text):
            throttle_seconds_total = int(throttle_seconds_pattern.findall(response.text)[0])
        if throttle_count_pattern.search(response.text):
            throttle_count_total = int(throttle_count_pattern.findall(response.text)[0])
        return throttle_count_total, throttle_seconds_total

    def test_kv_insert(self):
        before_throttle_count_total, before_throttle_seconds_total = self.get_throttling_metrics()
        self.log.info(f'Throttle before, count: {before_throttle_count_total}, seconds: {before_throttle_seconds_total}')
        self.run_cbq_query(f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {{"city": repeat("a", {self.kv_wu})}} as v FROM array_range(0,{self.kvThrottleLimit}) d')
        after_throttle_count_total, after_throttle_seconds_total = self.get_throttling_metrics()
        self.log.info(f'Throttle after, count: {after_throttle_count_total}, seconds: {after_throttle_seconds_total}')
        self.assertTrue(after_throttle_count_total > before_throttle_count_total)

    def test_kv_select(self):
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {{"city": repeat("a", {self.kv_ru})}} as v FROM array_range(0,{self.kvThrottleLimit}) d')
        before_throttle_count_total, before_throttle_seconds_total = self.get_throttling_metrics()
        self.log.info(f'Throttle before, count: {before_throttle_count_total}, seconds: {before_throttle_seconds_total}')
        self.sleep(3)
        self.run_cbq_query(f'SELECT count(city) FROM {self.bucket}')
        after_throttle_count_total, after_throttle_seconds_total = self.get_throttling_metrics()
        self.log.info(f'Throttle after, count: {after_throttle_count_total}, seconds: {after_throttle_seconds_total}')
        self.assertTrue(after_throttle_count_total > before_throttle_count_total)

    def test_kv_below_throttle(self):
        before_throttle_count_total, before_throttle_seconds_total = self.get_throttling_metrics()
        self.log.info(f'Throttle before, count: {before_throttle_count_total}, seconds: {before_throttle_seconds_total}')
        self.run_cbq_query(f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {{"city": repeat("a", {self.kv_wu})}} as v FROM array_range(0,{self.kvThrottleLimit/10}) d')
        after_throttle_count_total, after_throttle_seconds_total = self.get_throttling_metrics()
        self.log.info(f'Throttle after, count: {after_throttle_count_total}, seconds: {after_throttle_seconds_total}')
        self.assertEqual(after_throttle_count_total, before_throttle_count_total)

    def test_kv_set_throttle(self):
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {{"city": repeat("a", {self.kv_ru})}} as v FROM array_range(0,{self.kvThrottleLimit}) d')

        self.set_throttle_limits(value=int(self.kvThrottleLimit*30/100), service='kvThrottleLimit')
        result = self.run_cbq_query(f'SELECT count(city) FROM {self.bucket}')
        execution_time_at_30pct = float(result['metrics']['executionTime'][:-1])

        self.set_throttle_limits(value=self.kvThrottleLimit, service='kvThrottleLimit')
        result = self.run_cbq_query(f'SELECT count(city) FROM {self.bucket}')
        execution_time_at_100pct = float(result['metrics']['executionTime'][:-1])

        # We expect the execution with higher throttle level (100pct) to be faster than the run at lower
        # throttle level (30pct) by some factor, in this case 1.75, to be within margin as we cannot 
        # garantee expected execution time to be consistant.
        self.assertTrue(execution_time_at_30pct > execution_time_at_100pct * 1.5, f"Expected Ratio of 1.5 or higher but got: {execution_time_at_30pct/execution_time_at_100pct}")

    def test_gsi_create(self):
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'DROP INDEX idx_name IF EXISTS on {self.bucket}')
        self.run_cbq_query(f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {{"city": repeat("a", {self.index_wu})}} as v FROM array_range(0,{self.kvThrottleLimit}) d')

        before_throttle_count_total, before_throttle_seconds_total = self.get_throttling_metrics(service='index')
        self.log.info(f'Throttle before, count: {before_throttle_count_total}, seconds: {before_throttle_seconds_total}')
        self.run_cbq_query(f'CREATE INDEX idx_name on {self.bucket}(city)')
        self.wait_for_all_indexes_online
        after_throttle_count_total, after_throttle_seconds_total = self.get_throttling_metrics(service='index')
        self.log.info(f'Throttle after, count: {after_throttle_count_total}, seconds: {after_throttle_seconds_total}')
        self.assertTrue(after_throttle_count_total > before_throttle_count_total)

    def test_gsi_below_throttle(self):
        self.run_cbq_query(f'DELETE from {self.bucket}')
        self.run_cbq_query(f'DROP INDEX idx_name IF EXISTS on {self.bucket}')
        self.run_cbq_query(f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {{"city": repeat("a", {self.kv_wu})}} as v FROM array_range(0,{self.kvThrottleLimit/10}) d')

        before_throttle_count_total, before_throttle_seconds_total = self.get_throttling_metrics()
        self.log.info(f'Throttle before, count: {before_throttle_count_total}, seconds: {before_throttle_seconds_total}')
        self.run_cbq_query(f'CREATE INDEX idx_name on {self.bucket}(city)')
        after_throttle_count_total, after_throttle_seconds_total = self.get_throttling_metrics()
        self.log.info(f'Throttle after, count: {after_throttle_count_total}, seconds: {after_throttle_seconds_total}')
        self.assertEqual(after_throttle_count_total, before_throttle_count_total)

