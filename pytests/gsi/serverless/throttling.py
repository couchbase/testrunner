from serverless.serverless_basetestcase import ServerlessBaseTestCase
from lib.metering_throttling import throttling

class GSIThrottleSanity(ServerlessBaseTestCase):
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

    def test_throttle_create_index(self):
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
