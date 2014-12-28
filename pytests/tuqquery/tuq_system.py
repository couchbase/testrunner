from tuqquery.tuq import QueryTests
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from membase.api.exception import CBQError

class SysCatalogTests(QueryTests):

    def setUp(self):
        super(SysCatalogTests, self).setUp()

    def suite_setUp(self):
        super(SysCatalogTests, self).suite_setUp()

    def tearDown(self):
        super(SysCatalogTests, self).tearDown()

    def suite_tearDown(self):
        super(SysCatalogTests, self).suite_tearDown()

    def test_sites(self):
        self.query = "SELECT * FROM system:datastores"
        result = self.run_cbq_query()
        host = (self.master.ip, self.input.tuq_client)[self.input.tuq_client and "client" in self.input.tuq_client]
        for res in result['results']:
            self.assertEqual(res['datastores']['id'], res['datastores']['url'],
                             "Id and url don't match")
            self.assertTrue(res['datastores']['url'].find(host) != -1,
                            "Expected: %s, actual: %s" % (host, result))

    def test_pools(self):
        self.query = "SELECT * FROM system:namespaces"
        result = self.run_cbq_query()

        host = 'localhost'
        if self.input.tuq_client and "client" in self.input.tuq_client:
            host = self.input.tuq_client.client
        for res in result['results']:
            self.assertEqual(res['namespaces']['id'], res['namespaces']['name'],
                        "Id and url don't match")
            self.assertTrue(res['namespaces']['store_id'].find(host) != -1 or\
                            res['namespaces']['store_id'].find(self.master.ip) != -1,
                            "Expected: %s, actual: %s" % (host, result))

    def test_buckets(self):
        self.query = "SELECT * FROM system:keyspaces"
        result = self.run_cbq_query()
        buckets = [bucket.name for bucket in self.buckets]
        self.assertFalse(set(buckets) - set([b['keyspaces']['id'] for b in result['results']]),
                        "Expected ids: %s. Actual result: %s" % (buckets, result))
        self.assertFalse(set(buckets) - set([b['keyspaces']['name'] for b in result['results']]),
                        "Expected names: %s. Actual result: %s" % (buckets, result))
        pools = self.run_cbq_query(query='SELECT * FROM system:namespaces')
        self.assertFalse(set([b['keyspaces']['namespace_id'] for b in result['results']]) -\
                        set([p['namespaces']['id'] for p in pools['results']]),
                        "Expected pools: %s, actual: %s" % (pools, result))
        self.assertFalse(set([b['keyspaces']['store_id'] for b in result['results']]) -\
                        set([p['namespaces']['store_id'] for p in pools['results']]),
                        "Expected site_ids: %s, actual: %s" % (pools, result))

    def test_prepared_buckets(self):
        for bucket in self.buckets:
            self.query = "SELECT * FROM system:keyspaces"
            self.prepared_common_body()
