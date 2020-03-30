import time
from TestInput import TestInputSingleton
from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection

class QueryWorkbenchTests(BaseTestCase):
    n1ql_port =8093
    _input = TestInputSingleton.input
    num_items = _input.param("items", 100)
    _value_size = _input.param("value_size", 256)
    gen_create = BlobGenerator('loadOne', 'loadOne', _value_size, end=num_items)
    #bucket and ram quota
    buckets_ram = {
        "CUSTOMER": 100,
        "DISTRICT": 100,
        "HISTORY": 100,
        "ITEM": 100,
        "NEW_ORDER": 100,
        "ORDERS": 100,
        "ORDER_LINE": 100}
        #"default:": 100}

    def setUp(self):
        super(QueryWorkbenchTests, self).setUp()
        server = self.master
        if self.input.tuq_client and "client" in self.input.tuq_client:
            server = self.tuq_client
        self.rest = RestConnection(server)
        #self.rest.delete_bucket("default")
        time.sleep(20)
        # drop and recreate buckets
        for i, bucket_name in enumerate(self.buckets_ram.keys()):
            self.rest.create_bucket(bucket=bucket_name,
                                   ramQuotaMB=int(self.buckets_ram[bucket_name]),
                                   replicaNumber=0,
                                   proxyPort=11218+i)
            self.log.info(self.servers[0])
            #bucket = self.src_cluster.get_bucket_by_name(bucket_name)
        time.sleep(20)
        #self.rest.create_bucket(bucket="default",
                                   #ramQuotaMB=int(self.buckets_ram["default"]),
                                   #replicaNumber=0,
                                   #proxyPort=11218)
        self._load_all_buckets(self, self.servers[0], self.gen_create, "create", 0)
        #time.sleep(20)

    def tearDown(self):
        super(QueryWorkbenchTests, self).tearDown()

    def test_describe(self):
        for bucket_name in self.rest.get_buckets():
            query = "infer %s" % bucket_name
            self.log.info(query)
            result = self.rest.query_tool(query, self.n1ql_port)
            self.log.info(result)
