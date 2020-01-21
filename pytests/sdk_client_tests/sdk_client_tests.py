import json
from basetestcase import BaseTestCase
from membase.helper.cluster_helper import ClusterOperationHelper
from sdk_client import SDKClient

class SDKClientTests(BaseTestCase):
    """ Class for defining tests for python sdk """

    def setUp(self):
        super(SDKClientTests, self).setUp()
        self.cluster.rebalance(self.servers[:self.num_servers], self.servers[1:self.num_servers], [])
        self.log.info("==============  SDKClientTests setup was finished for test #{0} {1} =============="\
                      .format(self.case_number, self._testMethodName))
    def tearDown(self):
        super(SDKClientTests, self).tearDown()

    def test_sdk_subddoc(self):
        """
            Test SDK Client Calls
        """
        scheme = "couchbase"
        host=self.master.ip
        if self.master.ip == "127.0.0.1":
            scheme = "http"
            host="{0}:{1}".format(self.master.ip, self.master.port)

        client = SDKClient(scheme=scheme, hosts = [host], bucket = "default")
        json_document = {"1":1, "2":2, "array": [1]}
        document_key = "1"
        client.insert("1", json_document)
        client.insert_in(document_key, "3", 3)
        client.upsert_in(document_key, "4", 4)
        client.upsert_in(document_key, "4", "change_4")
        client.replace_in(document_key, "4", "crap_4")
        client.arrayprepend_in(document_key, "array", "0")
        client.arrayappend_in(document_key, "array", "2")
        client.arrayinsert_in(document_key, "array[1]", "INSERT_VALUE_AT_INDEX_1")
        client.arrayaddunique_in(document_key, "array", "INSERT_UNIQUE_VALUE")
        print(json.dumps(client.get(document_key)))


    def test_sdk_client(self):
        """
            Test SDK Client Calls
        """
        scheme = "couchbase"
        host=self.master.ip
        if self.master.ip == "127.0.0.1":
            scheme = "http"
            host="{0}:{1}".format(self.master.ip, self.master.port)
        client = SDKClient(scheme=scheme, hosts = [host], bucket = "default")
        client.remove("1", quiet=True)
        client.insert("1", "{1:2}")
        flag, cas, val = client.get("1")
        self.log.info("val={0},flag={1},cas={2}".format(val, flag, cas))
        client.upsert("1", "{1:3}")
        client.touch("1", ttl=100)
        flag, cas, val = client.get("1")
        self.assertTrue(val == "{1:3}", val)
        self.log.info("val={0},flag={1},cas={2}".format(val, flag, cas))
        client.remove("1", cas = cas)
        client.incr("key", delta=20, initial=5)
        flag, cas, val = client.get("key")
        self.assertTrue(val == 5)
        self.log.info("val={0},flag={1},cas={2}".format(val, flag, cas))
        client.incr("key", delta=20, initial=5)
        flag, cas, val = client.get("key")
        self.assertTrue(val == 25)
        self.log.info("val={0},flag={1},cas={2}".format(val, flag, cas))
        client.decr("key", delta=20, initial=5)
        flag, cas, val = client.get("key")
        self.assertTrue(val == 5)
        print(flag, cas, val)
        client.upsert("key1", "document1")
        client.upsert("key2", "document2")
        client.upsert("key3", "document3")
        set = client.get_multi(["key1", "key2"])
        self.log.info(set)
        client.upsert_multi({"key1":"{1:2}","key2":"{3:2}"})
        set = client.get_multi(["key1", "key2"])
        self.log.info(set)
        client.touch_multi(["key1", "key2"], ttl=200)
        set = client.get_multi(["key1", "key2"])
        self.log.info(set)
        data = client.observe("key1")
        self.log.info(data)
        data = client.observe_multi(["key1", "key2"])
        self.log.info(data)
        stats = client.stats(["key1"])
        self.log.info(stats)
        client.n1ql_request(client.n1ql_query('create primary index on default')).execute()
        query = client.n1ql_query('select * from default')
        request = client.n1ql_request(query)
        obj = request.get_single_result()._jsobj
        self.log.info(obj)
        client.close()