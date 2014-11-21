from basetestcase import BaseTestCase
from membase.helper.cluster_helper import ClusterOperationHelper
from sdk_client import SDKClient

class SDKClientTests(BaseTestCase):
    """ Class for defining tests for python sdk """

    def setUp(self):
        super(SDKClientTests, self).setUp()
        self.cluster.rebalance(self.servers[:self.num_servers], self.servers[1:self.num_servers], [])
        credentials = self.input.membase_settings
        self.log = logger.Logger.get_logger()
        credentials = self.input.membase_settings
        self.log.info("==============  SDKClientTests setup was finished for test #{0} {1} =============="\
                      .format(self.case_number, self._testMethodName))
    def tearDown(self):
        super(SDKClientTests, self).tearDown()

    def test_sdk_client(self):
        """
            Test SDK Client Calls
        """
        client = SDKClient(scheme="couchbase",hosts = [self.master], bucket = "default")
        client.remove("1",quiet=True)
        client.insert("1","{1:2}")
        val, flag, cas = client.get("1")
        self.log.info("val={0},flag={1},cas={2}".format(val, flag, cas))
        client.upsert("1","{1:3}")
        client.touch("1",ttl=100)
        val, flag, cas = client.get("1")
        self.assertTrue(val == "{1:3}")
        self.log.info("val={0},flag={1},cas={2}".format(val, flag, cas))
        client.remove("1",cas = cas)
        client.incr("key", delta=20, initial=5)
        val, flag, cas = client.get("key")
        self.assertTrue(val == 5)
        self.log.info("val={0},flag={1},cas={2}".format(val, flag, cas))
        client.incr("key", delta=20, initial=5)
        val, flag, cas = client.get("key")
        self.assertTrue(val == 25)
        self.log.info("val={0},flag={1},cas={2}".format(val, flag, cas))
        client.decr("key", delta=20, initial=5)
        val, flag, cas = client.get("key")
        self.assertTrue(val == 5)
        print val, flag, cas
        client.upsert("key1","document1")
        client.upsert("key2","document2")
        client.upsert("key3","document3")
        set = client.get_multi(["key1","key2"])
        self.log.info(set)
        client.upsert_multi({"key1":"{1:2}","key2":"{3:2}"})
        set = client.get_multi(["key1","key2","key3"])
        self.log.info(set)
        client.touch_multi(["key1","key2","key3"],ttl=200)
        set = client.get_multi(["key1","key2","key3"])
        self.log.info(set)
        set = client.get_multi(["key1","key2","key3"],replica=True)
        self.log.info(set)
        data = client.observe("key1")
        self.log.info(data)
        data = client.observe_multi(["key1","key2"])
        self.log.info(data)
        stats = client.stats(["key1"])
        self.log.info(stats)
        client.close()
