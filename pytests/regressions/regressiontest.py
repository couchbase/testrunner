from basetestcase import BaseTestCase
from memcached.helper.data_helper import MemcachedClientHelper
from mc_bin_client import MemcachedError
import memcacheConstants

class RegressionTests(BaseTestCase):
    def setUp(self):
        super(RegressionTests, self).setUp()

    def tearDown(self):
        super(RegressionTests, self).tearDown()

    def test_MB_12751(self):
        mc = MemcachedClientHelper.direct_client(self.master, "default")
        mc.set("hello", 0, 0, "world")
        mc.getl("hello", 15)
        try:
            ret = mc.replace("hello", 0, 0, "hello")
            self.fail("The document should be locked")
        except MemcachedError, e:
            if e.status != memcacheConstants.ERR_EXISTS:
                self.fail("Expected replace to return EEXISTS, returned: {0}".format(e.status))

    def test_MB_14288(self):
        mc = MemcachedClientHelper.proxy_client(self.master, "default")
        blob = bytearray(1024 * 1024 * 10)
        mc.set("MB-14288", 0, 0, blob)
        flags_v, cas_v, retrieved = mc.get("MB-14288")
        if not blob == retrieved:
            self.fail("It should be possible to store and retrieve values > 1M")
