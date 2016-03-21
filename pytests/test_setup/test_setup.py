from basetestcase import BaseTestCase
from lib.mc_bin_client import MemcachedClient, MemcachedError
from membase.api.rest_client import RestConnection, RestHelper
from memcached.helper.data_helper import VBucketAwareMemcached
import copy

class TestSetup(BaseTestCase):
    def setUp(self):
        super(TestSetup, self).setUp()
        self.server = self.input.servers[0]

    def tearDown(self):
        super(TestSetup, self).tearDown()

    def test_setup_cluster(self):
        pass
