import time
import logger
from memcacheConstants import ERR_NOT_FOUND
from castest.cas_base import CasBaseTest
from upgrade.upgrade_tests import UpgradeTests
from epengine.bucket_config import BucketConfig
from epengine.opschangecas import OpsChangeCasTests
from couchbase_helper.documentgenerator import BlobGenerator
from mc_bin_client import MemcachedError

from membase.api.rest_client import RestConnection, RestHelper
from memcached.helper.data_helper import VBucketAwareMemcached, MemcachedClientHelper
import json


from remote.remote_util import RemoteMachineShellConnection

class Upgrade_EpTests(UpgradeTests):

    def setUp(self):
        super(Upgrade_EpTests, self).setUp()
        print self.master
        self.rest = RestConnection(self.master)
        self.bucket = 'default' # temp fix
        self.client = VBucketAwareMemcached(self.rest, self.bucket)
        self.time_synchronization ='disabled'
        print 'checking for self.servers '.format(self.servers[1])


    def tearDown(self):
        super(Upgrade_EpTests, self).tearDown()

    def test_upgrade(self):
        self.log.info('Starting upgrade tests...')

        o = OpsChangeCasTests()

        self.log.info('Inserting few items pre upgrade')
        o._load_ops(ops='set', mutations=20, master=self.master, bucket=self.bucket)
        self.log.info('Upgrading ..')
        try:
            UpgradeTests.test_upgrade(self)
        finally:
            self.log.info(' Done with Upgrade ')

        self.log.info('Testing the meta details on items post upgrade')
        #self._check_config()
        o._check_cas(check_conflict_resolution=True, master=self.servers[1], bucket=self.bucket)

    def _check_config(self):
        result = self.rest.get_bucket_json(self.bucket)["timeSynchronization"]
        print result
        self.assertEqual(result,self.time_synchronization, msg='ERROR, Mismatch on expected time synchronization values')
        self.log.info("Verified results")