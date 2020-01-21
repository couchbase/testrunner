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
from membase.helper.bucket_helper import BucketOperationHelper
import json
from membase.helper.cluster_helper import ClusterOperationHelper
from TestInput import TestInput, TestInputSingleton

from remote.remote_util import RemoteMachineShellConnection

class Upgrade_EpTests(UpgradeTests):

    def setUp(self):
        super(Upgrade_EpTests, self).setUp()
        print(self.master)
        self.rest = RestConnection(self.master)
        self.bucket = 'default' # temp fix
        self.client = VBucketAwareMemcached(self.rest, self.bucket)
        self.time_synchronization ='disabled'
        print('checking for self.servers '.format(self.servers[1]))
        self.prefix = "test_"
        self.expire_time = 5
        self.item_flag = 0
        self.value_size = 256


    def tearDown(self):
        #super(Upgrade_EpTests, self).tearDown()
        self.testcase = '2'
        if not "skip_cleanup" in TestInputSingleton.input.test_params:
            BucketOperationHelper.delete_all_buckets_or_assert(
                self.servers, self.testcase)
            ClusterOperationHelper.cleanup_cluster(self.servers)
            ClusterOperationHelper.wait_for_ns_servers_or_assert(
                self.servers, self.testcase)

    def test_upgrade(self):
        self.log.info('Starting upgrade tests...')

        #o = OpsChangeCasTests()

        self.log.info('Inserting few items pre upgrade')
        self._load_ops(ops='set', mutations=20, master=self.master, bucket=self.bucket)
        self.log.info('Upgrading ..')
        try:
            UpgradeTests.test_upgrade(self)
        finally:
            self.log.info(' Done with Upgrade ')

        self.log.info('Testing the meta details on items post upgrade')
        #self._check_config()
        self._check_cas(check_conflict_resolution=True, master=self.servers[1], bucket=self.bucket)

    def _check_config(self):
        result = self.rest.get_bucket_json(self.bucket)["timeSynchronization"]
        print(result)
        self.assertEqual(result, self.time_synchronization, msg='ERROR, Mismatch on expected time synchronization values')
        self.log.info("Verified results")

    def _load_ops(self, ops=None, mutations=1, master=None, bucket=None):

        if master:
            self.rest = RestConnection(master)
        if bucket:
            self.client = VBucketAwareMemcached(self.rest, bucket)

        k=0
        payload = MemcachedClientHelper.create_value('*', self.value_size)

        while k<10:
            key = "{0}{1}".format(self.prefix, k)
            k += 1
            for i in range(mutations):
                if ops=='set':
                    #print 'set'
                    self.client.memcached(key).set(key, 0, 0, payload)
                elif ops=='add':
                    #print 'add'
                    self.client.memcached(key).add(key, 0, 0, payload)
                elif ops=='replace':
                    self.client.memcached(key).replace(key, 0, 0, payload)
                    #print 'Replace'
                elif ops=='delete':
                    #print 'delete'
                    self.client.memcached(key).delete(key)
                elif ops=='expiry':
                    #print 'expiry'
                    self.client.memcached(key).set(key, self.expire_time, 0, payload)
                elif ops=='touch':
                    #print 'touch'
                    self.client.memcached(key).touch(key, 10)

        self.log.info("Done with specified {0} ops".format(ops))

    ''' Common function to verify the expected values on cas
    '''
    def _check_cas(self, check_conflict_resolution=False, master=None, bucket=None, time_sync=None):
        self.log.info(' Verifying cas and max cas for the keys')
        if master:
            self.rest = RestConnection(master)
            self.client = VBucketAwareMemcached(self.rest, bucket)

        k=0

        while k<10:
            key = "{0}{1}".format(self.prefix, k)
            k += 1
            mc_active = self.client.memcached(key)

            cas = mc_active.getMeta(key)[4]
            max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(self.client._get_vBucket_id(key)) + ':max_cas'] )
            #print 'max_cas is {0}'.format(max_cas)
            self.assertTrue(cas == max_cas, '[ERROR]Max cas  is not 0 it is {0}'.format(cas))

            if check_conflict_resolution:
                get_meta_resp = mc_active.getMeta(key, request_extended_meta_data=True)
                if time_sync == 'enabledWithoutDrift':
                    self.assertTrue( get_meta_resp[5] == 1, msg='[ERROR] Metadata indicate conflict resolution is not set')
                elif time_sync == 'disabled':
                    self.assertTrue( get_meta_resp[5] == 0, msg='[ERROR] Metadata indicate conflict resolution is set')
