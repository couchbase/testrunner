import time
import logger
from memcacheConstants import ERR_NOT_FOUND
from castest.cas_base import CasBaseTest
from epengine.bucket_config import BucketConfig
from couchbase_helper.documentgenerator import BlobGenerator
from mc_bin_client import MemcachedError

from membase.api.rest_client import RestConnection, RestHelper
from memcached.helper.data_helper import VBucketAwareMemcached, MemcachedClientHelper
from membase.helper.cluster_helper import ClusterOperationHelper
import json

from remote.remote_util import RemoteMachineShellConnection

class OpsChangeCasTests(BucketConfig):

    def setUp(self):
        super(OpsChangeCasTests, self).setUp()
        self.prefix = "test_"
        self.expire_time = self.input.param("expire_time", 35)
        self.item_flag = self.input.param("item_flag", 0)
        self.value_size = self.input.param("value_size", 256)
        self.items = self.input.param("items", 20)
        self.rest = RestConnection(self.master)
        self.client = VBucketAwareMemcached(self.rest, self.bucket)

    def tearDown(self):
        super(OpsChangeCasTests, self).tearDown()

    def test_meta_rebalance_out(self):
        KEY_NAME = 'key1'

        rest = RestConnection(self.master)
        client = VBucketAwareMemcached(rest, self.bucket)

        for i in range(10):
            # set a key
            value = 'value' + str(i)
            client.memcached(KEY_NAME).set(KEY_NAME, 0, 0,json.dumps({'value':value}))
            vbucket_id = client._get_vBucket_id(KEY_NAME)
            #print 'vbucket_id is {0}'.format(vbucket_id)
            mc_active = client.memcached(KEY_NAME)
            mc_master = client.memcached_for_vbucket( vbucket_id )
            mc_replica = client.memcached_for_replica_vbucket(vbucket_id)

            cas_active = mc_active.getMeta(KEY_NAME)[4]
            #print 'cas_a {0} '.format(cas_active)

        max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(client._get_vBucket_id(KEY_NAME)) + ':max_cas'] )

        self.assertTrue(cas_active == max_cas, '[ERROR]Max cas  is not 0 it is {0}'.format(cas_active))

        # remove that node
        self.log.info('Remove the node with active data')

        rebalance = self.cluster.async_rebalance(self.servers[-1:], [] ,[self.master])

        rebalance.result()
        replica_CAS = mc_replica.getMeta(KEY_NAME)[4]
        get_meta_resp = mc_replica.getMeta(KEY_NAME,request_extended_meta_data=True)
        print 'replica CAS {0}'.format(replica_CAS)
        print 'replica ext meta {0}'.format(get_meta_resp)

        # add the node back
        self.log.info('Add the node back, the max_cas should be healed')
        rebalance = self.cluster.async_rebalance(self.servers[-1:], [self.master], [])

        rebalance.result()

        # verify the CAS is good
        client = VBucketAwareMemcached(rest, self.bucket)
        mc_active = client.memcached(KEY_NAME)
        active_CAS = mc_active.getMeta(KEY_NAME)[4]
        print 'active cas {0}'.format(active_CAS)

        self.assertTrue(replica_CAS == active_CAS, 'cas mismatch active: {0} replica {1}'.format(active_CAS,replica_CAS))
        self.assertTrue( get_meta_resp[5] == 1, msg='Metadata indicate conflict resolution is not set')

    def test_meta_failover(self):
        KEY_NAME = 'key2'

        rest = RestConnection(self.master)
        client = VBucketAwareMemcached(rest, self.bucket)

        for i in range(10):
            # set a key
            value = 'value' + str(i)
            client.memcached(KEY_NAME).set(KEY_NAME, 0, 0,json.dumps({'value':value}))
            vbucket_id = client._get_vBucket_id(KEY_NAME)
            print 'vbucket_id is {0}'.format(vbucket_id)
            mc_active = client.memcached(KEY_NAME)
            mc_master = client.memcached_for_vbucket( vbucket_id )
            mc_replica = client.memcached_for_replica_vbucket(vbucket_id)

            cas_active = mc_active.getMeta(KEY_NAME)[4]
            print 'cas_a {0} '.format(cas_active)

        max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(client._get_vBucket_id(KEY_NAME)) + ':max_cas'] )

        self.assertTrue(cas_active == max_cas, '[ERROR]Max cas  is not 0 it is {0}'.format(cas_active))

        # failover that node
        self.log.info('Failing over node with active data {0}'.format(self.master))
        self.cluster.failover(self.servers, [self.master])

        self.log.info('Remove the node with active data {0}'.format(self.master))

        rebalance = self.cluster.async_rebalance(self.servers[:], [] ,[self.master])

        rebalance.result()
        time.sleep(60)

        replica_CAS = mc_replica.getMeta(KEY_NAME)[4]
        #print 'replica CAS {0}'.format(replica_CAS)

        # add the node back
        self.log.info('Add the node back, the max_cas should be healed')
        rebalance = self.cluster.async_rebalance(self.servers[-1:], [self.master], [])

        rebalance.result()

        # verify the CAS is good
        client = VBucketAwareMemcached(rest, self.bucket)
        mc_active = client.memcached(KEY_NAME)
        active_CAS = mc_active.getMeta(KEY_NAME)[4]
        print 'active cas {0}'.format(active_CAS)

        get_meta_resp = mc_active.getMeta(KEY_NAME,request_extended_meta_data=True)
        print 'replica CAS {0}'.format(replica_CAS)
        print 'replica ext meta {0}'.format(get_meta_resp)

        self.assertTrue(replica_CAS == active_CAS, 'cas mismatch active: {0} replica {1}'.format(active_CAS,replica_CAS))
        self.assertTrue( get_meta_resp[5] == 1, msg='Metadata indicate conflict resolution is not set')

    def test_meta_soft_restart(self):
        KEY_NAME = 'key2'

        rest = RestConnection(self.master)
        client = VBucketAwareMemcached(rest, self.bucket)

        for i in range(10):
            # set a key
            value = 'value' + str(i)
            client.memcached(KEY_NAME).set(KEY_NAME, 0, 0,json.dumps({'value':value}))
            vbucket_id = client._get_vBucket_id(KEY_NAME)
            print 'vbucket_id is {0}'.format(vbucket_id)
            mc_active = client.memcached(KEY_NAME)
            mc_master = client.memcached_for_vbucket( vbucket_id )
            mc_replica = client.memcached_for_replica_vbucket(vbucket_id)

            cas_pre = mc_active.getMeta(KEY_NAME)[4]
            print 'cas_a {0} '.format(cas_pre)

        max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(client._get_vBucket_id(KEY_NAME)) + ':max_cas'] )

        self.assertTrue(cas_pre == max_cas, '[ERROR]Max cas  is not 0 it is {0}'.format(cas_pre))

        # restart nodes
        self._restart_server(self.servers[:])

        # verify the CAS is good
        client = VBucketAwareMemcached(rest, self.bucket)
        mc_active = client.memcached(KEY_NAME)
        cas_post = mc_active.getMeta(KEY_NAME)[4]
        print 'post cas {0}'.format(cas_post)

        get_meta_resp = mc_active.getMeta(KEY_NAME,request_extended_meta_data=True)
        print 'post CAS {0}'.format(cas_post)
        print 'post ext meta {0}'.format(get_meta_resp)

        self.assertTrue(cas_pre == cas_post, 'cas mismatch active: {0} replica {1}'.format(cas_pre, cas_post))
        self.assertTrue( get_meta_resp[5] == 1, msg='Metadata indicate conflict resolution is not set')

    def test_meta_hard_restart(self):
        KEY_NAME = 'key2'

        rest = RestConnection(self.master)
        client = VBucketAwareMemcached(rest, self.bucket)

        for i in range(10):
            # set a key
            value = 'value' + str(i)
            client.memcached(KEY_NAME).set(KEY_NAME, 0, 0,json.dumps({'value':value}))
            vbucket_id = client._get_vBucket_id(KEY_NAME)
            #print 'vbucket_id is {0}'.format(vbucket_id)
            mc_active = client.memcached(KEY_NAME)
            mc_master = client.memcached_for_vbucket( vbucket_id )
            mc_replica = client.memcached_for_replica_vbucket(vbucket_id)

            cas_pre = mc_active.getMeta(KEY_NAME)[4]
            #print 'cas_a {0} '.format(cas_pre)

        max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(client._get_vBucket_id(KEY_NAME)) + ':max_cas'] )

        self.assertTrue(cas_pre == max_cas, '[ERROR]Max cas  is not 0 it is {0}'.format(cas_pre))

        # reboot nodes
        self._reboot_server()

        # verify the CAS is good
        client = VBucketAwareMemcached(rest, self.bucket)
        mc_active = client.memcached(KEY_NAME)
        cas_post = mc_active.getMeta(KEY_NAME)[4]
        print 'post cas {0}'.format(cas_post)

        get_meta_resp = mc_active.getMeta(KEY_NAME,request_extended_meta_data=True)
        print 'post CAS {0}'.format(cas_post)
        print 'post ext meta {0}'.format(get_meta_resp)

        self.assertTrue(cas_pre == cas_post, 'cas mismatch active: {0} replica {1}'.format(cas_pre, cas_post))
        self.assertTrue( get_meta_resp[5] == 1, msg='Metadata indicate conflict resolution is not set')

    ''' Test Incremental sets on cas and max cas values for keys
    '''
    def test_cas_set(self):
        self.log.info(' Starting test-sets')
        self._load_ops(ops='set', mutations=20)
        self._check_cas(check_conflict_resolution=True)

    ''' Test Incremental updates on cas and max cas values for keys
    '''
    def test_cas_updates(self):
        self.log.info(' Starting test-updates')
        self._load_ops(ops='set', mutations=20)
        #self._load_ops(ops='add')
        self._load_ops(ops='replace',mutations=20)
        #self._load_ops(ops='delete')
        self._check_cas(check_conflict_resolution=True)

    ''' Test Incremental deletes on cas and max cas values for keys
    '''
    def test_cas_deletes(self):
        self.log.info(' Starting test-deletes')
        self._load_ops(ops='set', mutations=20)
        #self._load_ops(ops='add')
        self._load_ops(ops='replace',mutations=20)
        self._load_ops(ops='delete')
        self._check_cas(check_conflict_resolution=True)

    ''' Test expiry on cas and max cas values for keys
    '''
    def test_cas_expiry(self):
        self.log.info(' Starting test-expiry')
        self._load_ops(ops='set', mutations=20)
        #self._load_ops(ops='add')
        #self._load_ops(ops='replace',mutations=20)
        self._load_ops(ops='expiry')
        self._check_cas(check_conflict_resolution=True)
        self._check_expiry()

    ''' Test touch on cas and max cas values for keys
    '''
    def test_cas_touch(self):
        self.log.info(' Starting test-touch')
        self._load_ops(ops='set', mutations=20)
        #self._load_ops(ops='add')
        #self._load_ops(ops='replace',mutations=20)
        self._load_ops(ops='touch')
        self._check_cas(check_conflict_resolution=True)

    ''' Test getMeta on cas and max cas values for keys
    '''
    def test_cas_getMeta(self):
        self.log.info(' Starting test-getMeta')
        self._load_ops(ops='set', mutations=20)
        self._check_cas(check_conflict_resolution=True)
        #self._load_ops(ops='add')
        self._load_ops(ops='replace',mutations=20)
        self._check_cas(check_conflict_resolution=True)

        self._load_ops(ops='delete')
        self._check_cas(check_conflict_resolution=True)

    def test_cas_setMeta_lower(self):

        self.log.info(' Starting test-getMeta')
        self._load_ops(ops='set', mutations=20)
        self._check_cas(check_conflict_resolution=True)

        k=0
        # Select arbit key
        while k<10:

            key = "{0}{1}".format(self.prefix, k)
            k += 1

            vbucket_id = self.client._get_vBucket_id(key)
            #print 'vbucket_id is {0}'.format(vbucket_id)
            mc_active = self.client.memcached(key)
            mc_master = self.client.memcached_for_vbucket( vbucket_id )
            mc_replica = self.client.memcached_for_replica_vbucket(vbucket_id)

            TEST_SEQNO = 123
            TEST_CAS = k

            cas = mc_active.getMeta(key)[4]
            set_with_meta_resp = mc_active.set_with_meta(key, 0, 0, TEST_SEQNO, TEST_CAS, '123456789',vbucket_id)
            cas_post_meta = mc_active.getMeta(key)[4]

            max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(self.client._get_vBucket_id(key)) + ':max_cas'] )
            self.assertTrue(cas_post_meta < max_cas, '[ERROR]Max cas  is not higher it is lower than {0}'.format(cas_post_meta))
            self.assertTrue(max_cas == cas, '[ERROR]Max cas  is not equal to original cas {0}'.format(cas))

            mc_active.set(key, 0, 0,json.dumps({'value':'value3'}))
            cas = mc_active.getMeta(key)[4]
            max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(self.client._get_vBucket_id(key)) + ':max_cas'] )
            self.assertTrue(max_cas == cas, '[ERROR]Max cas  is not equal to cas {0}'.format(cas))

            set_with_meta_resp = mc_active.set_with_meta(key, 0, 0, 125, TEST_CAS, '123456789',vbucket_id)
            cas_post_meta = mc_active.getMeta(key)[4]

            max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(self.client._get_vBucket_id(key)) + ':max_cas'] )
            self.assertTrue(cas_post_meta < max_cas, '[ERROR]Max cas  is not higher it is lower than {0}'.format(cas_post_meta))
            self.assertTrue(max_cas == cas, '[ERROR]Max cas  is not equal to original cas {0}'.format(cas))

            mc_active.set(key, 0, 0,json.dumps({'value':'value3'}))
            cas = mc_active.getMeta(key)[4]
            max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(self.client._get_vBucket_id(key)) + ':max_cas'] )
            self.assertTrue(max_cas == cas, '[ERROR]Max cas  is not equal to cas {0}'.format(cas))

    def test_cas_setMeta_higher(self):

        self.log.info(' Starting test-getMeta')
        self._load_ops(ops='set', mutations=20)
        self._check_cas(check_conflict_resolution=True)

        k=0

        while k<10:

            key = "{0}{1}".format(self.prefix, k)
            k += 1

            vbucket_id = self.client._get_vBucket_id(key)
            #print 'vbucket_id is {0}'.format(vbucket_id)
            mc_active = self.client.memcached(key)
            mc_master = self.client.memcached_for_vbucket( vbucket_id )
            mc_replica = self.client.memcached_for_replica_vbucket(vbucket_id)

            TEST_SEQNO = 123
            TEST_CAS = 9966180844186042368

            cas = mc_active.getMeta(key)[4]
            set_with_meta_resp = mc_active.set_with_meta(key, 0, 0, TEST_SEQNO, TEST_CAS, '123456789',vbucket_id)
            cas_post_meta = mc_active.getMeta(key)[4]

            max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(self.client._get_vBucket_id(key)) + ':max_cas'] )
            self.assertTrue(cas_post_meta == max_cas, '[ERROR]Max cas  is not equal it is {0}'.format(cas_post_meta))
            self.assertTrue(max_cas > cas, '[ERROR]Max cas  is not higher than original cas {0}'.format(cas))

            mc_active.set(key, 0, 0,json.dumps({'value':'value3'}))
            cas = mc_active.getMeta(key)[4]
            max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(self.client._get_vBucket_id(key)) + ':max_cas'] )
            self.assertTrue(max_cas == cas, '[ERROR]Max cas  is not equal to cas {0}'.format(cas))

            set_with_meta_resp = mc_active.set_with_meta(key, 0, 0, 125, TEST_CAS, '123456789',vbucket_id)
            cas_post_meta = mc_active.getMeta(key)[4]

            max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(self.client._get_vBucket_id(key)) + ':max_cas'] )
            self.assertTrue(cas_post_meta < max_cas, '[ERROR]Max cas  is not higher it is lower than {0}'.format(cas_post_meta))
            self.assertTrue(max_cas == cas, '[ERROR]Max cas  is not equal to original cas {0}'.format(cas))

            mc_active.set(key, 0, 0,json.dumps({'value':'value3'}))
            cas = mc_active.getMeta(key)[4]
            max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(self.client._get_vBucket_id(key)) + ':max_cas'] )
            self.assertTrue(max_cas == cas, '[ERROR]Max cas  is not equal to cas {0}'.format(cas))

    ''' Test deleteMeta on cas and max cas values for keys
    '''
    def test_cas_deleteMeta(self):

        self.log.info(' Starting test-deleteMeta')
        self._load_ops(ops='set', mutations=20)
        self._check_cas(check_conflict_resolution=True)

        k=0

        while k<1:

            key = "{0}{1}".format(self.prefix, k)
            k += 1

            vbucket_id = self.client._get_vBucket_id(key)
            mc_active = self.client.memcached(key)
            mc_master = self.client.memcached_for_vbucket( vbucket_id )
            mc_replica = self.client.memcached_for_replica_vbucket(vbucket_id)

            TEST_SEQNO = 123
            TEST_CAS = 456

            cas = mc_active.getMeta(key)[4]
            set_with_meta_resp = mc_active.set_with_meta(key, 0, 0, TEST_SEQNO, TEST_CAS, '123456789',vbucket_id)
            cas_post_meta = mc_active.getMeta(key)[4]

            max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(self.client._get_vBucket_id(key)) + ':max_cas'] )
            self.assertTrue(cas_post_meta < max_cas, '[ERROR]Max cas  is not higher it is lower than {0}'.format(cas_post_meta))
            self.assertTrue(max_cas == cas, '[ERROR]Max cas {0} is not equal to original cas {1}'.format(max_cas, cas))
            print 'cas post {0}'.format(cas_post_meta)


            mc_active.set(key, 0, 0,json.dumps({'value':'value3'}))
            cas = mc_active.getMeta(key)[4]
            max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(self.client._get_vBucket_id(key)) + ':max_cas'] )
            self.assertTrue(max_cas == cas, '[ERROR]Max cas  is not equal to cas {0}'.format(cas))

            set_with_meta_resp = mc_active.set_with_meta(key, 0, 0, 125, TEST_CAS, '123456789',vbucket_id)
            cas_post_meta = mc_active.getMeta(key)[4]

            max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(self.client._get_vBucket_id(key)) + ':max_cas'] )
            self.assertTrue(cas_post_meta < max_cas, '[ERROR]Max cas  is not higher it is lower than {0}'.format(cas_post_meta))
            self.assertTrue(max_cas == cas, '[ERROR]Max cas  is not equal to original cas {0}'.format(cas))

            mc_active.set(key, 0, 0,json.dumps({'value':'value3'}))
            cas = mc_active.getMeta(key)[4]
            max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(self.client._get_vBucket_id(key)) + ':max_cas'] )
            self.assertTrue(max_cas == cas, '[ERROR]Max cas  is not equal to cas {0}'.format(cas))

            self.log.info('Doing delete with meta, using a lower CAS value')
            get_meta_pre = mc_active.getMeta(key)[4]
            del_with_meta_resp = mc_active.del_with_meta(key, 0, 0, TEST_SEQNO, TEST_CAS, TEST_CAS+1)
            get_meta_post = mc_active.getMeta(key)[4]
            max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(self.client._get_vBucket_id(key)) + ':max_cas'] )
            self.assertTrue(max_cas > TEST_CAS+1, '[ERROR]Max cas {0} is not greater than delete cas {1}'.format(max_cas, TEST_CAS))

    ''' Testing skipping conflict resolution, whereby the last write wins, and it does neither cas CR nor rev id CR
    '''
    def test_cas_skip_conflict_resolution(self):

        self.log.info(' Starting test_cas_skip_conflict_resolution ..')
        self._load_ops(ops='set', mutations=20)
        self._check_cas(check_conflict_resolution=True)

        k=0

        #Check for first 20 keys
        while k<20:

            key = "{0}{1}".format(self.prefix, k)
            k += 1

            vbucket_id = self.client._get_vBucket_id(key)
            mc_active = self.client.memcached(key)
            mc_master = self.client.memcached_for_vbucket( vbucket_id )
            mc_replica = self.client.memcached_for_replica_vbucket(vbucket_id)

            low_seq=12

            cas = mc_active.getMeta(key)[4]
            pre_seq = mc_active.getMeta(key)[3]
            all = mc_active.getMeta(key)
            self.log.info('all meta data before set_meta_force {0}'.format(all))

            self.log.info('Forcing conflict_resolution to allow insertion of lower Seq Number')
            lower_cas = int(cas)-1
            set_with_meta_resp = mc_active.set_with_meta(key, 0, 0, low_seq, lower_cas, '123456789',vbucket_id, skipCR=True)
            cas_post_meta = mc_active.getMeta(key)[4]
            all_post_meta = mc_active.getMeta(key)
            post_seq = mc_active.getMeta(key)[3]
            max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(self.client._get_vBucket_id(key)) + ':max_cas'] )
            self.log.info('Expect No conflict_resolution to occur, and the last updated mutation to be the winner..')

            #print 'cas meta data after set_meta_force {0}'.format(cas_post_meta)
            #print 'all meta data after set_meta_force {0}'.format(all_post_meta)
            self.log.info('all meta data after set_meta_force {0}'.format(all_post_meta))

            self.assertTrue(max_cas == cas, '[ERROR]Max cas {0} is not equal to original cas {1}'.format(max_cas, cas))
            self.assertTrue(pre_seq > post_seq, '[ERROR]Pre rev id {0} is not greater than post rev id {1}'.format(pre_seq, post_seq))

    ''' Testing revid based conflict resolution with timeSync enabled, where cas on either mutations match and it does rev id CR
        '''
    def test_revid_conflict_resolution(self):

        self.log.info(' Starting test_cas_revid_conflict_resolution ..')
        self._load_ops(ops='set', mutations=20)
        self._check_cas(check_conflict_resolution=True)

        k=0

        #Check for first 20 keys
        while k<20:

            key = "{0}{1}".format(self.prefix, k)
            k += 1

            vbucket_id = self.client._get_vBucket_id(key)
            mc_active = self.client.memcached(key)
            mc_master = self.client.memcached_for_vbucket( vbucket_id )
            mc_replica = self.client.memcached_for_replica_vbucket(vbucket_id)

            new_seq=121

            cas = mc_active.getMeta(key)[4]
            pre_seq = mc_active.getMeta(key)[3]
            all = mc_active.getMeta(key)
            self.log.info('all meta data before set_meta_force {0}'.format(all))

            self.log.info('Forcing conflict_resolution to rev-id by matching inserting cas ')
            set_with_meta_resp = mc_active.set_with_meta(key, 0, 0, new_seq, cas, '123456789',vbucket_id)
            cas_post_meta = mc_active.getMeta(key)[4]
            all_post_meta = mc_active.getMeta(key)
            post_seq = mc_active.getMeta(key)[3]
            max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(self.client._get_vBucket_id(key)) + ':max_cas'] )
            self.log.info('Expect No conflict_resolution to occur, and the last updated mutation to be the winner..')
            self.log.info('all meta data after set_meta_force {0}'.format(all_post_meta))

            self.assertTrue(max_cas == cas, '[ERROR]Max cas {0} is not equal to original cas {1}'.format(max_cas, cas))
            self.assertTrue(pre_seq < post_seq, '[ERROR]Pre rev id {0} is not greater than post rev id {1}'.format(pre_seq, post_seq))

    ''' Testing conflict resolution, where timeSync is enabled and cas is lower but higher revid, expect Higher Cas to Win
        '''
    def test_cas_conflict_resolution(self):

        self.log.info(' Starting test_cas_conflict_resolution ..')
        self._load_ops(ops='set', mutations=20)
        self._check_cas(check_conflict_resolution=True)

        k=0

        #Check for first 20 keys
        while k<20:

            key = "{0}{1}".format(self.prefix, k)
            k += 1

            vbucket_id = self.client._get_vBucket_id(key)
            mc_active = self.client.memcached(key)
            mc_master = self.client.memcached_for_vbucket( vbucket_id )
            mc_replica = self.client.memcached_for_replica_vbucket(vbucket_id)

            new_seq=121

            cas = mc_active.getMeta(key)[4]
            pre_seq = mc_active.getMeta(key)[3]
            all = mc_active.getMeta(key)
            self.log.info('all meta data before set_meta_force {0}'.format(all))

            lower_cas = int(cas)-100
            self.log.info('Forcing lower rev-id to win with higher CAS value, instead of higher rev-id with Lower Cas ')
            set_with_meta_resp = mc_active.set_with_meta(key, 0, 0, new_seq, lower_cas, '123456789',vbucket_id)
            cas_post_meta = mc_active.getMeta(key)[4]
            all_post_meta = mc_active.getMeta(key)
            post_seq = mc_active.getMeta(key)[3]
            max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(self.client._get_vBucket_id(key)) + ':max_cas'] )
            self.log.info('Expect CAS conflict_resolution to occur, and the first mutation to be the winner..')

            self.log.info('all meta data after set_meta_force {0}'.format(all_post_meta))
            self.assertTrue(max_cas == cas, '[ERROR]Max cas {0} is not equal to original cas {1}'.format(max_cas, cas))
            self.assertTrue(pre_seq < post_seq, '[ERROR]Pre rev id {0} is not greater than post rev id {1}'.format(pre_seq, post_seq))

    ''' Testing revid based conflict resolution with timeSync enabled, where cas on either mutations match and it does rev id CR
        and retains it after a restart server'''
    def test_restart_revid_conflict_resolution(self):

        self.log.info(' Starting test_restart_revid_conflict_resolution ..')
        self._load_ops(ops='set', mutations=20)

        rest = RestConnection(self.master)
        client = VBucketAwareMemcached(rest, self.bucket)

        k=0

        key = "{0}{1}".format(self.prefix, k)

        vbucket_id = self.client._get_vBucket_id(key)
        mc_active = self.client.memcached(key)
        mc_master = self.client.memcached_for_vbucket( vbucket_id )
        mc_replica = self.client.memcached_for_replica_vbucket(vbucket_id)


        # set a key
        value = 'value0'
        client.memcached(key).set(key, 0, 0,json.dumps({'value':value}))
        vbucket_id = client._get_vBucket_id(key)
        print 'vbucket_id is {0}'.format(vbucket_id)
        mc_active = client.memcached(key)
        mc_master = client.memcached_for_vbucket( vbucket_id )
        mc_replica = client.memcached_for_replica_vbucket(vbucket_id)

        new_seq=121

        pre_cas = mc_active.getMeta(key)[4]
        pre_seq = mc_active.getMeta(key)[3]
        pre_max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(self.client._get_vBucket_id(key)) + ':max_cas'] )
        all = mc_active.getMeta(key)
        get_meta_1 = mc_active.getMeta(key,request_extended_meta_data=True)
        print 'cr {0}'.format(get_meta_1)
        self.log.info('all meta data before set_meta_force {0}'.format(all))
        self.log.info('max_cas before set_meta_force {0}'.format(pre_max_cas))

        self.log.info('Forcing conflict_resolution to rev-id by matching inserting cas ')
        set_with_meta_resp = mc_active.set_with_meta(key, 0, 0, new_seq, pre_cas, '123456789',vbucket_id)
        cas_post = mc_active.getMeta(key)[4]
        all_post_meta = mc_active.getMeta(key)
        post_seq = mc_active.getMeta(key)[3]
        get_meta_2 = mc_active.getMeta(key,request_extended_meta_data=True)
        print 'cr {0}'.format(get_meta_2)
        max_cas_post = int( mc_active.stats('vbucket-details')['vb_' + str(self.client._get_vBucket_id(key)) + ':max_cas'] )
        self.log.info('Expect RevId conflict_resolution to occur, and the last updated mutation to be the winner..')
        self.log.info('all meta data after set_meta_force {0}'.format(all_post_meta))

        self.assertTrue(max_cas_post == pre_cas, '[ERROR]Max cas {0} is not equal to original cas {1}'.format(max_cas_post, pre_cas))
        self.assertTrue(pre_seq < post_seq, '[ERROR]Pre rev id {0} is not greater than post rev id {1}'.format(pre_seq, post_seq))


        # Restart Nodes
        self._restart_server(self.servers[:])

        # verify the CAS is good
        client = VBucketAwareMemcached(rest, self.bucket)
        mc_active = client.memcached(key)
        cas_restart = mc_active.getMeta(key)[4]
        #print 'post cas {0}'.format(cas_post)

        get_meta_resp = mc_active.getMeta(key,request_extended_meta_data=True)
        #print 'post CAS {0}'.format(cas_post)
        #print 'post ext meta {0}'.format(get_meta_resp)

        self.assertTrue(pre_cas == cas_post, 'cas mismatch active: {0} replica {1}'.format(pre_cas, cas_post))
        #self.assertTrue( get_meta_resp[5] == 1, msg='Metadata indicate conflict resolution is not set')

    ''' Testing revid based conflict resolution with timeSync enabled, where cas on either mutations match and it does rev id CR
        and retains it after a rebalance server'''
    def test_rebalance_revid_conflict_resolution(self):

        self.log.info(' Starting test_rebalance_revid_conflict_resolution ..')
        self._load_ops(ops='set', mutations=20)

        rest = RestConnection(self.master)
        client = VBucketAwareMemcached(rest, self.bucket)

        k=0

        key = "{0}{1}".format(self.prefix, k)

        vbucket_id = self.client._get_vBucket_id(key)
        mc_active = self.client.memcached(key)
        mc_master = self.client.memcached_for_vbucket( vbucket_id )
        mc_replica = self.client.memcached_for_replica_vbucket(vbucket_id)


        # set a key
        value = 'value0'
        client.memcached(key).set(key, 0, 0,json.dumps({'value':value}))
        vbucket_id = client._get_vBucket_id(key)
        print 'vbucket_id is {0}'.format(vbucket_id)
        mc_active = client.memcached(key)
        mc_master = client.memcached_for_vbucket( vbucket_id )
        mc_replica = client.memcached_for_replica_vbucket(vbucket_id)

        new_seq=121

        pre_cas = mc_active.getMeta(key)[4]
        pre_seq = mc_active.getMeta(key)[3]
        pre_max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(self.client._get_vBucket_id(key)) + ':max_cas'] )
        all = mc_active.getMeta(key)
        get_meta_1 = mc_active.getMeta(key,request_extended_meta_data=True)
        #print 'cr {0}'.format(get_meta_1)
        self.log.info('all meta data before set_meta_force {0}'.format(all))
        self.log.info('max_cas before set_meta_force {0}'.format(pre_max_cas))

        self.log.info('Forcing conflict_resolution to rev-id by matching inserting cas ')
        set_with_meta_resp = mc_active.set_with_meta(key, 0, 0, new_seq, pre_cas, '123456789',vbucket_id)
        cas_post = mc_active.getMeta(key)[4]
        all_post_meta = mc_active.getMeta(key)
        post_seq = mc_active.getMeta(key)[3]
        get_meta_2 = mc_active.getMeta(key,request_extended_meta_data=True)
        #print 'cr {0}'.format(get_meta_2)
        max_cas_post = int( mc_active.stats('vbucket-details')['vb_' + str(self.client._get_vBucket_id(key)) + ':max_cas'] )
        self.log.info('Expect RevId conflict_resolution to occur, and the last updated mutation to be the winner..')
        self.log.info('all meta data after set_meta_force {0}'.format(all_post_meta))

        self.assertTrue(max_cas_post == pre_cas, '[ERROR]Max cas {0} is not equal to original cas {1}'.format(max_cas_post, pre_cas))
        self.assertTrue(pre_seq < post_seq, '[ERROR]Pre rev id {0} is not greater than post rev id {1}'.format(pre_seq, post_seq))

        # remove that node
        self.log.info('Remove the node with active data')

        rebalance = self.cluster.async_rebalance(self.servers[-1:], [] ,[self.master])

        rebalance.result()
        replica_CAS = mc_replica.getMeta(key)[4]
        get_meta_resp = mc_replica.getMeta(key,request_extended_meta_data=True)
        #print 'replica CAS {0}'.format(replica_CAS)
        #print 'replica ext meta {0}'.format(get_meta_resp)

        # add the node back
        self.log.info('Add the node back, the max_cas should be healed')
        rebalance = self.cluster.async_rebalance(self.servers[-1:], [self.master], [])

        rebalance.result()

        # verify the CAS is good
        client = VBucketAwareMemcached(rest, self.bucket)
        mc_active = client.memcached(key)
        active_CAS = mc_active.getMeta(key)[4]
        print 'active cas {0}'.format(active_CAS)

        self.assertTrue(replica_CAS == active_CAS, 'cas mismatch active: {0} replica {1}'.format(active_CAS,replica_CAS))
        #self.assertTrue( get_meta_resp[5] == 1, msg='Metadata indicate conflict resolution is not set')

    ''' Testing revid based conflict resolution with timeSync enabled, where cas on either mutations match and it does rev id CR
        and retains it after a failover server'''
    def test_failover_revid_conflict_resolution(self):

        self.log.info(' Starting test_rebalance_revid_conflict_resolution ..')
        self._load_ops(ops='set', mutations=20)

        rest = RestConnection(self.master)
        client = VBucketAwareMemcached(rest, self.bucket)

        k=0

        key = "{0}{1}".format(self.prefix, k)

        vbucket_id = self.client._get_vBucket_id(key)
        mc_active = self.client.memcached(key)
        mc_master = self.client.memcached_for_vbucket( vbucket_id )
        mc_replica = self.client.memcached_for_replica_vbucket(vbucket_id)


        # set a key
        value = 'value0'
        client.memcached(key).set(key, 0, 0,json.dumps({'value':value}))
        vbucket_id = client._get_vBucket_id(key)
        print 'vbucket_id is {0}'.format(vbucket_id)
        mc_active = client.memcached(key)
        mc_master = client.memcached_for_vbucket( vbucket_id )
        mc_replica = client.memcached_for_replica_vbucket(vbucket_id)

        new_seq=121

        pre_cas = mc_active.getMeta(key)[4]
        pre_seq = mc_active.getMeta(key)[3]
        pre_max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(self.client._get_vBucket_id(key)) + ':max_cas'] )
        all = mc_active.getMeta(key)
        get_meta_1 = mc_active.getMeta(key,request_extended_meta_data=True)
        #print 'cr {0}'.format(get_meta_1)
        self.log.info('all meta data before set_meta_force {0}'.format(all))
        self.log.info('max_cas before set_meta_force {0}'.format(pre_max_cas))

        self.log.info('Forcing conflict_resolution to rev-id by matching inserting cas ')
        set_with_meta_resp = mc_active.set_with_meta(key, 0, 0, new_seq, pre_cas, '123456789',vbucket_id)
        cas_post = mc_active.getMeta(key)[4]
        all_post_meta = mc_active.getMeta(key)
        post_seq = mc_active.getMeta(key)[3]
        get_meta_2 = mc_active.getMeta(key,request_extended_meta_data=True)
        #print 'cr {0}'.format(get_meta_2)
        max_cas_post = int( mc_active.stats('vbucket-details')['vb_' + str(self.client._get_vBucket_id(key)) + ':max_cas'] )
        self.log.info('Expect RevId conflict_resolution to occur, and the last updated mutation to be the winner..')
        self.log.info('all meta data after set_meta_force {0}'.format(all_post_meta))

        self.assertTrue(max_cas_post == pre_cas, '[ERROR]Max cas {0} is not equal to original cas {1}'.format(max_cas_post, pre_cas))
        self.assertTrue(pre_seq < post_seq, '[ERROR]Pre rev id {0} is not greater than post rev id {1}'.format(pre_seq, post_seq))

        # failover that node
        self.log.info('Failing over node with active data {0}'.format(self.master))
        self.cluster.failover(self.servers, [self.master])

        self.log.info('Remove the node with active data {0}'.format(self.master))

        rebalance = self.cluster.async_rebalance(self.servers[:], [] ,[self.master])

        rebalance.result()
        replica_CAS = mc_replica.getMeta(key)[4]
        get_meta_resp = mc_replica.getMeta(key,request_extended_meta_data=True)
        #print 'replica CAS {0}'.format(replica_CAS)
        #print 'replica ext meta {0}'.format(get_meta_resp)

        # add the node back
        self.log.info('Add the node back, the max_cas should be healed')
        rebalance = self.cluster.async_rebalance(self.servers[-1:], [self.master], [])

        rebalance.result()

        # verify the CAS is good
        client = VBucketAwareMemcached(rest, self.bucket)
        mc_active = client.memcached(key)
        active_CAS = mc_active.getMeta(key)[4]
        #print 'active cas {0}'.format(active_CAS)

        self.assertTrue(replica_CAS == active_CAS, 'cas mismatch active: {0} replica {1}'.format(active_CAS,replica_CAS))
        #self.assertTrue( get_meta_resp[5] == 1, msg='Metadata indicate conflict resolution is not set')

    ''' Test getMeta on cas and max cas values for empty vbucket
    '''
    def test_cas_getMeta_empty_vBucket(self):
        self.log.info(' Starting test-getMeta')
        self._load_ops(ops='set', mutations=20)

        k=0
        all_keys = []
        while k<10:
            k+=1
            key = "{0}{1}".format(self.prefix, k)
            all_keys.append(key)

        vbucket_ids = self.client._get_vBucket_ids(all_keys)

        i=0
        if i not in vbucket_ids and i <= 1023:
            vb_non_existing=i
        elif i>1023:
            i +=1
        else:
            self.log.info('ERROR generating empty vbucket id')

        mc_active = self.client.memcached(all_keys[0]) #Taking a temp connection to the mc.
        max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(vb_non_existing) + ':max_cas'] )
        self.assertTrue( max_cas == 0, msg='[ERROR] Max cas is non-zero')


    ''' Test addMeta on cas and max cas values for keys
    '''
    def test_meta_backup(self):
        self.log.info(' Starting test-getMeta')
        self._load_ops(ops='set', mutations=20)

        '''Do the backup on the bucket '''
        self.shell = RemoteMachineShellConnection(self.master)
        self.buckets = RestConnection(self.master).get_buckets()
        self.couchbase_login_info = "%s:%s" % (self.input.membase_settings.rest_username,
                                               self.input.membase_settings.rest_password)
        self.backup_location = "/tmp/backup"
        self.command_options = self.input.param("command_options", '')
        try:
            shell = RemoteMachineShellConnection(self.master)
            self.shell.execute_cluster_backup(self.couchbase_login_info, self.backup_location, self.command_options)

            time.sleep(5)
            shell.restore_backupFile(self.couchbase_login_info, self.backup_location, [bucket.name for bucket in self.buckets])
            print 'Done with restore'
        finally:
            self._check_cas(check_conflict_resolution=True)

    ''' Common function to verify the expected values on cas
    '''
    def _check_cas(self, check_conflict_resolution=False, master=None, bucket=None, time_sync=None):
        self.log.info(' Verifying cas and max cas for the keys')
        #select_count = 20 #Verifying top 20 keys
        if master:
            self.rest = RestConnection(master)
            self.client = VBucketAwareMemcached(self.rest, bucket)

        k=0

        while k < self.items:
            key = "{0}{1}".format(self.prefix, k)
            k += 1
            mc_active = self.client.memcached(key)

            cas = mc_active.getMeta(key)[4]
            max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(self.client._get_vBucket_id(key)) + ':max_cas'] )
            #print 'max_cas is {0}'.format(max_cas)
            self.assertTrue(cas == max_cas, '[ERROR]Max cas  is not 0 it is {0}'.format(cas))

            if check_conflict_resolution:
                get_meta_resp = mc_active.getMeta(key,request_extended_meta_data=True)
                if time_sync == 'enabledWithoutDrift':
                    self.assertTrue( get_meta_resp[5] == 1, msg='[ERROR] Metadata indicate conflict resolution is not set')
                elif time_sync == 'disabled':
                    self.assertTrue( get_meta_resp[5] == 0, msg='[ERROR] Metadata indicate conflict resolution is set')

    ''' Common function to add set delete etc operations on the bucket
    '''
    def _load_ops(self, ops=None, mutations=1, master=None, bucket=None):

        if master:
            self.rest = RestConnection(master)
        if bucket:
            self.client = VBucketAwareMemcached(self.rest, bucket)

        k=0
        payload = MemcachedClientHelper.create_value('*', self.value_size)

        while k < self.items:
            key = "{0}{1}".format(self.prefix, k)
            k += 1
            for i in range(mutations):
                if ops=='set':
                    #print 'set'
                    self.client.memcached(key).set(key, 0, 0,payload)
                elif ops=='add':
                    #print 'add'
                    self.client.memcached(key).add(key, 0, 0,payload)
                elif ops=='replace':
                    self.client.memcached(key).replace(key, 0, 0,payload)
                    #print 'Replace'
                elif ops=='delete':
                    #print 'delete'
                    self.client.memcached(key).delete(key)
                elif ops=='expiry':
                    #print 'expiry'
                    self.client.memcached(key).set(key, self.expire_time ,0, payload)
                elif ops=='touch':
                    #print 'touch'
                    self.client.memcached(key).touch(key, 10)

        self.log.info("Done with specified {0} ops".format(ops))

    '''Check if items are expired as expected'''
    def _check_expiry(self):
        time.sleep(self.expire_time+30)

        k=0
        while k<10:
            key = "{0}{1}".format(self.prefix, k)
            k += 1
            mc_active = self.client.memcached(key)
            cas = mc_active.getMeta(key)[4]
            self.log.info("Try to mutate an expired item with its previous cas {0}".format(cas))
            try:
                all = mc_active.getMeta(key)
                a=self.client.memcached(key).get(key)
                self.client.memcached(key).cas(key, 0, self.item_flag, cas, 'new')
                all = mc_active.getMeta(key)

                raise Exception("The item should already be expired. We can't mutate it anymore")
            except MemcachedError as error:
            #It is expected to raise MemcachedError becasue the key is expired.
                if error.status == ERR_NOT_FOUND:
                    self.log.info("<MemcachedError #%d ``%s''>" % (error.status, error.msg))
                    pass
                else:
                    raise Exception(error)

