import copy
import json

from basetestcase import BaseTestCase
from membase.helper.cluster_helper import ClusterOperationHelper
from mc_bin_client import MemcachedClient, MemcachedError


from membase.api.rest_client import RestConnection, RestHelper

from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from couchbase_helper.stats_tools import StatsCommon




from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import VBucketAwareMemcached, MemcachedClientHelper

#import StatsCommon

import memcacheConstants
import struct
import time
import traceback


""" Note: these test case test the new API calls used to support time sync which in turn supports LWW.
          So actually there is no direct testing of LWW
"""


class LWW_EP_Engine(BaseTestCase):



    TEST_KEY = 'xkey'
    TEST_VALUE = 'yvalue'

    have_modified_rbac_file = False

    def setUp(self):



        self._cleanup_nodes = []
        self._failed_nodes = []
        super(LWW_EP_Engine, self).setUp()


        # need to enable set drift counter and get adjusted time for the clients. This is only enabled for the
        # XDCR user so we need to do a bit of a hack by using sed to edit the rbac.json file
        # TODO: implement for Windows

        if self.master.ip != '127.0.0.1' and not LWW_EP_Engine.have_modified_rbac_file:
            # first stop the servers
            for s in self.servers:
                self.stop_server(s)

            CMD =  'sed -i -e \'s/"SET_WITH_META",/"SET_WITH_META","SET_DRIFT_COUNTER_STATE","GET_ADJUSTED_TIME",/\' /opt/couchbase/etc/security/rbac.json'
            # do the sed thing
            for s in self.servers:
                shell = RemoteMachineShellConnection(s)
                shell.execute_command(CMD)
            for s in self.servers:
                self.start_server(s)

            LWW_EP_Engine.have_modified_rbac_file = True



    def get_stats_drift_counter(self, client, vbucket_id):
        stats = client.stats('vbucket-details')
        return stats[ 'vb_' + str(vbucket_id) + ':drift_counter']


    def get_adjusted_time(self, client, vbucket_id):
        try:
            res = client.get_adjusted_time(vbucket_id)
            return True, struct.unpack( memcacheConstants.GET_ADJUSTED_TIME_RES_FMT, res[2])[0]
        except Exception as e :
            if e.status == memcacheConstants.ERR_NOT_SUPPORTED:
                return False, 0
            else:
                raise e




    # When comparing times, some differences may be accounted to by delays and should not be treated as error
    def times_are_reasonably_equal(self, time1, time2):
        return abs(time1 - time2) < 1000000



    """ there is an as yet unimplemented means to enable time synchronization. Once it is implemented I will complete
    this test case
    """

    def enable_time_synchronization(self):
        self.log.info('\n\nStarting set_enables_time_synchronization')

        client = VBucketAwareMemcached(RestConnection(self.master), 'default')

        vbucket_id = client._get_vBucket_id(LWW_EP_Engine.TEST_KEY)
        mc_master = client.memcached_for_vbucket( vbucket_id )
        mc_replica = client.memcached_for_replica_vbucket(vbucket_id)

        # enable synchronization - the exact means has not yet been implemented

        self.log.info('\n\nVerify adjusted time is initially set')

        time_is_set, master_val = self.get_adjusted_time( mc_master, vbucket_id)
        self.assertTrue(time_is_set, msg='On startup time should be set on master' )


        time_is_set, replica_val = self.get_adjusted_time( mc_replica, vbucket_id)
        self.assertTrue(time_is_set, msg='On startup time should be set on replica' )


        self.log.info('\n\nEnding enable_time_synchronization')




    def verify_meta_data_when_not_enabled(self):

        self.log.info('\n\nStarting verify_meta_data_when_not_enabled')

        client = VBucketAwareMemcached(RestConnection(self.master), 'default')

        vbucket_id = client._get_vBucket_id(LWW_EP_Engine.TEST_KEY)
        mc_master = client.memcached_for_vbucket( vbucket_id )

        mc_master.set(LWW_EP_Engine.TEST_KEY, 0, 0, LWW_EP_Engine.TEST_VALUE)

        get_meta_resp = mc_master.getMeta(LWW_EP_Engine.TEST_KEY, request_extended_meta_data=True)

        self.assertTrue( get_meta_resp[5] == 0, msg='Metadata indicate conflict resolution is set')

        self.log.info('\n\nEnding verify_meta_data_when_not_enabled')



    def verify_one_node_has_time_sync_and_one_does_not(self):

        # need to explicitly enable and disable sync when it is supported
        self.log.info('\n\nStarting verify_one_node_has_time_sync_and_one_does_not')

        client = VBucketAwareMemcached(RestConnection(self.master), 'default')

        vbucket_id = client._get_vBucket_id(LWW_EP_Engine.TEST_KEY)
        mc_master = client.memcached_for_vbucket( vbucket_id )
        mc_replica = client.memcached_for_replica_vbucket(vbucket_id)

        # set for master but not for the replica
        result = mc_master.set_time_drift_counter_state(vbucket_id, 0, 1)

        mc_master.set(LWW_EP_Engine.TEST_KEY, 0, 0, LWW_EP_Engine.TEST_VALUE)

        get_meta_resp = mc_master.getMeta(LWW_EP_Engine.TEST_KEY, request_extended_meta_data=True)

        self.assertTrue( get_meta_resp[5] ==1, msg='Metadata indicates conflict resolution is not set')

        self.log.info('\n\nEnding verify_one_node_has_time_sync_and_one_does_not')




    def basic_functionality(self):

        self.log.info('\n\nStarting basic_functionality')

        client = VBucketAwareMemcached(RestConnection(self.master), 'default')

        vbucket_id = client._get_vBucket_id(LWW_EP_Engine.TEST_KEY)
        mc_master = client.memcached_for_vbucket( vbucket_id )
        mc_replica = client.memcached_for_replica_vbucket(vbucket_id)





        self.log.info('\n\nVerify adjusted time is initially unset')

        time_is_set, master_val = self.get_adjusted_time( mc_master, vbucket_id)
        self.assertFalse(time_is_set, msg='On startup time should not be set on master' )


        time_is_set, replica_val = self.get_adjusted_time( mc_replica, vbucket_id)
        self.assertFalse(time_is_set, msg='On startup time should not be set on replica' )



        self.log.info('\n\nVerify adjusted time is set properly')
        result = mc_master.set_time_drift_counter_state(vbucket_id, 0, 1)
        time_is_set, master_val = self.get_adjusted_time( mc_master, vbucket_id)
        self.assertTrue(time_is_set, msg='After setting time of master get adjusted time should return the time' )

        time_is_set, replica_val = self.get_adjusted_time( mc_replica, vbucket_id)
        self.assertFalse(time_is_set, msg='After setting time of master replica should be unset' )




        self.log.info('\n\nEnding basic_functionality')



    """  Verify the conflict resolution flag for set-with-meta and delete-with-meta, both when it is set and not set
    """

    def meta_commands(self):
        self.log.info('\n\nStarting meta_commands')


        client = VBucketAwareMemcached(RestConnection(self.master), 'default')
        vbucket_id = client._get_vBucket_id(LWW_EP_Engine.TEST_KEY)
        mc_master = client.memcached_for_vbucket( vbucket_id )
        mc_replica = client.memcached_for_replica_vbucket(vbucket_id)

        result = mc_master.set_time_drift_counter_state(vbucket_id, 0, 1)

        time_is_set, master_val = self.get_adjusted_time( mc_master, vbucket_id)


        # verify conflict res is set
        set_with_meta_resp = mc_master.set_with_meta(LWW_EP_Engine.TEST_KEY, 0, 0, 0, 0, LWW_EP_Engine.TEST_VALUE,
            vbucket_id, add_extended_meta_data=True, adjusted_time=master_val, conflict_resolution_mode=1)

        get_meta_resp = mc_master.getMeta(LWW_EP_Engine.TEST_KEY, request_extended_meta_data=True)
        self.assertTrue( get_meta_resp[5] == 1, msg='Metadata indicate conflict resolution is not set')



        # now do with conflict resolution as false
        new_key = LWW_EP_Engine.TEST_KEY + '1'
        vbucket_id = client._get_vBucket_id( new_key )
        set_with_meta_resp = client.memcached_for_vbucket( vbucket_id ).\
                 set_with_meta(new_key, 0, 0, 0, 0, LWW_EP_Engine.TEST_VALUE, vbucket_id)


        get_meta_resp = client.memcached_for_vbucket( vbucket_id ).getMeta(new_key) #, request_extended_meta_data=True)
        self.assertTrue( get_meta_resp[5] == 0, msg='Metadata indicate conflict resolution is set')




        # and now check conflict resolution for deletes
        # first conflict resolution is set
        new_key = LWW_EP_Engine.TEST_KEY + '2'
        vbucket_id = client._get_vBucket_id( new_key )

        del_with_meta_resp = client.memcached_for_vbucket( vbucket_id ).del_with_meta(new_key, 0, 0, 0, 0, 0,
            vbucket_id, add_extended_meta_data=True, adjusted_time=master_val, conflict_resolution_mode=1)


        get_meta_resp = client.memcached_for_vbucket( vbucket_id ).getMeta(new_key, request_extended_meta_data=True)
        self.assertTrue( get_meta_resp[5] == 1, msg='Metadata indicate conflict resolution is not set')




        # and then it is not set
        new_key = LWW_EP_Engine.TEST_KEY + '3'
        vbucket_id = client._get_vBucket_id( new_key )

        del_with_meta_resp = client.memcached_for_vbucket( vbucket_id ).del_with_meta(new_key, 0, 0, 0, 0, 0,
            vbucket_id)


        get_meta_resp = client.memcached_for_vbucket( vbucket_id ).getMeta(new_key, request_extended_meta_data=True)
        self.assertTrue( get_meta_resp[5] == 0, msg='Metadata indicate conflict resolution is not set')

        self.log.info('\n\nComplete meta_commands')


    """ do repeated setsand verify the CAS is increasing
    """
    def test_monotonic_cas(self):
        self.log.info('\n\nStarting test_monotonic_cas')

        client = VBucketAwareMemcached(RestConnection(self.master), 'default')
        vbucket_id = client._get_vBucket_id(LWW_EP_Engine.TEST_KEY)
        mc_master = client.memcached_for_vbucket( vbucket_id )

        result = mc_master.set_time_drift_counter_state(vbucket_id, 0, 1)

        old_cas = mc_master.stats('vbucket-details')['vb_' + str(vbucket_id) + ':max_cas']

        for i in range(10):
            mc_master.set(LWW_EP_Engine.TEST_KEY, 0, 0, LWW_EP_Engine.TEST_VALUE)

            cas = mc_master.stats('vbucket-details')['vb_' + str(vbucket_id) + ':max_cas']
            self.assertTrue( cas > old_cas, msg='CAS did not increase. Old {0} new {1}'.format(old_cas, cas))
            old_cas = cas


        self.log.info('\n\nComplete test_monotonic_cas')
