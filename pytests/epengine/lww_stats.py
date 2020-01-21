import time
import os
from basetestcase import BaseTestCase


from sdk_client import SDKClient


from memcached.helper.data_helper import VBucketAwareMemcached, MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection
from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection, RestHelper

import zlib

class LWWStatsTests(BaseTestCase):

    # The stats related epctl vbucket commands actually apply to the whole bucket but we need a vbucket parameter,
    # (which is apparently ignore)
    DUMMY_VBUCKET = ' 123'    # the leading space is needed
    DEFAULT_THRESHOLD = 5000000
    ONE_HOUR_IN_SECONDS = 3600

    def setUp(self):

        super(LWWStatsTests, self).setUp()
        self.rest = RestConnection(self.servers[0])

    def tearDown(self):
        super(LWWStatsTests, self).tearDown()

    def test_time_sync_threshold_setting(self):
        '''
        @summary: This method checks for the change in drift threshold
        settings. We change the drift_ahead_threshold value using REST
        and then verify it against retreived value
        '''

        self.log.info('starting test_time_sync_threshold_setting')
        # bucket is created with lww in base test case using the LWW parameter
        # get the stats
        client = MemcachedClientHelper.direct_client(self.servers[0], self.buckets[0])
        ahead_threshold = int(client.stats()["ep_hlc_drift_ahead_threshold_us"])
        self.assertTrue(ahead_threshold == LWWStatsTests.DEFAULT_THRESHOLD,
                        'Ahead threshold mismatch expected: {0} '\
                        'actual {1}'.format(LWWStatsTests.DEFAULT_THRESHOLD,
                                            ahead_threshold))
        # change the setting and verify it is per the new setting - this may or may not be supported
        cmd = "curl -X POST -u Administrator:password -d "\
        "'driftAheadThresholdMs={0}' http://{1}:8091/pools/default/"\
        "buckets/default".format(str(LWWStatsTests.DEFAULT_THRESHOLD/2000),#Rest API accepts value in milli-seconds
                                 self.servers[0].ip)
        self.log.info("Executing command: %s"%cmd)
        try:
            os.system(cmd)
        except Exception as err:
            self.fail('Exception occurred: %s'%str(err))
        time.sleep(10)
        cl_stats = client.stats()
        ahead_threshold = int(cl_stats["ep_hlc_drift_ahead_threshold_us"])
        self.assertTrue(ahead_threshold == LWWStatsTests.DEFAULT_THRESHOLD/2,
                        'Ahead threshold mismatch expected: {0} actual {1}'.format(LWWStatsTests.DEFAULT_THRESHOLD/2, ahead_threshold))
        # generally need to fill out a matrix here behind/ahead - big and small

    def test_time_sync_threshold_setting_rest_call(self):

        self.log.info('starting test_time_sync_threshold_setting_rest_call')
        # bucket is created with lww in base test case using the LWW parameter
        client = MemcachedClientHelper.direct_client(self.servers[0],
                                                     self.buckets[0])
        rest = RestConnection(self.master)
        self.assertTrue( rest.set_cas_drift_threshold(self.buckets[0],
                                                      100000, 200000),
                        'Unable to set the CAS drift threshold')
        time.sleep(15)# take a few seconds for the stats to settle in
        stats = client.stats()

        self.assertTrue(
            int(stats['ep_hlc_drift_ahead_threshold_us']) == 100000 * 1000,
             'Ahead threshold incorrect. Expected {0} actual {1}'.format(
                 100000 * 1000,
                 stats['ep_hlc_drift_ahead_threshold_us']))

        self.assertTrue(
            int(stats['ep_hlc_drift_behind_threshold_us']) == 200000 * 1000,
             'Ahead threshold incorrect. Expected {0} actual {1}'.format(
                 200000 * 1000,
                 stats['ep_hlc_drift_behind_threshold_us']))
        # generally need to fill out a matrix here behind/ahead - big and small

    def test_poisoned_cas(self):
        """
        @note:  - set the clock ahead
                - do lots of sets and get some CASs
                - do a set and get the CAS (flag, CAS, value) and save it
                - set the clock back
                - verify the CAS is still big on new sets
                - reset the CAS
                - do the vbucket max cas and verify
                - do a new mutation and verify the CAS is smaller
        """
        #creating a user 'default' for the bucket
        self.log.info('starting test_poisoned_cas')
        payload = "name={0}&roles=admin&password=password".format(
            self.buckets[0].name)
        self.rest.add_set_builtin_user(self.buckets[0].name, payload)
        sdk_client = SDKClient(scheme='couchbase', hosts = [self.servers[0].ip], bucket = self.buckets[0].name)
        mc_client = MemcachedClientHelper.direct_client(self.servers[0], self.buckets[0])
        # move the system clock ahead to poison the CAS
        shell = RemoteMachineShellConnection(self.servers[0])
        self.assertTrue(  shell.change_system_time( LWWStatsTests.ONE_HOUR_IN_SECONDS ), 'Failed to advance the clock')

        output, error = shell.execute_command('date')
        self.log.info('Date after is set forward {0}'.format( output ))
        rc = sdk_client.set('key1', 'val1')
        rc = mc_client.get('key1' )
        poisoned_cas = rc[1]
        self.log.info('The poisoned CAS is {0}'.format(poisoned_cas))
        # do lots of mutations to set the max CAS for all vbuckets
        gen_load  = BlobGenerator('key-for-cas-test', 'value-for-cas-test-', self.value_size, end=10000)
        self._load_all_buckets(self.master, gen_load, "create", 0)
        # move the clock back again and verify the CAS stays large
        self.assertTrue(  shell.change_system_time( -LWWStatsTests.ONE_HOUR_IN_SECONDS ), 'Failed to change the clock')
        output, error = shell.execute_command('date')
        self.log.info('Date after is set backwards {0}'.format( output))
        use_mc_bin_client = self.input.param("use_mc_bin_client", True)

        if use_mc_bin_client:
            rc = mc_client.set('key2', 0, 0, 'val2')
            second_poisoned_cas = rc[1]
        else:
            rc = sdk_client.set('key2', 'val2')
            second_poisoned_cas = rc.cas
        self.log.info('The second_poisoned CAS is {0}'.format(second_poisoned_cas))
        self.assertTrue(  second_poisoned_cas > poisoned_cas,
                'Second poisoned CAS {0} is not larger than the first poisoned cas'.format(second_poisoned_cas, poisoned_cas))
        # reset the CAS for all vbuckets. This needs to be done in conjunction with a clock change. If the clock is not
        # changed then the CAS will immediately continue with the clock. I see two scenarios:
        # 1. Set the clock back 1 hours and the CAS back 30 minutes, the CAS should be used
        # 2. Set the clock back 1 hour, set the CAS back 2 hours, the clock should be use
        # do case 1, set the CAS back 30 minutes.  Calculation below assumes the CAS is in nanoseconds
        earlier_max_cas = poisoned_cas - 30 * 60 * 1000000000
        for i in range(self.vbuckets):
            output, error = shell.execute_cbepctl(self.buckets[0], "", "set_vbucket_param",
                              "max_cas ", str(i) + ' ' + str(earlier_max_cas)  )
            if len(error) > 0:
                self.fail('Failed to set the max cas')
        # verify the max CAS
        for i in range(self.vbuckets):
            max_cas = int( mc_client.stats('vbucket-details')['vb_' + str(i) + ':max_cas'] )
            self.assertTrue(max_cas == earlier_max_cas,
                    'Max CAS not properly set for vbucket {0} set as {1} and observed {2}'.format(i, earlier_max_cas, max_cas ) )
            self.log.info('Per cbstats the max cas for bucket {0} is {1}'.format(i, max_cas) )

        rc1 = sdk_client.set('key-after-resetting cas', 'val1')
        rc2 = mc_client.get('key-after-resetting cas' )
        set_cas_after_reset_max_cas = rc2[1]
        self.log.info('The later CAS is {0}'.format(set_cas_after_reset_max_cas))
        self.assertTrue( set_cas_after_reset_max_cas < poisoned_cas,
             'For {0} CAS has not decreased. Current CAS {1} poisoned CAS {2}'.format('key-after-resetting cas', set_cas_after_reset_max_cas, poisoned_cas))
        # do a bunch of sets and verify the CAS is small - this is really only one set, need to do more
        gen_load  = BlobGenerator('key-for-cas-test-after-cas-is-reset', 'value-for-cas-test-', self.value_size, end=1000)
        self._load_all_buckets(self.master, gen_load, "create", 0)
        gen_load.reset()
        while gen_load.has_next():
            key, value = next(gen_load)
            try:
                rc = mc_client.get( key )
                #rc = sdk_client.get(key)
                cas = rc[1]
                self.assertTrue( cas < poisoned_cas, 'For key {0} CAS has not decreased. Current CAS {1} poisoned CAS {2}'.format(key, cas, poisoned_cas))
            except:
                self.log.info('get error with {0}'.format(key))

        rc = sdk_client.set('key3', 'val1')
        better_cas = rc.cas
        self.log.info('The better CAS is {0}'.format(better_cas))
        self.assertTrue( better_cas < poisoned_cas, 'The CAS was not improved')
        # set the clock way ahead - remote_util_OS.py (new)
        # do a bunch of mutations - not really needed
        # do the fix command - cbepctl, the existing way (remote util)
        # do some mutations, verify they conform to the new CAS - build on the CAS code,
        #     where to iterate over the keys and get the CAS?
        """
                    use the SDK client
                    while gen.has_next():
                        key, value = gen.next()
                        get the cas for these
                        also do the vbucket stats
        """
        # also can be checked in the vbucket stats somewhere
        # revert the clock

    def test_drift_stats(self):
        '''
        @note: An exercise in filling out the matrix with the right amount of code,
               we want to test (ahead,behind) and (setwithmeta, deleteWithmeta)
               and (active,replica).
               So for now let's do the set/del in sequences
        '''
        self.log.info('starting test_drift_stats')
        #Creating a user with the bucket name having admin access
        payload = "name={0}&roles=admin&password=password".format(
            self.buckets[0].name)
        self.rest.add_set_builtin_user(self.buckets[0].name, payload)
        check_ahead_threshold = self.input.param("check_ahead_threshold",
                                                 True)

        self.log.info('Checking the ahead threshold? {0}'.format(
            check_ahead_threshold))

        sdk_client = SDKClient(scheme='couchbase',
                               hosts = [self.servers[0].ip],
                               bucket = self.buckets[0].name)
        mc_client = MemcachedClientHelper.direct_client(self.servers[0],
                                                        self.buckets[0])
        shell = RemoteMachineShellConnection(self.servers[0])

        # get the current time
        rc = sdk_client.set('key1', 'val1')
        current_time_cas = rc.cas

        test_key = 'test-set-with-metaxxxx'
        vbId = (((zlib.crc32(test_key.encode())) >> 16) & 0x7fff) & (self.vbuckets- 1)

        #import pdb;pdb.set_trace()
        # verifying the case where we are within the threshold, do a set and del, neither should trigger
        #mc_active.setWithMeta(key, '123456789', 0, 0, 123, cas)
        rc = mc_client.setWithMeta(test_key, 'test-value',
                                   0, 0, 1, current_time_cas)
        #rc = mc_client.setWithMetaLWW(test_key, 'test-value', 0, 0, current_time_cas)
        #rc = mc_client.delWithMetaLWW(test_key, 0, 0, current_time_cas+1)

        vbucket_stats = mc_client.stats('vbucket-details')
        ahead_exceeded  = int(vbucket_stats['vb_' + str(vbId) + ':drift_ahead_threshold_exceeded'])
        self.assertTrue(ahead_exceeded == 0,
                        'Ahead exceeded expected is 0 but is {0}'.format( ahead_exceeded))
        behind_exceeded  = int( vbucket_stats['vb_' + str(vbId) + ':drift_behind_threshold_exceeded'] )
        self.assertTrue( behind_exceeded == 0, 'Behind exceeded expected is 0 but is {0}'.format( behind_exceeded))
        # out of curiousity, log the total counts
        self.log.info('Total stats: total abs drift {0} and total abs drift count {1}'.
                      format(vbucket_stats['vb_' + str(vbId) + ':total_abs_drift'],
                             vbucket_stats['vb_' + str(vbId) + ':total_abs_drift_count']))

        # do the ahead set with meta case - verify: ahead threshold exceeded, total_abs_drift count and abs_drift
        if check_ahead_threshold:
            stat_descriptor = 'ahead'
            cas = current_time_cas + 5000 * LWWStatsTests.DEFAULT_THRESHOLD

        else:
            stat_descriptor = 'behind'
            cas = current_time_cas -(5000 * LWWStatsTests.DEFAULT_THRESHOLD)
        rc = mc_client.setWithMeta(test_key, 'test-value', 0, 0, 0, cas)
        #rc = mc_client.delWithMetaLWW(test_key, 0, 0, cas+1)
        # verify the vbucket stats
        vbucket_stats = mc_client.stats('vbucket-details')
        drift_counter_stat = 'vb_' + str(vbId) + ':drift_' + stat_descriptor + '_threshold_exceeded'
        threshold_exceeded  = int( mc_client.stats('vbucket-details')[drift_counter_stat] )
        # MB-21450 self.assertTrue( ahead_exceeded == 2, '{0} exceeded expected is 1 but is {1}'.
        # format( stat_descriptor, threshold_exceeded))

        self.log.info('Total stats: total abs drift {0} and total abs drift count {1}'.
                      format(vbucket_stats['vb_' + str(vbId) + ':total_abs_drift'],
                             vbucket_stats['vb_' + str(vbId) + ':total_abs_drift_count']))

        # and verify the bucket stats: ep_active_hlc_drift_count, ep_clock_cas_drift_threshold_exceeded,
        # ep_active_hlc_drift
        bucket_stats = mc_client.stats()
        ep_active_hlc_drift_count = int(bucket_stats['ep_active_hlc_drift_count'])
        ep_clock_cas_drift_threshold_exceeded = int(bucket_stats['ep_clock_cas_drift_threshold_exceeded'])
        ep_active_hlc_drift = int(bucket_stats['ep_active_hlc_drift'])

        # Drift count appears to be the number of mutations
        self.assertTrue( ep_active_hlc_drift_count > 0, 'ep_active_hlc_drift_count is 0, expected a positive value')

        # drift itself is the sum of the absolute values of all drifts, so check that it is greater than 0
        self.assertTrue( ep_active_hlc_drift > 0, 'ep_active_hlc_drift is 0, expected a positive value')

        # the actual drift count is a little more granular
        expected_drift_threshold_exceed_count = 1
        self.assertTrue( expected_drift_threshold_exceed_count == ep_clock_cas_drift_threshold_exceeded,
                         'ep_clock_cas_drift_threshold_exceeded is incorrect. Expected {0}, actual {1}'.
                             format(expected_drift_threshold_exceed_count,
                                    ep_clock_cas_drift_threshold_exceeded) )

    def test_logical_clock_ticks(self):

        self.log.info('starting test_logical_clock_ticks')

        payload = "name={0}&roles=admin&password=password".format(
            self.buckets[0].name)
        self.rest.add_set_builtin_user(self.buckets[0].name, payload)
        sdk_client = SDKClient(scheme='couchbase', hosts = [self.servers[0].ip], bucket = self.buckets[0].name)
        mc_client = MemcachedClientHelper.direct_client(self.servers[0], self.buckets[0])
        shell = RemoteMachineShellConnection(self.servers[0])


        # do a bunch of mutations to set the max cas
        gen_load  = BlobGenerator('key-for-cas-test-logical-ticks', 'value-for-cas-test-', self.value_size, end=10000)
        self._load_all_buckets(self.master, gen_load, "create", 0)

        vbucket_stats = mc_client.stats('vbucket-details')
        base_total_logical_clock_ticks = 0
        for i in range(self.vbuckets):
            #print vbucket_stats['vb_' + str(i) + ':logical_clock_ticks']
            base_total_logical_clock_ticks = base_total_logical_clock_ticks + int(vbucket_stats['vb_' + str(i) + ':logical_clock_ticks'])
        self.log.info('The base total logical clock ticks is {0}'.format( base_total_logical_clock_ticks))

        # move the system clock back so the logical counter part of HLC is used and the logical clock ticks
        # stat is incremented
        self.assertTrue(  shell.change_system_time( -LWWStatsTests.ONE_HOUR_IN_SECONDS ), 'Failed to advance the clock')

        # do more mutations
        NUMBER_OF_MUTATIONS = 10000
        gen_load  = BlobGenerator('key-for-cas-test-logical-ticks', 'value-for-cas-test-', self.value_size, end=NUMBER_OF_MUTATIONS)
        self._load_all_buckets(self.master, gen_load, "create", 0)

        vbucket_stats = mc_client.stats('vbucket-details')
        time.sleep(30)
        total_logical_clock_ticks = 0
        for i in range(self.vbuckets):
            total_logical_clock_ticks = total_logical_clock_ticks + int(vbucket_stats['vb_' + str(i) + ':logical_clock_ticks'])

        self.log.info('The total logical clock ticks is {0}'.format( total_logical_clock_ticks))

        self.assertTrue( total_logical_clock_ticks - base_total_logical_clock_ticks == NUMBER_OF_MUTATIONS,
                         'Expected clock tick {0} actual {1}'.format(NUMBER_OF_MUTATIONS,
                                                    total_logical_clock_ticks- base_total_logical_clock_ticks  ))

        # put the clock back, do mutations, the HLC and the tick counter should increment
        #LWWStatsTests
        # will it wrap?
