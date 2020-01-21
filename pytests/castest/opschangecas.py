import time
from memcacheConstants import ERR_NOT_FOUND
from castest.cas_base import CasBaseTest
from couchbase_helper.documentgenerator import BlobGenerator
from mc_bin_client import MemcachedError

from membase.api.rest_client import RestConnection, RestHelper
from memcached.helper.data_helper import VBucketAwareMemcached, MemcachedClientHelper
import json
from sdk_client import SDKClient
from couchbase.exceptions import NotFoundError


from remote.remote_util import RemoteMachineShellConnection


class OpsChangeCasTests(CasBaseTest):

    def setUp(self):
        super(OpsChangeCasTests, self).setUp()

    def tearDown(self):
        super(OpsChangeCasTests, self).tearDown()

    def ops_change_cas(self):
        """CAS value manipulation by update, delete, expire test.

        We load a certain number of items. Then for half of them, we use
        MemcachedClient cas() method to mutate those item values in order
        to change CAS value of those items.
        We use MemcachedClient set() method to set a quarter of the items expired.
        We also use MemcachedClient delete() to delete a quarter of the items"""

        gen_load_mysql = BlobGenerator('nosql', 'nosql-', self.value_size, end=self.num_items)
        gen_update = BlobGenerator('nosql', 'nosql-', self.value_size, end=(self.num_items // 2 - 1))
        gen_delete = BlobGenerator('nosql', 'nosql-', self.value_size, start=self.num_items // 2, end=(3*self.num_items // 4 - 1))
        gen_expire = BlobGenerator('nosql', 'nosql-', self.value_size, start=3*self.num_items // 4, end=self.num_items)
        self._load_all_buckets(self.master, gen_load_mysql, "create", 0, flag=self.item_flag)

        if self.doc_ops is not None:
            if "update" in self.doc_ops:
                self.verify_cas("update", gen_update)
            if "touch" in self.doc_ops:
                self.verify_cas("touch", gen_update)
            if "delete" in self.doc_ops:
                self.verify_cas("delete", gen_delete)
            if "expire" in self.doc_ops:
                self.verify_cas("expire", gen_expire)
        self._wait_for_stats_all_buckets([self.master])  # we only need 1 node to do cas test

    def verify_cas(self, ops, generator):
        """Verify CAS value manipulation.

        For update we use the latest CAS value return by set()
        to do the mutation again to see if there is any exceptions.
        We should be able to mutate that item with the latest CAS value.
        For delete(), after it is called, we try to mutate that item with the
        cas vavlue returned by delete(). We should see Memcached Error. Otherwise
        the test should fail.
        For expire, We want to verify using the latest CAS value of that item
        can not mutate it because it is expired already."""

        for bucket in self.buckets:
            client = VBucketAwareMemcached(RestConnection(self.master), bucket.name)
            gen = generator
            cas_error_collection = []
            data_error_collection = []
            while gen.has_next():
                key, value = next(gen)

                if ops in ["update", "touch"]:
                    for x in range(self.mutate_times):
                        o_old, cas_old, d_old = client.get(key)
                        if ops == 'update':
                            client.memcached(key).cas(key, 0, 0, cas_old, "{0}-{1}".format("mysql-new-value", x))
                        else:
                            client.memcached(key).touch(key, 10)

                        o_new, cas_new, d_new = client.memcached(key).get(key)
                        if cas_old == cas_new:
                            print('cas did not change')
                            cas_error_collection.append(cas_old)

                        if ops == 'update':
                            if d_new != "{0}-{1}".format("mysql-new-value", x):
                                data_error_collection.append((d_new, "{0}-{1}".format("mysql-new-value", x)))
                            if cas_old != cas_new and d_new == "{0}-{1}".format("mysql-new-value", x):
                                self.log.info("Use item cas {0} to mutate the same item with key {1} successfully! Now item cas is {2} "
                                              .format(cas_old, key, cas_new))

                        mc_active = client.memcached( key )
                        mc_replica = client.memcached( key, replica_index=0 )

                        active_cas = int( mc_active.stats('vbucket-details')['vb_' + str(client._get_vBucket_id(key)) + ':max_cas'] )
                        self.assertTrue(active_cas == cas_new,
                                        'cbstats cas mismatch. Expected {0}, actual {1}'.format( cas_new, active_cas))

                        replica_cas = int( mc_replica.stats('vbucket-details')['vb_' + str(client._get_vBucket_id(key)) + ':max_cas'] )

                        poll_count = 0
                        while replica_cas != active_cas and poll_count < 5:
                            time.sleep(1)
                            replica_cas = int( mc_replica.stats('vbucket-details')['vb_' + str(client._get_vBucket_id(key)) + ':max_cas'] )
                            poll_count = poll_count + 1

                        if poll_count > 0:
                            self.log.info('Getting the desired CAS was delayed {0} seconds'.format( poll_count) )

                        self.assertTrue(active_cas == replica_cas,
                                        'replica cas mismatch. Expected {0}, actual {1}'.format( cas_new, replica_cas))

                elif ops == "delete":
                    o, cas, d = client.memcached(key).delete(key)
                    time.sleep(10)
                    self.log.info("Delete operation set item cas with key {0} to {1}".format(key, cas))
                    try:
                        client.memcached(key).cas(key, 0, self.item_flag, cas, value)
                        raise Exception("The item should already be deleted. We can't mutate it anymore")
                    except MemcachedError as error:
                        # It is expected to raise MemcachedError because the key is deleted.
                        if error.status == ERR_NOT_FOUND:
                            self.log.info("<MemcachedError #%d ``%s''>" % (error.status, error.msg))
                            pass
                        else:
                            raise Exception(error)
                elif ops == "expire":
                    o, cas, d = client.memcached(key).set(key, self.expire_time, 0, value)
                    time.sleep(self.expire_time+1)
                    self.log.info("Try to mutate an expired item with its previous cas {0}".format(cas))
                    try:
                        client.memcached(key).cas(key, 0, self.item_flag, cas, value)
                        raise Exception("The item should already be expired. We can't mutate it anymore")
                    except MemcachedError as error:
                        # It is expected to raise MemcachedError becasue the key is expired.
                        if error.status == ERR_NOT_FOUND:
                            self.log.info("<MemcachedError #%d ``%s''>" % (error.status, error.msg))
                            pass
                        else:
                            raise Exception(error)

            if len(cas_error_collection) > 0:
                for cas_value in cas_error_collection:
                    self.log.error("Set operation fails to modify the CAS {0}".format(cas_value))
                raise Exception("Set operation fails to modify the CAS value")
            if len(data_error_collection) > 0:
                for data_value in data_error_collection:
                    self.log.error("Set operation fails. item-value is {0}, expected is {1}".format(data_value[0], data_value[1]))
                raise Exception("Set operation fails to change item value")

    def touch_test(self):
        timeout = 900  # 15 minutes
        payload = MemcachedClientHelper.create_value('*', self.value_size)
        mc = MemcachedClientHelper.proxy_client(self.servers[0], "default")
        prefix = "test_"
        self.num_items = self.input.param("items", 10000)
        k = 0
        while k < 100:
            key = "{0}{1}".format(prefix, k)
            mc.set(key, 0, 0, payload)
            k += 1
        active_resident_threshold = 30
        threshold_reached = False
        end_time = time.time() + float(timeout)
        while not threshold_reached and time.time() < end_time:
            if int(mc.stats()["vb_active_perc_mem_resident"]) >= active_resident_threshold:
                self.log.info("vb_active_perc_mem_resident_ratio reached at %s " % (mc.stats()["vb_active_perc_mem_resident"]))
                self.num_items += self.input.param("items", 40000)
                random_key = self.key_generator()
                generate_load = BlobGenerator(random_key, '%s-' % random_key, self.value_size, end=self.num_items)
                self._load_all_buckets(self.servers[0], generate_load, "create", 0, True, batch_size=40000, pause_secs=3)
            else:
                threshold_reached = True
                self.log.info("DGM {0} state achieved!!!!".format(active_resident_threshold))
        if time.time() > end_time and int(mc.stats()["vb_active_perc_mem_resident"]) >= active_resident_threshold:
            raise Exception("failed to load items into bucket")
        """ at active resident ratio above, the first 100 keys
            insert into bucket will be non resident.  Then do touch command to test it """
        self.log.info("Run touch command to test items which are in non resident.")
        k = 0
        while k < 100:
            key = "{0}{1}".format(prefix, k)
            try:
                mc.touch(key, 0)
                k += 1
            except Exception as ex:
                raise Exception(ex)

    def _corrupt_max_cas(self, mcd, key):

        # set the CAS to -2 and then mutate to increment to -1 and then it should stop there
        mcd.setWithMetaInvalid(key, json.dumps({'value':'value2'}), 0, 0, 0, -2)
        # print 'max cas pt1', mcd.getMeta(key)[4]
        mcd.set(key, 0, 0, json.dumps({'value':'value3'}))
        # print 'max cas pt2', mcd.getMeta(key)[4]
        mcd.set(key, 0, 0, json.dumps({'value':'value4'}))
        # print 'max cas pt3', mcd.getMeta(key)[4]

    # test for MB-17517 - verify that if max CAS somehow becomes -1 we can recover from it
    def corrupt_cas_is_healed_on_rebalance_out_in(self):

        self.log.info('Start corrupt_cas_is_healed_on_rebalance_out_in')

        KEY_NAME = 'key1'

        rest = RestConnection(self.master)
        client = VBucketAwareMemcached(rest, 'default')

        # set a key
        client.memcached(KEY_NAME).set(KEY_NAME, 0, 0, json.dumps({'value':'value1'}))

        # figure out which node it is on
        mc_active = client.memcached(KEY_NAME)
        mc_replica = client.memcached( KEY_NAME, replica_index=0 )

        # set the CAS to -2 and then mutate to increment to -1 and then it should stop there
        self._corrupt_max_cas(mc_active, KEY_NAME)

        # CAS should be 0 now, do some gets and sets to verify that nothing bad happens

        resp = mc_active.get(KEY_NAME)
        self.log.info( 'get for {0} is {1}'.format(KEY_NAME, resp))

        # remove that node
        self.log.info('Remove the node with -1 max cas')

        rebalance = self.cluster.async_rebalance(self.servers[-1:], [], [self.master])
        # rebalance = self.cluster.async_rebalance([self.master], [], self.servers[-1:])

        rebalance.result()
        replica_CAS = mc_replica.getMeta(KEY_NAME)[4]

        # add the node back
        self.log.info('Add the node back, the max_cas should be healed')
        rebalance = self.cluster.async_rebalance(self.servers[-1:], [self.master], [])
        # rebalance = self.cluster.async_rebalance([self.master], self.servers[-1:],[])

        rebalance.result()

        # verify the CAS is good
        client = VBucketAwareMemcached(rest, 'default')
        mc_active = client.memcached(KEY_NAME)
        active_CAS = mc_active.getMeta(KEY_NAME)[4]

        self.assertTrue(replica_CAS == active_CAS, 'cas mismatch active: {0} replica {1}'.format(active_CAS, replica_CAS))

    # One node only needed for this test
    def corrupt_cas_is_healed_on_reboot(self):
        self.log.info('Start corrupt_cas_is_healed_on_reboot')

        KEY_NAME = 'key1'

        rest = RestConnection(self.master)
        client = VBucketAwareMemcached(rest, 'default')

        # set a key
        client.memcached(KEY_NAME).set(KEY_NAME, 0, 0, json.dumps({'value':'value1'}))
        # client.memcached(KEY_NAME).set('k2', 0, 0,json.dumps({'value':'value2'}))

        # figure out which node it is on
        mc_active = client.memcached(KEY_NAME)

        # set the CAS to -2 and then mutate to increment to -1 and then it should stop there
        self._corrupt_max_cas(mc_active, KEY_NAME)

        # print 'max cas k2', mc_active.getMeta('k2')[4]

        # CAS should be 0 now, do some gets and sets to verify that nothing bad happens

        # self._restart_memcache('default')
        remote = RemoteMachineShellConnection(self.master)
        remote.stop_server()
        time.sleep(30)
        remote.start_server()
        time.sleep(30)

        rest = RestConnection(self.master)
        client = VBucketAwareMemcached(rest, 'default')
        mc_active = client.memcached(KEY_NAME)

        maxCas = mc_active.getMeta(KEY_NAME)[4]
        self.assertTrue(maxCas == 0, 'max cas after reboot is not 0 it is {0}'.format(maxCas))

    #MB-21448 bug test
    #Description: REPLACE_WITH_CAS on a key that has recently been deleted and then requested 
    #sometimes returns key exists with different CAS instead of key not exists error, this test
    #only requires one node 
    def key_not_exists_test(self):
        self.assertTrue(len(self.buckets) > 0, 'at least 1 bucket required')
        bucket = self.buckets[0].name
        client = VBucketAwareMemcached(RestConnection(self.master), bucket)
        KEY_NAME = 'key'

        for i in range(1500):
            client.set(KEY_NAME, 0, 0, "x")
            # delete and verify get fails
            client.delete(KEY_NAME)
            err = None
            try:
                rc = client.get(KEY_NAME)
            except MemcachedError as error:
                 # It is expected to raise MemcachedError because the key is deleted.
                 err = error.status
            self.assertTrue(err == ERR_NOT_FOUND, 'expected key to be deleted {0}'.format(KEY_NAME))

            #cas errors do not sleep the test for 10 seconds, plus we need to check that the correct
            #error is being thrown
            err = None
            try:
                #For some reason replace instead of cas would not reproduce the bug
                mc_active = client.memcached(KEY_NAME)
                mc_active.replace(KEY_NAME, 0, 10, "value")
            except MemcachedError as error:
                err = error.status
            self.assertTrue(err == ERR_NOT_FOUND, 'was able to replace cas on removed key {0}'.format(KEY_NAME))
