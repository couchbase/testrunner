import time
import logger
from memcacheConstants import ERR_NOT_FOUND
from castest.cas_base import CasBaseTest
from couchbase_helper.documentgenerator import BlobGenerator
from mc_bin_client import MemcachedError

from membase.api.rest_client import RestConnection, RestHelper
from memcached.helper.data_helper import VBucketAwareMemcached, MemcachedClientHelper

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
        gen_update = BlobGenerator('nosql', 'nosql-', self.value_size, end=(self.num_items / 2 - 1))
        gen_delete = BlobGenerator('nosql', 'nosql-', self.value_size, start=self.num_items / 2, end=(3* self.num_items / 4 - 1))
        gen_expire = BlobGenerator('nosql', 'nosql-', self.value_size, start=3* self.num_items / 4, end=self.num_items)
        self._load_all_buckets(self.master, gen_load_mysql, "create", 0, flag=self.item_flag)

        if(self.doc_ops is not None):
            if("update" in self.doc_ops):
                self.verify_cas("update", gen_update)
            if("delete" in self.doc_ops):
                self.verify_cas("delete", gen_delete)
            if("expire" in self.doc_ops):
                self.verify_cas("expire", gen_expire)
        self._wait_for_stats_all_buckets([self.master]) #we only need 1 node to do cas test

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


        client = VBucketAwareMemcached(RestConnection(self.master), 'default')


        for bucket in self.buckets:
            gen = generator
            cas_error_collection = []
            data_error_collection = []
            while gen.has_next():
                key, value = gen.next()


                if ops == "update":
                    for x in range(self.mutate_times):
                        o_old, cas_old, d_old = client.get(key)
                        client.memcached(key).cas(key, 0, 0, cas_old, "{0}-{1}".format("mysql-new-value", x))

                        o_new, cas_new, d_new = client.get(key)
                        if cas_old == cas_new:
                            cas_error_collection.append(cas_old)
                        if d_new != "{0}-{1}".format("mysql-new-value", x):
                            data_error_collection.append((d_new,"{0}-{1}".format("mysql-new-value", x)))
                        if cas_old != cas_new and d_new == "{0}-{1}".format("mysql-new-value", x):
                            self.log.info("Use item cas {0} to mutate the same item with key {1} successfully! Now item cas is {2} "
                                          .format(cas_old, key, cas_new))

                        mc_active = client.memcached( key )
                        mc_replica = client.memcached( key, replica_index=0 )

                        active_cas = int( mc_active.stats('vbucket-details')['vb_' + str(client._get_vBucket_id(key)) + ':max_cas'] )
                        replica_cas = int( mc_replica.stats('vbucket-details')['vb_' + str(client._get_vBucket_id(key)) + ':max_cas'] )
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
                    #It is expected to raise MemcachedError because the key is deleted.
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
                    #It is expected to raise MemcachedError becasue the key is expired.
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
        stats_all_buckets = {}
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
                items = self.num_items
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
