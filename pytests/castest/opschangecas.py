import time
import logger
from castest.cas_base import CasBaseTest
from couchbase.documentgenerator import BlobGenerator

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
        self._load_all_buckets(self.master, gen_load_mysql, "create", 0)

        if(self.doc_ops is not None):
            if("update" in self.doc_ops):
                self.verify_cas("update", gen_update)
            if("delete" in self.doc_ops):
                self.verify_cas("delete", gen_delete)
            if("expire" in self.doc_ops):
                self.verify_cas("expire", gen_expire)
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

    def verify_cas(self, ops, generator):
        """Verify CAS value manipulation. Since for delete and expire, they are
        negative result test. So the test is expected to fail with an Memcached Error

        For update we use the latest CAS value return by set()
        to do the mutation again to see if there is any exceptions.
        We should be able to mutate that item with the latest CAS value.
        For delete(), after it is called, the item cas is always reset to zero.
        The cas() operation followed always succeeds.
        For expire, We want to verify using the latest CAS value of that item
        can not mutate it because it is expired already."""

        for bucket in self.buckets:
            gen = generator
            cas_error_collection = []
            data_error_collection = []
            while gen.has_next():
                key, value = gen.next()
                if ops == "update":
                    for x in range(self.mutate_times):
                        o_old, cas_old, d_old = self.clients[bucket.name].get(key)
                        self.clients[bucket.name].cas(key, 0, 0, cas_old, "{0}-{1}".format("mysql-new-value", x))
                        o_new, cas_new, d_new = self.clients[bucket.name].get(key)
                        if cas_old == cas_new:
                            cas_error_collection.append(cas_old)
                        if d_new != "{0}-{1}".format("mysql-new-value", x):
                            data_error_collection.append((d_new,"{0}-{1}".format("mysql-new-value", x)))
                        if cas_old != cas_new and d_new == "{0}-{1}".format("mysql-new-value", x):
                            self.log.info("Use item cas {0} to mutate the same item with key {1} successfully! Now item cas is {2} "
                                          .format(cas_old, key, cas_new))
                elif ops == "delete":
                    o, cas, d = self.clients[bucket.name].delete(key)
                    self.log.info("Delete operation set item cas with key {0} to {1}".format(key, cas))
                    self.clients[bucket.name].cas(key, 0, 0, cas, value)
                    #There is no way to verify CAS value changed by delete until now.
                elif ops == "expire":
                    o, cas, d = self.clients[bucket.name].set(key, self.expire_time, 0, value)
                    time.sleep(self.expire_time+1)
                    self.log.info("Try to mutate an expired item with its previous cas {0}".format(cas))
                    self.clients[bucket.name].cas(key, 0, 0, cas, value)
                    #It is expected to raise CAS exception becasue the key is expired.
                    #It is a negative result test.

            if len(cas_error_collection) > 0:
                for cas_value in cas_error_collection:
                    self.log.error("Set operation fails to modify the CAS {0}".format(cas_value))
                raise Exception("Set operation fails to modify the CAS value")
            if len(data_error_collection) > 0:
                for data_value in data_error_collection:
                    self.log.error("Set operation fails. item-value is {0}, expected is {1}".format(data_value[0], data_value[1]))
                raise Exception("Set operation fails to change item value")
