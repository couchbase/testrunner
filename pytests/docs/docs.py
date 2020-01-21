import time
import logger
from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import DocumentGenerator
from membase.api.rest_client import RestConnection
from couchbase_helper.documentgenerator import BlobGenerator

class DocsTests(BaseTestCase):

    def setUp(self):
        super(DocsTests, self).setUp()

    def tearDown(self):
        super(DocsTests, self).tearDown()

    def test_docs_int_big_values(self):
        degree = self.input.param("degree", 53)
        error = self.input.param("error", False)
        number = 2**degree
        first = ['james', 'sharon']
        template = '{{ "number": {0}, "first_name": "{1}" }}'
        gen_load = DocumentGenerator('test_docs', template, [number,], first,
                                     start=0, end=self.num_items)
        self.log.info("create %s documents..." % (self.num_items))
        try:
            self._load_all_buckets(self.master, gen_load, "create", 0)
            self._verify_stats_all_buckets([self.master])
        except Exception as e:
            if error:
               self.log.info("Unable to create documents as expected: %s" % str(e))
            else:
                raise e
        else:
            if error:
                self.fail("Able to create documents with value: %s" % str(number))

    #docs.docs.DocsTests.test_load_memory,nodes_init=3,standard_buckets=3,memcached_buckets=1,replicas=2,quota_percent=75
    """
    1) Configure a cluster with 4 Couchbase Buckets and 1 Memcached Buckets.
    2) Total memory quota allocated for Couchbase should be approx. 75% (12G) of total RAM.
    3) Load initial data on all buckets upto 60% of each memory quota
    4) Pick one bucket and do the following (5) to (8)
    5) Insert new items upto high_wat_mark (75% of memory quota)
    6) Expire/Delete/update random items (ratio of expiration vs delete ~= 8:2)
    7) Repeat (6) until "ep_total_del_items" is ~= (3 X # of items being loaded in (3))
    8) Expire 90% of remaining items
    9) Insert new items or update existing items across buckets
    10) See if we can run into "Hard out of Memory" error (UI)
    """
    def test_load_memory(self):
        num_items = self.quota * 1024 * 0.6 // self.value_size
        num_items = num_items // len(self.buckets)

        self.log.info("Load initial data on all buckets upto 60% of each memory quota")
        gen_load = BlobGenerator('mike', 'mike-', self.value_size, start=0,
                              end=num_items)
        self._load_all_buckets(self.master, gen_load, "create", 0)
        self.log.info("Insert new items upto high_wat_mark (75% of memory quota)")
        for bucket in self.buckets:
            if bucket.type != 'memcached':
                bucket_to_load = bucket
                break
        new_num_items = self.quota * 1024 * 0.15 // self.value_size
        gen_load = BlobGenerator('mike', 'mike-', self.value_size, start=num_items,
                                 end=new_num_items + num_items)
        load = self.cluster.async_load_gen_docs(self.master, bucket_to_load.name, gen_load,
                                 bucket_to_load.kvs[1], 'create', compression=self.sdk_compression)
        load.result()
        end_time = time.time() + 60*60*3
        while time.time() < end_time:
            self.log.info("check memUsed")
            rest = RestConnection(self.master)
            for bucket in rest.get_buckets():
                self.log.info("*****************************\
                                bucket %s: memUsed %s\
                                ****************************" % (bucket.name,
                                                                 bucket.stats.memUsed))
            self.log.info("Expire/Delete/update random items (ratio \
                            of expiration vs delete ~= 8:2)")
            current_num = 0
            wait_task = self.cluster.async_wait_for_stats(self.servers[:self.nodes_init], bucket_to_load,
                                       'all', 'ep_total_del_items', '==', num_items * 3)
            while wait_task.state != "FINISHED":
                gen_update = BlobGenerator('mike', 'mike-', self.value_size, start=current_num,
                                      end=current_num + 5000)
                gen_expire = BlobGenerator('mike', 'mike-', self.value_size, start=current_num + 5000,
                                      end=current_num + 6600)
                gen_delete = BlobGenerator('mike', 'mike-', self.value_size, start=current_num + 6600,
                                      end=current_num + 7000)
                tasks = []
                tasks.append(self.cluster.async_load_gen_docs(self.master, bucket_to_load.name,
                                     gen_update, bucket_to_load.kvs[1], 'update', compression=self.sdk_compression))
                tasks.append(self.cluster.async_load_gen_docs(self.master, bucket_to_load.name,
                                     gen_expire, bucket_to_load.kvs[1], 'update', exp=1,
                                                              compression=self.sdk_compression))
                tasks.append(self.cluster.async_load_gen_docs(self.master, bucket_to_load.name,
                                     gen_delete, bucket_to_load.kvs[1], 'delete', compression=self.sdk_compression))
                for task in tasks:
                    task.result()
                current_num += 7000
        self.log.info("Expire 90% of remaining items")
        remain_keys, _ = bucket_to_load.kvs[1].key_set()
        last_key_to_expire = remain_keys[0.9 * len(remain_keys)][4:]
        gen_expire = BlobGenerator('mike', 'mike-', self.value_size, start=0,
                                  end=last_key_to_expire)
        load = self.cluster.async_load_gen_docs(self.master, bucket_to_load.name,
                                 gen_expire, bucket_to_load.kvs[1], 'update', exp=1, compression=self.sdk_compression)
        load.result()
        self.log.info("Insert new items or update existing items across buckets")
        gen_load = BlobGenerator('mike', 'mike-', self.value_size, start=new_num_items + num_items,
                                 end=new_num_items * 2 + num_items)
        self._load_all_buckets(self.master, gen_load, "create", 0)
