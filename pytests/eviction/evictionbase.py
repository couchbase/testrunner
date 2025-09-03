from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import BlobGenerator
from membase.helper.cluster_helper import ClusterOperationHelper
from couchbase_helper.stats_tools import StatsCommon
from mc_bin_client import MemcachedError
from memcached.helper.data_helper import VBucketAwareMemcached
from membase.api.rest_client import RestConnection
import memcacheConstants as constants


class EvictionBase(BaseTestCase):

    def setUp(self):
        super(EvictionBase, self).setUp()
        self.keys_count = self.input.param("keys-count", 1000)
        self.expires = self.input.param("expires", 60)

    def tearDown(self):
        super(EvictionBase, self).tearDown()

    def load_set_to_be_evicted(self, ttl, end=100):
        gen_create = BlobGenerator('ejected', 'ejected-', 128, start=0,
                                   end=end)
        self._load_all_buckets(self.master, gen_create, "create", ttl)

    def load_ejected_set(self, end=100):
        gen_create = BlobGenerator('ejected', 'ejected-', 128, start=0,
                                   end=end)
        self._load_all_buckets(self.master, gen_create, "create", 0)

    def stat(self, key):
        stats = StatsCommon.get_stats([self.master], 'default', "", key)
        val = list(stats.values())[0]
        if val.isdigit():
            val = int(val)
        elif val.replace('.', '').replace('-', '').isdigit():  # Handle float
            val = float(val)
        return val

    def load_to_dgm(self, active=75, ttl=0, prefix='dgmkv'):
        """
            decides how many items to load to enter active% dgm state
            where active is an integer value between 0 and 100
        """
        doc_size = 1024
        curr_active = self.stat('vb_active_perc_mem_resident')
        total_items = curr_active
        batch_items = 50000

        # go into heavy dgm
        while float(curr_active) > float(active):
            curr_items = self.stat('curr_items')
            gen_create = BlobGenerator('dgmkv', 'dgmkv-', doc_size,
                                       start=curr_items + 1,
                                       end=curr_items+50000)
            gen_create = BlobGenerator(prefix, prefix+'-', doc_size,
                                       start=curr_items + 1,
                                       end=curr_items+batch_items)
            total_items += batch_items
            try:
                self._load_all_buckets(self.master, gen_create, "create", ttl)
            except Exception:
                pass
            curr_active = self.stat('vb_active_perc_mem_resident')

        return total_items

    def get_kv_store(self, index=1):
        bucket = [b for b in self.buckets if b.name == 'default'][0]
        kv_store = bucket.kvs[index]
        return kv_store

    def ops_on_ejected_set(self, action, start=0, end=100, ttl=0):
        kv_store = self.get_kv_store()
        gen_reader = BlobGenerator('ejected', 'ejected-', 128, start=start,
                                   end=end)
        self.cluster.load_gen_docs(self.master, 'default', gen_reader,
                                   kv_store, action, exp=ttl,
                                   compression=self.sdk_compression)

    def run_expiry_pager(self, ts=15):
        ClusterOperationHelper.flushctl_set(self.master, "exp_pager_stime", ts)
        self.log.info("wait for expiry pager to run on all these nodes")

    def verify_missing_keys(self, prefix, count):
        """
           verify that none of the keys exist with specified prefix.
        """
        client = VBucketAwareMemcached(
                    RestConnection(self.servers[0]),
                    self.buckets[0])
        for i in range(count):
            key = "{0}{1}".format(prefix, i)
            try:
                client.get(key)
            except MemcachedError as error:
                self.assertEqual(
                    error.status,
                    constants.ERR_NOT_FOUND,
                    "expected error NOT_FOUND, got {0}".format(error.status))
            else:
                self.fail("Able to retrieve expired document")
