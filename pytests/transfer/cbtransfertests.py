from transfer.transfer_base import TransferBaseTest
from membase.api.rest_client import RestConnection
from couchbase_helper.documentgenerator import BlobGenerator, DocumentGenerator
from mc_bin_client import MemcachedClient
from memcached.helper.kvstore import KVStore
from memcached.helper.data_helper import VBucketAwareMemcached
import copy


class CBTransferTests(TransferBaseTest):

    def setUp(self):
        super(CBTransferTests, self).setUp()
        self.origin_buckets = list(self.buckets)
        self.master = self.server_recovery
        self._bucket_creation()
        self.buckets = list(self.origin_buckets)

    def tearDown(self):
           super(CBTransferTests, self).tearDown()

    def test_load_regexp(self):
        template = '{{ "mutated" : 0, "age": {0}, "first_name": "{1}" }}'
        gen_load = DocumentGenerator('load_by_id_test', template, list(range(5)), ['james', 'john'], start=0, end=self.num_items)
        gen_load2 = DocumentGenerator('cbtransfer', template, list(range(5)), ['james', 'john'], start=0, end=self.num_items)
        verify_gen = copy.deepcopy(gen_load2)
        for bucket in self.buckets:
            bucket.kvs[2] = KVStore()
            self.cluster.load_gen_docs(self.server_origin, bucket.name, gen_load,
                                       self.buckets[0].kvs[2], "create", exp=0, flag=0, only_store_hash=True,
                                       batch_size=1000, compression=self.sdk_compression)
            self.cluster.load_gen_docs(self.server_origin, bucket.name, gen_load2,
                                       self.buckets[0].kvs[1], "create", exp=0, flag=0, only_store_hash=True,
                                       batch_size=1000, compression=self.sdk_compression)
        transfer_source = 'http://%s:%s' % (self.server_origin.ip, self.server_origin.port)
        transfer_destination = 'http://%s:%s' % (self.server_recovery.ip, self.server_recovery.port)
        self._run_cbtransfer_all_buckets(transfer_source, transfer_destination,
                                         "-k cbtransfer-[0-9]+")
        self._wait_curr_items_all_buckets()
        self._verify_data_all_buckets(verify_gen)

    def test_dry_run(self):
        gen_load = BlobGenerator('load_by_id_test-', 'load_by_id_test-', self.value_size, end=self.num_items)
        self.cluster.load_gen_docs(self.server_origin, self.buckets[0].name, gen_load,
                                   self.buckets[0].kvs[1], "create", exp=0, flag=0, only_store_hash=True,
                                   batch_size=1000, compression=self.sdk_compression)
        transfer_source = 'http://%s:%s' % (self.server_origin.ip, self.server_origin.port)
        transfer_destination = 'http://%s:%s' % (self.server_recovery.ip, self.server_recovery.port)
        output = self.shell.execute_cbtransfer(transfer_source, transfer_destination,
                                      "-b %s -B %s -n" % (self.buckets[0].name, self.buckets[0].name))
        self.buckets = self.buckets[:1]
        self.assertTrue(' '.join(output).find('done') > -1, 'Transfer is not done')
        self._wait_curr_items_all_buckets(curr_items=0)

    def test_vbucket_id_option(self):
        bucket = RestConnection(self.server_origin).get_bucket(self.buckets[0])
        self.num_items = self.num_items - (self.num_items % len(bucket.vbuckets))
        num_items_per_vb = self.num_items // len(bucket.vbuckets)
        template = '{{ "mutated" : 0, "age": {0}, "first_name": "{1}" }}'
        gen_load = DocumentGenerator('cbtransfer', template, list(range(5)), ['james', 'john'], start=0, end=self.num_items)
        client = MemcachedClient(self.server_origin.ip,
                                 int(bucket.vbuckets[0].master.split(':')[1]))
        kv_value_dict = {}
        vb_id_to_check = bucket.vbuckets[-1].id
        for vb_id in range(len(bucket.vbuckets)):
            cur_items_per_vb = 0
            while cur_items_per_vb < num_items_per_vb:
                key, value = next(gen_load)

                client.set(key, 0, 0, value, vb_id)
                if vb_id_to_check == vb_id:
                    kv_value_dict[key] = value
                cur_items_per_vb += 1

        transfer_source = 'http://%s:%s' % (self.server_origin.ip, self.server_origin.port)
        transfer_destination = 'http://%s:%s' % (self.server_recovery.ip, self.server_recovery.port)
        output = self.shell.execute_cbtransfer(transfer_source, transfer_destination,
                                      "-b %s -B %s -i %s" % (bucket.name, bucket.name, vb_id_to_check))
        client = MemcachedClient(self.server_recovery.ip,
                                 int(bucket.vbuckets[0].master.split(':')[1]))
        for key, value in kv_value_dict.items():
            _, _, d = client.get(key, vbucket=vb_id_to_check)
            self.assertEqual(d, value, 'Key: %s expected. Value expected %s. Value actual %s' % (
                                        key, value, d))

    def test_vbucket_state_option(self):
        template = '{{ "mutated" : 0, "age": {0}, "first_name": "{1}" }}'
        gen_load = DocumentGenerator('cbtransfer', template, list(range(5)), ['james', 'john'], start=0, end=self.num_items)
        verify_gen = copy.deepcopy(gen_load)
        for bucket in self.buckets:
            self.cluster.load_gen_docs(self.server_origin, bucket.name, gen_load,
                                       bucket.kvs[1], "create", exp=0, flag=0, only_store_hash=True, batch_size=1000,
                                       compression=self.sdk_compression)
        transfer_source = 'http://%s:%s' % (self.server_origin.ip, self.server_origin.port)
        transfer_destination = 'http://%s:%s' % (self.server_recovery.ip, self.server_recovery.port)
        self._run_cbtransfer_all_buckets(transfer_source, transfer_destination,
                                         "--source-vbucket-state='replica'")
        self._wait_curr_items_all_buckets(curr_items=0)
        self._run_cbtransfer_all_buckets(transfer_source, transfer_destination,
                                         "--source-vbucket-state='active'")
        self._wait_curr_items_all_buckets()
        self._verify_data_all_buckets(verify_gen)

    def test_destination_operation(self):
        dest_op = self.input.param('dest_op', 'set')
        template = '{{ "mutated" : 0, "age": {0}, "first_name": "{1}" }}'
        gen_load = DocumentGenerator('cbtransfer', template, list(range(5)), ['james', 'john'], start=0, end=self.num_items)
        gen_load2 = DocumentGenerator('cbtransfer', template, list(range(5)), ['helena', 'karen'], start=0, end=self.num_items)
        verify_gen = copy.deepcopy((gen_load2, gen_load)[dest_op == 'set'])
        self.cluster.load_gen_docs(self.server_origin, self.buckets[0].name, gen_load,
                                   self.buckets[0].kvs[1], "create", exp=0, flag=0, only_store_hash=True,
                                   batch_size=1000, compression=self.sdk_compression)
        self.cluster.load_gen_docs(self.server_recovery, self.buckets[0].name, gen_load2,
                                   self.buckets[0].kvs[1], "create", exp=0, flag=0, only_store_hash=True,
                                   batch_size=1000, compression=self.sdk_compression)
        transfer_source = 'http://%s:%s' % (self.server_origin.ip, self.server_origin.port)
        transfer_destination = 'http://%s:%s' % (self.server_recovery.ip, self.server_recovery.port)
        self.buckets = list(self.buckets[:1])
        self._run_cbtransfer_all_buckets(transfer_source, transfer_destination,
                                         "--destination-operation=%s" % dest_op)
        self._wait_curr_items_all_buckets()
        self._verify_data_all_buckets(verify_gen)

    def _verify_data_all_buckets(self, gen_check):
        for bucket in self.buckets:
            self.log.info("Check bucket %s" % bucket.name)
            gen = copy.deepcopy(gen_check)
            rest = RestConnection(self.server_recovery)
            client = VBucketAwareMemcached(rest, bucket)
            while gen.has_next():
                key, value = next(gen)
                try:
                    _, _, d = client.get(key)
                    self.assertEqual(d, value, 'Key: %s expected. Value expected %s. Value actual %s' % (
                                            key, value, d))
                except Exception as ex:
                    raise Exception('Key %s not found %s' % (key, str(ex)))

    def _run_cbtransfer_all_buckets(self, source, dest, options):
        for bucket in self.buckets:
            options = "-b %s -B %s " % (bucket.name, bucket.name) + options
            self.shell.execute_cbtransfer(source, dest, options)

    def _wait_curr_items_all_buckets(self, curr_items=None):
        if curr_items is None:
            curr_items = self.num_items
        for bucket in self.buckets:
            self.cluster.wait_for_stats([self.server_recovery], bucket,
                                        '', 'curr_items', '==', curr_items, timeout=30)
