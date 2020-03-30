import json
import zlib
from clitest.cli_base import CliBaseTest
from memcached.helper.data_helper import  MemcachedClientHelper
from membase.api.rest_client import RestConnection
from couchbase_helper.documentgenerator import DocumentGenerator
from mc_bin_client import MemcachedClient
from remote.remote_util import RemoteMachineShellConnection


class VBucketToolTests(CliBaseTest):
    def setUp(self):
        super(VBucketToolTests, self).setUp()
        self.timeout = 6000
        self.num_items = self.input.param("items", 1024)
        self.keys_per_vbuckets_dict = {}
        self.clients = {}

    def tearDown(self):
        super(VBucketToolTests, self).tearDown()

    def test_vbuckets_ids(self):
        self._load_by_vbuckets(self.buckets[0])
        shell = RemoteMachineShellConnection(self.master)
        try:
            prefix = 'curl -g http://localhost:%s/pools/default/buckets/%s |' % (
                      self.master.port or '8091', self.buckets[0].name)
            for vb, items in self.keys_per_vbuckets_dict.items():
                o, _ = shell.execute_vbuckettool(items, prefix)
                result = self._parse_vbuckets(o)
                for item in items:
                    self.assertEqual(str(result[item][0]), str(vb.id),
                                     'Key: %s. Vbucket expected is %s. Actual: %s' % (
                                      item, vb.id, result[item]))
        finally:
            shell.disconnect()

    def test_vbuckets_master_replicas(self):
        self.assertTrue(self.num_servers >= self.num_replicas + 1,
                        'This test requires more servers')
        self._load_by_vbuckets(self.buckets[0])
        bucket = RestConnection(self.master).get_bucket(self.buckets[0])
        shell = RemoteMachineShellConnection(self.master)
        try:
            prefix = 'curl -g http://localhost:%s/pools/default/buckets/%s |' % (
                      self.master.port or '8091', bucket.name)
            for vb, items in self.keys_per_vbuckets_dict.items():
                o, _ = shell.execute_vbuckettool(items, prefix)
                result = self._parse_vbuckets(o)
                for item in items:
                    self.assertTrue(result[item][1].startswith(bucket.vbuckets[vb].master),
                                    'Key: %s. Vbucket master expected is %s. Actual: %s' % (
                                     item, bucket.vbuckets[vb].master, result[item]))
                    self.assertFalse({replica[:replica.index(':')]
                                          for replica in result[item][2]} -
                                     set(bucket.vbuckets[vb].replica),
                                    'Key: %s. Vbucket master expected is %s. Actual: %s' % (
                                     item, bucket.vbuckets[vb].replica, result[item]))
        finally:
            shell.disconnect()

    def _load_by_vbuckets(self, bucket):
        bucket = RestConnection(self.master).get_bucket(bucket)
        self.num_items = self.num_items - (self.num_items % len(bucket.vbuckets))
        num_items_per_vb = self.num_items/len(bucket.vbuckets)
        template = '{{ "mutated" : 0, "age": {0}, "first_name": "{1}" }}'
        gen_load = DocumentGenerator('vbuckettool', template, list(range(5)), ['james', 'john'],
                                     start=0, end=self.num_items)
        self._get_clients(bucket)
        for vb in bucket.vbuckets:
            self.keys_per_vbuckets_dict[vb] = []
        for i in range(gen_load.end):
            key, value = next(gen_load)
            vb_id = self._get_vBucket_id(key)
            self.clients[vb.master].set(key, 0, 0, value, vb_id)
            self.keys_per_vbuckets_dict[vb_id].append(key)

    def _get_vBucket_id(self, key):
        return (zlib.crc32(key.encode()) >> 16) & (len(self.vBucketMap) - 1)

    def _get_clients(self, bucket):
        for vbucket in bucket.vbuckets:
            if vbucket.master not in self.clients:
                ip, port = vbucket.master.split(':')
                if bucket.port:
                    port = bucket.port
                self.clients[vbucket.master] = MemcachedClient(ip, int(port))

    def _parse_vbuckets(self, output):
        """ Returns dict with keys as key and value as
            tuple of vb_id, master, replicas"""
        result = {}
        output = [line for line in output if line.startswith('key')]
        for line in output:
            key = line.split()[line.split().index('key:') + 1]
            result[key] = (line.split()[line.split().index('vBucketId:') + 1],
                           line.split()[line.split().index('master:') + 1],
                           line[line.index('replicas:') + 9:].strip().split())
        return result
