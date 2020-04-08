import time
from couchbase_helper.documentgenerator import BlobGenerator
from eviction.evictionbase import EvictionBase
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection

from membase.api.rest_client import RestConnection, RestHelper
from memcached.helper.data_helper import VBucketAwareMemcached, MemcachedClientHelper

from mc_bin_client import MemcachedError

import json


class EvictionChangePolicy(EvictionBase):
    def test_reproducer_MB_11698(self):
        gen_create = BlobGenerator('eviction', 'eviction-', self.value_size, end=self.num_items)
        gen_create2 = BlobGenerator('eviction2', 'eviction2-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen_create, "create", 0)
        self._wait_for_stats_all_buckets(self.servers[:self.nodes_init])
        self._verify_stats_all_buckets(self.servers[:self.nodes_init])
        remote = RemoteMachineShellConnection(self.master)
        for bucket in self.buckets:
            output, _ = remote.execute_couchbase_cli(cli_command='bucket-edit',
                                                     cluster_host="localhost:8091",
                                                     user=self.master.rest_username,
                                                     password=self.master.rest_password,
                                                     options='--bucket=%s --bucket-eviction-policy=valueOnly' % bucket.name)
            self.assertTrue(' '.join(output).find('SUCCESS') != -1, 'Eviction policy wasn\'t changed')
        ClusterOperationHelper.wait_for_ns_servers_or_assert(
            self.servers[:self.nodes_init], self,
            wait_time=self.wait_timeout, wait_if_warmup=True)
        self.sleep(10, 'Wait some time before next load')
        self._load_all_buckets(self.master, gen_create2, "create", 0)
        self._wait_for_stats_all_buckets(self.servers[:self.nodes_init], timeout=self.wait_timeout * 5)
        self._verify_stats_all_buckets(self.servers[:self.nodes_init])

    def test_warm_up_with_eviction(self):
        gen_create = BlobGenerator('eviction', 'eviction-', self.value_size, end=self.num_items)
        gen_create2 = BlobGenerator('eviction2', 'eviction2-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen_create, "create", 0)
        self._wait_for_stats_all_buckets(self.servers[:self.nodes_init])
        self._verify_stats_all_buckets(self.servers[:self.nodes_init])
        self.timeout = self.wait_timeout
        self.without_access_log = False
        self._stats_befor_warmup(self.buckets[0])
        self._restart_memcache(self.buckets[0])
        ClusterOperationHelper.wait_for_ns_servers_or_assert(
            self.servers[:self.nodes_init], self,
            wait_time=self.wait_timeout, wait_if_warmup=True)
        self.sleep(10, 'Wait some time before next load')
        self._load_all_buckets(self.master, gen_create2, "create", 0)
        self._wait_for_stats_all_buckets(self.servers[:self.nodes_init], timeout=self.wait_timeout * 5)
        self._verify_stats_all_buckets(self.servers[:self.nodes_init])

    # MB-19371
    def test_full_eviction_changed_to_value_eviction(self):

        KEY_NAME = 'key1'

        gen_create = BlobGenerator('eviction', 'eviction-', self.value_size, end=self.num_items)
        gen_create2 = BlobGenerator('eviction2', 'eviction2-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen_create, "create", 0)

        self._wait_for_stats_all_buckets(self.servers[:self.nodes_init])
        self._verify_stats_all_buckets(self.servers[:self.nodes_init])
        remote = RemoteMachineShellConnection(self.master)
        for bucket in self.buckets:
            output, _ = remote.execute_couchbase_cli(cli_command='bucket-edit',
                                                     cluster_host="localhost:8091",
                                                     user=self.master.rest_username,
                                                     password=self.master.rest_password,
                                                     options='--bucket=%s --bucket-eviction-policy=valueOnly' % bucket.name)
            self.assertTrue(' '.join(output).find('SUCCESS') != -1, 'Eviction policy wasn\'t changed')
        ClusterOperationHelper.wait_for_ns_servers_or_assert(
            self.servers[:self.nodes_init], self,
            wait_time=self.wait_timeout, wait_if_warmup=True)
        self.sleep(10, 'Wait some time before next load')
        # self._load_all_buckets(self.master, gen_create2, "create", 0)

        rest = RestConnection(self.master)
        client = VBucketAwareMemcached(rest, 'default')
        mcd = client.memcached(KEY_NAME)
        try:
            rc = mcd.set(KEY_NAME, 0, 0, json.dumps({'value': 'value2'}))
            self.fail('Bucket is incorrectly functional')
        except MemcachedError as e:
            pass  # this is the exception we are hoping for
