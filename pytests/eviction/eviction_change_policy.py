import time
from couchbase.documentgenerator import BlobGenerator
from eviction.evictionbase import EvictionBase
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection

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
                                                         cluster_host="localhost",
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
