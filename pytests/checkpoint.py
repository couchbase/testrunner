import re
import time
import uuid
import logger
import unittest

from threading import Thread
from tasks.future import TimeoutError
from TestInput import TestInputSingleton
from couchbase.cluster import Cluster
from couchbase.stats_tools import StatsCommon
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from memcached.helper.data_helper import MemcachedClientHelper

log = logger.Logger.get_logger()

ACTIVE="active"
REPLICA1="replica1"
REPLICA2="replica2"
REPLICA3="replica3"

class CheckpointTests(unittest.TestCase):

    def setUp(self):
        self.cluster = Cluster()

        self.input = TestInputSingleton.input
        self.servers = self.input.servers
        self.num_servers = self.input.param("servers", 1)

        master = self.servers[0]
        num_replicas = self.input.param("replicas", 1)
        self.bucket = 'default'

        # Start: Should be in a before class function
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        for server in self.servers:
            ClusterOperationHelper.cleanup_cluster([server])
        ClusterOperationHelper.wait_for_ns_servers_or_assert([master], self)
        # End: Should be in a before class function

        self.quota = self.cluster.init_node(master)
        self.old_vbuckets = self._get_vbuckets(master)
        ClusterOperationHelper.set_vbuckets(master, 1)
        self.cluster.create_default_bucket(master, self.quota, num_replicas)
        self.cluster.rebalance(self.servers[:self.num_servers],
                               self.servers[1:self.num_servers], [])

    def tearDown(self):
        master = self.servers[0]
        ClusterOperationHelper.set_vbuckets(master, self.old_vbuckets)
        rest = RestConnection(master)
        rest.stop_rebalance()
        self.cluster.rebalance(self.servers[:self.num_servers], [],
                               self.servers[1:self.num_servers])
        self.cluster.bucket_delete(master, self.bucket)
        self.cluster.shutdown()

    def checkpoint_create_items(self):
        param = 'checkpoint'
        stat_key = 'vb_0:open_checkpoint_id'
        num_items = 6000

        master = self._get_server_by_state(self.servers[:self.num_servers], self.bucket, ACTIVE)
        self._set_checkpoint_size(self.servers[:self.num_servers], self.bucket, '5000')
        chk_stats = StatsCommon.get_stats(self.servers[:self.num_servers], self.bucket,
                                          param, stat_key)
        load_thread = self.generate_load(master, self.bucket, num_items)
        load_thread.join()
        tasks = []
        for server, value in chk_stats.items():
            tasks.append(self.cluster.async_wait_for_stats([server], self.bucket, param,
                                                           stat_key, '>', value))
        for task in tasks:
            try:
                timeout = 30 if (num_items * .001) < 30 else num_items * .001
                task.result(timeout)
            except TimeoutError:
                self.fail("New checkpoint not created")

    def checkpoint_create_time(self):
        param = 'checkpoint'
        timeout = 60
        stat_key = 'vb_0:open_checkpoint_id'

        master = self._get_server_by_state(self.servers[:self.num_servers], self.bucket, ACTIVE)
        self._set_checkpoint_timeout(self.servers[:self.num_servers], self.bucket, str(timeout))
        chk_stats = StatsCommon.get_stats(self.servers[:self.num_servers], self.bucket,
                                          param, stat_key)
        load_thread = self.generate_load(master, self.bucket, 1)
        load_thread.join()
        log.info("Sleeping for {0} seconds)".format(timeout))
        time.sleep(timeout)
        tasks = []
        for server, value in chk_stats.items():
            tasks.append(self.cluster.async_wait_for_stats([server], self.bucket, param,
                                                           stat_key, '>', value))
        for task in tasks:
            try:
                task.result(60)
            except TimeoutError:
                self.fail("New checkpoint not created")
        self._set_checkpoint_timeout(self.servers[:self.num_servers], self.bucket, str(600))

    def checkpoint_collapse(self):
        param = 'checkpoint'
        chk_size = 5000
        num_items = 25000
        stat_key = 'vb_0:last_closed_checkpoint_id'
        stat_chk_itms = 'vb_0:num_checkpoint_items'

        master = self._get_server_by_state(self.servers[:self.num_servers], self.bucket, ACTIVE)
        slave1 = self._get_server_by_state(self.servers[:self.num_servers], self.bucket, REPLICA1)
        slave2 = self._get_server_by_state(self.servers[:self.num_servers], self.bucket, REPLICA2)
        self._set_checkpoint_size(self.servers[:self.num_servers], self.bucket, str(chk_size))
        m_stats = StatsCommon.get_stats([master], self.bucket, param, stat_key)
        self._stop_replication(slave2, self.bucket)
        load_thread = self.generate_load(master, self.bucket, num_items)
        load_thread.join()

        tasks = []
        chk_pnt = str(int(m_stats[m_stats.keys()[0]]) + (num_items / chk_size))
        tasks.append(self.cluster.async_wait_for_stats([master], self.bucket, param, stat_key,
                                                       '==', chk_pnt))
        tasks.append(self.cluster.async_wait_for_stats([slave1], self.bucket, param, stat_key,
                                                       '==', chk_pnt))
        tasks.append(self.cluster.async_wait_for_stats([slave1], self.bucket, param,
                                                       stat_chk_itms, '>=', str(num_items)))
        for task in tasks:
            try:
                task.result(60)
            except TimeoutError:
                self.fail("Checkpoint not collapsed")

        tasks = []
        self._start_replication(slave2, self.bucket)
        chk_pnt = str(int(m_stats[m_stats.keys()[0]]) + (num_items / chk_size))
        tasks.append(self.cluster.async_wait_for_stats([slave2], self.bucket, param, stat_key,
                                                       '==', chk_pnt))
        tasks.append(self.cluster.async_wait_for_stats([slave1], self.bucket, param,
                                                       stat_chk_itms, '<', num_items))
        for task in tasks:
            try:
                task.result(60)
            except TimeoutError:
                self.fail("Checkpoints not replicated to secondary slave")

    def checkpoint_deduplication(self):
        param = 'checkpoint'
        stat_key = 'vb_0:num_checkpoint_items'

        master = self._get_server_by_state(self.servers[:self.num_servers], self.bucket, ACTIVE)
        slave1 = self._get_server_by_state(self.servers[:self.num_servers], self.bucket, REPLICA1)
        self._set_checkpoint_size(self.servers[:self.num_servers], self.bucket, '5000')
        self._stop_replication(slave1, self.bucket)
        load_thread = self.generate_load(master, self.bucket, 4500)
        load_thread.join()
        load_thread = self.generate_load(master, self.bucket, 1000)
        load_thread.join()
        self._start_replication(slave1, self.bucket)

        tasks = []
        tasks.append(self.cluster.async_wait_for_stats([master], self.bucket, param,
                                                       stat_key, '==', 4501))
        tasks.append(self.cluster.async_wait_for_stats([slave1], self.bucket, param,
                                                       stat_key, '==', 4501))
        for task in tasks:
            try:
                task.result(60)
            except TimeoutError:
                self.fail("Items weren't deduplicated")

    def _set_checkpoint_size(self, servers, bucket, size):
        ClusterOperationHelper.flushctl_set(servers[0], 'chk_max_items', size, bucket)

    def _set_checkpoint_timeout(self, servers, bucket, time):
        ClusterOperationHelper.flushctl_set(servers[0], 'chk_period', time, bucket)

    def _stop_replication(self, server, bucket):
        ClusterOperationHelper.flushctl_set_per_node(server, 'tap_throttle_queue_cap', 0, bucket)

    def _start_replication(self, server, bucket):
        ClusterOperationHelper.flushctl_set_per_node(server, 'tap_throttle_queue_cap', 1000000, bucket)

    def _get_vbuckets(self, server):
        rest = RestConnection(server)
        command = "ns_config:search(couchbase_num_vbuckets_default)"
        status, content = rest.diag_eval(command)

        try:
            vbuckets = int(re.sub('[^\d]', '', content))
        except:
            vbuckets = 1024
        return vbuckets

    def _get_server_by_state(self, servers, bucket, vb_state):
        rest = RestConnection(servers[0])
        vbuckets = rest.get_vbuckets(self.bucket)[0]
        addr = None
        if vb_state == ACTIVE:
            addr = vbuckets.master
        elif vb_state == REPLICA1:
            addr = vbuckets.replica[0].encode("ascii", "ignore")
        elif vb_state == REPLICA2:
            addr = vbuckets.replica[1].encode("ascii", "ignore")
        elif vb_state == REPLICA3:
            addr = vbuckets.replica[2].encode("ascii", "ignore")
        else:
            return None

        addr = addr.split(':', 1)[0]
        for server in servers:
            if addr == server.ip:
                return server
        return None

    def generate_load(self, server, bucket, num_items):
        class LoadGen(Thread):
            def __init__(self, server, bucket, num_items):
                Thread.__init__(self)
                self.server = server
                self.bucket = bucket
                self.num_items = num_items

            def run(self):
                client = MemcachedClientHelper.direct_client(server, bucket)
                for i in range(num_items):
                    key = "key-{0}".format(i)
                    value = "value-{0}".format(str(uuid.uuid4())[:7])
                    client.set(key, 0, 0, value, 0)
                log.info("Loaded {0} key".format(num_items))

        load_thread = LoadGen(server, bucket, num_items)
        load_thread.start()
        return load_thread


