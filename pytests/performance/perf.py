import unittest
import logger
import time
import os

from TestInput import TestInputSingleton

from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper

import mcsoda

def TODO():
    pass

class PerfBase(unittest.TestCase):
    spec = "http://hub.internal.couchbase.org/confluence/display/cbit/Black+Box+Performance+Test+Matrix"

    def mem_quota(self):
        return self.parami("mem_quota", 6000) # In MB.

    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input

        self.tearDown() # Helps when a previous broken test never reached tearDown.

        master = self.input.servers[0]
        bucket = self.param("bucket", "default")

        self.rest        = rest        = RestConnection(master)
        self.rest_helper = rest_helper = RestHelper(rest)

        rest.init_cluster(master.rest_username, master.rest_password)
        rest.init_cluster_memoryQuota(master.rest_username,
                                      master.rest_password,
                                      memoryQuota=self.mem_quota())

        rest.create_bucket(bucket=bucket, ramQuotaMB=self.mem_quota())
        self.assertTrue(BucketOperationHelper.wait_for_memcached(master, bucket),
                        msg="wait_for_memcached failed for {0}".format(bucket))
        self.assertTrue(rest_helper.bucket_exists(bucket),
                        msg="unable to create {0} bucket".format(bucket))

        # Number of items loaded by load() method.
        # Does not include or count any items that came from setUp_dgm().
        #
        self.num_items_loaded = 0

        self.setUp_moxi()
        self.setUp_dgm()
        self.wait_until_warmed_up()

    def tearDown(self):
        self.tearDown_moxi()

        BucketOperationHelper.delete_all_buckets_or_assert(self.input.servers, self)
        ClusterOperationHelper.cleanup_cluster(self.input.servers)
        ClusterOperationHelper.wait_for_ns_servers_or_assert(self.input.servers, self)

    def setUp_moxi(self):
        if len(self.input.moxis) > 0:
            moxi = self.input.moxis[0]
            TODO()

    def tearDown_moxi(self):
        if len(self.input.moxis) > 0:
            moxi = self.input.moxis[0]
            TODO()

    def target_moxi(self, bucket='default'): # Returns "host:port" of moxi to hit.
        rv = self.param('moxi', None)
        if rv:
            return rv
        if len(self.input.moxis) > 0:
            return "%s:%s" % (self.input.moxis[0].ip,
                              self.input.moxis[0].port)
        return "%s:%s" % (self.input.servers[0].ip,
                          self.rest.get_bucket(bucket).nodes[0].moxi)

    def restart_moxi(self, bucket='default'):
        TODO()

    def setUp_dgm(self):
        # Download fragmented, DGM dataset onto each cluster node, if
        # not already locally available.
        #
        # The number of vbuckets and database schema must match the
        # target cluster.
        #
        # Shutdown all cluster nodes.
        #
        # Do a cluster-restore.
        #
        # Restart all cluster nodes.
        #
        TODO()

    def spec(self, reference):
        self.spec = reference
        self.log.info("spec: " + reference)

    def start_stats(self, test_name, servers=None,
                    process_names=['memcached', 'beam.smp', 'couchjs']):
        servers = servers or self.input.servers
        TODO()
        return "stats-capture-id"

    def end_stats(self, stats_capture_id):
        TODO()

    def json(self, kind):
        if kind == 'json':
            return 1
        return 0

    def load(self, num_items, min_value_size=None,
             kind='binary',
             protocol='binary',
             expiration=None):
        cfg = { 'max-items': num_items,
                'max-creates': num_items,
                'min-value-size': min_value_size or self.parami("min_value_size", 1024),
                'ratio-sets': 1.0,
                'ratio-creates': 1.0,
                'exit-after-creates': 1,
                'json': int(kind == 'json')
                }
        self.log.info("mcsoda - host_port: " + self.target_moxi())
        self.log.info("mcsoda - cfg: " + str(cfg))
        mcsoda.run(cfg, {},
                   'memcached-' + protocol,
                   self.target_moxi(),
                   '', '')
        self.num_items_loaded = num_items

    def load_dgm(self, min_value_size=None,
                 kind='binary',
                 protocol='binary',
                 expiration=None,
                 dgm_factor=5):
        min_value_size = min_value_size or self.parami("min_value_size", 1024)
        dgm_num_items = self.mem_quota() * 1024 * 1024 * dgm_factor / min_value_size
        self.load(dgm_num_items, min_value_size=min_value_size,
                  kind=kind, expiration=expiration)

    def nodes(self, num_remote_nodes):
        TODO()

    def delayed_rebalance(self, target_num_nodes, delay_seconds=10):
        TODO()

    def delayed_compaction(self, delay_seconds=10):
        TODO()

    def loop(self, num_ops,
             num_items=None,
             max_creates=None,
             min_value_size=None,
             kind='binary',
             protocol='binary',
             clients=1,
             expiration=None,
             ratio_misses=0.0, ratio_sets=0.0, ratio_creates=0.0,
             ratio_hot=0.2, ratio_hot_sets=0.95, ratio_hot_gets=0.95):
        num_items = num_items or self.num_items_loaded

        stats = self.start_stats(self.spec + ".loop")
        cfg = { 'cur-items': num_items,
                'max-ops': num_ops,
                'max-items': num_items,
                'max-creates': max_creates or 0,
                'min-value-size': min_value_size or self.parami("min_value_size", 1024),
                'ratio-sets': ratio_sets,
                'ratio-misses': ratio_misses,
                'ratio-creates': ratio_creates,
                'ratio-hot': ratio_hot,
                'ratio-hot-sets': ratio_hot_sets,
                'ratio_hot-gets': ratio_hot_gets,
                'threads': clients,
                'json': int(kind == 'json')
                }
        self.log.info("mcsoda - moxi: " + self.target_moxi())
        self.log.info("mcsoda - cfg: " + str(cfg))
        mcsoda.run(cfg, {},
                   'memcached-' + protocol,
                   self.target_moxi(),
                   '', '')
        self.end_stats(stats)

    def loop_bg(self, num_ops, num_items=None, min_value_size=None,
                kind='binary',
                protocol='binary',
                clients=1,
                expiration=None,
                ratio_misses=0.0, ratio_sets=0.0, ratio_creates=0.0,
                ratio_hot=0.2, ratio_hot_sets=0.95, ratio_hot_gets=0.95):
        min_value_size = min_value_size or self.parami("min_value_size", 1024)
        num_items = num_items or self.num_items_loaded

    def view(self, views_per_client, clients=1):
        TODO()

    def stop_bg(self):
        TODO()

    def start_cluster(self):
        # lib/remote/remote_util.py: def start_couchbase(self):
        TODO()

    def stop_cluster(self):
        # lib/remote/remote_util.py: def stop_couchbase(self):
        TODO()

    def measure_db_size(self):
        TODO()

    def force_expirations(self):
        TODO()

    def wait_until_drained(self):
        master = self.input.servers[0]
        bucket = self.param("bucket", "default")

        RebalanceHelper.wait_for_stats_on_all(master, bucket, 'ep_queue_size', 0)
        RebalanceHelper.wait_for_stats_on_all(master, bucket, 'ep_flusher_todo', 0)

    def wait_until_warmed_up(self):
        master = self.input.servers[0]
        bucket = self.param("bucket", "default")

        RebalanceHelper.wait_for_stats_on_all(master, bucket,
                                              'ep_warmup_thread', 'complete')

    def assert_perf_was_ok(self):
        TODO()

    def param(self, name, default_value):
        if name in self.input.test_params:
            return self.input.test_params[name]
        return default_value
    def parami(self, name, default_int):
        return int(self.param(name, default_int))
    def paramf(self, name, default_float):
        return float(self.param(name, default_float))


class NodePeakPerformance(PerfBase):

    def test_get_1client(self):
        self.spec('NPP-01-1k')
        self.load(self.parami("items", 1000000),
                  self.parami('size', 1024),
                  kind=self.param('kind', 'binary'))
        self.wait_until_drained()
        self.restart_moxi()
        self.loop(self.parami("items", 1000000),
                  kind           = self.param('kind', 'binary'),
                  protocol       = self.param('protocol', 'binary'),
                  clients        = self.parami('clients', 1),
                  ratio_sets     = self.paramf('ratio_sets', 0.0),
                  ratio_misses   = self.paramf('ratio_misses', 0.0),
                  ratio_hot      = self.paramf('ratio_hot', 0.2),
                  ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95))

    def test_get_4client(self):
        self.spec('NPP-02-1k')
        self.load(self.parami("items", 1000000),
                  self.parami('size', 1024),
                  kind=self.param('kind', 'binary'))
        self.wait_until_drained()
        self.loop(self.parami("items", 4000000),
                  kind           = self.param('kind', 'binary'),
                  protocol       = self.param('protocol', 'binary'),
                  clients        = self.parami('clients', 4),
                  ratio_sets     = self.paramf('ratio_sets', 0.0),
                  ratio_misses   = self.paramf('ratio_misses', 0.0),
                  ratio_hot      = self.paramf('ratio_hot', 0.2),
                  ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95))

    def test_set_1client(self):
        self.spec('NPP-03-1k')
        self.load(self.parami("items", 1000000),
                  self.parami('size', 1024),
                  kind=self.param('kind', 'binary'))
        self.wait_until_drained()
        self.loop(self.parami("items", 1000000),
                  self.parami('size', 1024),
                  kind           = self.param('kind', 'binary'),
                  protocol       = self.param('protocol', 'binary'),
                  clients        = self.parami('clients', 1),
                  ratio_sets     = self.paramf('ratio_sets', 1.0),
                  ratio_creates  = self.paramf('ratio_creates', 0.0),
                  ratio_hot      = self.paramf('ratio_hot', 0.2),
                  ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95))

    def test_mixed_1client(self):
        self.spec('NPP-04-1k')
        self.load(self.parami("items", 1000000),
                  self.parami('size', 1024),
                  kind=self.param('kind', 'binary'))
        self.wait_until_drained()
        self.loop(self.parami("items", 1000000),
                  self.parami('size', 1024),
                  kind           = self.param('kind', 'binary'),
                  protocol       = self.param('protocol', 'binary'),
                  clients        = self.parami('clients', 1),
                  ratio_sets     = self.paramf('ratio_sets', 0.2),
                  ratio_misses   = self.paramf('ratio_misses', 0.2),
                  ratio_creates  = self.paramf('ratio_creates', 0.5),
                  ratio_hot      = self.paramf('ratio_hot', 0.2),
                  ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                  ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95))

    def TODO_test_get_30client(self):
        self.spec('NPP-05-1k')
        self.load(self.parami("items", 1000000),
                  self.parami('size', 1024),
                  kind=self.param('kind', 'binary'))
        self.wait_until_drained()
        self.loop(('seconds', 60 * 60),
                  kind           = self.param('kind', 'binary'),
                  protocol       = self.param('protocol', 'binary'),
                  clients        = self.parami('clients', 30),
                  ratio_sets     = self.paramf('ratio_sets', 0.0),
                  ratio_misses   = self.paramf('ratio_misses', 0.0),
                  ratio_hot      = self.paramf('ratio_hot', 0.2),
                  ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95))

    def test_get_5client_2node(self):
        self.spec('NPP-06-1k')
        self.nodes(2)
        self.load(self.parami("items", 1000000),
                  self.parami('size', 1024),
                  kind=self.param('kind', 'binary'))
        self.wait_until_drained()
        self.loop(self.parami("items", 1000000),
                  kind           = self.param('kind', 'binary'),
                  protocol       = self.param('protocol', 'binary'),
                  clients        = self.parami('clients', 5),
                  ratio_sets     = self.paramf('ratio_sets', 0.0),
                  ratio_misses   = self.paramf('ratio_misses', 0.0),
                  ratio_hot      = self.paramf('ratio_hot', 0.2),
                  ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95))

    def test_get_5client_3node(self):
        self.spec('NPP-07-1k')
        self.nodes(3)
        self.load(self.parami("items", 1000000),
                  self.parami('size', 1024),
                  kind=self.param('kind', 'binary'))
        self.wait_until_drained()
        self.loop(self.parami("items", 1000000),
                  kind           = self.param('kind', 'binary'),
                  protocol       = self.param('protocol', 'binary'),
                  clients        = self.parami('clients', 5),
                  ratio_sets     = self.paramf('ratio_sets', 0.0),
                  ratio_misses   = self.paramf('ratio_misses', 0.0),
                  ratio_hot      = self.paramf('ratio_hot', 0.2),
                  ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95))

    def test_get_5client_5node(self):
        self.spec('NPP-08-1k')
        self.nodes(5)
        self.load(self.parami("items", 1000000),
                  self.parami('size', 1024),
                  kind=self.param('kind', 'binary'))
        self.wait_until_drained()
        self.loop(self.parami("items", 1000000),
                  kind           = self.param('kind', 'binary'),
                  protocol       = self.param('protocol', 'binary'),
                  clients        = self.parami('clients', 5),
                  ratio_sets     = self.paramf('ratio_sets', 0.0),
                  ratio_misses   = self.paramf('ratio_misses', 0.0),
                  ratio_hot      = self.paramf('ratio_hot', 0.2),
                  ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95))

    def test_get_1client_rebalance(self):
        self.spec('NPP-09-5k')
        self.nodes(2)
        self.load(self.parami("items", 1000000),
                  self.parami('size', 5000),
                  kind=self.param('kind', 'binary'))
        self.wait_until_drained()
        self.delayed_rebalance(4, delay_seconds=10)
        self.loop(self.parami("items", 1000000),
                  kind           = self.param('kind', 'binary'),
                  protocol       = self.param('protocol', 'binary'),
                  clients        = self.parami('clients', 1),
                  ratio_sets     = self.paramf('ratio_sets', 0.0),
                  ratio_misses   = self.paramf('ratio_misses', 0.0),
                  ratio_hot      = self.paramf('ratio_hot', 0.2),
                  ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95))

    def test_mixed_1client_rebalance_json(self):
        self.spec('NPP-10-1k')
        self.nodes(2)
        self.load(self.parami("items", 1000000),
                  self.parami('size', 1024),
                  kind=self.param('kind', 'json'))
        self.wait_until_drained()
        self.delayed_rebalance(4, delay_seconds=10)
        self.loop(self.parami("items", 1000000),
                  self.parami('size', 1024),
                  kind           = self.param('kind', 'json'),
                  protocol       = self.param('protocol', 'binary'),
                  clients        = self.parami('clients', 1),
                  ratio_sets     = self.paramf('ratio_sets', 0.2),
                  ratio_misses   = self.paramf('ratio_misses', 0.2),
                  ratio_creates  = self.paramf('ratio_creates', 0.5),
                  ratio_hot      = self.paramf('ratio_hot', 0.2),
                  ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                  ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95))

    def test_set_1client_json(self):
        self.spec('NPP-12-1k')
        self.nodes(1)
        self.load(self.parami("items", 1000000),
                  self.parami('size', 1024),
                  kind=self.param('kind', 'json'))
        self.wait_until_drained()
        self.loop(self.parami("items", 1000000),
                  self.parami('size', 1024),
                  kind           = self.param('kind', 'json'),
                  protocol       = self.param('protocol', 'binary'),
                  clients        = self.parami('clients', 1),
                  ratio_sets     = self.paramf('ratio_sets', 1.0),
                  ratio_creates  = self.paramf('ratio_creates', 0.0),
                  ratio_hot      = self.paramf('ratio_hot', 0.2),
                  ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                  ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95))


class TODO_PerfBase():
    TODO()


class TODO_DiskDrainRate(TODO_PerfBase):

    def test_1M_2k(self):
        self.spec('DRR-01')
        self.load(1000000, 2048,
                  kind='binary')
        self.wait_until_drained()

    def test_9M_1k(self):
        self.spec('DRR-02')
        self.load(9000000, 1024,
                  kind='binary')
        self.wait_until_drained()

    def test_1M_rebalance(self):
        self.spec('DRR-03')
        self.nodes(2)
        self.load_dgm()
        self.delayed_rebalance(4)
        self.load(1000000, 1024,
                  kind='binary')
        self.loop(1000000, ratio_sets=1.0)
        self.wait_until_drained()

    def test_1M_compaction(self):
        self.spec('DRR-04')
        self.load_dgm()
        self.delayed_compaction()
        self.loop(1000000, ratio_sets=1.0)
        self.wait_until_drained()


class TODO_RAMUsage(TODO_PerfBase):

    def test_GetSet_ops(self):
        self.spec('RU-001')
        self.loop(1000000, ratio_sets=0.5)
        self.assert_perf_was_ok()

    # TODO: Revisit advanced mutations...
    #
    # def test_All_ops(self):
    #     self.spec('RU-002')
    #     self.loop(1000000, ratio_sets=0.5, ratio_advanced_mutation=0.5)
    #     self.assert_perf_was_ok()


class TODO_DiskBackfill(TODO_PerfBase):

    def test_10M_ops(self):
        self.spec('DBF-001')
        self.load(10000000)
        self.wait_until_drained()
        self.stop_cluster()
        self.start_cluster()
        self.wait_until_warmed_up()

    def test_All_ops(self):
        expiration = 20 # In seconds.
        self.spec('DBF-002')
        self.load(10000000, expiration=[0.1, expiration])
        self.wait_until_drained()
        self.stop_cluster()
        self.log.info("sleepng {0} seconds to ensure expirations".format(expiration + 2))
        time.sleep(expiration + 2)
        self.start_cluster()
        self.wait_until_warmed_up()

    def test_2nodes(self):
        self.spec('DBF-003')
        self.nodes(2)
        self.load(10000000)
        self.wait_until_drained()
        self.stop_cluster()
        self.start_cluster()
        self.wait_until_warmed_up()


class TODO_CacheMisses(TODO_PerfBase):

    def test_get(self):
        self.spec('CM-001')
        min_value_size = self.parami("min_value_size", 1024)
        dgm_factor = 3
        num_items = self.mem_quota() * 1024 * 1024 * dgm_factor / min_value_size
        self.log.info("loading {0} items, {0}x more than mem_quota of {0}MB".format(
                num_items, dgm_factor, min_value_size))
        self.load(num_items)
        self.wait_until_drained()
        self.loop(num_items)


class TODO_ViewPerformance(TODO_PerfBase):

    def test_1view_1node(self):
        self.spec('VP-001')
        self.load(1000000)
        self.wait_until_drained()
        self.view(1)

    def test_1view_2node(self):
        self.spec('VP-002')
        self.nodes(2)
        self.load(1000000)
        self.wait_until_drained()
        self.view(1)

    def test_100view_1node_10client(self):
        self.spec('VP-003')
        self.load(1000000)
        self.wait_until_drained()
        self.view(100, 10)

    def test_100view_2node_10client(self):
        self.spec('VP-004')
        self.nodes(2)
        self.load(1000000)
        self.wait_until_drained()
        self.view(100, 10)

    def test_compaction(self):
        self.spec('VP-005')
        self.load(1000000)
        self.wait_until_drained()
        self.delayed_compaction()
        self.view(100, 10)

    def test_rebalance(self):
        self.spec('VP-006')
        self.nodes(2)
        self.load(1000000)
        self.wait_until_drained()
        self.delayed_rebalance(4)
        self.view(100, 10)

    def test_mutation(self):
        self.spec('VP-007')
        self.load(1000000)
        self.wait_until_drained()
        self.loop_bg(-1)
        self.view(100, 10)
        self.stop_bg()


class TODO_SmartClients(TODO_PerfBase):

    def test_client_matrix(self):
        for kind in ['moxi', 'java', 'ruby', 'php', 'python', 'c']:
            for start_num_nodes, end_num_nodes in [[1, 1], [2, 2], [2, 4]]:
                self.nodes(start_num_nodes)
                self.load(1000000, kind=kind)
                self.wait_until_drained()
                self.delayed_rebalance(end_num_nodes)
                self.loop(1000000, kind=kind, clients=5)


class TODO_DatabaseFileSize(TODO_PerfBase):

    def test_10M_with_100_mutations(self):
        self.spec('DBF-01')

        m10 = 10000000
        m1  =  1000000

        self.load(m10)
        self.wait_until_drained()
        self.measure_db_size()

        for i in range(100):
            self.loop(m10)
            self.measure_db_size()

        # TODO: self.loop(m1, ratio_sets=1.0, ratio_creates=0.0, ratio_deletes=1.0)
        # TODO: self.measure_db_size()

        self.loop(m1, ratio_sets=1.0, ratio_creates=0.0, expiration=[1.0, 10])
        self.measure_db_size()

        self.force_expirations()
        self.measure_db_size()

