import unittest
import uuid
import logger
import time
import json
import os
import threading

from TestInput import TestInputSingleton

from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from membase.performance.stats import StatsCollector
from remote.remote_util import RemoteMachineShellConnection
import testconstants

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
        self.sc = None
        self.vbucket_count = 0
        self.tearDown() # Helps when a previous broken test never reached tearDown.
        master = self.input.servers[0]
        bucket = self.param("bucket", "default")

        self.rest        = rest        = RestConnection(master)
        self.rest_helper = rest_helper = RestHelper(rest)
        self.data_path   = master.data_path
        rest.set_data_path(master.data_path)
        rest.init_cluster(master.rest_username, master.rest_password)
        rest.init_cluster_memoryQuota(master.rest_username,
                                      master.rest_password,
                                      memoryQuota=self.mem_quota())

        rest.create_bucket(bucket=bucket, ramQuotaMB=self.mem_quota())
        self.assertTrue(BucketOperationHelper.wait_for_memcached(master, bucket),
                        msg="wait_for_memcached failed for {0}".format(bucket))
        self.assertTrue(rest_helper.bucket_exists(bucket),
                        msg="unable to create {0} bucket".format(bucket))

        vBuckets = RestConnection(master).get_vbuckets(bucket)
        self.vbucket_count = len(vBuckets)

        # Number of items loaded by load() method.
        # Does not include or count any items that came from setUp_dgm().
        #
        self.num_items_loaded = 0

        self.setUp_moxi()
        try:
            if self.dgm is None:
                self.dgm = self.parami("dgm", 1)
        except AttributeError:
            self.dgm = True
        if self.dgm:
            self.setUp_dgm()

        time.sleep(10)
        self.wait_until_warmed_up()
        ClusterOperationHelper.flush_os_caches(self.input.servers)

    def tearDown(self):
        self.tearDown_moxi()
        if self.sc is not None:
            self.sc.stop()
        BucketOperationHelper.delete_all_buckets_or_assert(self.input.servers, self)
        ClusterOperationHelper.cleanup_cluster(self.input.servers)
        ClusterOperationHelper.wait_for_ns_servers_or_assert(self.input.servers, self)

    def setUp_moxi(self, bucket=None):
        bucket = bucket or self.param("bucket", "default")
        if len(self.input.moxis) > 0:
            shell = RemoteMachineShellConnection(self.input.moxis[0])
            shell.start_moxi(self.input.servers[0].ip, bucket,
                             self.input.moxis[0].port)
            shell.disconnect()

    def tearDown_moxi(self):
        if len(self.input.moxis) > 0:
            shell = RemoteMachineShellConnection(self.input.moxis[0])
            shell.stop_moxi()
            shell.disconnect()

    def target_moxi(self, bucket='default', use_direct=False): # Returns "host:port" of moxi to hit.
        rv = self.param('moxi', None)
        if use_direct:
            return "%s:%s" % (self.input.servers[0].ip,
                              '11210')
        if rv:
            return rv
        if len(self.input.moxis) > 0:
            return "%s:%s" % (self.input.moxis[0].ip,
                              self.input.moxis[0].port)
        return "%s:%s" % (self.input.servers[0].ip,
                          self.rest.get_bucket(bucket).nodes[0].moxi)

    def restart_moxi(self, bucket=None):
        self.tearDown_moxi()
        self.setUp_moxi(bucket)

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
        bucket = self.param("bucket", "default")
        ClusterOperationHelper.stop_cluster(self.input.servers)
        for server in self.input.servers:
            remote = RemoteMachineShellConnection(server)
            #TODO: Better way to pass num_nodes and db_size?
            self.get_data_files(remote, bucket, 1, 10)
            remote.disconnect()
        ClusterOperationHelper.start_cluster(self.input.servers)

    def get_data_files(self, remote, bucket, num_nodes, db_size):
        dir = '/tmp/'
        if remote.is_membase_installed():
            dir = dir + '/membase/{0}-{1}-{2}/'.format(num_nodes, 1024, db_size)
            output, error = remote.execute_command('mkdir -p {0}'.format(dir))
            remote.log_command_output(output, error)
            file = '{0}_mb.tar.gz'.format(bucket)
            base_url = 'https://s3.amazonaws.com/database-analysis/membase/{0}-{1}-{2}/{3}'.format(num_nodes, \
                                                                                                1024, db_size, file)
        else:
            dir = dir + '/couchbase/{0}-{1}-{2}/'.format(num_nodes, 256, db_size)
            output, error = remote.execute_command('mkdir -p {0}'.format(dir))
            remote.log_command_output(output, error)
            file = '{0}_cb.tar.gz'.format(bucket)
            base_url = 'https://s3.amazonaws.com/database-analysis/couchbase/{0}-{1}-{2}/{3}'.format(num_nodes, \
                                                                                                  256, db_size, file)

        info = remote.extract_remote_info()
        wget_command = 'wget'
        if info.type.lower() == 'windows':
            wget_command = "cd {0} ;cmd /c 'c:\\automation\\wget.exe --no-check-certificate".format(dir)

        # Check if the file exists on the remote server else download the gzipped version
        # Extract if necessary
        exist = remote.file_exists(dir, file)
        if not exist:
            additional_quote = ""
            if info.type.lower() == 'windows':
                additional_quote = "'"
            command = "{0} -v -O {1}{2} {3} {4} ".format(wget_command, dir, file, base_url, additional_quote)
            output, error = remote.execute_command(command)
            remote.log_command_output(output, error)

        if remote.is_membase_installed():
            if info.type.lower() == 'windows':
                destination_folder = testconstants.WIN_MEMBASE_DATA_PATH
            else:
                destination_folder = testconstants.MEMBASE_DATA_PATH
        else:
            if info.type.lower() == 'windows':
                destination_folder = testconstants.WIN_COUCHBASE_DATA_PATH
            else:
                destination_folder = testconstants.COUCHBASE_DATA_PATH
        if self.data_path:
            destination_folder = self.data_path
        untar_command = 'cd {1}; tar -xzf {0}'.format(dir+file , destination_folder)
        output, error = remote.execute_command(untar_command)
        remote.log_command_output(output, error)

    def spec(self, reference):
        self.spec_reference = reference
        self.log.info("spec: " + reference)

    def start_stats(self, test_name, servers=None,
                    process_names=['memcached', 'beam.smp', 'couchjs'], test_params = None):
        servers = servers or self.input.servers
        sc = StatsCollector(False)
        sc.start(servers, "default", process_names, test_name, 10)
        self.test_params = test_params
        self.sc = sc
        return self.sc

    def end_stats(self, sc, total_stats=None):
        if total_stats:
            sc.total_stats(total_stats)
        sc.stop()
        sc.export(self.spec_reference, self.test_params)

    def load(self, num_items, min_value_size=None,
             kind='binary',
             protocol='binary',
             expiration=None,
             ratio_sets=1.0,
             ratio_hot_sets=0.0,
             ratio_hot_gets=0.0):
        cfg = { 'max-items': num_items,
                'max-creates': num_items,
                'min-value-size': min_value_size or self.parami("min_value_size", 1024),
                'ratio-sets': ratio_sets,
                'ratio-misses': 0.0,
                'ratio-creates': 1.0,
                'ratio-hot': 0.0,
                'ratio-hot-sets': ratio_hot_sets,
                'ratio-hot-gets': ratio_hot_gets,
                'exit-after-creates': 1,
                'json': int(kind == 'json'),
                'batch': 1000,
                'vbuckets': self.vbucket_count,
                'doc-cache': 1
                }
        self.log.info("mcsoda - host_port: " + self.target_moxi(use_direct=True))
        self.log.info("mcsoda - cfg: " + str(cfg))
        cur, start_time, end_time = mcsoda.run(cfg, {},
                                               'memcached-' + protocol,
                                               self.target_moxi(use_direct=True),
                                               '', '')
        self.num_items_loaded = num_items
        ops = { 'tot-sets': cur.get('cur-sets', 0),
                'tot-gets': cur.get('cur-gets', 0),
                'tot-items': cur.get('cur-items', 0),
                'tot-creates': cur.get('cur-creates', 0),
                'tot-misses': cur.get('cur-misses', 0),
                "start-time": start_time,
                "end-time": end_time }
        return ops, start_time, end_time

    def nodes(self, num_nodes):
        self.assertTrue(RebalanceHelper.rebalance_in(self.input.servers, num_nodes - 1))

    @staticmethod
    def delayed_rebalance_worker(servers, num_nodes, delay_seconds):
        time.sleep(delay_seconds)
        RebalanceHelper.rebalance_in(servers, num_nodes - 1)

    def delayed_rebalance(self, num_nodes, delay_seconds=10):
        t = threading.Thread(target=PerfBase.delayed_rebalance_worker,
                             args=(self.input.servers, num_nodes, delay_seconds))
        t.daemon = True
        t.start()

    @staticmethod
    def set_auto_compaction(server, parallel_compaction, percent_threshold):
        rest = RestConnection(server)
        rest.set_autoCompaction(parallel_compaction, percent_threshold, percent_threshold)

    @staticmethod
    def delayed_compaction_worker(servers, parallel_compaction, percent_threshold, delay_seconds):
        time.sleep(delay_seconds)
        PerfBase.set_auto_compaction(servers[0], parallel_compaction, percent_threshold)

    def delayed_compaction(self, parallel_compaction="false",
                           percent_threshold=0.01,
                           delay_seconds=10):
        t = threading.Thread(target=PerfBase.delayed_compaction_worker,
                             args=(self.input.servers,
                                   parallel_compaction,
                                   percent_threshold,
                                   delay_seconds))
        t.daemon = True
        t.start()

    def loop_prep(self):
        self.wait_until_drained()
        self.restart_moxi()

    def loop(self, num_ops=None,
             num_items=None,
             max_creates=None,
             min_value_size=None,
             kind='binary',
             protocol='binary',
             clients=1,
             expiration=None,
             ratio_misses=0.0, ratio_sets=0.0, ratio_creates=0.0,
             ratio_hot=0.2, ratio_hot_sets=0.95, ratio_hot_gets=0.95, test_name=None):
        num_items = num_items or self.num_items_loaded

        cfg = { 'max-items': num_items,
                'max-creates': max_creates or 0,
                'min-value-size': min_value_size or self.parami("min_value_size", 1024),
                'ratio-sets': ratio_sets,
                'ratio-misses': ratio_misses,
                'ratio-creates': ratio_creates,
                'ratio-hot': ratio_hot,
                'ratio-hot-sets': ratio_hot_sets,
                'ratio_hot-gets': ratio_hot_gets,
                'threads': clients,
                'json': int(kind == 'json'),
                'batch': 1000,
                'vbuckets': self.vbucket_count,
                'doc-cache': 1
                }
        cfg_params = cfg.copy()
        cfg_params['test_time'] = time.time()
        cfg_params['test_name'] = test_name
        sc = self.start_stats(self.spec_reference + ".loop", test_params = cfg_params)

        cur = { 'cur-items': num_items }
        if num_ops is None:
            num_ops = num_items
        if type(num_ops) == type(0):
            cfg['max-ops'] = num_ops
        else:
            # Here, we num_ops looks like "time to run" tuple of...
            # ('seconds', integer_num_of_seconds_to_run)
            cfg['time'] = num_ops[1]
        self.log.info("mcsoda - moxi: " + self.target_moxi(use_direct=True))
        self.log.info("mcsoda - cfg: " + str(cfg))
        cur, start_time, end_time = mcsoda.run(cfg, cur,
                                               'memcached-' + protocol,
                                               self.target_moxi(use_direct=True),
                                               '', '', sc)
        ops = { 'tot-sets': cur.get('cur-sets', 0),
                'tot-gets': cur.get('cur-gets', 0),
                'tot-items': cur.get('cur-items', 0),
                'tot-creates': cur.get('cur-creates', 0),
                'tot-misses': cur.get('cur-misses', 0),
                "start-time": start_time,
                "end-time": end_time }
        self.end_stats(sc, ops)
        self.wait_until_drained()
        return ops, start_time, end_time

    def loop_bg(self, num_ops, num_items=None, min_value_size=None,
                kind='binary',
                protocol='binary',
                clients=1,
                expiration=None,
                ratio_misses=0.0, ratio_sets=0.0, ratio_creates=0.0,
                ratio_hot=0.2, ratio_hot_sets=0.95, ratio_hot_gets=0.95):
        min_value_size = min_value_size or self.parami("min_value_size", 1024)
        num_items = num_items or self.num_items_loaded
        TODO()

    def wait_until_drained(self):
        master = self.input.servers[0]
        bucket = self.param("bucket", "default")

        RebalanceHelper.wait_for_stats_on_all(master, bucket, 'ep_queue_size', 0, \
                                              fn=RebalanceHelper.wait_for_stats_no_timeout)
        RebalanceHelper.wait_for_stats_on_all(master, bucket, 'ep_flusher_todo', 0, \
                                              fn=RebalanceHelper.wait_for_stats_no_timeout)

        return time.time()

    def wait_until_warmed_up(self):
        master = self.input.servers[0]
        bucket = self.param("bucket", "default")

        RebalanceHelper.wait_for_stats_on_all(master, bucket,
                                              'ep_warmup_thread', 'complete',
                                              fn=RebalanceHelper.wait_for_mc_stats_no_timeout)

    def clog_cluster(self):
        ClusterOperationHelper.flushctl_stop(self.input.servers)

    def unclog_cluster(self):
        ClusterOperationHelper.flushctl_start(self.input.servers)

    def view(self, views_per_client, clients=1):
        TODO()

    def stop_bg(self):
        TODO()

    def measure_db_size(self):
        bucket = self.param("bucket", "default")
        status, db_size = self.rest.get_database_disk_size(bucket)
        return db_size

    def force_expirations(self):
        TODO()

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
        self.spec('NPP-01-1k.1')
        self.load(self.parami("items", 1000000),
                  self.parami('size', 1024),
                  kind=self.param('kind', 'binary'))
        self.loop_prep()
        self.loop(num_ops        = self.parami("ops", 20000000),
                  num_items      = self.parami("items", 1000000),
                  kind           = self.param('kind', 'binary'),
                  protocol       = self.param('protocol', 'binary'),
                  clients        = self.parami('clients', 1),
                  ratio_sets     = self.paramf('ratio_sets', 0.0),
                  ratio_misses   = self.paramf('ratio_misses', 0.0),
                  ratio_hot      = self.paramf('ratio_hot', 0.2),
                  ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                  test_name      = self.id())

    def test_get_4client(self):
        self.spec('NPP-02-1k.1')
        self.load(self.parami("items", 1000000),
                  self.parami('size', 1024),
                  kind=self.param('kind', 'binary'))
        self.loop_prep()
        self.loop(num_ops        = self.parami("ops", 20000000),
                  num_items      = self.parami("items", 1000000),
                  kind           = self.param('kind', 'binary'),
                  protocol       = self.param('protocol', 'binary'),
                  clients        = self.parami('clients', 4),
                  ratio_sets     = self.paramf('ratio_sets', 0.0),
                  ratio_misses   = self.paramf('ratio_misses', 0.0),
                  ratio_hot      = self.paramf('ratio_hot', 0.2),
                  ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                  test_name      = self.id())

    def test_set_1client(self):
        self.spec('NPP-03-1k.1')
        self.load(self.parami("items", 1000000),
                  self.parami('size', 1024),
                  kind=self.param('kind', 'binary'))
        self.loop_prep()
        self.loop(num_ops        = self.parami("ops", 20000000),
                  num_items      = self.parami("items", 1000000),
                  min_value_size = self.parami('size', 1024),
                  kind           = self.param('kind', 'binary'),
                  protocol       = self.param('protocol', 'binary'),
                  clients        = self.parami('clients', 1),
                  ratio_sets     = self.paramf('ratio_sets', 1.0),
                  ratio_creates  = self.paramf('ratio_creates', 0.0),
                  ratio_hot      = self.paramf('ratio_hot', 0.2),
                  ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                  test_name      = self.id())

    def test_mixed_1client(self):
        self.spec('NPP-04-1k.1')
        self.load(self.parami("items", 1000000),
                  self.parami('size', 1024),
                  kind=self.param('kind', 'binary'))
        self.loop_prep()
        self.loop(num_ops        = self.parami("ops", 20000000),
                  num_items      = self.parami("items", 1000000),
                  min_value_size = self.parami('size', 1024),
                  kind           = self.param('kind', 'binary'),
                  protocol       = self.param('protocol', 'binary'),
                  clients        = self.parami('clients', 1),
                  ratio_sets     = self.paramf('ratio_sets', 0.2),
                  ratio_misses   = self.paramf('ratio_misses', 0.2),
                  ratio_creates  = self.paramf('ratio_creates', 0.5),
                  ratio_hot      = self.paramf('ratio_hot', 0.2),
                  ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                  ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                  test_name      = self.id())

    def test_get_30client(self):
        self.spec('NPP-05-1k')
        self.load(self.parami("items", 1000000),
                  self.parami('size', 1024),
                  kind=self.param('kind', 'binary'))
        self.loop_prep()
        seconds = self.parami('seconds', 60 * 60)
        self.loop(('seconds', seconds),
                  kind           = self.param('kind', 'binary'),
                  protocol       = self.param('protocol', 'binary'),
                  clients        = self.parami('clients', 30),
                  ratio_sets     = self.paramf('ratio_sets', 0.0),
                  ratio_misses   = self.paramf('ratio_misses', 0.0),
                  ratio_hot      = self.paramf('ratio_hot', 0.2),
                  ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                  test_name      = self.id())

    def test_get_5client_2node(self):
        self.spec('NPP-06-1k.1')
        self.nodes(2)
        self.load(self.parami("items", 1000000),
                  self.parami('size', 1024),
                  kind=self.param('kind', 'binary'))
        self.loop_prep()
        self.loop(num_ops        = self.parami("ops", 20000000),
                  num_items      = self.parami("items", 1000000),
                  kind           = self.param('kind', 'binary'),
                  protocol       = self.param('protocol', 'binary'),
                  clients        = self.parami('clients', 5),
                  ratio_sets     = self.paramf('ratio_sets', 0.0),
                  ratio_misses   = self.paramf('ratio_misses', 0.0),
                  ratio_hot      = self.paramf('ratio_hot', 0.2),
                  ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                  test_name      = self.id())

    def test_get_5client_3node(self):
        self.spec('NPP-07-1k.1')
        self.nodes(3)
        self.load(self.parami("items", 1000000),
                  self.parami('size', 1024),
                  kind=self.param('kind', 'binary'))
        self.loop_prep()
        self.loop(num_ops        = self.parami("ops", 20000000),
                  num_items      = self.parami("items", 1000000),
                  kind           = self.param('kind', 'binary'),
                  protocol       = self.param('protocol', 'binary'),
                  clients        = self.parami('clients', 5),
                  ratio_sets     = self.paramf('ratio_sets', 0.0),
                  ratio_misses   = self.paramf('ratio_misses', 0.0),
                  ratio_hot      = self.paramf('ratio_hot', 0.2),
                  ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                  test_name      = self.id())

    def test_get_5client_5node(self):
        self.spec('NPP-08-1k.1')
        self.nodes(5)
        self.load(self.parami("items", 1000000),
                  self.parami('size', 1024),
                  kind=self.param('kind', 'binary'))
        self.loop_prep()
        self.loop(num_ops        = self.parami("ops", 20000000),
                  num_items      = self.parami("items", 1000000),
                  kind           = self.param('kind', 'binary'),
                  protocol       = self.param('protocol', 'binary'),
                  clients        = self.parami('clients', 5),
                  ratio_sets     = self.paramf('ratio_sets', 0.0),
                  ratio_misses   = self.paramf('ratio_misses', 0.0),
                  ratio_hot      = self.paramf('ratio_hot', 0.2),
                  ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                  test_name      = self.id())

    def test_get_1client_rebalance(self):
        self.spec('NPP-09-5k.1')
        self.nodes(2)
        self.load(self.parami("items", 1000000),
                  self.parami('size', 5000),
                  kind=self.param('kind', 'binary'))
        self.loop_prep()
        self.delayed_rebalance(4, delay_seconds=10)
        self.loop(num_ops        = self.parami("ops", 20000000),
                  num_items      = self.parami("items", 1000000),
                  kind           = self.param('kind', 'binary'),
                  protocol       = self.param('protocol', 'binary'),
                  clients        = self.parami('clients', 1),
                  ratio_sets     = self.paramf('ratio_sets', 0.0),
                  ratio_misses   = self.paramf('ratio_misses', 0.0),
                  ratio_hot      = self.paramf('ratio_hot', 0.2),
                  ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                  test_name      = self.id())

    def test_mixed_1client_rebalance_json(self):
        self.spec('NPP-10-1k.1')
        self.nodes(2)
        self.load(self.parami("items", 1000000),
                  self.parami('size', 1024),
                  kind=self.param('kind', 'json'))
        self.loop_prep()
        self.delayed_rebalance(4, delay_seconds=10)
        self.loop(num_ops        = self.parami("ops", 20000000),
                  num_items      = self.parami("items", 1000000),
                  min_value_size = self.parami('size', 1024),
                  kind           = self.param('kind', 'json'),
                  protocol       = self.param('protocol', 'binary'),
                  clients        = self.parami('clients', 1),
                  ratio_sets     = self.paramf('ratio_sets', 0.2),
                  ratio_misses   = self.paramf('ratio_misses', 0.2),
                  ratio_creates  = self.paramf('ratio_creates', 0.5),
                  ratio_hot      = self.paramf('ratio_hot', 0.2),
                  ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                  ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                  test_name      = self.id())

    def test_set_1client_json(self):
        self.spec('NPP-12-1k.1')
        self.load(self.parami("items", 1000000),
                  self.parami('size', 1024),
                  kind=self.param('kind', 'json'))
        self.loop_prep()
        self.loop(num_ops        = self.parami("ops", 20000000),
                  num_items      = self.parami("items", 1000000),
                  min_value_size = self.parami('size', 1024),
                  kind           = self.param('kind', 'json'),
                  protocol       = self.param('protocol', 'binary'),
                  clients        = self.parami('clients', 1),
                  ratio_sets     = self.paramf('ratio_sets', 1.0),
                  ratio_creates  = self.paramf('ratio_creates', 0.0),
                  ratio_hot      = self.paramf('ratio_hot', 0.2),
                  ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                  ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                  test_name      = self.id())


class DiskDrainRate(PerfBase):

    def test_1M_2k(self):
        self.spec('DRR-01')
        sc = self.start_stats(self.spec_reference, test_params={'test_name':self.id(),
                                                                'test_time':time.time()})
        ops, start_time, end_time = self.load(self.parami("items", 1000000),
                                              self.parami('size', 2048),
                                              kind=self.param('kind', 'binary'))
        ops['end-time'] = self.wait_until_drained()
        self.end_stats(sc, ops)

    def test_1M_1k(self):
        self.spec('DRR-02.1')
        sc = self.start_stats(self.spec_reference, test_params={'test_name':self.id(),
                                                                'test_time':time.time()})
        ops, start_time, end_time = self.load(self.parami("items", 1000000),
                                              self.parami('size', 1024),
                                              kind=self.param('kind', 'binary'),
                                              ratio_sets=self.paramf('ratio-sets', 0.9))
        ops['end-time'] = self.wait_until_drained()
        self.end_stats(sc, ops)

    def test_1M_rebalance(self):
        self.spec('DRR-03')
        self.nodes(2)
        self.delayed_rebalance(4)
        sc = self.start_stats(self.spec_reference, test_params={'test_name':self.id(),
                                                                'test_time':time.time()})
        ops, start_time, end_time = self.load(self.parami("items", 1000000),
                                              self.parami('size', 1024),
                                              kind=self.param('kind', 'binary'))
        ops['end-time'] = self.wait_until_drained()
        self.end_stats(sc, ops)

    def test_1M_compaction(self):
        self.spec('DRR-04')
        self.delayed_compaction()
        sc = self.start_stats(self.spec_reference, test_params={'test_name':self.id(),
                                                                'test_time':time.time()})
        ops, start_time, end_time = self.load(self.parami("items", 1000000),
                                              self.parami('size', 1024),
                                              kind=self.param('kind', 'binary'))
        ops['end-time'] = self.wait_until_drained()
        self.end_stats(sc, ops)

    def test_1M_clog(self):
        self.spec('DRR-06')
        self.clog_cluster()
        ops, load_start_time, load_end_time = self.load(self.parami("items", 1000000),
                                                        self.parami('size', 1024),
                                                        kind=self.param('kind', 'binary'))
        sc = self.start_stats(self.spec_reference, test_params={'test_name':self.id(),
                                                                'test_time':time.time()})
        start_time_unclog = time.time()
        self.unclog_cluster()
        end_time_unclog = self.wait_until_drained()

        ops['start-time'] = start_time_unclog
        ops['end-time'] = end_time_unclog
        self.end_stats(sc, ops)


class MapReduce(PerfBase):

    def create_view(self, rest, bucket, view, map, reduce=''):
        dict = {"language": "javascript", "_id": "_design/{0}".format(view)}
        try:
            view_response = rest.get_view(bucket, view)
            if view_response:
                dict["_rev"] = view_response["_rev"]
        except:
            pass
        if reduce:
            dict["views"] = {view: {"map": map, "reduce": reduce}}
        else:
            dict["views"] = {view: {"map": map}}

        rest.create_view(bucket, view, json.dumps(dict))

    def test_VP_001(self):
        self.spec('VP-001')

        items  = self.parami("items", 1000000)
        limit  = self.parami('limit', 1)
        bucket = "default"
        view   = "myview"

        self.load(items,
                  self.parami('size', 1024),
                  kind=self.param('kind', 'json'))
        self.loop_prep()

        rest = RestConnection(self.input.servers[0])
        self.create_view(rest, bucket, view,
                         "function(doc) { emit(doc.email, doc); }")

        ops = {}

        self.log.info("building view: %s" % (view,))
        ops["view-build-start-time"] = time.time()
        rest.view_results(bucket, view, { "startkey":"a" }, limit)
        ops["view-build-end-time"] = time.time()

        sc = self.start_stats(self.spec_reference,
                              test_params={'test_name':self.id(),
                                           'test_time':time.time()})

        self.log.info("accessing view: %s" % (view,))
        ops["start-time"] = time.time()
        for i in range(self.parami("ops", 10000)):
            k = i % items
            e = mcsoda.key_to_email(k, mcsoda.prepare_key(k))
            start = time.time()
            rest.view_results(bucket, view, { "startkey":e, "endkey":e }, limit)
            end = time.time()
            self.sc.latency_stats({ 'tot-views': 1,
                                    'start-time': start,
                                    'end-time': end })
        ops["end-time"] = time.time()
        ops["tot-views"] = i
        self.end_stats(sc, ops)


class TransactionSize(PerfBase):

    def setUp(self):
        self.dgm = False
        super(TransactionSize, self).setUp()

    def go(self, settings):
        for key, val in settings:
            ClusterOperationHelper.flushctl_set(self.input.servers, key, val)

        for key, val in settings:
            key = 'ep_' + key
            ClusterOperationHelper.get_mb_stats(self.input.servers, key)
        # Using the same conditions as NPP-03-1k.1 here...
        self.load(self.parami("items", 1000000),
                  self.parami('size', 1024),
                  kind=self.param('kind', 'binary'))
        self.loop_prep()
        self.loop(num_ops        = self.parami("ops", 20000000),
                  num_items      = self.parami("items", 1000000),
                  min_value_size = self.parami('size', 1024),
                  kind           = self.param('kind', 'binary'),
                  protocol       = self.param('protocol', 'binary'),
                  clients        = self.parami('clients', 1),
                  ratio_sets     = self.paramf('ratio_sets', 1.0),
                  ratio_creates  = self.paramf('ratio_creates', 0.0),
                  ratio_hot      = self.paramf('ratio_hot', 0.2),
                  ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                  test_name      = self.id())

        for key, val in settings:
            key = 'ep_' + key
            ClusterOperationHelper.get_mb_stats(self.input.servers, key)

    def test_TS1_0(self):
        self.spec('TS1.0')
        self.go([('max_txn_size', '1000'),
                 ('couch_vbucket_batch_count', '4')])

    def test_TS1_1(self):
        self.spec('TS1.1')
        self.go([('max_txn_size', '1000'),
                 ('couch_vbucket_batch_count', '8')])

    def test_TS1_2(self):
        self.spec('TS1.2')
        self.go([('max_txn_size', '2000'),
                 ('couch_vbucket_batch_count', '4')])

    def test_TS1_3(self):
        self.spec('TS1.3')
        self.go([('max_txn_size', '2000'),
                 ('couch_vbucket_batch_count', '8')])

    def test_TS1_4(self):
        self.spec('TS1.4')
        self.go([('max_txn_size', '4000'),
                 ('couch_vbucket_batch_count', '4')])

    def test_TS1_5(self):
        self.spec('TS1.5')
        self.go([('max_txn_size', '4000'),
                 ('couch_vbucket_batch_count', '8')])

    def test_TS1_6(self):
        self.spec('TS1.6')
        self.go([('max_txn_size', '8000'),
                 ('couch_vbucket_batch_count', '4')])

    def test_TS1_7(self):
        self.spec('TS1.6')
        self.go([('max_txn_size', '8000'),
                 ('couch_vbucket_batch_count', '8')])

    def test_TS1_8(self):
        self.spec('TS1.6')
        self.go([('max_txn_size', '10000'),
                 ('couch_vbucket_batch_count', '4')])

    def test_TS1_9(self):
        self.spec('TS1.6')
        self.go([('max_txn_size', '10000'),
                 ('couch_vbucket_batch_count', '8')])



class TODO_PerfBase():
    TODO()


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
        ClusterOperationHelper.stop_cluster(self.input.servers)
        ClusterOperationHelper.start_cluster(self.input.servers)
        self.wait_until_warmed_up()

    def test_All_ops(self):
        expiration = 20 # In seconds.
        self.spec('DBF-002')
        self.load(10000000, expiration=[0.1, expiration])
        self.wait_until_drained()
        ClusterOperationHelper.stop_cluster(self.input.servers)
        self.log.info("sleepng {0} seconds to ensure expirations".format(expiration + 2))
        time.sleep(expiration + 2)
        ClusterOperationHelper.start_cluster(self.input.servers)
        self.wait_until_warmed_up()

    def test_2nodes(self):
        self.spec('DBF-003')
        self.nodes(2)
        self.load(10000000)
        self.wait_until_drained()
        ClusterOperationHelper.stop_cluster(self.input.servers)
        ClusterOperationHelper.start_cluster(self.input.servers)
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
        self.loop_prep()
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
        self.loop_prep()
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

        self.loop(m1, ratio_sets=1.0, ratio_creates=0.0, expiration=[1.0, 10])
        self.measure_db_size()

        self.force_expirations()
        self.measure_db_size()

class Experimental(PerfBase):

    def test_experimental(self):
        self.spec('Experimental')
        self.load(self.parami("items", 1000000),
                  self.parami('size', 1024),
                  kind=self.param('kind', 'binary'))
        self.loop_prep()
        seconds = self.parami('seconds', 60 * 20)
        self.loop(('seconds', seconds),
                  num_items      = self.parami("items", 1000000),
                  min_value_size = self.parami('size', 1024),
                  kind           = self.param('kind', 'binary'),
                  protocol       = self.param('protocol', 'binary'),
                  clients        = self.parami('clients', 1),
                  ratio_sets     = self.paramf('ratio_sets', 0.2),
                  ratio_misses   = self.paramf('ratio_misses', 0.2),
                  ratio_creates  = self.paramf('ratio_creates', 0.5),
                  ratio_hot      = self.paramf('ratio_hot', 0.2),
                  ratio_hot_sets = self.paramf('ratio_hot_sets', 0.95),
                  ratio_hot_gets = self.paramf('ratio_hot_gets', 0.95),
                  test_name      = self.id())
