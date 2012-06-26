import unittest
import logger
import time
import threading
import os
import testconstants
import subprocess

# membase imports
from couchbase.document import View
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from membase.performance.stats import StatsCollector
from remote.remote_util import RemoteMachineShellConnection
from cbsoda import StoreCouchbase

# testrunner imports
from TestInput import TestInputSingleton
from perf_defaults import PerfDefaults
import mcsoda
import testconstants

def TODO():
    pass

class PerfBase(unittest.TestCase):
    specURL = "http://hub.internal.couchbase.org/confluence/display/cbit/Black+Box+Performance+Test+Matrix"

    # The setUpBaseX() methods allow subclasses to resequence the
    # setUp() and skip cluster configuration.
    #
    def setUpBase0(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.vbucket_count = PerfDefaults.vbuckets
        self.sc = None
        if self.parami("tear_down_on_setup", PerfDefaults.tear_down_on_setup) == 1:
            self.tearDown() # Tear down in case previous run had unclean death.
        master = self.input.servers[0]
        self.set_up_rest(master)

    def setUpBase1(self):
        if self.parami('num_buckets', 1) > 1:
            bucket = 'bucket-0'
        else:
            bucket = self.param('bucket', 'default')
        vBuckets = self.rest.get_vbuckets(bucket)
        self.vbucket_count = len(vBuckets)

    def setUp(self):
        self.setUpBase0()

        master = self.input.servers[0]

        self.is_multi_node = False
        self.data_path = master.data_path

        # Number of items loaded by load() method.
        # Does not include or count any items that came from set_up_dgm().
        #
        self.num_items_loaded = 0

        if self.input.clusters:
            for cluster in self.input.clusters.values():
                master = cluster[0]
                self.set_up_rest(master)
                self.set_up_cluster(master)
        else:
            master = self.input.servers[0]
            self.set_up_cluster(master)

        # Rebalance
        num_nodes = self.parami("num_nodes", 10)
        self.rebalance_nodes(num_nodes)

        if self.input.clusters:
            for cluster in self.input.clusters.values():
                master = cluster[0]
                self.set_up_rest(master)
                self.set_up_buckets()
        else:
            self.set_up_buckets()

        self.set_up_proxy()

        self.reconfigure()

        if self.parami("dgm", getattr(self, "dgm", 1)):
            self.set_up_dgm()

        time.sleep(10)
        self.setUpBase1()

        if self.input.clusters:
            for cluster in self.input.clusters.values():
                self.wait_until_warmed_up(cluster[0])
        else:
            self.wait_until_warmed_up()
        ClusterOperationHelper.flush_os_caches(self.input.servers)

    def set_up_rest(self, master):
        self.rest = RestConnection(master)
        self.rest_helper = RestHelper(self.rest)

    def set_up_cluster(self, master):
        """Initialize cluster"""

        print "[perf.setUp] Setting up cluster"

        self.rest.init_cluster(master.rest_username, master.rest_password)

        memory_quota = self.parami('mem_quota', PerfDefaults.mem_quota)
        self.rest.init_cluster_memoryQuota(master.rest_username,
                                           master.rest_password,
                                           memoryQuota=memory_quota)

    def set_up_buckets(self):
        """Set up data bucket(s)"""

        print "[perf.setUp] Setting up buckets"

        num_buckets = self.parami('num_buckets', 1)
        if num_buckets > 1:
            self.buckets = ['bucket-{0}'.format(i) for i in range(num_buckets)]
        else:
            self.buckets = [self.param('bucket', 'default')]

        for bucket in self.buckets:
            bucket_ram_quota = self.parami('mem_quota', PerfDefaults.mem_quota)
            bucket_ram_quota = bucket_ram_quota / num_buckets
            replicas = self.parami('replicas', getattr(self, 'replicas', 1))

            self.rest.create_bucket(bucket=bucket, ramQuotaMB=bucket_ram_quota,
                                    replicaNumber=replicas, authType='sasl')

            status = self.rest_helper.vbucket_map_ready(bucket, 60)
            self.assertTrue(status, msg='vbucket_map not ready .. timed out')
            status = self.rest_helper.bucket_exists(bucket)
            self.assertTrue(status,
                            msg='unable to create {0} bucket'.format(bucket))

    def reconfigure(self):
        """Customize basic Couchbase setup"""

        # Set custom loglevel
        loglevel = self.param('loglevel', None)
        if loglevel:
            if self.input.clusters:
                for cluster in self.input.clusters.values():
                    master = cluster[0]
                    self.set_up_rest(master)
                    self.rest.set_global_loglevel(loglevel)
            else:
                self.rest.set_global_loglevel(loglevel)

        # Set custom MAX_CONCURRENT_REPS_PER_DOC
        max_concurrent_reps_per_doc = self.param('max_concurrent_reps_per_doc',
                                                 None)
        if max_concurrent_reps_per_doc:
            for server in self.input.servers:
                rc = RemoteMachineShellConnection(server)
                rc.set_environment_variable('MAX_CONCURRENT_REPS_PER_DOC',
                                            max_concurrent_reps_per_doc)

        # Set custom autocompaction settings
        try:
            # Parallel database and view compaction
            parallel_compaction = self.param("parallel_compaction",
                                              PerfDefaults.parallel_compaction)
            # Database fragmentation threshold
            db_compaction = self.parami("db_compaction",
                                        PerfDefaults.db_compaction)
            # View fragmentation threshold
            view_compaction = self.parami("view_compaction",
                                          PerfDefaults.view_compaction)
            # Set custom auto-compaction settings
            self.rest.set_auto_compaction(parallelDBAndVC=parallel_compaction,
                                          dbFragmentThresholdPercentage=db_compaction,
                                          viewFragmntThresholdPercentage=view_compaction)
        except Exception as e:
            # It's very hard to determine what exception it can raise.
            # Therefore we have to use general handler.
            print "ERROR while changing compaction settings: {0}".format(e)

    def tearDown(self):
        if self.parami("tear_down", 0) == 1:
            print "[perf.tearDown] tearDown routine skipped"
            return

        print "[perf.tearDown] tearDown routine starts"

        if self.parami("tear_down_proxy", 1) == 1:
            self.tear_down_proxy()
        else:
            print "[perf.tearDown] Proxy tearDown skipped"

        if self.sc is not None:
            self.sc.stop()
            self.sc = None

        if self.parami("tear_down_bucket", 0) == 1:
            self.tear_down_buckets()
        else:
            print "[perf.tearDown] Bucket tearDown skipped"

        if self.parami("tear_down_cluster", 1) == 1:
            self.tear_down_cluster()
        else:
            print "[perf.tearDown] Cluster tearDown skipped"

        print "[perf.tearDown] tearDown routine finished"

    def tear_down_buckets(self):
        print "[perf.tearDown] Tearing down bucket"
        BucketOperationHelper.delete_all_buckets_or_assert(self.input.servers, self)
        print "[perf.tearDown] Bucket teared down"

    def tear_down_cluster(self):
        print "[perf.tearDown] Tearing down cluster"
        ClusterOperationHelper.cleanup_cluster(self.input.servers)
        ClusterOperationHelper.wait_for_ns_servers_or_assert(self.input.servers, self)
        print "[perf.tearDown] Cluster teared down"

    def set_up_proxy(self, bucket=None):
        """Set up and start Moxi"""

        if self.input.moxis:
            print '[perf.setUp] Setting up proxy'

            bucket = bucket or self.param('bucket', 'default')

            shell = RemoteMachineShellConnection(self.input.moxis[0])
            shell.start_moxi(self.input.servers[0].ip, bucket,
                             self.input.moxis[0].port)
            shell.disconnect()

    def tear_down_proxy(self):
        if len(self.input.moxis) > 0:
            shell = RemoteMachineShellConnection(self.input.moxis[0])
            shell.stop_moxi()
            shell.disconnect()

    # Returns "host:port" of moxi to hit.
    def target_host_port(self, bucket='default', use_direct=False):
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

    def protocol_parse(self, protocol_in, use_direct=False):
        if protocol_in.find('://') >= 0:
            if protocol_in.find("couchbase:") >= 0:
                protocol = "couchbase"
            else:
                protocol = \
                    '-'.join(((["membase"] + \
                                   protocol_in.split("://"))[-2] + "-binary").split('-')[0:2])
            host_port = ('@' + protocol_in.split("://")[-1]).split('@')[-1]
            user, pswd = (('@' + protocol_in.split("://")[-1]).split('@')[-2] + ":").split(':')[0:2]
        else:
            protocol = 'memcached-' + protocol_in
            host_port = self.target_host_port(use_direct=use_direct)
            user = self.param("rest_username", "Administrator")
            pswd = self.param("rest_password", "password")
        return protocol, host_port, user, pswd

    def mk_protocol(self, host, port='8091', prefix='membase-binary'):
        return self.param('protocol',
                          prefix + '://' + host + ':' + port)

    def restartProxy(self, bucket=None):
        self.tear_down_proxy()
        self.set_up_proxy(bucket)

    def set_up_dgm(self):
        """Download fragmented, DGM dataset onto each cluster node, if not
        already locally available.

        The number of vbuckets and database schema must match the
        target cluster.

        Shutdown all cluster nodes.

        Do a cluster-restore.

        Restart all cluster nodes."""

        bucket = self.param("bucket", "default")
        ClusterOperationHelper.stop_cluster(self.input.servers)
        for server in self.input.servers:
            remote = RemoteMachineShellConnection(server)
            #TODO: Better way to pass num_nodes and db_size?
            self.get_data_files(remote, bucket, 1, 10)
            remote.disconnect()
        ClusterOperationHelper.start_cluster(self.input.servers)

    def get_data_files(self, remote, bucket, num_nodes, db_size):
        base = 'https://s3.amazonaws.com/database-analysis'
        dir = '/tmp/'
        if remote.is_membase_installed():
            dir = dir + '/membase/{0}-{1}-{2}/'.format(num_nodes, 1024, db_size)
            output, error = remote.execute_command('mkdir -p {0}'.format(dir))
            remote.log_command_output(output, error)
            file = '{0}_mb.tar.gz'.format(bucket)
            base_url = base + '/membase/{0}-{1}-{2}/{3}'.format(num_nodes, \
                                                                    1024, db_size, file)
        else:
            dir = dir + '/couchbase/{0}-{1}-{2}/'.format(num_nodes, 256, db_size)
            output, error = remote.execute_command('mkdir -p {0}'.format(dir))
            remote.log_command_output(output, error)
            file = '{0}_cb.tar.gz'.format(bucket)
            base_url = base + '/couchbase/{0}-{1}-{2}/{3}'.format(num_nodes, \
                                                               256, db_size, file)

        info = remote.extract_remote_info()
        wget_command = 'wget'
        if info.type.lower() == 'windows':
            wget_command = \
                "cd {0} ;cmd /c 'c:\\automation\\wget.exe --no-check-certificate".format(dir)

        # Check if the file exists on the remote server else download the gzipped version
        # Extract if necessary
        exist = remote.file_exists(dir, file)
        if not exist:
            additional_quote = ""
            if info.type.lower() == 'windows':
                additional_quote = "'"
            command = "{0} -v -O {1}{2} {3} {4} ".format(wget_command, dir, file,
                                                         base_url, additional_quote)
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
        untar_command = 'cd {1}; tar -xzf {0}'.format(dir+file, destination_folder)
        output, error = remote.execute_command(untar_command)
        remote.log_command_output(output, error)

    def _exec_and_log(self, shell, cmd):
        """helper method to execute a command and log output"""
        if not cmd or not shell:
            return

        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)

    def _build_tar_name(self, bucket, version="unknown_version", file_base=None):
        """
        build tar file name.
        {file_base}-{version}-{bucket}.tar.gz
        """
        if not file_base:
            file_base = os.path.splitext(
                os.path.basename(self.param("conf_file", PerfDefaults.conf_file)))[0]
        return "{0}-{1}-{2}.tar.gz".format(file_base, version, bucket)

    def _save_snapshot(self, server, bucket, file_base=None):
        """Save data files to a snapshot"""

        src_data_path = os.path.dirname(server.data_path or testconstants.COUCHBASE_DATA_PATH)
        dest_data_path = "{0}-snapshots".format(src_data_path)

        print "[perf: _save_snapshot] server = {0} , src_data_path = {1}, dest_data_path = {2}"\
                .format(server.ip, src_data_path, dest_data_path)

        shell = RemoteMachineShellConnection(server)

        build_name, short_version, full_version = \
            shell.find_build_version("/opt/couchbase/", "VERSION.txt", "cb")

        dest_file = self._build_tar_name(bucket, full_version, file_base)

        self._exec_and_log(shell, "mkdir -p {0}".format(dest_data_path))

        # save as gzip file, if file exsits, overwrite
        # TODO: multiple buckets
        zip_cmd = "cd {0}; tar -cvzf {1}/{2} {3} {3}-data _*"\
                    .format(src_data_path, dest_data_path, dest_file, bucket)
        self._exec_and_log(shell, zip_cmd)

        shell.disconnect()
        return True

    def _load_snapshot(self, server, bucket, file_base=None, overwrite=True):
        """Load data files from a snapshot"""

        dest_data_path = os.path.dirname(server.data_path or testconstants.COUCHBASE_DATA_PATH)
        src_data_path = "{0}-snapshots".format(dest_data_path)

        print "[perf: _load_snapshot] server = {0} , src_data_path = {1}, dest_data_path = {2}"\
                .format(server.ip, src_data_path, dest_data_path)

        shell = RemoteMachineShellConnection(server)

        build_name, short_version, full_version = \
            shell.find_build_version("/opt/couchbase/", "VERSION.txt", "cb")

        src_file = self._build_tar_name(bucket, full_version, file_base)

        if not shell.file_exists(src_data_path, src_file):
            print "[perf: _load_snapshot] file '{0}/{1}' does not exist"\
                .format(src_data_path, src_file)
            shell.disconnect()
            return False

        if not overwrite:
            self._save_snapshot(server,
                               bucket,
                               "{0}.tar.gz".format(time.strftime('%X-%x-%Z'))) # TODO: filename

        rm_cmd = "rm -rf {0}/{1} {0}/{1}-data {0}/_*".format(dest_data_path, bucket)
        self._exec_and_log(shell, rm_cmd)

        unzip_cmd = "cd {0}; tar -xvzf {1}/{2}".format(dest_data_path, src_data_path, src_file)
        self._exec_and_log(shell, unzip_cmd)

        shell.disconnect()
        return True

    def save_snapshots(self, file_base, bucket):
        """Save snapshots on all servers"""
        if not self.input.servers or not bucket:
            print "[perf: save_snapshot] invalid server list or bucket name"
            return False

        ClusterOperationHelper.stop_cluster(self.input.servers)

        for server in self.input.servers:
            self._save_snapshot(server, bucket, file_base)

        ClusterOperationHelper.start_cluster(self.input.servers)

        return True

    def load_snapshots(self, file_base, bucket):
        """Load snapshots on all servers"""
        if not self.input.servers or not bucket:
            print "[perf: load_snapshot] invalid server list or bucket name"
            return False

        ClusterOperationHelper.stop_cluster(self.input.servers)

        for server in self.input.servers:
            if not self._load_snapshot(server, bucket, file_base):
                ClusterOperationHelper.start_cluster(self.input.servers)
                return False

        ClusterOperationHelper.start_cluster(self.input.servers)

        return True

    def spec(self, reference):
        self.spec_reference = self.param("spec", reference)
        self.log.info("spec: " + reference)

    def mk_stats(self, verbosity):
        return StatsCollector(verbosity)

    def _get_src_version(self):
        """get testrunner version"""
        try:
            result = subprocess.Popen(['git', 'rev-parse', 'HEAD'], stdout=subprocess.PIPE).communicate()[0]
        except subprocess.CalledProcessError as e:
            print "[perf] unable to get src code version : {0}".format(str(e))
            return "unknown version"
        return result.rstrip()[:7]

    def start_stats(self, stats_spec, servers=None,
                    process_names=['memcached', 'beam.smp', 'couchjs'],
                    test_params = None, client_id = '',
                    collect_server_stats = True):
        if self.parami('stats', 1) == 0:
            return None

        servers = servers or self.input.servers
        sc = self.mk_stats(False)
        bucket = self.param("bucket", "default")
        sc.start(servers, bucket, process_names, stats_spec, 10, client_id,
                 collect_server_stats = collect_server_stats)
        test_params['testrunner'] = self._get_src_version()
        self.test_params = test_params
        self.sc = sc
        return self.sc

    def end_stats(self, sc, total_stats=None, stats_spec=None):
        if sc is None:
            return
        if stats_spec is None:
            stats_spec = self.spec_reference
        if total_stats:
            sc.total_stats(total_stats)
        self.log.info("stopping stats collector")
        sc.stop()
        self.log.info("stats collector is stopped")
        sc.export(stats_spec, self.test_params)

    def load(self, num_items, min_value_size=None,
             kind='binary',
             protocol='binary',
             ratio_sets=1.0,
             ratio_hot_sets=0.0,
             ratio_hot_gets=0.0,
             ratio_expirations=0.0,
             expiration=None,
             prefix="",
             doc_cache=1,
             use_direct=True,
             report=0,
             start_at=-1,
             collect_server_stats=True,
             is_eperf=False,
             hot_shift=0):
        cfg = { 'max-items': num_items,
                'max-creates': num_items,
                'max-ops-per-sec': self.parami("load_mcsoda_max_ops_sec", PerfDefaults.mcsoda_max_ops_sec),
                'min-value-size': min_value_size or self.parami("min_value_size", 1024),
                'ratio-sets': self.paramf("load_ratio_sets", ratio_sets),
                'ratio-misses': self.paramf("load_ratio_misses", 0.0),
                'ratio-creates': self.paramf("load_ratio_creates", 1.0),
                'ratio-deletes': self.paramf("load_ratio_deletes", 0.0),
                'ratio-hot': 0.0,
                'ratio-hot-sets': ratio_hot_sets,
                'ratio-hot-gets': ratio_hot_gets,
                'ratio-expirations': ratio_expirations,
                'expiration': expiration or 0,
                'exit-after-creates': 1,
                'json': int(kind == 'json'),
                'batch': self.parami("batch", PerfDefaults.batch),
                'vbuckets': self.vbucket_count,
                'doc-cache': doc_cache,
                'prefix': prefix,
                'report': report,
                'hot-shift': hot_shift,
                'cluster_name': self.param("cluster_name", "")
                }
        cur = {}
        if start_at >= 0:
            cur['cur-items'] = start_at
            cur['cur-gets'] = start_at
            cur['cur-sets'] = start_at
            cur['cur-creates'] = start_at
            cfg['max-creates'] = start_at + num_items
            cfg['max-items'] = cfg['max-creates']

        cfg_params = cfg.copy()
        cfg_params['test_time'] = time.time()
        cfg_params['test_name'] = self.id()

        # phase: 'load' or 'reload'
        phase = "load"
        if self.parami("hot_load_phase", 0) == 1:
            phase = "reload"

        if is_eperf:
            collect_server_stats = self.parami("prefix", 0) == 0
            client_id = self.parami("prefix", 0)
            sc = self.start_stats("{0}.{1}".format(self.spec_reference, phase), # stats spec e.x: testname.load
                              test_params = cfg_params, client_id = client_id,
                              collect_server_stats = collect_server_stats)

        # For Black box, multi node tests
        # always use membase-binary
        if self.is_multi_node:
            protocol = self.mk_protocol(host=self.input.servers[0].ip,
                                        port=self.input.servers[0].port)

        protocol, host_port, user, pswd = self.protocol_parse(protocol, use_direct=use_direct)

        if not user.strip():
            user = self.input.servers[0].rest_username
        if not pswd.strip():
            pswd = self.input.servers[0].rest_password

        self.log.info("mcsoda - %s %s %s %s" % (protocol, host_port, user, pswd))
        self.log.info("mcsoda - cfg: " + str(cfg))
        self.log.info("mcsoda - cur: " + str(cur))

        cur, start_time, end_time = self.mcsoda_run(cfg, cur, protocol, host_port, user, pswd,
                                                    heartbeat=self.parami("mcsoda_heartbeat", 0), why="load",
                                                    bucket=self.param("bucket", "default"))
        self.num_items_loaded = num_items
        ops = { 'tot-sets': cur.get('cur-sets', 0),
                'tot-gets': cur.get('cur-gets', 0),
                'tot-items': cur.get('cur-items', 0),
                'tot-creates': cur.get('cur-creates', 0),
                'tot-misses': cur.get('cur-misses', 0),
                "start-time": start_time,
                "end-time": end_time }

        if is_eperf:
            if self.parami("load_wait_until_drained", 1) == 1:
                self.wait_until_drained()
            self.end_stats(sc, ops, "{0}.{1}".format(self.spec_reference, phase))

        return ops, start_time, end_time

    def mcsoda_run(self, cfg, cur, protocol, host_port, user, pswd,
                   stats_collector = None, stores = None, ctl = None,
                   heartbeat = 0, why = "", bucket = "default"):
        return mcsoda.run(cfg, cur, protocol, host_port, user, pswd,
                          stats_collector=stats_collector,
                          stores=stores,
                          ctl=ctl,
                          heartbeat=heartbeat,
                          why=why,
                          bucket=bucket)

    def rebalance_nodes(self, num_nodes):
        """Rebalance cluster(s) if more than 1 node provided"""

        if len(self.input.servers) == 1 or num_nodes == 1:
            print "WARNING: running on single node cluster"
            return
        else:
            print "[perf.setUp] rebalancing nodes: num_nodes = {0}".format(num_nodes)
            self.is_multi_node = True

        if self.input.clusters:
            for cluster in self.input.clusters.values():
                status, _ = RebalanceHelper.rebalance_in(cluster,
                                                         num_nodes - 1,
                                                         do_shuffle=False)
                self.assertTrue(status)
        else:
            status, _ = RebalanceHelper.rebalance_in(self.input.servers,
                                                     num_nodes - 1,
                                                     do_shuffle=False)
            self.assertTrue(status)

    @staticmethod
    def delayed_rebalance_worker(servers, num_nodes, delay_seconds, sc):
        time.sleep(delay_seconds)
        if not sc:
            print "[delayed_rebalance_worker] invalid stats collector"
            return
        start_time = time.time()
        RebalanceHelper.rebalance_in(servers, num_nodes - 1)
        end_time = time.time()
        sc.reb_stats(end_time - start_time)

    def delayed_rebalance(self, num_nodes, delay_seconds=10):
        t = threading.Thread(target=PerfBase.delayed_rebalance_worker,
                             args=(self.input.servers, num_nodes, delay_seconds, self.sc))
        t.daemon = True
        t.start()

    @staticmethod
    def set_auto_compaction(server, parallel_compaction, percent_threshold):
        rest = RestConnection(server)
        rest.set_auto_compaction(parallel_compaction, dbFragmentThresholdPercentage=percent_threshold, viewFragmntThresholdPercentage=percent_threshold)

    @staticmethod
    def delayed_compaction_worker(servers, parallel_compaction,
                                  percent_threshold, delay_seconds):
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
        self.restartProxy()

    def loop(self, num_ops=None,
             num_items=None,
             max_items=None,
             max_creates=None,
             min_value_size=None,
             exit_after_creates=0,
             kind='binary',
             protocol='binary',
             clients=1,
             ratio_misses=0.0, ratio_sets=0.0, ratio_creates=0.0, ratio_deletes=0.0,
             ratio_hot=0.2, ratio_hot_sets=0.95, ratio_hot_gets=0.95,
             ratio_expirations=0.0,
             expiration=None,
             test_name=None,
             prefix="",
             doc_cache=1,
             use_direct=True,
             collect_server_stats=True,
             start_at=-1,
             report=0,
             ctl=None,
             hot_shift=0,
             is_eperf=False,
             ratio_queries = 0,
             queries = 0):
        num_items = num_items or self.num_items_loaded

        cfg = { 'max-items': max_items or num_items,
                'max-creates': max_creates or 0,
                'max-ops-per-sec': self.parami("mcsoda_max_ops_sec", PerfDefaults.mcsoda_max_ops_sec),
                'min-value-size': min_value_size or self.parami("min_value_size", 1024),
                'exit-after-creates': exit_after_creates,
                'ratio-sets': ratio_sets,
                'ratio-misses': ratio_misses,
                'ratio-creates': ratio_creates,
                'ratio-deletes': ratio_deletes,
                'ratio-hot': ratio_hot,
                'ratio-hot-sets': ratio_hot_sets,
                'ratio-hot-gets': ratio_hot_gets,
                'ratio-expirations': ratio_expirations,
                'ratio-queries' : ratio_queries,
                'expiration': expiration or 0,
                'threads': clients,
                'json': int(kind == 'json'),
                'batch': self.parami("batch", PerfDefaults.batch),
                'vbuckets': self.vbucket_count,
                'doc-cache': doc_cache,
                'prefix': prefix,
                'queries': queries,
                'report': report,
                'hot-shift': hot_shift,
                'cluster_name': self.param("cluster_name", "")
                }
        cfg_params = cfg.copy()
        cfg_params['test_time'] = time.time()
        cfg_params['test_name'] = test_name
        client_id = ''
        stores = None

        if is_eperf:
            client_id = self.parami("prefix", 0)
        sc = None
        if self.parami("collect_stats", 1):
            sc = self.start_stats(self.spec_reference + ".loop",
                                  test_params = cfg_params, client_id = client_id,
                                  collect_server_stats = collect_server_stats)

        cur = { 'cur-items': num_items }
        if start_at >= 0:
            cur['cur-gets'] = start_at
        if num_ops is None:
            num_ops = num_items
        if type(num_ops) == type(0):
            cfg['max-ops'] = num_ops
        else:
            # Here, we num_ops looks like "time to run" tuple of...
            # ('seconds', integer_num_of_seconds_to_run)
            cfg['time'] = num_ops[1]

        # For Black box, multi node tests
        # always use membase-binary
        if self.is_multi_node:
            protocol = self.mk_protocol(host=self.input.servers[0].ip,
                                        port=self.input.servers[0].port)

        self.log.info("mcsoda - protocol %s" % protocol)
        protocol, host_port, user, pswd = self.protocol_parse(protocol, use_direct=use_direct)

        if not user.strip():
            user = self.input.servers[0].rest_username
        if not pswd.strip():
            pswd = self.input.servers[0].rest_password

        self.log.info("mcsoda - %s %s %s %s" % (protocol, host_port, user, pswd))
        self.log.info("mcsoda - cfg: " + str(cfg))
        self.log.info("mcsoda - cur: " + str(cur))

        # For query tests always use StoreCouchbase
        if protocol == "couchbase":
            stores = [StoreCouchbase()]

        cur, start_time, end_time = self.mcsoda_run(cfg, cur,
                                                    protocol, host_port, user, pswd,
                                                    stats_collector=sc,
                                                    ctl=ctl, stores=stores,
                                                    heartbeat=self.parami("mcsoda_heartbeat", 0),
                                                    why="loop",
                                                    bucket=self.param("bucket", "default"))

        ops = { 'tot-sets': cur.get('cur-sets', 0),
                'tot-gets': cur.get('cur-gets', 0),
                'tot-items': cur.get('cur-items', 0),
                'tot-creates': cur.get('cur-creates', 0),
                'tot-misses': cur.get('cur-misses', 0),
                "start-time": start_time,
                "end-time": end_time }

        # Wait until there are no active indexing tasks
        if self.parami('wait_for_indexer', 0):
            ClusterOperationHelper.wait_for_completion(self.rest, 'indexer')

        # Wait until there are no active view compaction tasks
        if self.parami('wait_for_compaction', 0):
            ClusterOperationHelper.wait_for_completion(self.rest, 'view_compaction')

        if self.parami("collect_stats", 1):
            self.end_stats(sc, ops, self.spec_reference + ".loop")

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
        print "[drain] draining disk write queue ..."

        master = self.input.servers[0]
        bucket = self.param("bucket", "default")

        RebalanceHelper.wait_for_stats_on_all(master, bucket, 'ep_queue_size', 0, \
                                              fn=RebalanceHelper.wait_for_stats_no_timeout)
        RebalanceHelper.wait_for_stats_on_all(master, bucket, 'ep_flusher_todo', 0, \
                                              fn=RebalanceHelper.wait_for_stats_no_timeout)

        print "[drain] disk write queue has been drained"

        return time.time()

    def warmup(self, collect_stats=True, flush_os_cache=False):
        """
        Restart cluster and wait for it to warm up.
        In current version, affect the master node only.
        """
        if not self.input.servers:
            print "[warmup error] empty server list"
            return

        if collect_stats:
            client_id = self.parami("prefix", 0)
            test_params = {'test_time': time.time(),
                           'test_name': self.id(),
                           'json': 0}
            sc = self.start_stats(self.spec_reference + ".warmup",
                                  test_params=test_params,
                                  client_id=client_id)

        print "[warmup] preparing to warmup cluster ..."

        server = self.input.servers[0]
        shell = RemoteMachineShellConnection(server)

        start_time = time.time()

        print "[warmup] stopping couchbase ... ({0}, {1})"\
            .format(server.ip, time.strftime('%X %x %Z'))
        shell.stop_couchbase()
        print "[warmup] couchbase stopped ({0}, {1})"\
            .format(server.ip, time.strftime('%X %x %Z'))

        if flush_os_cache:
            print "[warmup] flushing os cache ..."
            shell.flush_os_caches()

        shell.start_couchbase()
        print "[warmup] couchbase restarted ({0}, {1})"\
            .format(server.ip, time.strftime('%X %x %Z'))

        self.wait_until_warmed_up()
        print "[warmup] warmup finished"

        end_time = time.time()
        ops = { 'tot-sets': 0,
                'tot-gets': 0,
                'tot-items': 0,
                'tot-creates': 0,
                'tot-misses': 0,
                "start-time": start_time,
                "end-time": end_time }

        if collect_stats:
            self.end_stats(sc, ops, self.spec_reference + ".warmup")

    def wait_until_warmed_up(self, master=None):
        if not master:
            master = self.input.servers[0]

        bucket = self.param("bucket", "default")

        fn = RebalanceHelper.wait_for_mc_stats_no_timeout
        for bucket in self.buckets:
            RebalanceHelper.wait_for_stats_on_all(master, bucket,
                                                  'ep_warmup_thread',
                                                  'complete', fn=fn)

    def clog_cluster(self, servers):
        ClusterOperationHelper.flushctl_stop(servers)

    def unclog_cluster(self, servers):
        ClusterOperationHelper.flushctl_start(servers)

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
        input = getattr(self, "input", TestInputSingleton.input)
        return input.test_params.get(name, default_value)

    def parami(self, name, default_int):
        return int(self.param(name, default_int))

    def paramf(self, name, default_float):
        return float(self.param(name, default_float))

    def params(self, name, default_str):
        return str(self.param(name, default_str))


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
        self.rebalance_nodes(2)
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
        self.rebalance_nodes(3)
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
        self.rebalance_nodes(5)
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
        self.rebalance_nodes(2)
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
        self.rebalance_nodes(2)
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
                                              kind=self.param('kind', 'binary'),
                                              doc_cache=self.parami('doc_cache', 0))
        ops['end-time'] = self.wait_until_drained()
        self.end_stats(sc, ops)

    def test_1M_1k(self):
        self.spec('DRR-02.1')
        sc = self.start_stats(self.spec_reference, test_params={'test_name':self.id(),
                                                                'test_time':time.time()})
        ops, start_time, end_time = self.load(self.parami("items", 1000000),
                                              self.parami('size', 1024),
                                              kind=self.param('kind', 'binary'),
                                              ratio_sets=self.paramf('ratio-sets', 0.9),
                                              doc_cache=self.parami('doc_cache', 0))
        ops['end-time'] = self.wait_until_drained()
        self.end_stats(sc, ops)

    def test_1M_rebalance(self):
        self.spec('DRR-03')
        self.rebalance_nodes(2)
        self.delayed_rebalance(4)
        sc = self.start_stats(self.spec_reference, test_params={'test_name':self.id(),
                                                                'test_time':time.time()})
        ops, start_time, end_time = self.load(self.parami("items", 1000000),
                                              self.parami('size', 1024),
                                              kind=self.param('kind', 'binary'),
                                              doc_cache=self.parami('doc_cache', 0))
        ops['end-time'] = self.wait_until_drained()
        self.end_stats(sc, ops)

    def test_1M_compaction(self):
        self.spec('DRR-04')
        self.delayed_compaction()
        sc = self.start_stats(self.spec_reference, test_params={'test_name':self.id(),
                                                                'test_time':time.time()})
        ops, start_time, end_time = self.load(self.parami("items", 1000000),
                                              self.parami('size', 1024),
                                              kind=self.param('kind', 'binary'),
                                              doc_cache=self.parami('doc_cache', 0))
        ops['end-time'] = self.wait_until_drained()
        self.end_stats(sc, ops)

    def test_1M_clog(self):
        self.spec('DRR-06')
        self.clog_cluster([self.input.servers[0]])
        ops, load_start_time, load_end_time = self.load(self.parami("items", 1000000),
                                                        self.parami('size', 1024),
                                                        kind=self.param('kind', 'binary'),
                                                        doc_cache=self.parami('doc_cache', 0))
        sc = self.start_stats(self.spec_reference, test_params={'test_name':self.id(),
                                                                'test_time':time.time()})
        start_time_unclog = time.time()
        self.unclog_cluster([self.input.servers[0]])
        end_time_unclog = self.wait_until_drained()

        ops['start-time'] = start_time_unclog
        ops['end-time'] = end_time_unclog
        self.end_stats(sc, ops)


class MapReduce(PerfBase):

    def go_load(self):
        items  = self.parami("items", 1000000)
        self.load(items,
                  self.parami('size', 1024),
                  kind=self.param('kind', 'json'),
                  doc_cache=self.parami('doc_cache', 0))
        self.loop_prep()

    def go(self, key_name, map_fun, reduce_fun=None,
           limit=1, view_clients=1, load=True):
        if load:
            self.go_load()

        items  = self.parami("items", 1000000)
        limit  = self.parami('limit', limit)
        bucket = "default"
        view   = "myview"

        self.rest.create_view(view, bucket, [View(view, map_fun, reduce_fun)])

        self.log.info("building view: %s = %s; %s" % (view, map_fun, reduce_fun))
        ops = {}
        ops["view-build-start-time"] = time.time()
        self.rest.view_results(bucket, view, { "startkey":"a" }, limit, timeout=480)
        ops["view-build-end-time"] = time.time()

        sc = self.start_stats(self.spec_reference,
                              test_params={'test_name':self.id(),
                                           'test_time':time.time()})
        self.log.info("accessing view: %s" % (view,))
        ops["start-time"] = time.time()
        key_maker = getattr(mcsoda, key_name)
        n = self.parami("ops", 10000)
        report = int(n * 0.1)

        ctl = { 'run_ok': True }
        if view_clients <= 1:
            MapReduce.go_view_worker(ctl, self.rest, 0, n, items,
                                     key_name, key_maker,
                                     bucket, view, limit,
                                     self.sc, report)
        else:
            threads = []
            offset = items / view_clients
            try:
                for x in range(view_clients):
                    t = threading.Thread(target=MapReduce.go_view_worker,
                                         args=(ctl, self.rest, offset, n, items,
                                               key_name, key_maker,
                                               bucket, view, limit,
                                               self.sc, report))
                    threads.append(t)
                    t.daemon = True
                    t.start()
                while len(threads) > 0:
                    threads[0].join(1)
                    threads = [t for t in threads if t.isAlive()]
            except KeyboardInterrupt:
                ctl['run_ok'] = False

        ops["end-time"] = time.time()
        ops["tot-views"] = n * view_clients
        self.end_stats(sc, ops)

    @staticmethod
    def go_view_worker(ctl, rest, offset, n, items,
                       key_name, key_maker, bucket, view, limit, sc, report):
        for i in range(n):
            if not ctl['run_ok']:
                return
            k = (i + offset) % items
            e = key_maker(k, mcsoda.prepare_key(k))
            start = time.time()
            r = rest.view_results(bucket, view,
                                  { "startkey":e, "endkey":e }, limit)
            if i % report == 0:
                print("ex: view result for %s, %s = %s" % \
                          (key_name, e, r))
            end = time.time()
            sc.ops_stats({ 'tot-gets': 0,
                           'tot-sets': 0,
                           'tot-deletes': 0,
                           'tot-arpas': 0,
                           'tot-views': 1,
                           'start-time': start,
                           'end-time': end })

    def test_VP_001(self):
        self.spec('VP-001')
        self.go("key_to_email",
                "function(doc) { emit(doc.email, 1); }")

    def test_VP_002(self):
        self.spec('VP-002')
        self.go("key_to_realm",
                "function(doc) { emit(doc.realm, 1); }",
                reduce_fun="_count", limit=10)

    def test_VP_004(self):
        self.spec('VP-004')
        self.go("key_to_email",
                "function(doc) { emit(doc.email, 1); }",
                view_clients=self.parami('view_clients', 4))

    def test_VP_005(self):
        self.spec('VP-005')
        self.rebalance_nodes(self.parami("nodes", 2))
        self.go("key_to_email",
                "function(doc) { emit(doc.email, 1); }",
                view_clients=self.parami('view_clients', 4))

    def test_VP_006(self):
        self.spec('VP-006')
        self.delayed_compaction()
        self.go("key_to_email",
                "function(doc) { emit(doc.email, 1); }",
                view_clients=self.parami('view_clients', 4))

    def test_VP_007(self):
        self.spec('VP-007')
        self.rebalance_nodes(self.parami("nodes", 2))
        self.go_load()
        self.delayed_rebalance(self.parami("nodes_after", 4))
        self.go("key_to_email",
                "function(doc) { emit(doc.email, 1); }",
                view_clients=self.parami('view_clients', 4),
                load=False)

    def go_with_mcsoda(self):
        self.go_load()
        cfg = { 'min-value-size': self.parami("min_value_size", 1024),
                'vbuckets': self.vbucket_count,
                'batch':  PerfDefaults.batch,
                'json': 1 }
        cur = { 'cur-items': self.parami("items", 1000000) }
        ctl = { 'run_ok': True }
        protocol = self.param('protocol', 'binary')
        protocol, host_port, user, pswd = \
            self.protocol_parse(protocol, use_direct=True)
        try:
            t = threading.Thread(target=mcsoda.run,
                                 args=(cfg, cur, protocol, host_port,
                                       user, pswd, None, None, ctl))
            t.daemon = True
            t.start()
            self.go("key_to_email",
                    "function(doc) { emit(doc.email, 1); }",
                    view_clients=self.parami('view_clients', 4),
                    load=False)
            ctl['run_ok'] = False
            t.join()
        except KeyboardInterrupt:
            ctl['run_ok'] = False

    def test_VP_008(self):
        self.spec('VP-008')
        self.go_with_mcsoda()

    def test_VP_009(self):
        self.spec('VP-009')
        self.rebalance_nodes(self.parami("nodes", 2))
        self.go_with_mcsoda()


class ErlangAsyncDrainingTests(PerfBase):

    def setUp(self):
        self.dgm = False
        super(ErlangAsyncDrainingTests, self).setUp()

    def go(self, original, modified, mode):
        ClusterOperationHelper.change_erlang_async(self.input.servers, original, modified)
        #restart
        ClusterOperationHelper.stop_cluster(self.input.servers)
        ClusterOperationHelper.start_cluster(self.input.servers)
        time.sleep(10)
        self.wait_until_warmed_up()

        sc = self.start_stats(self.spec_reference, test_params={'test_name':self.id(),
                                                                'test_time':time.time()})
        ops, start_time, end_time = self.load(self.parami("items", 1000000),
                                              self.parami('size', 2048),
                                              kind=self.param('kind', mode),
                                              doc_cache=self.parami('doc_cache', 0))
        ops['end-time'] = self.wait_until_drained()
        self.end_stats(sc, ops)

    def test_EA_B_0(self):
        self.spec('EA.B.0')
        self.go(0, 16, "binary")

    def test_EA_B_16(self):
        self.spec('EA.B.16')
        self.go(16, 0, "binary")

    def test_EA_J_0(self):
        self.spec('EA.J.0')
        self.go(0, 16, "json")

    def test_EA_J_16(self):
        self.spec('EA.J.16')
        self.go(16, 0, "json")


class TransactionSize(PerfBase):

    def setUp(self):
        self.dgm = False
        super(TransactionSize, self).setUp()

    def go(self, settings):
        for key, val in settings:
            val = self.param(key, val)
            ClusterOperationHelper.flushctl_set(self.input.servers[0], key, val)

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
        self.spec('TS1.7')
        self.go([('max_txn_size', '8000'),
                 ('couch_vbucket_batch_count', '8')])

    def test_TS1_8(self):
        self.spec('TS1.8')
        self.go([('max_txn_size', '10000'),
                 ('couch_vbucket_batch_count', '4')])

    def test_TS1_9(self):
        self.spec('TS1.9')
        self.go([('max_txn_size', '10000'),
                 ('couch_vbucket_batch_count', '8')])


class Warmup(PerfBase):

    def setUp(self):
        self.dgm = self.parami("dgm", 0) # By default, no DGM for Warmup tests.
        super(Warmup, self).setUp()

    def go(self, items=None, kind='binary', expiration=0, ratio_expirations=0.0):
        items = items or self.parami("items", 2000000)
        self.load(items,
                  kind=kind,
                  expiration=expiration,
                  ratio_expirations=ratio_expirations,
                  doc_cache=self.parami('doc_cache', 0))
        self.wait_until_drained()
        ClusterOperationHelper.stop_cluster(self.input.servers)
        ClusterOperationHelper.start_cluster(self.input.servers)
        start_time = time.time()
        time.sleep(max(10, expiration + 2)) # Sleep longer than expiration.
        sc = self.start_stats(self.spec_reference, test_params={'test_name':self.id(),
                                                                'test_time':start_time})
        self.wait_until_warmed_up()
        end_time = time.time()
        self.end_stats(sc, { 'tot-items': self.num_items_loaded,
                             "start-time": start_time,
                             "end-time": end_time })

    def test_WARM_01_1(self):
        self.spec('WARM-01-1')
        self.go(kind='binary')

    def test_WARM_01j_1(self):
        self.spec('WARM-01j-1')
        self.go(kind='json')


class WarmupDGM(Warmup):

    def setUp(self):
        PerfBase.setUp(self) # Skip Warmup.setUp() to get default DGM behavior.

    def test_WARM_02_1(self):
        self.spec('WARM-02.1')
        self.go(kind='binary',
                expiration=self.parami("expiration", 20), # 20 seconds.
                ratio_expirations=self.paramf("ratio_expirations", 1.0))


class WarmupWithMoreReplicas(Warmup):

    def setUp(self):
        self.replicas = self.parami("replicas", 2)
        super(WarmupWithMoreReplicas, self).setUp()

    def test_WARM_03(self):
        self.spec('WARM-03')
        self.rebalance_nodes(self.parami("nodes", 3))
        self.go(self.parami("items", 15000000),
                kind='binary')


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
        self.rebalance_nodes(2)
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
        self.rebalance_nodes(2)
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
        self.rebalance_nodes(2)
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
                self.rebalance_nodes(start_num_nodes)
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
