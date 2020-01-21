import sys
import json
import time
import socket
import functools
from multiprocessing import Process, Event
from multiprocessing.sharedctypes import Value
from collections import defaultdict

from lib.membase.api import httplib2
from lib.membase.api.rest_client import RestConnection
from lib.membase.helper.rebalance_helper import RebalanceHelper
from btrc import CouchbaseClient, StatsReporter

from pytests.performance.eperf import EPerfClient, EVPerfClient
from pytests.performance.perf_defaults import PerfDefaults
from pytests.performance.viewgen import ViewGen


class PerfWrapper(object):

    """Class of decorators to run complicated tests (multiclient, rebalance,
    rampup, xdcr and etc.) based on general performance tests (from eperf and
    perf modules).
    """

    @staticmethod
    def multiply(test):
        @functools.wraps(test)
        def wrapper(self, *args, **kargs):
            """This wrapper allows to launch multiple tests on the same
            client (machine). Number of concurrent processes depends on phase
            and "total_clients" parameter in *.conf file.

            There is no need to specify "prefix" and "num_clients".

            Processes don't share memory. However they share stdout/stderr.
            """
            is_bi_xperf = isinstance(self, XPerfTests) and 'bi' in self.id()

            total_clients = self.parami('total_clients', 1)

            if is_bi_xperf:
                total_clients *= 2
            else:
                # Limit number of workers during load phase
                if self.parami('load_phase', 0):
                    total_clients = min(8, total_clients)

            self.input.test_params['num_clients'] = total_clients

            if self.parami('index_phase', 0) or \
                    self.parami('incr_index_phase', 0):
                # Single-threaded tasks (hot load phase, index phase)
                self.input.test_params['prefix'] = 0
                return test(self, *args, **kargs)
            else:
                # Concurrent tasks (load_phase, access phase)
                executors = list()

                if is_bi_xperf:
                    prefix_range = range(total_clients // 2)
                else:
                    prefix_range = range(total_clients)

                for prefix in prefix_range:
                    self.input.test_params['prefix'] = prefix
                    self.is_leader = bool(prefix == 0)
                    executor = Process(target=test, args=(self, ))
                    executor.daemon = True
                    executor.start()
                    executors.append(executor)

                for executor in executors:
                    executor.join()

                return executors[0]
        return wrapper

    @staticmethod
    def rampup(test):
        @functools.wraps(test)
        def wrapper(self, *args, **kargs):
            """This wrapper launches two groups of processes:
            -- constant background load (mainly memcached sets/gets)
            -- constant foreground load (mainly view queries)

            However during the test number of foreground processes increases.
            So consistency only means number of operation per second.

            Processes use shared objects (ctype wrappers) for synchronization.
            """
            if self.parami('index_phase', 0) or self.parami('load_phase', 0):
                return PerfWrapper.multiply(test)(self, *args, **kargs)

            total_bg_clients = self.parami('total_bg_clients', 1)
            total_fg_clients = self.parami('total_fg_clients', 1)
            total_clients = total_bg_clients + total_fg_clients
            self.input.test_params['num_clients'] = total_clients

            executors = list()
            self.shutdown_event = Event()

            # Background load (memcached)
            original_delay = self.parami('start_delay', 30)
            original_fg_max_ops_per_sec = self.parami('fg_max_ops_per_sec',
                                                      1000)
            self.input.test_params['start_delay'] = 5
            self.input.test_params['fg_max_ops_per_sec'] = 100
            for prefix in range(0, total_bg_clients):
                self.input.test_params['prefix'] = prefix
                self.is_leader = bool(prefix == 0)
                executor = Process(target=test, args=(self, ))
                executor.start()
                executors.append(executor)

            # Foreground load (queries)
            self.input.test_params['start_delay'] = original_delay
            self.input.test_params['fg_max_ops_per_sec'] = \
                original_fg_max_ops_per_sec
            self.input.test_params['bg_max_ops_per_sec'] = 1

            self.active_fg_workers = Value('i', 0)  # signed int

            for prefix in range(total_bg_clients, total_clients):
                self.input.test_params['prefix'] = prefix
                self.is_leader = False
                executor = Process(target=test, args=(self, ))
                executor.start()
                executors.append(executor)

            # Shutdown
            for executor in executors:
                executor.join()
                self.active_fg_workers.value -= 1

            return executors[-1]
        return wrapper

    @staticmethod
    def xperf(bidir=False):
        def decorator(test):
            @functools.wraps(test)
            def wrapper(self, *args, **kargs):
                # Execute performance test
                region = XPerfTests.get_region()
                if 'bi' in self.id():
                    # Resolve conflicting keyspaces
                    self.input.test_params['cluster_prefix'] = region

                if region == 'east':
                    self.input.servers = self.input.clusters[0]
                    self.input.test_params['bucket'] = self.get_buckets()[0]
                    xdc_test = PerfWrapper.multiply(test)(self, *args, **kargs)
                elif region == 'west':
                    self.input.servers = self.input.clusters[1]
                    self.input.test_params['bucket'] = \
                        self.get_buckets(reversed=True)[0]
                    xdc_test = PerfWrapper.multiply(test)(self, *args, **kargs)

                # Define remote cluster and start replication (only after load
                # phase)
                if self.parami('load_phase', 0) and \
                        not self.parami('hot_load_phase', 0):
                    master = self.input.clusters[0][0]
                    slave = self.input.clusters[1][0]
                    try:
                        self.start_replication(master, slave, bidir=bidir)
                    except Exception as error:
                        self.log.warn(error)
                    self.wait_for_xdc_replication()
                return xdc_test
            return wrapper
        return decorator

    @staticmethod
    def xperf_load(test):
        @functools.wraps(test)
        def wrapper(self, *args, **kargs):
                """Run load phase, start unidirectional replication, collect
                and print XDCR related metrics"""
                PerfWrapper.multiply(test)(self, *args, **kargs)

                self.start_replication(self.input.clusters[0][0],
                                       self.input.clusters[1][0])

                self.collect_stats()
        return wrapper

    @staticmethod
    def rebalance(test):
        @functools.wraps(test)
        def wrapper(self, *args, **kargs):
            """Start parallel rebalancer thread"""
            self.shutdown_event = Event()
            if 'XRebalanceTests' in self.id():
                return test(self, *args, **kargs)
            else:
                return PerfWrapper.multiply(test)(self, *args, **kargs)
        return wrapper


class MultiClientTests(EVPerfClient):

    """Load tests with consistent number of clients. Each client performs the
    same work.
    """

    @PerfWrapper.multiply
    def test_kvperf(self):
        super(MultiClientTests, self).test_eperf_mixed()

    @PerfWrapper.multiply
    def test_vperf(self):
        super(MultiClientTests, self).test_vperf()


class RampUpTests(EVPerfClient):

    """Ramup-up load tests with increasing number of foreground workers.
    """

    @PerfWrapper.rampup
    def test_view_rampup_1(self):
        super(RampUpTests, self).test_vperf()


class XPerfTests(EPerfClient):

    """XDCR large-scale performance tests
    """

    def start_replication(self, master, slave, replication_type='continuous',
                          buckets=None, bidir=False, suffix='A'):
        """Add remote cluster and start replication"""

        master_rest_conn = RestConnection(master)
        remote_reference = 'remote_cluster_' + suffix

        master_rest_conn.add_remote_cluster(slave.ip, slave.port,
                                            slave.rest_username,
                                            slave.rest_password,
                                            remote_reference)

        if not buckets:
            buckets = self.get_buckets()
        else:
            buckets = self.get_buckets(reversed=True)

        for bucket in buckets:
            master_rest_conn.start_replication(replication_type, bucket,
                                               remote_reference)

        if self.parami('xdcr_num_buckets', 1) > 1 and suffix == 'A':
            self.start_replication(slave, master, replication_type, buckets,
                                   suffix='B')

        if bidir:
            self.start_replication(slave, master, replication_type, buckets,
                                   suffix='B')

    def get_buckets(self, reversed=False):
        """Return list of buckets to be replicated"""

        xdcr_num_buckets = self.parami('xdcr_num_buckets', 1)
        if xdcr_num_buckets > 1:
            num_replicated_buckets = self.parami('num_replicated_buckets',
                                                 xdcr_num_buckets)
            buckets = ['bucket-{0}'.format(i) for i in range(xdcr_num_buckets)]
            if not reversed:
                return buckets[:num_replicated_buckets]
            else:
                return buckets[-1:-1 - num_replicated_buckets:-1]
        else:
            return [self.param('bucket', 'default')]

    @staticmethod
    def get_region():
        """Try to identify public hostname and return corresponding EC2 region.

        In case of socket exception (it may happen when client is local VM) use
        VM hostname.

        Reference: http://bit.ly/instancedata
        """

        try:
            uri = 'http://169.254.169.254/latest/meta-data/public-hostname'
            http = httplib2.Http(timeout=5)
            response, hostname = http.request(uri)
        except (socket.timeout, socket.error):
            hostname = socket.gethostname()
        if 'west' in hostname:
            return 'west'
        else:
            return 'east'

    def wait_for_xdc_replication(self):
        rest = RestConnection(self.input.servers[0])
        bucket = self.param('bucket', 'default')
        while True:  # we have to wait at least once
            self.log.info("waiting for another 15 seconds")
            time.sleep(15)
            if not rest.get_xdc_queue_size(bucket):
                break

    def get_samples(self, rest, server='explorer'):
        # TODO: fix hardcoded cluster names

        api = rest.baseUrl + \
            "pools/default/buckets/default/" + \
            "nodes/{0}.server.1:8091/stats?zoom=minute".format(server)

        _, content, _ = rest._http_request(api)

        return json.loads(content)['op']['samples']

    def collect_stats(self):
        # TODO: fix hardcoded cluster names

        # Initialize rest connection to master and slave servers
        master_rest_conn = RestConnection(self.input.clusters[0][0])
        slave_rest_conn = RestConnection(self.input.clusters[1][0])

        # Define list of metrics and stats containers
        metrics = ('mem_used', 'curr_items', 'vb_active_ops_create',
                   'ep_bg_fetched', 'cpu_utilization_rate')
        stats = {'slave': defaultdict(list), 'master': defaultdict(list)}

        # Calculate approximate number of relicated items per node
        num_nodes = self.parami('num_nodes', 1) // 2
        total_items = self.parami('items', 1000000)
        items = 0.99 * total_items // num_nodes

        # Get number of relicated items
        curr_items = self.get_samples(slave_rest_conn)['curr_items']

        # Collect stats until all items are replicated
        while curr_items[-1] < items:
            # Collect stats every 20 seconds
            time.sleep(19)

            # Slave stats
            samples = self.get_samples(slave_rest_conn)
            for metric in metrics:
                stats['slave'][metric].extend(samples[metric][:20])

            # Master stats
            samples = self.get_samples(master_rest_conn, 'nirvana')
            for metric in metrics:
                stats['master'][metric].extend(samples[metric][:20])

            # Update number of replicated items
            curr_items = stats['slave']['curr_items']

        # Aggregate and display stats
        vb_active_ops_create = sum(stats['slave']['vb_active_ops_create']) /\
            len(stats['slave']['vb_active_ops_create'])
        print("slave> AVG vb_active_ops_create: {0}, items/sec"\
            .format(vb_active_ops_create))

        ep_bg_fetched = sum(stats['slave']['ep_bg_fetched']) /\
            len(stats['slave']['ep_bg_fetched'])
        print("slave> AVG ep_bg_fetched: {0}, reads/sec".format(ep_bg_fetched))

        for server in stats:
            mem_used = max(stats[server]['mem_used'])
            print("{0}> MAX memory used: {1}, MB".format(server,
                                                         mem_used // 1024 ** 2))
            cpu_rate = sum(stats[server]['cpu_utilization_rate']) /\
                len(stats[server]['cpu_utilization_rate'])
            print("{0}> AVG CPU rate: {1}, %".format(server, cpu_rate))

    @PerfWrapper.xperf()
    def test_mixed_unidir(self):
        """Mixed KV workload"""
        super(XPerfTests, self).test_eperf_mixed()

    @PerfWrapper.xperf(bidir=True)
    def test_mixed_bidir(self):
        """Mixed KV workload"""
        super(XPerfTests, self).test_eperf_mixed()


class XVPerfTests(XPerfTests, EVPerfClient):

    """XDCR large-scale performance tests with views
    """

    @PerfWrapper.xperf()
    def test_vperf_unidir(self):
        super(XVPerfTests, self).test_vperf()

    @PerfWrapper.xperf(bidir=True)
    def test_vperf_bidir(self):
        super(XVPerfTests, self).test_vperf()

    @PerfWrapper.xperf_load
    def test_vperf_load(self):
        super(XVPerfTests, self).test_vperf()


class RebalanceTests(EVPerfClient):

    """Performance tests with rebalance during test execution.
    """

    @PerfWrapper.rebalance
    def test_views_rebalance(self):
        # Consistent view setup
        rc = RestConnection(self.input.servers[0])
        if self.parami('consistent_view', 1):
            rc.set_reb_cons_view(disable=False)
        else:
            rc.set_reb_cons_view(disable=True)
        super(RebalanceTests, self).test_vperf()

    @PerfWrapper.rebalance
    def test_mixed_rebalance(self):
        """Mixed read/write test w/o views"""
        super(RebalanceTests, self).test_eperf_mixed()

    def rebalance(self):
        """Trigger cluster rebalance (in, out, swap) after 1 hour. Stop the
        test once rebalance completed (with 1 hour delay).
        """
        if not self.parami('access_phase', 0):
            return
        else:
            self.log.info("started rebalance thread")

        time.sleep(self.parami("rebalance_after", 3600))

        server = "{0}:{1}".format(self.input.servers[0].ip,
                                  self.input.servers[0].port)
        bucket = self.params("bucket", "default")
        cb = CouchbaseClient(server, bucket)
        cb.reset_utilization_stats()

        self.delayed_rebalance(
            num_nodes=self.parami("num_nodes_after",
                                  PerfDefaults.num_nodes_after),
            delay_seconds=1,
            max_retries=self.parami("reb_max_retries",
                                    PerfDefaults.reb_max_retries),
            reb_mode=self.parami("reb_mode", PerfDefaults.reb_mode),
            sync=True)

        reporter = StatsReporter(cb)
        reporter.report_stats("util_stats")

        time.sleep(self.parami("shutdown_after", 3600))
        self.log.info("trigerring shutdown event")
        self.shutdown_event.set()

    def test_alk_rebalance(self):
        """Alk's specification.

        Cluster setup:
        -- 4 nodes
        -- 1 bucket
        -- 8GB total RAM
        -- 5GB bucket quota
        -- no data replica
        -- no index replica
        -- no view compaction

        All phases are enabled by default.
        Load phase:
        -- 10M items x 2KB average values size
        -- no expiration
        Index phase:
        -- 1 design ddoc, 1 view
        Access phase:
        -- no front-end workload
        -- rebalance out, from 4 to 3 nodes
        -- stale=false query after rebalance
        """
        # Legacy
        self.spec(self.__str__().split(" ")[0])

        # Disable stats
        self.input.test_params['stats'] = 0

        # View compaction setup
        rc = RestConnection(self.input.servers[0])
        vt = 30 if self.parami('view_compaction', 1) else 100
        rc.set_auto_compaction(dbFragmentThresholdPercentage=30,
                               viewFragmntThresholdPercentage=vt)

        # Consistent view setup
        if self.parami('consistent_view', 1):
            rc.set_reb_cons_view(disable=False)
        else:
            rc.set_reb_cons_view(disable=True)

        # rebalance_moves_per_second setup
        rmps = self.parami('rebalance_moves_per_node', 1)
        cmd = 'ns_config:set(rebalance_moves_per_node, {0}).'.format(rmps)
        rc.diag_eval(cmd)

        # index_pausing_disabled setup
        ipd = str(bool(self.parami('index_pausing_disabled', 0))).lower()
        cmd = 'ns_config:set(index_pausing_disabled, {0}).'.format(ipd)
        rc.diag_eval(cmd)

        # rebalance_index_waiting_disabled setup
        riwd = str(bool(self.parami('rebalance_index_waiting_disabled', 0))).lower()
        cmd = 'ns_config:set(rebalance_index_waiting_disabled, {0}).'.format(riwd)
        rc.diag_eval(cmd)

        # Customize number of design docs
        view_gen = ViewGen()
        views = self.param("views", None)
        if views is not None:
            views = [int(v) for v in eval(views)]
            ddocs = view_gen.generate_ddocs(views)
        elif self.parami('ddocs', 1) == 1:
            ddocs = view_gen.generate_ddocs([1])
        elif self.parami('ddocs', 1) == 8:
            ddocs = view_gen.generate_ddocs([1, 1, 1, 1, 1, 1, 1, 1])
        else:
            sys.exit('Only 1 or 8 ddocs supported.')

        # Load phase
        if self.parami('load_phase', 0):
            num_nodes = self.parami('num_nodes', PerfDefaults.num_nodes)
            self.load_phase(num_nodes)

        # Index phase
        if self.parami('index_phase', 0):
            self.index_phase(ddocs)

        # Access phase
        if self.parami('access_phase', 0):
            if self.param('rebalance', 'out') == 'out':
                RebalanceHelper.rebalance_out(servers=self.input.servers,
                                              how_many=1, monitor=True)
            elif self.param('rebalance', 'out') == 'swap':
                RebalanceHelper.rebalance_swap(servers=self.input.servers,
                                               how_many=1, monitor=True)
            elif self.param('rebalance', 'out') == 'in':
                RebalanceHelper.rebalance_in(servers=self.input.servers,
                                             how_many=1, monitor=True)
            else:
                sys.exit('Wrong "rebalance" parameter')
            self.measure_indexing_time(rc, ddocs)

    def measure_indexing_time(self, rc, ddocs):
        t0 = time.time()
        query = {'stale': 'false', 'limit': 100,
                 'connection_timeout': 14400000}
        for ddoc_name, ddoc in ddocs.items():
            for view_name in ddoc['views'].keys():
                rc.query_view(design_doc_name=ddoc_name,
                              view_name=view_name,
                              bucket=self.params('bucket', 'default'),
                              query=query, timeout=14400)
        t1 = time.time()
        self.log.info('time taken to build index: {0} sec'.format(t1 - t0))


class XRebalanceTests(XVPerfTests):

    @PerfWrapper.rebalance
    def test_mixed_bidir_rebalance(self):
        """Mixed read/write test w/o views"""
        super(XRebalanceTests, self).test_mixed_bidir()

    @PerfWrapper.rebalance
    def test_vperf_unidir_rebalance(self):
        """Mixed read/write test with views"""
        super(XRebalanceTests, self).test_vperf_unidir()
