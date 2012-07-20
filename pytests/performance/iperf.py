import copy
import socket
import functools
from multiprocessing import Process

from membase.api import httplib2
from membase.api.rest_client import RestConnection

from eperf import EVPerfClient


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
            total_clients = self.parami('total_clients', 1)
            # Limit number of workers during load phase
            if self.parami('load_phase', 0):
                total_clients = min(4, total_clients)
            self.input.test_params['num_clients'] = total_clients

            if self.parami('index_phase', 0) or self.parami('hot_load_phase', 0):
                # Single-threaded tasks (hot load phase, index phase)
                self.input.test_params['prefix'] = 0
                return test(self, *args, **kargs)
            else:
                # Concurrent tasks (load_phase, access phase)
                executors = list()

                for prefix in range(total_clients):
                    self.input.test_params['prefix'] = prefix
                    self.is_leader = bool(prefix == 0)
                    executor = Process(target=test, args=(self, ))
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
            -- ramping up load (mainly view queries)

            Currenly these processes don't have synchronization points.
            """
            if self.parami('index_phase', 0) or self.parami('load_phase', 0):
                return PerfWrapper.multiply(test(self, *args, **kargs))

            total_bg_clients = self.parami('total_bg_clients', 1)
            total_fg_clients = self.parami('total_fg_clients', 1)
            total_clients = total_bg_clients + total_fg_clients
            self.input.test_params['num_clients'] = total_clients

            executors = list()

            # Background load (memcached)
            original_delay = self.parami('start_delay', 30)
            self.input.test_params['start_delay'] = 5
            for prefix in range(0, total_bg_clients):
                self.input.test_params['prefix'] = prefix
                self.is_leader = bool(prefix == 0)
                executor = Process(target=test, args=(self, ))
                executor.start()
                executors.append(executor)

            # Foreground load (memcached)
            self.input.test_params['start_delay'] = original_delay
            self.input.test_params['bg_max_ops_per_sec'] = 1
            for prefix in range(total_bg_clients, total_clients):
                self.input.test_params['prefix'] = prefix
                self.is_leader = False
                executor = Process(target=test, args=(self, ))
                executor.start()
                executors.append(executor)

            for executor in executors:
                executor.join()

            return executors[-1]
        return wrapper

    @staticmethod
    def xperf(bidir=False):
        def decorator(test):
            @functools.wraps(test)
            def wrapper(self, *args, **kargs):
                """Define remote cluster and start replication (only before
                load phase).
                """
                if (self.parami('prefix', -1) == 0 and
                    self.parami('load_phase', 0) and
                    not self.parami('hot_load_phase', 0) and
                    not self.parami('disable_xdcr', 0)):
                    master = self.input.clusters[0][0]
                    slave = self.input.clusters[1][0]
                    try:
                        self.start_replication(master, slave, bidir=bidir)
                    except Exception:
                        pass

                # Execute performance test
                region = XPerfTests.get_ec2_region()
                if region == 'east':
                    self.input.servers = self.input.clusters[0]
                    self.input.test_params['bucket'] = self.get_buckets()[0]
                    return test(self, *args, **kargs)
                elif region == 'west':
                    self.input.servers = self.input.clusters[1]
                    self.input.test_params['bucket'] = self.get_buckets(reversed=True)[0]
                    return test(self, *args, **kargs)
                else:
                    # 1st test executor:
                    self_copy = copy.copy(self)
                    self_copy.input.servers = self_copy.input.clusters[0]
                    self_copy.input.test_params['bucket'] = self_copy.get_buckets()[0]
                    primary = Process(target=test, args=(self_copy, ))
                    primary.start()

                    # 2nd test executor:
                    self.input.servers = self.input.clusters[1]
                    self.input.test_params['bucket'] = self.get_buckets(reversed=True)[0]
                    self.input.test_params['stats'] = 0
                    secondary = test(self, *args, **kargs)

                    primary.join()
                    return secondary
            return wrapper
        return decorator

    @staticmethod
    def rebalance(test):
        @functools.wraps(test)
        def wrapper(self, *args, **kargs):
            """Trigger cluster rebalance (in and out) when ~half of queries
            reached the goal.
            """
            total_clients = self.parami('total_clients', 1)
            rebalance_after = self.parami('rebalance_after', 0) / total_clients
            self.level_callbacks = [('cur-queries', rebalance_after,
                                     self.latched_rebalance)]
            return test(self, *args, **kargs)
        return wrapper


class MultiClientTests(EVPerfClient):

    """Load tests with consistent number of clients. Each client performs the
    same work.
    """

    @PerfWrapper.multiply
    def test_evperf2(self):
        super(MultiClientTests, self).test_evperf2()

    @PerfWrapper.multiply
    def test_vperf2(self):
        super(MultiClientTests, self).test_vperf2()


class RampUpTests(EVPerfClient):

    """Ramup-up load tests with increasing number of foreground workers.
    """

    @PerfWrapper.rampup
    def test_view_rampup_1(self):
        super(RampUpTests, self).test_vperf2()


class XPerfTests(EVPerfClient):

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

        if self.parami('num_buckets', 1) > 1 and suffix == 'A':
            self.start_replication(slave, master, replication_type, buckets,
                                   suffix='B')

        if bidir:
            self.start_replication(slave, master, replication_type, buckets,
                                   suffix='B')

    def get_buckets(self, reversed=False):
        """Return list of buckets to be replicated"""

        num_buckets = self.parami('num_buckets', 1)
        if num_buckets > 1:
            num_replicated_buckets = self.parami('num_replicated_buckets',
                                                 num_buckets)
            buckets = ['bucket-{0}'.format(i) for i in range(num_buckets)]
            if not reversed:
                return buckets[:num_replicated_buckets]
            else:
                return buckets[-1:-1 - num_replicated_buckets:-1]
        else:
            return [self.param('bucket', 'default')]

    @staticmethod
    def get_ec2_region():
        """Try to identify public hostname and return corresponding EC2 region.

        Reference: http://bit.ly/instancedata
        """

        try:
            uri = 'http://169.254.169.254/latest/meta-data/public-hostname'
            http = httplib2.Http(timeout=5)
            response, content = http.request(uri)
            if 'west' in content:
                return 'west'
            else:
                return 'east'
        except socket.timeout:
            return

    @PerfWrapper.xperf()
    def test_vperf_unidir(self):
        super(XPerfTests, self).test_vperf2()


class RebalanceTests(EVPerfClient):

    """Performance tests with rebalance during test execution.
    """

    @PerfWrapper.rebalance
    def test_view_rebalance_1(self):
        super(RebalanceTests, self).test_vperf2()
