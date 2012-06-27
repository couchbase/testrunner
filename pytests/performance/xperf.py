import functools
import time
import copy
from threading import Thread
from multiprocessing import Process

from membase.api.rest_client import RestConnection

from eperf import EVPerfClient

class XPerfTest(EVPerfClient):
    """XDCR performance tests"""

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

    def benchmark_manager(test):
        @functools.wraps(test)
        def wrapper(self, *args, **kargs):
            # Execute performance test
            result = test(self, *args, **kargs)

            # Define remote cluster and start replication
            master = self.input.clusters[0][0]
            slave = self.input.clusters[1][0]
            self.start_replication(master, slave)

            # Start stats collection thread
            sc = Thread(target=self.collect_replication_stats)
            sc.start()

            # Wait for stats collection thread to stop
            sc.join()

            return result
        return wrapper

    def xperf_manager(bidir=False):
        def decorator(test):
            @functools.wraps(test)
            def wrapper(self, *args, **kargs):
                # Define remote cluster and start replication (only before
                # load phase)
                if (self.parami('prefix', -1) == 0 and
                    self.parami('load_phase', 0) and
                    not self.parami('hot_load_phase', 0)):
                    master = self.input.clusters[0][0]
                    slave = self.input.clusters[1][0]
                    self.start_replication(master, slave, bidir=bidir)

                # Execute performance test
                # 1st test executor:
                self_copy = copy.copy(self)
                self_copy.input.servers = self_copy.input.clusters[0]
                self_copy.input.test_params['bucket'] = self.get_buckets()[0]
                primary = Process(target=test, args=(self_copy, ))
                primary.start()

                # 2nd test executor:
                self.input.servers = self_copy.input.clusters[1]
                self.input.test_params['bucket'] = self.get_buckets(reversed=True)[0]
                self.input.test_params['stats'] = 0
                secondary = test(self, *args, **kargs)

                primary.join()
                return secondary
            return wrapper
        return decorator

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

    def collect_replication_stats(self):
        """Monitor remote replication job and report related stats"""

        # Start general stats collector
        test_params = {'test_time': time.time(), 'test_name': self.id(),
                       'json': 0}

        sc = self.start_stats('load', test_params=test_params, client_id=0)

        # Wait for all items to be replicated
        slave = self.input.clusters[1][0]
        rest = RestConnection(slave)

        num_clients = self.parami('num_clients', len(self.input.clients) or 1)
        target_items = self.parami('items', 0) / num_clients
        replicated_items = 0

        start_time = time.time()
        while replicated_items < target_items:
            stats = rest.fetch_bucket_stats()
            replicated_items = stats['op']['samples']['curr_items'][-1]

            print "Replicated items: {0}".format(replicated_items)
            time.sleep(10)

        # Print average rate
        end_time = time.time()
        elapsed_time = end_time - start_time
        rate = float(target_items/elapsed_time)
        print "Average replication rate: {0:.3f} items/sec".format(rate)

        # Stop general stats collector
        ops = {'start-time': start_time, 'end-time': end_time}
        self.end_stats(sc, ops, 'load')

    @benchmark_manager
    def test_eperf_mixed(self, save_snapshot=False):
        """Mixed workload, get/set commands only"""

        # Run parent test
        super(XPerfTest, self).test_eperf_mixed(save_snapshot)

    @xperf_manager()
    def test_vperf2_unidir(self):
        """1 design document, 8 views"""

        # Run parent test
        super(XPerfTest, self).test_vperf2()
