import functools
import time
from threading import Thread

from membase.api.rest_client import RestConnection

from eperf import EPerfClient, ViewGen

class XPerfTest(EPerfClient):
    """XDCR performance tests"""

    @staticmethod
    def start_replication(master, slave, replication_type='continuous',
                          buckets=['default'], bidir=False, suffix='A'):
        """Add remote cluster and start replication"""

        master_rest_conn = RestConnection(master)
        remote_reference = 'remote_cluster_' + suffix

        master_rest_conn.add_remote_cluster(slave.ip, slave.port,
                                            slave.rest_username,
                                            slave.rest_password,
                                            remote_reference)

        for bucket in buckets:
            master_rest_conn.start_replication(replication_type,
                                               bucket,
                                               remote_reference)
            ViewGen.create_stats_view(slave, bucket, 'cluster_{0}_'.format(suffix))

        if bidir:
            XPerfTest.start_replication(slave, master, replication_type,
                                        buckets, suffix='B')

    def xperf_manager(bidir=False):
        def _outer_wrapper(test):
            @functools.wraps(test)
            def _inner_wrapper(self, *args, **kargs):
                # Define remote cluster and start replication
                if self.parami('prefix', -1) == 0:
                    master = self.input.clusters[0][0]
                    slave = self.input.clusters[1][0]
                    XPerfTest.start_replication(master, slave, bidir=bidir)

                # Start stats collection thread
                sc = Thread(target=self.collect_replication_stats)
                sc.start()

                # Execute performance test
                result = test(self, *args, **kargs)

                # Wait for stats collection thread to stop
                sc.join()

                return result
            return _inner_wrapper
        return _outer_wrapper

    def collect_replication_stats(self):
        """Monitor remote replication job and report related stats"""

        slave = self.input.clusters[1][0]
        rest = RestConnection(slave)

        num_clients = self.parami('num_clients', len(self.input.clients) or 1)
        target_items = self.parami('items', 0) / num_clients
        replicated_items = 0

        start_time = time.time()
        while replicated_items < target_items:
            stats = rest.query_view('items', 'replicated', 'default',
                                    {'stale': 'false'})
            try:
                replicated_items = int(stats['rows'][0]['value'])
            except IndexError:
                pass

            print "Replicated items: {0}".format(replicated_items)
            time.sleep(10)
        elapsed_time = time.time() - start_time
        rate = float(target_items/elapsed_time)
        print "Average replication rate: {0:.3f} items/sec".format(rate)

    @xperf_manager()
    def test_eperf_mixed(self, save_snapshot=False):
        """Mixed workload, get/set commands only"""

        # Define prefix of source cluster items
        self.input.test_params['cluster_prefix'] = 'cluster_A_'

        # Run parent test
        super(XPerfTest, self).test_eperf_mixed(save_snapshot)
