import functools

from membase.api.rest_client import RestConnection

from eperf import EPerfClient

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

                # Execute performance test
                return test(self, *args, **kargs)
            return _inner_wrapper
        return _outer_wrapper
