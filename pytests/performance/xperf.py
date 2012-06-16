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
