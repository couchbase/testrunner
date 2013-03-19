import logger

from membase.api.rest_client import RestConnection
from basetestcase import BaseTestCase
from remote.remote_util import RemoteMachineShellConnection


class ConnectionTests(BaseTestCase):

    def setUp(self):
        super(ConnectionTests, self).setUp()

    def tearDown(self):
        super(ConnectionTests, self).tearDown()

    def create_connections_test(self):
        num_connections = self.input.param('num_connections', 1000)
        mem_usage_delta = self.input.param('mem_usage_max_delta', 5)
        servers_in = self.input.param('servers_in', 0)
        process = self.input.param('process', 'beam.smp')

        if servers_in:
            servs_in = self.servers[1:servers_in + 1]
            rebalance = self.cluster.async_rebalance([self.master], servs_in, [])
        shell = RemoteMachineShellConnection(self.master)
        initial_rate = shell.get_mem_usage_by_process(process)
        self.log.info("Usage of memory is %s" % initial_rate)
        connections = []
        try:
            for i in xrange(num_connections):
                rest = RestConnection(self.master)
                t = rest.open_sasl_streaming_connection(self.buckets[0])
                if t is None:
                    self.log.error("Can open only %s threads" % i)
                    break

            result_rate = shell.get_mem_usage_by_process(process)

            self.assertTrue((result_rate - initial_rate) < mem_usage_delta,
                            "Delta %s is more that expected %s" % (result_rate - initial_rate,
                                                                   mem_usage_delta))
            self.log.info("Delta is inside %s" % mem_usage_delta)
            if servers_in:
                rebalance.result()
        finally:
            try:
                rate = shell.get_mem_usage_by_process(process)
                self.log.info("Usage of memory is %s" % rate)
            except:
                pass
            for connection in connections:
                connection.join()
