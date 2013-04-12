import logger

import os

from membase.api.rest_client import RestConnection
from basetestcase import BaseTestCase
from remote.remote_util import RemoteMachineShellConnection
from scripts.memcachetest_runner import MemcachetestRunner


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

    def multiple_connections_using_memcachetest (self):
        mcsoda_items = self.input.param('mcsoda_items', 1000000)
        memcachetest_items = self.input.param('memcachetest_items', 100000)
        moxi_port = self.input.param('moxi_port', 51500)
        self.stop_moxi(self.master, moxi_port)
        self.stop_mcsoda_localy(moxi_port)
        try:
            self.run_moxi(self.master, moxi_port, self.master.ip, "default")
            self.run_mcsoda_localy(self.master.ip, moxi_port, "default", mcsoda_items=mcsoda_items)
            self.sleep(30)
            sd = MemcachetestRunner(self.master, num_items=memcachetest_items, extra_params="-W 320 -t 320 -c 0 -M 2")
            status = sd.start_memcachetest()
            if not status:
                self.fail("see logs above!")
        finally:
            self.stop_mcsoda_localy(moxi_port)
            if 'sd' in locals():
                sd.stop_memcachetest()


    def run_moxi(self, server, port_listen, node_ip, bucket_name):
        command = ("nohup /opt/couchbase/bin/moxi -u root -Z usr=Administrator,pwd=password,port_listen={0}," +
                "concurrency=1024,wait_queue_timeout=200,connect_timeout=400,connect_max_errors=3," +
                "connect_retry_interval=30000,auth_timeout=100,downstream_conn_max=16,downstream_timeout=5000" +
                ",cycle=200,default_bucket_name={2} http://{1}:8091/pools/default/bucketsStreaming/{2} -d").\
                   format(port_listen, node_ip, bucket_name)
        shell = RemoteMachineShellConnection(server)
        output, error = shell.execute_command_raw(command)
        shell.log_command_output(output, error)
        shell.disconnect()

    def stop_moxi(self, server, port_listen):
        command = "kill -9 $(ps aux | grep -v grep | grep {0} | awk '{{print $2}}')".format(port_listen)
        shell = RemoteMachineShellConnection(server)
        output, error = shell.execute_command_raw(command)
        shell.log_command_output(output, error)
        shell.disconnect()

    def run_mcsoda_localy(self, node_ip, port_ip, bucket_name, mcsoda_items):
        command = ("nohup python lib/perf_engines/mcsoda.py {0}:{1} vbuckets=1024 " +
            "doc-gen=0 doc-cache=0 ratio-creates=0.9 ratio-sets=0.9 min-value-size=1024 " +
            "max-items={2} ratio-deletes=0.1 exit-after-creates=0 threads=4 prefix={3} &").\
            format(node_ip, port_ip, mcsoda_items, bucket_name)
        os.system(command)

    def stop_mcsoda_localy(self, port_listen):
        command = "kill -9 $(ps aux | grep -v grep | grep {0} | awk '{{print $2}}')".format(port_listen)
        os.system(command)
