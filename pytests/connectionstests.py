import os

from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection
from basetestcase import BaseTestCase
from remote.remote_util import RemoteMachineShellConnection, RemoteMachineHelper
from scripts.memcachetest_runner import MemcachetestRunner
from testconstants import COUCHBASE_FROM_SPOCK


class ConnectionTests(BaseTestCase):

    def setUp(self):
        super(ConnectionTests, self).setUp()

    def tearDown(self):
        super(ConnectionTests, self).tearDown()

    def _run_moxi(self, server, port_listen, node_ip, bucket_name):
        command = ("nohup /opt/couchbase/bin/moxi -u root -Z usr=Administrator,pwd=password,port_listen={0}," +
                "concurrency=1024,wait_queue_timeout=200,connect_timeout=400,connect_max_errors=3," +
                "connect_retry_interval=30000,auth_timeout=100,downstream_conn_max=16,downstream_timeout=5000" +
                ",cycle=200,default_bucket_name={2} http://{1}:8091/pools/default/bucketsStreaming/{2} -d").\
                   format(port_listen, node_ip, bucket_name)
        shell = RemoteMachineShellConnection(server)
        output, error = shell.execute_command_raw(command)
        shell.log_command_output(output, error)
        shell.disconnect()

    def _stop_moxi(self, server, port_listen):
        command = "kill -9 $(ps aux | grep -v grep | grep {0} | awk '{{print $2}}')".format(port_listen)
        shell = RemoteMachineShellConnection(server)
        output, error = shell.execute_command_raw(command)
        shell.log_command_output(output, error)
        shell.disconnect()

    def _run_mcsoda_localy(self, node_ip, port_ip, bucket_name, mcsoda_items):
        command = ("nohup python lib/perf_engines/mcsoda.py {0}:{1} vbuckets=1024 " +
            "doc-gen=0 doc-cache=0 ratio-creates=0.9 ratio-sets=0.9 min-value-size=1024 " +
            "max-items={2} ratio-deletes=0.1 exit-after-creates=0 threads=4 prefix={3} &").\
            format(node_ip, port_ip, mcsoda_items, bucket_name)
        os.system(command)

    def _stop_mcsoda_localy(self, port_listen):
        command = "kill -9 $(ps aux | grep -v grep | grep {0} | awk '{{print $2}}')".format(port_listen)
        os.system(command)

    def create_connections_test(self):

        shell = RemoteMachineShellConnection(self.master)
        os_type = shell.extract_remote_info()
        if os_type.type != 'Linux':
            return

        num_connections = self.input.param('num_connections', 1000)
        mem_usage_delta = self.input.param('mem_usage_max_delta', 5)
        servers_in = self.input.param('servers_in', 0)
        process = self.input.param('process', 'beam.smp')

        shell = RemoteMachineShellConnection(self.master)
        initial_rate = shell.get_mem_usage_by_process(process)
        self.log.info("Usage of memory is %s" % initial_rate)
        if servers_in:
            servs_in = self.servers[1:servers_in + 1]
            rebalance = self.cluster.async_rebalance([self.master], servs_in, [])
        try:
            self.log.info("**** Start opening sasl streaming connection ***")
            for i in range(num_connections):
                rest = RestConnection(self.master)
                t = rest.open_sasl_streaming_connection(self.buckets[0])
                if t is None:
                    self.log.error("Can open only %s threads" % i)
                    break
                if i % 50 == 0:
                    rate = shell.get_mem_usage_by_process(process)
                    self.log.info("Usage of memory after {0} created connections: {1}".format(i, rate))

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

    """ this test need to install autoconf and automake on master vm """
    def multiple_connections_using_memcachetest (self):
        """ server side moxi is removed in spock as in MB-16661 """
        if self.cb_version[:5] in COUCHBASE_FROM_SPOCK:
            self.log.info("From spock, server side moxi is removed."
                          " More information could be found in MB-16661 ")
            return
        shell = RemoteMachineShellConnection(self.master)
        os_type = shell.extract_remote_info()
        if os_type.type != 'Linux':
            return
        mcsoda_items = self.input.param('mcsoda_items', 1000000)
        memcachetest_items = self.input.param('memcachetest_items', 100000)
        moxi_port = self.input.param('moxi_port', 51500)
        self._stop_moxi(self.master, moxi_port)
        self._stop_mcsoda_localy(moxi_port)
        try:
            self._run_moxi(self.master, moxi_port, self.master.ip, "default")
            self._run_mcsoda_localy(self.master.ip, moxi_port, "default",
                                                    mcsoda_items=mcsoda_items)
            self.sleep(30)
            sd = MemcachetestRunner(self.master, num_items=memcachetest_items, \
                                     extra_params="-W 16 -t 16 -c 0 -M 2")  # MB-8083
            status = sd.start_memcachetest()
            if not status:
                self.fail("see logs above!")
        finally:
            self._stop_mcsoda_localy(moxi_port)
            if 'sd' in locals():
                sd.stop_memcachetest()

    # CBQE-1245 implement verification tap client activity when the client is not using a TAP client
    def checks_tap_connections_tests(self):
        servs_init = self.servers[:self.nodes_init]
        servs_in = [self.servers[i + self.nodes_init] for i in range(self.nodes_in)]
        servs_out = [self.servers[self.nodes_init - i - 1] for i in range(self.nodes_out)]
        rest = RestConnection(self.master)
        buckets_stats_before = {}
        for bucket in self.buckets:
            _, result = rest.get_bucket_stats_json(bucket)
            buckets_stats_before[bucket.name] = result["op"]["samples"]["ep_tap_user_count"];
        self.log.info("current nodes : {0}".format([node.id for node in rest.node_statuses()]))
        self.log.info("adding nodes {0} to cluster".format(servs_in))
        self.log.info("removing nodes {0} from cluster".format(servs_out))
        result_nodes = set(servs_init + servs_in) - set(servs_out)
        self.cluster.rebalance(servs_init[:self.nodes_init], servs_in, servs_out)
        gen = BlobGenerator('mike2', 'mike2-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.verify_cluster_stats(result_nodes)
        buckets_stats_after = {}
        for bucket in self.buckets:
            _, result = rest.get_bucket_stats_json(bucket)
            buckets_stats_after[bucket.name] = result["op"]["samples"]["ep_tap_user_count"];
            for stat in buckets_stats_after[bucket.name][len(buckets_stats_before[bucket.name]) - 1:]:
                if stat != 0:
                    self.log.error("'ep_tap_user_count' for bucket '{0}' before test:{1}".format(bucket.name, buckets_stats_before[bucket.name]))
                    self.log.error("'ep_tap_user_count' for bucket '{0}' after test:{1}".format(bucket.name, buckets_stats_after[bucket.name]))
                    self.log.error("'ep_tap_user_count' != 0 as expected");
            self.log.info("'ep_tap_user_count' for bucket '{0}' = 0 for the entire test".format(bucket.name));

    # CBQE-1489 memcached recovery
    def test_kill_memcached(self):
        node_to_kill_mem = self.servers[:self.nodes_init][-1]
        iterations = self.input.param('iterations', 10)
        shell = RemoteMachineShellConnection(node_to_kill_mem)
        timeout = 10
        try:
            for i in range(iterations):
                shell.kill_memcached()
                self.sleep(timeout, "wait for memcached recovery...")
                self.assertTrue(RemoteMachineHelper(shell).is_process_running('memcached'),
                                "Memcached wasn't recover during %s seconds" % timeout)
        finally:
            shell.disconnect()

    def test_memcahed_t_option(self):
        rest = RestConnection(self.master)
        rest.change_memcached_t_option(8)
        self.sleep(5, 'wait some time before restart')
        remote = RemoteMachineShellConnection(self.master)
        o, _ = remote.execute_command('ls /tmp | grep dump')
        remote.stop_server()
        remote.start_couchbase()
        o_new, _ = remote.execute_command('ls /tmp | grep dump')
        self.assertTrue(len(o) == len(o_new), 'new core dump appeared: %s' % (set(o_new) - set(o)))
