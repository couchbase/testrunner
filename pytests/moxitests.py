import logger

from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection
from basetestcase import BaseTestCase
from remote.remote_util import RemoteMachineShellConnection
from memcached.helper.data_helper import MemcachedClientHelper

class MoxiTests(BaseTestCase):

    def setUp(self):
        super(MoxiTests, self).setUp()
        self.gen_load = BlobGenerator('moxi', 'moxi-', self.value_size, end=self.num_items)
        self.moxi_port = self.input.param('moxi_port', 51500)
        self.ops = self.input.param('doc_ops', 'create')
        self.cluster_ops = self.input.param("ops", [])
        if self.cluster_ops:
            self.cluster_ops = self.cluster_ops.split(';')
        try:
            self.assertTrue(self.master != self.moxi_server, 'There are not enough vms!')
            self._stop_moxi()
        except Exception, ex:
            self.tearDown()
            raise ex

    def tearDown(self):
        super(MoxiTests, self).tearDown()
        if hasattr(self, 'moxi_server') and hasattr(self, 'moxi_port'):
            self._stop_moxi()


    def _run_moxi(self, cb_server, bucket):
        command = ("nohup /opt/moxi/bin/moxi -u root -Z usr={0},pwd={1},port_listen={2}," +
                "concurrency=1024,wait_queue_timeout=200,connect_timeout=400,connect_max_errors=3," +
                "connect_retry_interval=30000,auth_timeout=100,downstream_conn_max=16,downstream_timeout=5000" +
                ",cycle=200,default_bucket_name={3} http://{4}:{5}/pools/default/bucketsStreaming/{3} -d").\
                   format(cb_server.rest_username, cb_server.rest_password, self.moxi_port, bucket.name,
                          cb_server.ip, (cb_server.port or '8091'))
        if bucket.name != 'default' and bucket.authType == "sasl":
            command = ("nohup /opt/moxi/bin/moxi -u root -Z usr={0},pwd={1},port_listen={2}," +
                "concurrency=1024,wait_queue_timeout=200,connect_timeout=400,connect_max_errors=3," +
                "connect_retry_interval=30000,auth_timeout=100,downstream_conn_max=16,downstream_timeout=5000" +
                ",cycle=200 http://{4}:{5}/pools/default/bucketsStreaming/{3} -d").\
                   format(bucket.name, bucket.saslPassword, self.moxi_port, bucket.name,
                          cb_server.ip, (cb_server.port or '8091'))
        shell = RemoteMachineShellConnection(self.moxi_server)
        output, error = shell.execute_command_raw(command)
        shell.log_command_output(output, error)
        shell.disconnect()
        self.sleep(5, "sleep during moxi start")

    def _stop_moxi(self):
        command = "kill -9 $(ps aux | grep -v grep | grep {0} | awk '{{print $2}}')".format(self.moxi_port)
        shell = RemoteMachineShellConnection(self.moxi_server)
        output, error = shell.execute_command_raw(command)
        shell.log_command_output(output, error)
        shell.disconnect()

    def test_moxi_ops (self):
        tasks = self.run_ops()
        for bucket in self.buckets:
            try:
                self._run_moxi(self.master, bucket)
                moxi_client = MemcachedClientHelper.standalone_moxi_client(self.moxi_server, bucket,
                                                                           moxi_port=self.moxi_port)
                self.sleep(30)
                self.cluster.load_gen_docs(self.master, bucket.name, self.gen_load,
                                           bucket.kvs[1],"create", proxy_client=moxi_client)
                if self.ops in ['update', 'delete', 'read']:
                    self.cluster.load_gen_docs(self.master, bucket.name, self.gen_load,
                                           bucket.kvs[1], self.ops, proxy_client=moxi_client)
            finally:
                self._stop_moxi()
        for task in tasks:
            task.result()
        self.verify_cluster_stats(self.servers[:self.nodes_init])

    def test_debug_symbols(self):
        shell = RemoteMachineShellConnection(self.moxi_server)
        try:
            command = 'file /opt/moxi/bin/moxi.actual'
            output, error = shell.execute_command_raw(command)
            shell.log_command_output(output, error)
            self.assertTrue(''.join(output).find('dynamically linked') != -1 and\
                            ''.join(output).find('not stripped') != -1,
                            "MB-9842 Debug symbols are not included")
        finally:
            shell.disconnect()

    def run_ops(self):
        tasks = []
        if not self.cluster_ops:
            return tasks
        if 'rebalance_in' in self.cluster_ops:
            tasks.append(self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                self.servers[self.nodes_init:self.nodes_init + self.nodes_in], []))
            self.nodes_init += self.nodes_in
        elif 'rebalance_out' in self.cluster_ops:
            tasks.append(self.cluster.async_rebalance(self.servers[:self.nodes_init],
                    [], self.servers[(self.nodes_init - self.nodes_out):self.nodes_init]))
            self.nodes_init -= self.nodes_out
        elif 'failover' in self.cluster_ops:
            tasks.append(self.cluster.failover(self.servers[:self.nodes_init],
                    self.servers[(self.nodes_init - self.nodes_out):self.nodes_init]))
            self.sleep(10)
            self.nodes_init -= self.nodes_out
        return tasks