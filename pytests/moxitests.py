import logger

from couchbase.documentgenerator import BlobGenerator
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


    def _run_moxi(self, cb_server, bucket_name):
        command = ("nohup /opt/moxi/bin/moxi -u root -Z usr={0},pwd={1},port_listen={2}," +
                "concurrency=1024,wait_queue_timeout=200,connect_timeout=400,connect_max_errors=3," +
                "connect_retry_interval=30000,auth_timeout=100,downstream_conn_max=16,downstream_timeout=5000" +
                ",cycle=200,default_bucket_name={3} http://{4}:{5}/pools/default/bucketsStreaming/{3} -d").\
                   format(cb_server.rest_username, cb_server.rest_password, self.moxi_port, bucket_name,
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
        for bucket in self.buckets:
            try:
                self._run_moxi(self.master, bucket.name)
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
        self.verify_cluster_stats(self.servers[:self.nodes_init])
