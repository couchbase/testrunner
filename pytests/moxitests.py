import logger
import struct, socket

from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection
from basetestcase import BaseTestCase
from remote.remote_util import RemoteMachineShellConnection
from memcached.helper.data_helper import MemcachedClientHelper
from memcacheConstants import MEMCACHED_REQUEST_MAGIC, DATA_TYPE,\
                              VBUCKET

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
        except Exception as ex:
            self.tearDown()
            raise ex

    def tearDown(self):
        super(MoxiTests, self).tearDown()
        if hasattr(self, 'moxi_server') and hasattr(self, 'moxi_port'):
            self._stop_moxi()


    def _run_moxi(self, cb_server, bucket, option = " "):
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
        command += option
        shell = RemoteMachineShellConnection(self.moxi_server)
        output, error = shell.execute_command_raw(command)
        shell.log_command_output(output, error)
        shell.disconnect()
        self.sleep(5, "sleep during moxi start")

    def _stop_moxi(self):
        self.log.info("kill moxi server at %s " % self.moxi_server.ip)
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
                                           bucket.kvs[1], "create", proxy_client=moxi_client,
                                           compression=self.sdk_compression)
                if self.ops in ['update', 'delete', 'read']:
                    self.cluster.load_gen_docs(self.master, bucket.name, self.gen_load,
                                           bucket.kvs[1], self.ops, proxy_client=moxi_client,
                                               compression=self.sdk_compression)
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


    def test_moxi_cve_2016_8704(self):
        """
        An integer overflow can cause heap overflow if bodylen less than keylen
        First, set a regular key
        Second, set key above again with bodylen < and keylen + extlen
        if there is no fixes, memcached could crash
        If issue cve_2016_8704 is fixed, there is no memcached crash.
        Memcached just close connection.
        Affected commands: append, appendQ, prepend, prependQ since there is no
        check on body length
        """
        OPCODE_PREPEND_Q = "\x1a"
        keylen = struct.pack("!H", 0xfa)
        extlen = "\x00"
        bodylen = struct.pack("!I", 0)
        opaque = struct.pack("!I", 0)
        CAS = struct.pack("!Q", 0)
        body = "A"*1024

        packet = MEMCACHED_REQUEST_MAGIC + OPCODE_PREPEND_Q + keylen + extlen
        packet += DATA_TYPE + VBUCKET + bodylen + opaque + CAS
        packet += body

        set_packet = "set testkey 0 60 4\r\ntest\r\n"
        get_packet = "get testkey\r\n"

        option = " -vv > %s.out 2>&1" % self.moxi_server.ip
        for bucket in self.buckets:
            try:
                self._run_moxi(self.master, bucket, option=option)
                mc1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                mc1.connect((self.moxi_server.ip, int(self.moxi_port)))
                mc1.sendall(set_packet)
                self.log.info("key status: %s " % mc1.recv(1024))
                mc1.close()

                mc2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                mc2.connect((self.moxi_server.ip, int(self.moxi_port)))
                mc2.sendall(packet)
                self.log.info("Invalid key status: %s " % mc2.recv(1024))
                mc2.close()
                shell = RemoteMachineShellConnection(self.moxi_server)
                grep_cmd = "grep 'Content of packet bigger than encoded body: 0xfa > 0x0' %s.out "\
                                                                              % self.moxi_server.ip
                o, e = shell.execute_command_raw(grep_cmd)
                if o:
                    self.assertTrue('Content of packet bigger than encoded body: 0xfa > 0x0' in o[0],
                                    "Memcached failed in bodylen < and keylen + extlen")
                else:
                    self.log.error("there is no error in output")
                shell.disconnect()

                mc3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                mc3.connect((self.moxi_server.ip, int(self.moxi_port)))
                mc3.sendall(get_packet)
                self.log.info("get key value: %s " % mc3.recv(1024))
                mc3.close()
            finally:
                self._stop_moxi()
                shell = RemoteMachineShellConnection(self.moxi_server)
                shell.execute_command_raw("rm %s.out" % self.moxi_server.ip)
                shell.disconnect()


    def test_moxi_cve_2016_8705(self):
        """
            This bug affected commands: set, setQ, add, addQ, replace, replaceQ
            The different of signed and unsigned integer could trigger an integer
            overflow and leads to heap buffer overflow
            To trigger this bug, bodylen must greater than keylen
        """

        OPCODE_ADD = "\x02"
        keylen = struct.pack("!H", 0xfa)
        extlen = "\x08"
        bodylen = struct.pack("!I", 0xffffffd0)
        opaque = struct.pack("!I", 0)
        CAS = struct.pack("!Q", 0)
        extras_flags = 0xdeadbeef
        extras_expiry = struct.pack("!I", 0xe10)
        body = "A"*1024

        packet = MEMCACHED_REQUEST_MAGIC + OPCODE_ADD + keylen + extlen
        packet += DATA_TYPE + VBUCKET + bodylen + opaque + CAS
        packet += body

        option = " -vv > %s.out 2>&1" % self.moxi_server.ip
        for bucket in self.buckets:
            try:
                self._run_moxi(self.master, bucket, option=option)
                mc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                mc.connect((self.moxi_server.ip, int(self.moxi_port)))
                mc.sendall(packet)
                self.log.info("key status: %s " % mc.recv(1024))
                mc.close()

                self.log.info("check if memcached handle this bug in log")
                shell = RemoteMachineShellConnection(self.moxi_server)
                grep_cmd = "grep 'Packet too big: 0xffffffd0' %s.out " % self.moxi_server.ip
                o, e = shell.execute_command_raw(grep_cmd)
                if o:
                    self.assertTrue('Packet too big: 0xffffffd0' in o[0],
                                "Memcached failed in bodylen < and keylen + extlen")
                else:
                    self.log.error("there is no error in output")
                shell.disconnect()
            finally:
                self._stop_moxi()
                shell = RemoteMachineShellConnection(self.moxi_server)
                shell.execute_command_raw("rm %s.out" % self.moxi_server.ip)
                shell.disconnect()

    def test_moxi_cve_2016_8706(self):
        """
            This test check the affected command: SASL Auth whose opcode is 0x21.
            if bodylen smaller than keylen, an integer overflow will occurs and
            trigger heap buffer overflow
            If this bug was fixed, there would be no memcached crashed
        """

        OPCODE_SET = "\x21"
        keylen = struct.pack("!H", 32)
        bodylen = struct.pack("!I", 1)
        packet = MEMCACHED_REQUEST_MAGIC + OPCODE_SET + keylen +   bodylen*2 + "A"*1000

        option = " -vv > %s.out 2>&1" % self.moxi_server.ip
        for bucket in self.buckets:
            try:
                self._run_moxi(self.master, bucket, option=option)
                mc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                mc.connect((self.moxi_server.ip, int(self.moxi_port)))
                mc.sendall(packet)
                self.log.info("key status: %s " % mc.recv(1024))
                mc.close()

                self.log.info("check if memcached handle this bug in log")
                shell = RemoteMachineShellConnection(self.moxi_server)
                grep_cmd = "grep 'Content of packet bigger than encoded body: 0x20 > 0x1' %s.out "\
                                                                              % self.moxi_server.ip
                o, e = shell.execute_command_raw(grep_cmd)
                shell.disconnect()
                if o:
                    self.assertTrue('Content of packet bigger than encoded body: 0x20 > 0x1' in o[0],
                                    "Memcached failed in bodylen < and keylen + extlen")
                else:
                    self.log.error("there is no error in output")
            finally:
                self._stop_moxi()
                shell = RemoteMachineShellConnection(self.moxi_server)
                shell.execute_command_raw("rm %s.out" % self.moxi_server.ip)
                shell.disconnect()
