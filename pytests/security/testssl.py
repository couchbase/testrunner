from pytests.basetestcase import BaseTestCase
from lib.Cb_constants.CBServer import CbServer
from pytests.security.testssl_util import TestSSL
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection


class TestSSLTests(BaseTestCase):

    def setUp(self):
        super(TestSSLTests, self).setUp()
        self.testssl = TestSSL()
        self.slave_host = TestSSL.SLAVE_HOST

    def tearDown(self):
        super(TestSSLTests, self).tearDown()

    def test_tls_min_version(self):
        """
        Verifies the TLS minimum version of the cluster with the check_version
        """
        self.check_version = self.input.param("check_version", "1.3")
        self.log.info("Verifying for minimum version = {0}".format(self.check_version))
        tls_versions = ["1.3 ", "1.2 ", "1.1 ", "1 "]
        for node in self.servers:
            self.log.info("Testing node {0}".format(node.ip))
            rest = RestConnection(node)
            node_info = "{0}:{1}".format(node.ip, node.port)
            node_services_list = rest.get_nodes_services()[node_info]
            node_port = "18091"
            for service in node_services_list:
                if service == 'kv':
                    node_port = str(CbServer.ssl_port)
                elif service == 'fts':
                    node_port = str(CbServer.ssl_fts_port)
                elif service == 'n1ql':
                    node_port = str(CbServer.ssl_n1ql_port)
                elif service == 'index':
                    node_port = str(CbServer.ssl_index_port)
                elif service == 'eventing':
                    node_port = str(CbServer.ssl_eventing_port)
                elif service == 'backup':
                    node_port = str(CbServer.ssl_backup_port)
                elif service == 'cbas':
                    node_port = str(CbServer.ssl_cbas_port)

                self.log.info("Port being tested: {0} :: Service is: {1}"
                              .format(node_port, service))
                cmd = self.testssl.TEST_SSL_FILENAME + " -p --warnings off --color 0 {0}:{1}" \
                    .format(node.ip, node_port)
                self.log.info('The command is {0}'.format(cmd))
                shell = RemoteMachineShellConnection(self.slave_host)
                output, error = shell.execute_command(cmd)
                output = output.decode().split("\n")
                output1 = ''.join(output)
                self.assertFalse("error" in output1.lower(), msg=output)
                self.assertTrue("tls" in output1.lower(), msg=output)
                for line in output:
                    for version in tls_versions:
                        if "TLS " + version in line and version >= str(self.check_version):
                            self.assertTrue("offered" in line,
                                            msg="TLS {0} is incorrect disabled".format(version))
                        elif "TLS " + version in line and version < str(self.check_version):
                            self.assertTrue("not offered" in line,
                                            msg="TLS {0} is incorrect enabled".format(version))