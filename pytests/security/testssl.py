import time

from lib.Cb_constants.CBServer import CbServer
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from pytests.basetestcase import BaseTestCase
from pytests.security.testssl_util import TestSSL
from pytests.security.x509_multiple_CA_util import x509main
from pytests.security.ntonencryptionBase import ntonencryptionBase


class TestSSLTests(BaseTestCase):

    def setUp(self):
        super(TestSSLTests, self).setUp()
        self.testssl = TestSSL()
        self.slave_host = TestSSL.SLAVE_HOST
        self.ports_to_scan = ["18091", "18092"]

    def tearDown(self):
        super(TestSSLTests, self).tearDown()

    @staticmethod
    def get_service_ports(node):
        rest = RestConnection(node)
        node_info = "{0}:{1}".format(node.ip, node.port)
        node_services_list = rest.get_nodes_services()[node_info]
        service_ports = []
        for service in node_services_list:
            if service == "kv":
                service_ports.append(CbServer.ssl_memcached_port)
            elif service == "fts":
                service_ports.append(CbServer.ssl_fts_port)
            elif service == "n1ql":
                service_ports.append(CbServer.ssl_n1ql_port)
            elif service == "index":
                service_ports.append(CbServer.ssl_index_port)
            elif service == "eventing":
                service_ports.append(CbServer.ssl_eventing_port)
            elif service == "backup":
                service_ports.append(CbServer.ssl_backup_port)
            elif service == "cbas":
                service_ports.append(CbServer.ssl_cbas_port)
        return service_ports

    def test_tls_min_version(self):
        """
        Verifies the TLS minimum version of the cluster with the check_version
        """
        self.check_version = self.input.param("check_version", "1.3")
        self.log.info("Verifying for minimum version = {0}".format(self.check_version))
        tls_versions = ["1.3  ", "1.2  ", "1.1  ", "1  "]
        for node in self.servers:
            self.log.info("Testing node {0}".format(node.ip))
            ports_to_scan = self.get_service_ports(node)
            ports_to_scan.extend(self.ports_to_scan)
            for node_port in ports_to_scan:
                self.log.info("Port being tested: {0}".format(node_port))
                cmd = self.testssl.TEST_SSL_FILENAME + " -p --warnings off --color 0 {0}:{1}" \
                    .format(node.ip, node_port)
                self.log.info("The command is {0}".format(cmd))
                shell = RemoteMachineShellConnection(self.slave_host)
                output, error = shell.execute_command(cmd)
                shell.disconnect()
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

    def test_tls_1_dot_2_blocking(self):
        """
        1. Set tls version = 1.3
        2. Restart couchbase server
        3. Verify tls version = 1.3 and not set to 1.2(default)
        """
        rest = RestConnection(self.master)
        rest.set_min_tls_version(version="tlsv1.3")
        self.test_tls_min_version()
        try:
            for node in self.servers:
                shell = RemoteMachineShellConnection(node)
                shell.stop_couchbase()
                time.sleep(10)
                shell.start_couchbase()
                shell.disconnect()
        except Exception as e:
            self.fail(e)
        self.test_tls_min_version()

    def test_port_security(self):
        """
        Scanning the ports to test vulnerabilities
        """
        for node in self.servers:
            self.log.info("Testing node {0}".format(node.ip))
            ports_to_scan = self.get_service_ports(node)
            ports_to_scan.extend(self.ports_to_scan)
            for node_port in ports_to_scan:
                self.log.info("Port being tested: {0}".format(node_port))
                cmd = self.testssl.TEST_SSL_FILENAME + " --warnings off --color 0 {0}:{1}" \
                    .format(node.ip, node_port)
                self.log.info("The command is {0}".format(cmd))
                shell = RemoteMachineShellConnection(self.slave_host)
                output, error = shell.execute_command(cmd)
                shell.disconnect()
                output = output.decode().split("\n")
                check_next = 0
                stmt = ""
                scan_count = 0
                for line in output:
                    if check_next == 1:
                        if stmt == "Certificate Validity":
                            if ">= 10 years is way too long" in line:
                                self.log.info(">= 10 years is way too long. (Fix moved to "
                                              "Morpheus MB-50249)".format(node_port))
                            check_next = 0
                            stmt = ""

                    # Testing Protocols
                    elif "SSLv2  " in line or "SSLv3  " in line:
                        scan_count = scan_count + 1
                        if "offered (NOT ok)" in line:
                            self.fail("SSLvx is offered on port {0}".format(node_port))

                    # Testing Cipher Categories
                    elif "LOW: 64 Bit + DES, RC[2,4]" in line:
                        scan_count = scan_count + 1
                        if "offered (NOT ok)" in line:
                            self.fail("Cipher is not ok on port {0}".format(node_port))

                    # Testing Server's Cipher Preferences
                    elif "Has server cipher order?" in line:
                        scan_count = scan_count + 1
                        if "no" in line:
                            self.fail("Server cipher ordering not set".format(node_port))

                    # Testing Server Defaults
                    elif "Certificate Validity" in line:
                        scan_count = scan_count + 1
                        check_next = 1
                        stmt = "Certificate Validity"

                    # Testing Vulnerabilities
                    elif "Heartbleed (CVE-2014-0160)" in line:
                        scan_count = scan_count + 1
                        if "VULNERABLE (NOT ok)" in line:
                            self.fail("Heartbleed vulnerability on port {0}".format(node_port))
                    elif "ROBOT" in line:
                        scan_count = scan_count + 1
                        if "VULNERABLE (NOT ok)" in line:
                            self.fail("Robot vulnerability on port {0}".format(node_port))
                    elif "Secure Client-Initiated Renegotiation" in line:
                        scan_count = scan_count + 1
                        if "VULNERABLE (NOT ok)" in line:
                            self.fail("Renegotiation vulnerability on port {0}"
                                      .format(node_port))
                    elif "CRIME, TLS" in line:
                        scan_count = scan_count + 1
                        if "VULNERABLE (NOT ok)" in line:
                            self.fail("Crime vulnerability on port {0}".format(node_port))
                    elif "POODLE, SSL (CVE-2014-3566)" in line:
                        scan_count = scan_count + 1
                        if "VULNERABLE (NOT ok)" in line:
                            self.fail("Poodle vulnerability on port {0}".format(node_port))
                    elif "SWEET32" in line:
                        scan_count = scan_count + 1
                        if "VULNERABLE (NOT ok)" in line:
                            self.fail("Sweet32 vulnerability on port {0}".format(node_port))
                    elif "LOGJAM (CVE-2015-4000)" in line:
                        scan_count = scan_count + 1
                        if "VULNERABLE (NOT ok)" in line:
                            self.fail("LogJam vulnerability on port {0}".format(node_port))
                    elif "LUCKY13 (CVE-2013-0169)" in line:
                        scan_count = scan_count + 1
                        if "VULNERABLE (NOT ok)" in line:
                            self.fail("Lucky13 vulnerability on port {0}".format(node_port))
                    elif "RC4 (CVE-2013-2566, CVE-2015-2808)" in line:
                        scan_count = scan_count + 1
                        if "VULNERABLE (NOT ok)" in line:
                            self.fail("RC4 ciphers detected on port {0}".format(node_port))

                    elif "Medium grade encryption" in line:
                        scan_count = scan_count + 1
                        if "not offered (OK)" not in line:
                            self.fail("Medium grade encryption is offered on port {0}"
                                      .format(node_port))
                self.assertTrue(scan_count == 14,
                                msg="Test didn't complete all the scans for port {0}"
                                .format(node_port))

    def test_port_security_with_encryption(self):
        """
        Scanning the ports to test vulnerabilities with cluster encryption enabled
        """
        ntonencryptionBase().setup_nton_cluster(servers=self.servers, ntonStatus="enable",
                                                clusterEncryptionLevel="all")
        self.test_port_security()
        CbServer.use_https = True
        ntonencryptionBase().setup_nton_cluster(servers=self.servers, ntonStatus="enable",
                                                clusterEncryptionLevel="strict")
        self.test_port_security()

    def test_port_security_with_certificates(self):
        """
        Scanning the ports to test vulnerabilities with certificates
        """
        self.x509 = x509main(host=self.master)
        self.x509.generate_multiple_x509_certs(servers=self.servers)
        for server in self.servers:
            _ = self.x509.upload_root_certs(server)
        self.x509.upload_node_certs(servers=self.servers)
        self.x509.delete_unused_out_of_the_box_CAs(server=self.master)
        self.x509.upload_client_cert_settings(server=self.servers[0])
        CbServer.use_https = True
        ntonencryptionBase().setup_nton_cluster(servers=self.servers,
                                                clusterEncryptionLevel="strict")
        self.test_port_security()
