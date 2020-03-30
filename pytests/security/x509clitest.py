import httplib2
import http.client
import base64
import json
import urllib.request, urllib.parse, urllib.error
import urllib.request, urllib.error, urllib.parse
import ssl
import socket
import paramiko


from security.x509main import x509main
from security.x509tests import x509tests
from remote.remote_util import RemoteMachineShellConnection
from clitest.cli_base import CliBaseTest

class ServerInfo():
    def __init__(self,
                 ip,
                 port,
                 ssh_username,
                 ssh_password,
                 ssh_key=''):

        self.ip = ip
        self.ssh_username = ssh_username
        self.ssh_password = ssh_password
        self.port = port
        self.ssh_key = ssh_key



class X509clitest(x509tests):

    def setUp(self):
        super(X509clitest, self).setUp()
        self.ldapUser = self.input.param('ldapuser', 'Administrator')
        self.ldapPass = self.input.param('ldappass', 'password')
        self.install_path = x509main()._get_install_path(self.master)
        self.slave_host = ServerInfo('127.0.0.1', 22, 'root', 'couchbase')

    def tearDown(self):
        super(X509clitest, self).tearDown()

    def _copy_root_crt(self):
        x509main(self.master)._create_inbox_folder(self.master)
        src_chain_file = x509main.CACERTFILEPATH + x509main.CACERTFILE
        dest_chain_file = self.install_path + x509main.CHAINFILEPATH + "/root.crt"
        x509main(self.master)._copy_node_key_chain_cert(self.master, src_chain_file, dest_chain_file)

    def setup_master(self):
        self._copy_root_crt()
        output, error = self._upload_cert_cli()

    def _upload_cert_cli(self):
        path_to_root_cert = self.install_path + x509main.CHAINFILEPATH + "/root.crt"
        self._copy_root_crt()
        cli_command = 'ssl-manage'
        options = "--upload-cluster-ca={0}".format(path_to_root_cert)
        remote_client = RemoteMachineShellConnection(self.master)
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host="localhost", user=self.ldapUser, password=self.ldapPass)
        return output, error

    def _setup_cluster_nodes(self, host):
        x509main(host)._create_inbox_folder(self.master)
        src_chain_file = x509main.CACERTFILEPATH + "long_chain" + host.ip + ".pem"
        print(src_chain_file)
        dest_chain_file = self.install_path + x509main.CHAINFILEPATH + "/" + x509main.CHAINCERTFILE
        print(dest_chain_file)
        src_node_key = x509main.CACERTFILEPATH + host.ip + ".key"
        print(src_node_key)
        dest_node_key = self.install_path + x509main.CHAINFILEPATH + "/" + x509main.NODECAKEYFILE
        print(dest_node_key)
        x509main(host)._copy_node_key_chain_cert(host, src_chain_file, dest_chain_file)
        x509main(host)._copy_node_key_chain_cert(host, src_node_key, dest_node_key)
        cli_command = 'ssl-manage'
        options = "--set-node-certificate"
        remote_client = RemoteMachineShellConnection(host)
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host="localhost", user=self.ldapUser, password=self.ldapPass)
        return output, error

    def _setup_all_cluster_nodes(self, servers):
        for server in servers:
            self._setup_cluster_nodes(server)

    def _read_crt(self, host, path_cert, cert_name):
        shell = RemoteMachineShellConnection(host)
        command = 'cat ' + (path_cert + cert_name)
        output = shell.execute_command(command)
        return output

    def _retrieve_cluster_cert_extended(self, server):
        cli_command = 'ssl-manage'
        options = "--cluster-cert-info --extended"
        remote_client = RemoteMachineShellConnection(server)
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host="localhost", user=self.ldapUser, password=self.ldapPass)
        return output, error

    def _download_cluster_cert(self, server):
        cli_command = 'ssl-manage'
        options = "--cluster-cert-info"
        remote_client = RemoteMachineShellConnection(server)
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host="localhost", user=self.ldapUser, password=self.ldapPass)
        return output, error

    def _download_node_cert(self, server):
        cli_command = 'ssl-manage'
        options = "--node-cert-info"
        remote_client = RemoteMachineShellConnection(server)
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host=server.ip + ":8091", user=self.ldapUser, password=self.ldapPass)
        return output, error


    def test_upload_cluster_ca(self):
        output, error = self._upload_cert_cli()
        self.assertTrue("SUCCESS: Uploaded cluster certificate" in output[0], "Error message is incorrect")

    def test_setup_nodes(self):
        self.setup_master()
        output, error = self._setup_cluster_nodes(self.master)
        if ("ERROR" in output[0]):
            self.assertTrue(False, "There are issues with command execution")
        else:
            self.assertTrue("SUCCESS: Node certificate set" in output[0], "Output message are incorrect")

    def test_end_to_end_single_node(self):
        output, error = self._upload_cert_cli()
        output, error = self._setup_cluster_nodes(self.master)
        status = x509main(self.master)._validate_ssl_login()
        self.assertEqual(status, 200, "Not able to login via SSL code")

    def test_end_to_end_cluster(self):
        output, error = self._upload_cert_cli()
        self._setup_all_cluster_nodes(self.servers)
        for server in self.servers:
            status = x509main(server)._validate_ssl_login()
            self.assertEqual(status, 200, "Not able to login via SSL code")

    def test_end_to_end_after_cluster(self):
        output, error = self._upload_cert_cli()
        self._setup_all_cluster_nodes(self.servers)
        servers_in = self.servers[1:]
        self.cluster.rebalance(self.servers, servers_in, [])
        for server in self.servers:
            status = x509main(server)._validate_ssl_login()
            self.assertEqual(status, 200, "Not able to login via SSL code")

    def test_retrieve_cluster_cert(self):
        i =0
        output, error = self._upload_cert_cli()
        output, error = self._retrieve_cluster_cert_extended(self.master)
        self.assertTrue("CN=Root Authority" in output[4], "Mismatch in Subject CN")
        self.assertTrue("uploaded" in output[5], "Mismatch in type of certifcate")
        self.assertTrue("Certificate is not signed with cluster CA." in output[9], "Mismatch in warning message")
        self.assertTrue("ns_1@"+self.master.ip in output[10], "Mismatch in node value")
        orig_cert = self._read_crt(self.slave_host, x509main.CACERTFILEPATH, x509main.CACERTFILE)
        self.assertTrue(((output[3])[45:92] in orig_cert[0]), "Certificates dont match")


    def test_download_cluster_cert(self):
        output, error = self._upload_cert_cli()
        output, error = self._setup_cluster_nodes(self.master)
        output, error = self._download_cluster_cert(self.master)
        orig_cert = self._read_crt(self.slave_host, x509main.CACERTFILEPATH, x509main.CACERTFILE)
        if output[1] not in orig_cert[0]:
            self.assertFalse(True, 'Downloaded cert and orig cert don"t match')


    def test_node_cert(self):
        output, error = self._upload_cert_cli()
        output, error = self._setup_cluster_nodes(self.master)
        output, error = self._download_node_cert(self.master)
        if "CN="+self.master.ip not in output[3]:
            self.assertFalse(True, "CN does not match")
        orig_cert = self._read_crt(self.slave_host, x509main.CACERTFILEPATH, self.master.ip + ".crt")
        if (output[2])[45:92] not in orig_cert[0]:
            self.assertFalse(True, "Certificate does not match")
