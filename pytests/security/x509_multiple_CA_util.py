import base64
import importlib
import json
import random
import os
import copy
import fileinput
import sys
import logger
import httplib2

from shutil import copyfile
from lib.Cb_constants.CBServer import CbServer
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection


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


class x509main:
    WININSTALLPATH = "C:/Program Files/Couchbase/Server/var/lib/couchbase/"
    LININSTALLPATH = "/opt/couchbase/var/lib/couchbase/"
    MACINSTALLPATH = "/Users/couchbase/Library/Application Support/Couchbase/var/lib/couchbase/"
    CACERTFILEPATH = "/tmp/multiple_certs_test_random" + str(random.randint(1, 100)) + "/"
    CHAINFILEPATH = "inbox"
    TRUSTEDCAPATH = "CA"
    SLAVE_HOST = ServerInfo('127.0.0.1', 22, 'root', 'couchbase')
    CLIENT_CERT_AUTH_JSON = 'client_cert_auth1.json'
    CLIENT_CERT_AUTH_TEMPLATE = 'client_cert_config_template.txt'
    IP_ADDRESS = '172.16.1.174'
    CLIENT_CERT_KEY = CACERTFILEPATH + IP_ADDRESS + ".key"
    CLIENT_CERT_PEM = CACERTFILEPATH + IP_ADDRESS + ".pem"
    INT_EXTENSIONS_FILE = "./pytests/security/v3_ca.ext"
    CERT_EXTENSIONS_FILE = "./pytests/security/clientconf.conf"

    root_ca_names = []  # list of active root certs
    manifest = {}  # active CA manifest
    node_ca_map = {}  # {<node_ip>: {signed_by: <int_ca_name>, path: <node_ca_dir>}}
    client_ca_map = {}  # {<client_ca_name>:  {signed_by: <int_ca_name>, path: <client_ca_dir>}}
    ca_count = 0  # total count of active root certs

    def __init__(self,
                 host=None,
                 client_cert_state="enable",
                 paths="subject.cn:san.dnsname:san.uri",
                 prefixs="www.cb-:us.:www.", delimeter=".:.:.",
                 client_ip="172.16.1.174", dns=None, uri=None,
                 alt_names="default",
                 ssltype="openssl", encryption_type="",
                 key_length=1024):
        self.log = logger.Logger.get_logger()

        if host is not None:
            self.host = host
            self.install_path = self._get_install_path(self.host)
        self.slave_host = x509main.SLAVE_HOST
        self.disable_ssl_certificate_validation = False
        if CbServer.use_https:
            self.disable_ssl_certificate_validation = True

        self.client_cert_state = client_cert_state
        self.paths = paths.split(":")
        self.prefixs = prefixs.split(":")
        self.delimeters = delimeter.split(":")
        self.client_ip = client_ip
        self.dns = dns
        self.uri = uri
        self.alt_names = alt_names
        self.ssltype = ssltype
        self.encryption_type = encryption_type
        self.key_length = key_length

    # Get the install path for different operating systems
    def _get_install_path(self, host):
        shell = RemoteMachineShellConnection(host)
        os_type = shell.extract_remote_info().distribution_type
        self.log.info("OS type is {0}".format(os_type))
        if os_type == 'windows':
            install_path = x509main.WININSTALLPATH
        elif os_type == 'Mac':
            install_path = x509main.MACINSTALLPATH
        else:
            # install_path = x509main.LININSTALLPATH
            install_path = str(self.get_data_path(host)) + "/"
        return install_path

    @staticmethod
    def get_data_path(node):
        """Gets couchbase log directory, even for cluster_run
                """
        _, dir = RestConnection(node).diag_eval(
            'filename:absname(element(2, application:'
            'get_env(ns_server,path_config_datadir))).')
        dir = dir.strip('"')
        return str(dir)

    @staticmethod
    def import_spec_file(spec_file):
        spec_package = importlib.import_module(
            'pytests.security.multiple_CAs_spec.' +
            spec_file)
        return copy.deepcopy(spec_package.spec)

    def create_directory(self, dir_name):
        shell = RemoteMachineShellConnection(self.slave_host)
        shell.execute_command("mkdir " + dir_name)
        shell.disconnect()

    def remove_directory(self, dir_name):
        shell = RemoteMachineShellConnection(self.slave_host)
        shell.execute_command("rm -rf " + dir_name)
        shell.disconnect()

    def delete_inbox_folder_on_server(self, server=None):
        if server is None:
            server = self.host
        shell = RemoteMachineShellConnection(server)
        final_path = self.install_path + x509main.CHAINFILEPATH
        shell.execute_command("rm -rf " + final_path)
        shell.disconnect()

    def create_inbox_folder_on_server(self, server=None):
        if server is None:
            server = self.host
        shell = RemoteMachineShellConnection(server)
        final_path = self.install_path + x509main.CHAINFILEPATH
        shell.create_directory(final_path)
        shell.disconnect()

    def create_CA_folder_on_server(self, server=None):
        if server is None:
            server = self.host
        shell = RemoteMachineShellConnection(server)
        final_path = self.install_path + x509main.CHAINFILEPATH \
                     + "/" + x509main.TRUSTEDCAPATH
        shell.create_directory(final_path)
        shell.disconnect()

    @staticmethod
    def copy_file_from_slave_to_server(server, src, dst):
        shell = RemoteMachineShellConnection(server)
        shell.copy_file_local_to_remote(src, dst)
        shell.disconnect()

    @staticmethod
    def get_a_root_cert(root_ca_name=None):
        """
        (returns): path to ca.pem

        (param):root_ca_name - a valid root_ca_name (optional)
            if not give a root_ca_name, it will return path to ca.pem
            of a random trusted CA
        """
        if root_ca_name is None:
            root_ca_name = random.choice(x509main.root_ca_names)
        return x509main.CACERTFILEPATH + root_ca_name + "/ca.pem"

    @staticmethod
    def get_node_cert(server):
        """
        returns pkey.key, chain.pem,  ie;
        node's cert's key and cert
        """

        node_ca_key_path = x509main.node_ca_map[str(server.ip)]["path"] + \
                           server.ip + ".key"
        node_ca_path = x509main.node_ca_map[str(server.ip)]["path"] + \
                       "long_chain" + server.ip + ".pem"
        return node_ca_key_path, node_ca_path

    def get_client_cert(self, int_ca_name):
        """
        returns client's cert's key and cert
        """
        client_ca_name = "client_" + int_ca_name
        node_ca_key_path = x509main.client_ca_map[str(client_ca_name)]["path"] + \
                           self.client_ip + ".key"
        node_ca_path = x509main.node_ca_map[str(client_ca_name)]["path"] + \
                       "long_chain" + self.client_ip + ".pem"
        return node_ca_key_path, node_ca_path

    def generate_root_certificate(self, root_ca_name, cn_name=None):
        root_ca_dir = x509main.CACERTFILEPATH + root_ca_name + "/"
        self.create_directory(root_ca_dir)

        root_ca_key_path = root_ca_dir + "ca.key"
        root_ca_path = root_ca_dir + "ca.pem"

        shell = RemoteMachineShellConnection(self.slave_host)
        # create ca.key
        output, error = shell.execute_command("openssl genrsa " + self.encryption_type +
                                              " -out " + root_ca_key_path +
                                              " " + str(self.key_length))
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))

        # create ca.pem
        if cn_name is None:
            cn_name = "My Company Root CA " + root_ca_name
        output, error = shell.execute_command("openssl req -new -x509  -days 3650 -sha256 " +
                                              " -key " + root_ca_key_path +
                                              " -out " + root_ca_path +
                                              " -subj '/C=UA/O=My Company/CN=" + cn_name + "'")
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))

        x509main.ca_count += 1
        x509main.root_ca_names.append(root_ca_name)
        x509main.manifest[root_ca_name] = dict()
        x509main.manifest[root_ca_name]["path"] = root_ca_dir
        x509main.manifest[root_ca_name]["intermediate"] = dict()
        shell.disconnect()

    def generate_intermediate_certificate(self, root_ca_name, int_ca_name):
        root_ca_dir = x509main.CACERTFILEPATH + root_ca_name + "/"
        int_ca_dir = root_ca_dir + int_ca_name + "/"
        self.create_directory(int_ca_dir)

        root_ca_key_path = root_ca_dir + "ca.key"
        root_ca_path = root_ca_dir + "ca.pem"
        int_ca_key_path = int_ca_dir + "int.key"
        int_ca_csr_path = int_ca_dir + "int.csr"
        int_ca_path = int_ca_dir + "int.pem"

        shell = RemoteMachineShellConnection(self.slave_host)
        # create int CA private key
        output, error = shell.execute_command("openssl genrsa " + self.encryption_type +
                                              " -out " + int_ca_key_path +
                                              " " + str(self.key_length))
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))

        # create int CA csr
        output, error = shell.execute_command("openssl req -new -key " + int_ca_key_path +
                                              " -out " + int_ca_csr_path +
                                              " -subj '/C=UA/O=My Company/CN=My Company Intermediate CA'")
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))

        # create int CA pem
        output, error = shell.execute_command("openssl x509 -req -in " + int_ca_csr_path +
                                              " -CA " + root_ca_path +
                                              " -CAkey " + root_ca_key_path +
                                              " -CAcreateserial -CAserial " +
                                              int_ca_dir + "rootCA.srl" +
                                              " -extfile " + x509main.INT_EXTENSIONS_FILE +
                                              " -out " + int_ca_path + " -days 365 -sha256")
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))

        x509main.manifest[root_ca_name]["intermediate"][int_ca_name] = dict()
        x509main.manifest[root_ca_name]["intermediate"][int_ca_name]["path"] = int_ca_dir
        x509main.manifest[root_ca_name]["intermediate"][int_ca_name]["nodes"] = dict()
        x509main.manifest[root_ca_name]["intermediate"][int_ca_name]["clients"] = dict()
        shell.disconnect()

    def generate_node_certificate(self, root_ca_name, int_ca_name, node_ip):
        root_ca_dir = x509main.CACERTFILEPATH + root_ca_name + "/"
        int_ca_dir = root_ca_dir + int_ca_name + "/"
        node_ca_dir = root_ca_dir + node_ip + "_" + int_ca_name + "/"
        self.create_directory(node_ca_dir)

        int_ca_key_path = int_ca_dir + "int.key"
        int_ca_path = int_ca_dir + "int.pem"
        node_ca_key_path = node_ca_dir + node_ip + ".key"
        node_ca_csr_path = node_ca_dir + node_ip + ".csr"
        node_ca_path = node_ca_dir + node_ip + ".pem"
        node_chain_ca_path = node_ca_dir + "long_chain" + node_ip + ".pem"

        # check if the ip address is ipv6 raw ip address, remove [] brackets
        if "[" in node_ip:
            node_ip = node_ip.replace("[", "").replace("]", "")
        # modify the extensions file to fill IP/DNS of the node
        temp_cert_extensions_file = "./pytests/security/clientconf3.conf"
        copyfile(x509main.CERT_EXTENSIONS_FILE, temp_cert_extensions_file)
        fin = open(temp_cert_extensions_file, "a+")
        if ".com" in node_ip and self.dns is None:
            fin.write("\nDNS.0 = {0}".format(node_ip))
        elif self.dns:
            fin.write("\nDNS.0 = {0}".format(self.dns))
        else:
            fin.write("\nIP.0 = {0}".format(node_ip))
        fin.close()
        for line in fileinput.input(temp_cert_extensions_file, inplace=1):
            if "ip_address" in line:
                line = line.replace("ip_address", node_ip)
            sys.stdout.write(line)
        # print file contents for easy debugging
        fout = open(temp_cert_extensions_file, "r")
        print((fout.read()))
        fout.close()

        shell = RemoteMachineShellConnection(self.slave_host)
        # create node CA private key
        output, error = shell.execute_command("openssl genrsa " + self.encryption_type +
                                              " -out " + node_ca_key_path +
                                              " " + str(self.key_length))
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))

        # create node CA csr
        output, error = shell.execute_command("openssl req -new -key " + node_ca_key_path +
                                              " -out " + node_ca_csr_path +
                                              " -config " + temp_cert_extensions_file)
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))

        # create node CA pem
        output, error = shell.execute_command("openssl x509 -req -in " + node_ca_csr_path +
                                              " -CA " + int_ca_path +
                                              " -CAkey " + int_ca_key_path +
                                              " -CAcreateserial -CAserial " +
                                              int_ca_dir + "intermediateCA.srl" +
                                              " -out " + node_ca_path +
                                              " -days 365 -sha256" +
                                              " -extfile " + temp_cert_extensions_file +
                                              " -extensions req_ext")
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))

        # concatenate node ca pem & int ca pem to give chain.pem
        output, error = shell.execute_command("cat " + node_ca_path +
                                              " " + int_ca_path +
                                              " > " + node_chain_ca_path)
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))

        os.remove(temp_cert_extensions_file)
        shell.disconnect()
        x509main.node_ca_map[str(node_ip)] = dict()
        x509main.node_ca_map[str(node_ip)]["signed_by"] = int_ca_name
        x509main.node_ca_map[str(node_ip)]["path"] = node_ca_dir
        x509main.manifest[root_ca_name]["intermediate"][int_ca_name]["nodes"][node_ip] = \
            node_ca_dir

    def generate_client_certificate(self, root_ca_name, int_ca_name):
        client_ca_name = "client_" + int_ca_name
        root_ca_dir = x509main.CACERTFILEPATH + root_ca_name + "/"
        int_ca_dir = root_ca_dir + int_ca_name + "/"
        client_ca_dir = root_ca_dir + client_ca_name + "/"
        self.create_directory(client_ca_dir)

        int_ca_key_path = int_ca_dir + "int.key"
        int_ca_path = int_ca_dir + "int.pem"
        client_ca_key_path = client_ca_dir + self.client_ip + ".key"
        client_ca_csr_path = client_ca_dir + self.client_ip + ".csr"
        client_ca_path = client_ca_dir + self.client_ip + ".pem"
        client_chain_ca_path = client_ca_dir + "long_chain" + self.client_ip + ".pem"

        # check if the ip address is ipv6 raw ip address, remove [] brackets
        if "[" in self.client_ip:
            self.client_ip = self.client_ip.replace("[", "").replace("]", "")
        # modify the extensions file to fill IP/DNS of the client for auth
        temp_cert_extensions_file = "./pytests/security/clientconf2.conf"
        copyfile(x509main.CERT_EXTENSIONS_FILE, temp_cert_extensions_file)
        fin = open(temp_cert_extensions_file, "a+")
        if self.alt_names == 'default':
            fin.write("\nDNS.1 = us.cbadminbucket.com")
            fin.write("\nURI.1 = www.cbadminbucket.com")
        elif self.alt_names == 'non_default':
            if self.dns is not None:
                dns = "\nDNS.1 = " + self.dns
                fin.write(dns)
            if self.uri is not None:
                uri = "\nURI.1 = " + self.dns
                fin.write(uri)
        if ".com" in self.client_ip:
            fin.write("\nDNS.0 = {0}".format(self.client_ip))
        else:
            fin.write("\nIP.0 = {0}".format(self.client_ip.replace('[', '').replace(']', '')))
        fin.close()

        # print file contents for easy debugging
        fout = open(temp_cert_extensions_file, "r")
        print((fout.read()))
        fout.close()

        shell = RemoteMachineShellConnection(self.slave_host)
        # create private key for client
        output, error = shell.execute_command("openssl genrsa " + self.encryption_type +
                                              " -out " + client_ca_key_path +
                                              " " + str(self.key_length))
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))

        # create client CA csr
        output, error = shell.execute_command("openssl req -new -key " + client_ca_key_path +
                                              " -out " + client_ca_csr_path +
                                              " -config " + temp_cert_extensions_file)
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))

        # create client CA pem
        output, error = shell.execute_command("openssl x509 -req -in " + client_ca_csr_path +
                                              " -CA " + int_ca_path +
                                              " -CAkey " + int_ca_key_path +
                                              " -CAcreateserial -CAserial " +
                                              client_ca_dir + "intermediateCA.srl" +
                                              " -out " + client_ca_path +
                                              " -days 365 -sha256" +
                                              " -extfile " + temp_cert_extensions_file +
                                              " -extensions req_ext")
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))

        # concatenate client ca pem & int ca pem to give client chain pem
        output, error = shell.execute_command("cat " + client_ca_path +
                                              " " + int_ca_path +
                                              " > " + client_chain_ca_path)
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))
        os.remove(temp_cert_extensions_file)
        shell.disconnect()
        x509main.client_ca_map[str(client_ca_name)] = dict()
        x509main.client_ca_map[str(client_ca_name)]["signed_by"] = int_ca_name
        x509main.client_ca_map[str(client_ca_name)]["path"] = client_ca_dir
        x509main.manifest[root_ca_name]["intermediate"][int_ca_name]["clients"][self.client_ip] = \
            client_ca_dir

    def generate_multiple_x509_certs(self, servers, spec_file_name="default"):
        """
        Generates x509 certs(root, intermediates, nodes, clients)

        params
        :servers: (list) - list of nodes for which to generate
        :spec file: (file name) - spec file name to decide numbers

        returns
        None
        """
        self.create_directory(x509main.CACERTFILEPATH)

        spec = self.import_spec_file(spec_file=spec_file_name)
        copy_servers = copy.deepcopy(servers)
        node_ptr = 0
        max_ptr = len(copy_servers)
        if "structure" in spec.keys():
            for root_ca_name, root_CA_dict in spec["structure"].items():
                self.generate_root_certificate(root_ca_name=root_ca_name)
                number_of_int_ca = root_CA_dict["i"]
                for i in range(number_of_int_ca):
                    int_ca_name = "i" + str(i + 1) + "_" + root_ca_name
                    self.generate_intermediate_certificate(root_ca_name, int_ca_name)
                    self.generate_client_certificate(root_ca_name, int_ca_name)
                    if node_ptr < max_ptr:
                        self.generate_node_certificate(root_ca_name, int_ca_name,
                                                       copy_servers[node_ptr].ip)
                        node_ptr = node_ptr + 1
                    if x509main.ca_count == spec["number_of_CAs"] and \
                            i == (number_of_int_ca - 1):
                        while node_ptr < max_ptr:
                            self.generate_node_certificate(root_ca_name, int_ca_name,
                                                           copy_servers[node_ptr].ip)
                            node_ptr = node_ptr + 1
        while x509main.ca_count < spec["number_of_CAs"]:
            root_ca_name = "r" + str(x509main.ca_count + 1)
            number_of_int_ca = spec["int_certs_per_CA"]
            for i in range(number_of_int_ca):
                int_ca_name = "i" + str(i + 1) + "_" + root_ca_name
                self.generate_root_certificate(root_ca_name=root_ca_name)
                self.generate_intermediate_certificate(root_ca_name, int_ca_name)
                self.generate_client_certificate(root_ca_name, int_ca_name)
                if node_ptr < max_ptr:
                    self.generate_node_certificate(root_ca_name, int_ca_name,
                                                   copy_servers[node_ptr].ip)
                    node_ptr = node_ptr + 1
                if x509main.ca_count == spec["number_of_CAs"] and \
                        i == (number_of_int_ca - 1):
                    while node_ptr < max_ptr:
                        self.generate_node_certificate(root_ca_name, int_ca_name,
                                                       copy_servers[node_ptr].ip)
                        node_ptr = node_ptr + 1
        self.write_client_cert_json_new()

    def rotate_certs(self, all_servers, root_ca_names="all"):
        """
        Rotates x509(root, node, client) certs

        params
        :all_servers: - list of all servers objects involved/affected (self.servers)
        :root_ca_names: (optional) - list of root_ca_names. Defaults to all
        """
        if root_ca_names == "all":
            root_ca_names = copy.deepcopy(x509main.root_ca_names)
        nodes_affected_ips = list()
        for root_ca_name in root_ca_names:
            root_ca_manifest = copy.deepcopy(x509main.manifest[root_ca_name])
            del x509main.manifest[root_ca_name]
            self.remove_directory(root_ca_manifest['path'])
            x509main.root_ca_names.remove(root_ca_name)
            cn_name = 'My Company Root CA ' + root_ca_name + ' rotated'
            self.generate_root_certificate(root_ca_name=root_ca_name,
                                           cn_name=cn_name)
            intermediate_cas_manifest = root_ca_manifest["intermediate"]
            for int_ca_name, int_ca_manifest in intermediate_cas_manifest.items():
                self.generate_intermediate_certificate(root_ca_name=root_ca_name,
                                                       int_ca_name=int_ca_name)
                nodes_cas_manifest = int_ca_manifest["nodes"]
                nodes_ips = nodes_cas_manifest.keys()
                nodes_affected_ips.extend(nodes_ips)
                for node_ip in nodes_ips:
                    del self.node_ca_map[node_ip]
                    self.generate_node_certificate(root_ca_name=root_ca_name,
                                                   int_ca_name=int_ca_name,
                                                   node_ip=node_ip)
                del self.client_ca_map["client_" + int_ca_name]
                self.generate_client_certificate(root_ca_name=root_ca_name,
                                                 int_ca_name=int_ca_name)
        self.log.info("Generation of new certs done!")
        servers = list()
        for node_affected_ip in nodes_affected_ips:
            for server in all_servers:
                if server.ip == node_affected_ip:
                    servers.append(copy.deepcopy(server))
                    break
        self.log.info("nodes affected {0}".format(servers))
        for server in servers:
            _ = self.upload_root_certs(server=server, root_ca_names=root_ca_names)
        self.upload_node_certs(servers=servers)
        # TODo delete off the old trusted CAs from the server

    def upload_root_certs(self, server=None, root_ca_names=None):
        """
        Uploads root certs

        params
        :server (optional): - server from which they are uploaded.
        :root_ca_names (optional): (list) - defaults to all CAs

        returns content from loadTrustedCAs call
        """
        if server is None:
            server = self.host
        if root_ca_names is None:
            root_ca_names = x509main.root_ca_names
        self.copy_trusted_CAs(server=server, root_ca_names=root_ca_names)
        content = self.load_trusted_CAs(server=server)
        return content

    def upload_node_certs(self, servers):
        """
        Uploads node certs

        params
        :servers: (list) - list of nodes

        returns None
        """
        for server in servers:
            self.copy_node_cert(server=server)
        self.reload_node_certificates(servers)

    def write_client_cert_json_new(self):
        template_path = './pytests/security/' + x509main.CLIENT_CERT_AUTH_TEMPLATE
        config_json = x509main.CACERTFILEPATH + x509main.CLIENT_CERT_AUTH_JSON
        target_file = open(config_json, 'w')
        source_file = open(template_path, 'r')
        client_cert = '{"state" : ' + "'" + self.client_cert_state + "'" + ", 'prefixes' : [ "
        for line in source_file:
            for path, prefix, delimeter in zip(self.paths, self.prefixs, self.delimeters):
                line1 = line.replace("@2", "'" + path + "'")
                line2 = line1.replace("@3", "'" + prefix + "'")
                line3 = line2.replace("@4", "'" + delimeter + "'")
                temp_client_cert = "{ " + line3 + " },"
                client_cert = client_cert + temp_client_cert
        client_cert = client_cert.replace("'", '"')
        client_cert = client_cert[:-1]
        client_cert = client_cert + " ]}"
        self.log.info("-- Log current config json file ---{0}".format(client_cert))
        target_file.write(client_cert)

    def upload_client_cert_settings(self, server=None):
        """
        Upload client cert settings to CB server that was initialized in init function
        """
        if server is None:
            server = self.host
        data = open(x509main.CACERTFILEPATH + x509main.CLIENT_CERT_AUTH_JSON, 'rb'). \
            read()
        rest = RestConnection(server)
        authorization = base64.encodebytes(('%s:%s' %
                                            (rest.username, rest.password)).encode()).decode()
        headers = {'Content-Type': 'application/octet-stream',
                   'Authorization': 'Basic %s' % authorization,
                   'Accept': '*/*'}
        url = "settings/clientCertAuth"
        api = rest.baseUrl + url
        http = httplib2.Http(disable_ssl_certificate_validation=True)
        response, content = http.request(api, 'POST', headers=headers, body=data)
        if response['status'] in ['200', '201', '202']:
            return content
        else:
            raise Exception(content)
        # TODo move some of this code to rest client

    def load_trusted_CAs(self, server=None, from_non_localhost=True):
        if not server:
            server = self.host
        if from_non_localhost:
            shell = RemoteMachineShellConnection(server)
            shell.non_local_CA_upload(allow=True)
            shell.disconnect()
        rest = RestConnection(server)
        status, content = rest.load_trusted_CAs()
        if from_non_localhost:
            shell = RemoteMachineShellConnection(server)
            shell.non_local_CA_upload(allow=False)
            shell.disconnect()
        if not status:
            msg = "Could not load Trusted CAs on %s; Failed with error %s" \
                  % (server.ip, content)
            raise Exception(msg)
        return content
        # ToDO write code to upload from localhost

    @staticmethod
    def reload_node_certificates(servers):
        """
        reload node certificates from inbox folder

        params
        :servers: list of nodes
        """
        for server in servers:
            rest = RestConnection(server)
            status, content = rest.reload_certificate()
            if not status:
                msg = "Could not load reload node cert on %s; Failed with error %s" \
                      % (server.ip, content)
                raise Exception(msg)

    def get_trusted_CAs(self, server=None):
        if server is None:
            server = self.host
        rest = RestConnection(server)
        status, content = rest.get_trusted_CAs()
        if not status:
            msg = "Could not get trusted CAs on %s; Failed with error %s" \
                  % (server.ip, content)
            raise Exception(msg)
        return content
        # ToDO write code to parse content

    def copy_trusted_CAs(self, root_ca_names, server=None):
        """
        create inbox/CA folder & copy CAs there
        """
        if server is None:
            server = self.host
        self.create_inbox_folder_on_server(server=server)
        self.create_CA_folder_on_server(server=server)
        for root_ca_name in root_ca_names:
            src_pem_path = self.get_a_root_cert(root_ca_name)
            dest_pem_path = self.install_path + x509main.CHAINFILEPATH + "/CA/" + \
                            root_ca_name + "_ca.pem"
            self.copy_file_from_slave_to_server(server, src_pem_path, dest_pem_path)

    def copy_node_cert(self, server):
        """
        copy chain.pem & pkey.key there to inbox of server
        """
        node_ca_key_path, node_ca_path = self.get_node_cert(server)
        dest_pem_path = self.install_path + x509main.CHAINFILEPATH + "/chain.pem"
        self.copy_file_from_slave_to_server(server, node_ca_path, dest_pem_path)
        dest_pkey_path = self.install_path + x509main.CHAINFILEPATH + "/pkey.key"
        self.copy_file_from_slave_to_server(server, node_ca_key_path, dest_pkey_path)

    def teardown_certs(self, servers):
        """
        1. Remove dir from slave
        2. Delete all trusted CAs & regenerate certs
        """
        self.remove_directory(x509main.CACERTFILEPATH)
        for server in servers:
            self.delete_inbox_folder_on_server(server=server)
        # ToDO delete trusted certs
