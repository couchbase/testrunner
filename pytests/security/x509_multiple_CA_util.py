import base64
import importlib
import json
import random
import os
import copy
import string
import time
from couchbase.auth import CertificateAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
import requests
import logger
import httplib2

import shutil
from shutil import copyfile
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
    SCRIPTSPATH = "scripts/"
    SCRIPTFILEPATH = "passphrase.sh"
    SCRIPTWINDOWSFILEPATH = "passphrase.bat"
    SLAVE_HOST = ServerInfo('127.0.0.1', 22, 'root', 'couchbase')
    CLIENT_CERT_AUTH_JSON = 'client_cert_auth1.json'
    CLIENT_CERT_AUTH_TEMPLATE = 'client_cert_config_template.txt'
    IP_ADDRESS = '172.16.1.174'  # dummy ip address
    ROOT_CA_CONFIG = "./pytests/security/x509_extension_files/config"
    CA_EXT = "./pytests/security/x509_extension_files/ca.ext"
    SERVER_EXT = "./pytests/security/x509_extension_files/server.ext"
    CLIENT_EXT = "./pytests/security/x509_extension_files/client.ext"
    CONFIG_PATH = "./pytests/security/x509_extension_files/"
    ALL_CAs_PATH = CACERTFILEPATH + "all/"  # a dir to store the combined root ca .pem files
    ALL_CAs_PEM_NAME = "all_ca.pem"  # file name of the CA bundle

    def __init__(self,
                 host=None,
                 wildcard_dns=None,
                 client_cert_state="enable",
                 paths="subject.cn:san.dnsname:san.uri",
                 prefixs="www.cb-:us.:www.", delimeter=".:.:.",
                 client_ip="172.16.1.174", dns=None, uri=None,
                 alt_names="default",
                 standard="pkcs8",
                 encryption_type="aes256",
                 key_length=2048,
                 passphrase_type="plain",
                 passphrase_script_args=None,
                 passhprase_url="https://testingsomething.free.beeceptor.com/",
                 passphrase_plain="default",
                 passphrase_load_timeout=5000,
                 https_opts=None,
                 slave_host_ip='127.0.0.1'):
        self.root_ca_names = list()  # list of active root certs
        self.manifest = dict()  # active CA manifest
        self.node_ca_map = dict()  # {<node_ip>: {signed_by: <int_ca_name>, path: <node_ca_dir>}}
        self.client_ca_map = dict()  # {<client_ca_name>:  {signed_by: <int_ca_name>, path: <client_ca_dir>}}
        self.private_key_passphrase_map = dict()  # {<node_ip>:<plain_passw>}
        self.ca_count = 0  # total count of active root certs

        if https_opts is None:
            self.https_opts = {"verifyPeer": 'false'}
        self.log = logger.Logger.get_logger()
        if host is not None:
            self.host = host
            self.install_path = self._get_install_path(self.host)
        self.slave_host = ServerInfo(slave_host_ip, 22, 'root', 'couchbase')
        self.windows_test = False  # will be set to True in generate_multiple_x509_certs if windows VMs are used

        # Node cert settings
        self.wildcard_dns = wildcard_dns

        # Client cert settings
        self.client_cert_state = client_cert_state  # enable/disable/mandatory
        self.paths = paths.split(":")  # client cert's SAN paths
        self.prefixs = prefixs.split(":")  # client cert's SAN prefixes
        self.delimeters = delimeter.split(":")  # client cert's SAN delimiters
        self.client_ip = client_ip  # a dummy client ip name.
        self.dns = dns  # client cert's san.dns
        self.uri = uri  # client cert's san.uri
        self.alt_names = alt_names  # either 'default' or 'non-default' SAN names

        # Node private key settings
        self.standard = standard  # PKCS standard; currently supports PKCS#1 & PKCS#8
        if encryption_type in ["none", "None", "", None]:
            encryption_type = None
        # encryption pkcs#5 v2 algo for private key in case of PKCS#8. Can be put to None
        self.encryption_type = encryption_type
        self.key_length = key_length
        # Node private key passphrase settings
        self.passphrase_type = passphrase_type  # 'script'/'rest'/'plain'
        self.passphrase_script_args = passphrase_script_args
        self.passphrase_url = passhprase_url
        self.passphrase_plain = passphrase_plain
        self.passphrase_load_timeout = passphrase_load_timeout

    def move_config_files_to_remote_host(self):
        config_dir = x509main.CACERTFILEPATH + "x509_extension_files/"
        self.create_directory(config_dir)
        x509main.CONFIG_PATH = config_dir
        dest_root_ca_config_path = config_dir + "config"
        self.copy_file_from_slave_to_server(self.slave_host, x509main.ROOT_CA_CONFIG,
                                            dest_root_ca_config_path)
        x509main.ROOT_CA_CONFIG = dest_root_ca_config_path
        dest_ca_ext_path = config_dir + "ca.ext"
        self.copy_file_from_slave_to_server(self.slave_host, x509main.CA_EXT,
                                            dest_ca_ext_path)
        x509main.CA_EXT = dest_ca_ext_path
        dest_server_ext_path = config_dir + "server.ext"
        self.copy_file_from_slave_to_server(self.slave_host, x509main.SERVER_EXT,
                                            dest_server_ext_path)
        x509main.SERVER_EXT = dest_server_ext_path
        dest_client_ext_path = config_dir = "client.ext"
        self.copy_file_from_slave_to_server(self.slave_host, x509main.CLIENT_EXT,
                                            dest_client_ext_path)
        x509main.CLIENT_EXT = dest_client_ext_path

    def _delete_inbox_folder(self):
        shell = RemoteMachineShellConnection(self.host)
        final_path = self.install_path + x509main.CHAINFILEPATH
        shell = RemoteMachineShellConnection(self.host)
        os_type = shell.extract_remote_info().distribution_type
        self.log.info ("OS type is {0}".format(os_type))
        shell.delete_file(final_path, "root.crt")
        shell.delete_file(final_path, "chain.pem")
        shell.delete_file(final_path, "pkey.key")
        if os_type == 'windows':
            final_path = '/cygdrive/c/Program Files/Couchbase/Server/var/lib/couchbase/inbox'
            shell.execute_command('rm -rf ' + final_path)
        else:
            shell.execute_command('rm -rf ' + final_path)
        shell.disconnect()

    # Get the install path for different operating systems
    def _get_install_path(self, host):
        shell = RemoteMachineShellConnection(host)
        os_type = shell.extract_remote_info().distribution_type
        shell.disconnect()
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

    def delete_scripts_folder_on_server(self, server=None):
        if server is None:
            server = self.host
        shell = RemoteMachineShellConnection(server)
        final_path = self.install_path + x509main.SCRIPTSPATH
        shell.execute_command("rm -rf " + final_path)
        shell.disconnect()

    def create_inbox_folder_on_server(self, server=None):
        if server is None:
            server = self.host
        shell = RemoteMachineShellConnection(server)
        final_path = self.install_path + x509main.CHAINFILEPATH
        self.log.info("Creating inbox directory on {}".format(server.ip))
        shell.create_directory(final_path)
        shell.disconnect()

    def create_scripts_folder_on_server(self, server=None):
        if server is None:
            server = self.host
        shell = RemoteMachineShellConnection(server)
        final_path = self.install_path + x509main.SCRIPTSPATH
        self.log.info("Creating scripts folder on {}".format(server.ip))
        shell.create_directory(final_path)
        shell.disconnect()

    def create_CA_folder_on_server(self, server=None):
        if server is None:
            server = self.host
        shell = RemoteMachineShellConnection(server)
        self.log.info("Creating CA folder on {}".format(server.ip))
        final_path = self.install_path + x509main.CHAINFILEPATH \
                     + "/" + x509main.TRUSTEDCAPATH
        shell.create_directory(final_path)
        shell.disconnect()

    @staticmethod
    def copy_file_from_slave_to_server(server, src, dst):
        shell = RemoteMachineShellConnection(server)
        shell.copy_file_local_to_remote(src, dst)
        shell.disconnect()

    def copy_file_from_host_to_slave(self, src, dst):
        shell_host = RemoteMachineShellConnection(self.slave_host)
        shell_host.copy_file_remote_to_local(src, dst)
        shell_host.disconnect()

    def copy_file_from_host_to_server(self, server, src, dst):
        # Extract the file extension
        _, file_extension = os.path.splitext(src)

        local_copy_path = "/tmp/remote_copy_" + str(random.randint(1, 100)) + "/"
        os.makedirs(local_copy_path)
        local_copy_file = local_copy_path + "temp_file_" + str(random.randint(1,100)) + \
                          file_extension
        try:
            # Copy file from host to local
            shell_host = RemoteMachineShellConnection(self.slave_host)
            shell_host.copy_file_remote_to_local(src, local_copy_file)
            shell_host.disconnect()
            # Copy file from local to servef
            shell_server = RemoteMachineShellConnection(server)
            shell_server.copy_file_local_to_remote(local_copy_file, dst)
            shell_server.disconnect()
        finally:
            # Clean up the temporary directory
            if os.path.exists(local_copy_path):
                shutil.rmtree(local_copy_path)


    def get_a_root_cert(self, root_ca_name=None):
        """
        (returns): path to ca.pem

        (param):root_ca_name - a valid root_ca_name (optional)
            if not give a root_ca_name, it will return path to ca.pem
            of a random trusted CA
        """
        if root_ca_name is None:
            root_ca_name = random.choice(self.root_ca_names)
        return x509main.CACERTFILEPATH + root_ca_name + "/ca.pem"

    def get_node_cert(self, server):
        """
        returns pkey.key, chain.pem,  ie;
        node's cert's key and cert
        """
        if self.standard == "pkcs12":
            node_ca_key_path = self.node_ca_map[str(server.ip)]["path"] + \
                            "couchbase.p12"
            return node_ca_key_path
        else:
            node_ca_key_path = self.node_ca_map[str(server.ip)]["path"] + \
                            server.ip + ".key"
            node_ca_path = self.node_ca_map[str(server.ip)]["path"] + \
                        "long_chain" + server.ip + ".pem"
            return node_ca_key_path, node_ca_path

    def get_node_private_key_passphrase_script(self, server):
        """
        Given a server object,
        returns the path of the bash script(which prints pkey passphrase for that node) on slave
        """
        shell = RemoteMachineShellConnection(server)
        if shell.extract_remote_info().distribution_type == "windows":
            shell.disconnect()
            return self.node_ca_map[str(server.ip)]["path"] + x509main.SCRIPTWINDOWSFILEPATH
        else:
            shell.disconnect()
            return self.node_ca_map[str(server.ip)]["path"] + x509main.SCRIPTFILEPATH

    def get_client_cert(self, int_ca_name):
        """
        returns client's cert and key
        """
        client_ca_name = "client_" + int_ca_name
        client_ca_key_path = self.client_ca_map[str(client_ca_name)]["path"] + \
                             self.client_ip + ".key"
        client_ca_path = self.client_ca_map[str(client_ca_name)]["path"] + \
                         "long_chain" + self.client_ip + ".pem"
        return client_ca_path, client_ca_key_path

    def create_ca_bundle(self):
        """
        Creates/updates a pem file with all trusted CAs combined
        """
        self.remove_directory(dir_name=x509main.ALL_CAs_PATH)
        self.create_directory(dir_name=x509main.ALL_CAs_PATH)
        cat_cmd = "cat "
        for root_ca, root_ca_manifest in self.manifest.items():
            root_ca_dir_path = root_ca_manifest["path"]
            root_ca_path = root_ca_dir_path + "ca.pem"
            cat_cmd = cat_cmd + root_ca_path + " "
        cat_cmd = cat_cmd + "> " + x509main.ALL_CAs_PATH + x509main.ALL_CAs_PEM_NAME
        shell = RemoteMachineShellConnection(self.slave_host)
        shell.execute_command(cat_cmd)
        shell.disconnect()

    def convert_to_pkcs12(self, node_ip, key_path, node_ca_dir, node_chain_ca_path):
        """
        converts pkcs#1 key to encrypted pkcs#12

        :node_ip: ip_addr of the node
        :key_path: pkcs#1 key path on slave
        :node_ca_dir: node's dir which contains related cert documents.
        """
        tmp_encrypted_key_path = node_ca_dir + "couchbase.p12"
        shell = RemoteMachineShellConnection(self.slave_host)
        if self.passphrase_type == "plain":
            if self.passphrase_plain != "default":
                passw = self.passphrase_plain
            else:
                # generate passw
                passw = ''.join(random.choice(string.ascii_uppercase + string.digits)
                                for _ in range(20))
            self.private_key_passphrase_map[str(node_ip)] = passw

        elif self.passphrase_type == "script":
            # generate passw
            passw = ''.join(random.choice(string.ascii_uppercase + string.digits)
                            for _ in range(20))
            # create bash file with "echo <passw>"
            if self.windows_test:
                passphrase_path = node_ca_dir + "passphrase.bat"
                bash_content = "@echo off\n"
                bash_content = bash_content + "ECHO " + passw
                shell.create_file(passphrase_path, bash_content)
                shell.execute_command("chmod 777 " + passphrase_path)
            else:
                passphrase_path = node_ca_dir + "passphrase.sh"
                bash_content = "#!/bin/bash\n"
                bash_content = bash_content + "echo '" + passw + "'"
                shell.create_file(passphrase_path, bash_content)
                shell.execute_command("chmod 777 " + passphrase_path)
        else:
            response = requests.get(self.passphrase_url)
            passw = response.content.decode('utf-8')

        self.log.info("Converting node {} cert to pkcs 12".format(node_ip))
        convert_cmd = "openssl pkcs12 -export -out " + tmp_encrypted_key_path + " -inkey " + \
                        key_path + " -in " + node_chain_ca_path + \
                        " -passout 'pass:" + passw + "'"
        output, error = shell.execute_command(convert_cmd)
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))

        del_cmd = "rm -rf " + key_path
        output, error = shell.execute_command(del_cmd)
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))

    def convert_to_pkcs8(self, node_ip, key_path, node_ca_dir):
        """
        converts pkcs#1 key to encrypted/un-encrypted pkcs#8 key
        with the same name as pkcs#1 key

        :node_ip: ip_addr of the node
        :key_path: pkcs#1 key path on slave
        :node_ca_dir: node's dir which contains related cert documents.
        """
        tmp_encrypted_key_path = node_ca_dir + "enckey.key"
        shell = RemoteMachineShellConnection(self.slave_host)
        if self.encryption_type:
            if self.passphrase_type == "plain":
                if self.passphrase_plain != "default":
                    passw = self.passphrase_plain
                else:
                    # generate passw
                    passw = ''.join(random.choice(string.ascii_uppercase + string.digits)
                                    for _ in range(20))
                self.private_key_passphrase_map[str(node_ip)] = passw
            elif self.passphrase_type == "script":
                # generate passw
                passw = ''.join(random.choice(string.ascii_uppercase + string.digits)
                                for _ in range(20))
                # create bash file with "echo <passw>"
                if self.windows_test:
                    passphrase_path = node_ca_dir + "passphrase.bat"
                    bash_content = "@echo off\n"
                    bash_content = bash_content + "ECHO " + passw
                    with open(passphrase_path, "w") as fh:
                        fh.write(bash_content)
                    os.chmod(passphrase_path, 0o777)
                else:
                    passphrase_path = node_ca_dir + "passphrase.sh"
                    bash_content = "#!/bin/bash\n"
                    bash_content = bash_content + "echo '" + passw + "'"
                    with open(passphrase_path, "w") as fh:
                        fh.write(bash_content)
                    os.chmod(passphrase_path, 0o777)
            else:
                response = requests.get(self.passphrase_url)
                passw = response.content.decode('utf-8')

            # convert cmd
            convert_cmd = "openssl pkcs8 -in " + key_path + " -passout 'pass:" + passw + "' " + \
                          "-topk8 -v2 " + self.encryption_type + \
                          " -out " + tmp_encrypted_key_path
            output, error = shell.execute_command(convert_cmd)
            self.log.info('Output message is {0} and error message is {1}'.format(output, error))

        else:
            convert_cmd = "openssl pkcs8 -in " + key_path + \
                          " -topk8 -nocrypt -out " + tmp_encrypted_key_path
            output, error = shell.execute_command(convert_cmd)
            self.log.info('Output message is {0} and error message is {1}'.format(output, error))

        # delete old pkcs1 key & rename encrypted key
        del_cmd = "rm -rf " + key_path
        output, error = shell.execute_command(del_cmd)
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))
        mv_cmd = "mv " + tmp_encrypted_key_path + " " + key_path
        output, error = shell.execute_command(mv_cmd)
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))
        shell.disconnect()

    def generate_root_certificate(self, root_ca_name, cn_name=None):
        root_ca_dir = x509main.CACERTFILEPATH + root_ca_name + "/"
        self.create_directory(root_ca_dir)


        root_ca_key_path = root_ca_dir + "ca.key"
        root_ca_path = root_ca_dir + "ca.pem"
        config_path = x509main.ROOT_CA_CONFIG

        if self.slave_host.ip != '127.0.0.1':
            dest_cofig_path = x509main.CONFIG_PATH + "config"
            self.copy_file_from_slave_to_server(self.slave_host, config_path,
                                                dest_cofig_path)
            config_path = dest_cofig_path

        shell = RemoteMachineShellConnection(self.slave_host)
        # create ca.key
        self.log.info("Creating Root CA private key")
        output, error = shell.execute_command("openssl genrsa " +
                                              " -out " + root_ca_key_path +
                                              " " + str(self.key_length))
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))
        if cn_name is None:
            cn_name = root_ca_name
        # create ca.pem
        self.log.info("Creating Root CA certificate")
        output, error = shell.execute_command("openssl req -config " + config_path +
                                              " -new -x509 -days 3650" +
                                              " -sha256 -key " + root_ca_key_path +
                                              " -out " + root_ca_path +
                                              " -subj '/C=UA/O=MyCompany/CN=" + cn_name + "'")
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))

        self.ca_count += 1
        self.root_ca_names.append(root_ca_name)
        self.manifest[root_ca_name] = dict()
        self.manifest[root_ca_name]["path"] = root_ca_dir
        self.manifest[root_ca_name]["intermediate"] = dict()
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
        int_ca_ext = x509main.CA_EXT
        if self.slave_host.ip != '127.0.0.1':
            dest_int_ca_ext_path = x509main.CONFIG_PATH + "ca.ext"
            self.copy_file_from_slave_to_server(self.slave_host, x509main.CA_EXT,
                                                dest_int_ca_ext_path)
            int_ca_ext = dest_int_ca_ext_path
        shell = RemoteMachineShellConnection(self.slave_host)
        # create int CA private key
        self.log.info("Creating intermediate CA private key")
        output, error = shell.execute_command("openssl genrsa " +
                                              " -out " + int_ca_key_path +
                                              " " + str(self.key_length))
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))

        # create int CA csr
        self.log.info("Creating intermediate CA signing request")
        output, error = shell.execute_command("openssl req -new -key " + int_ca_key_path +
                                              " -out " + int_ca_csr_path +
                                              " -subj '/C=UA/O=MyCompany/OU=Servers/CN=ClientAndServerSigningCA'")
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))

        # create int CA pem
        self.log.info("Creating intermediate CA certificate")
        output, error = shell.execute_command("openssl x509 -req -in " + int_ca_csr_path +
                                              " -CA " + root_ca_path +
                                              " -CAkey " + root_ca_key_path +
                                              " -CAcreateserial -CAserial " +
                                              int_ca_dir + "rootCA.srl" +
                                              " -extfile " + int_ca_ext +
                                              " -out " + int_ca_path + " -days 365 -sha256")
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))

        self.manifest[root_ca_name]["intermediate"][int_ca_name] = dict()
        self.manifest[root_ca_name]["intermediate"][int_ca_name]["path"] = int_ca_dir
        self.manifest[root_ca_name]["intermediate"][int_ca_name]["nodes"] = dict()
        self.manifest[root_ca_name]["intermediate"][int_ca_name]["clients"] = dict()
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
        temp_cert_extensions_file = "./pytests/security/x509_extension_files/server2.ext"
        copyfile(x509main.SERVER_EXT, temp_cert_extensions_file)
        with open(temp_cert_extensions_file, "a+") as fin:
            if ".com" in node_ip and self.wildcard_dns is None:
                fin.write("\nsubjectAltName = DNS:{0}".format(node_ip))
            elif self.wildcard_dns:
                fin.write("\nsubjectAltName = DNS:{0}".format(self.wildcard_dns))
            else:
                fin.write("\nsubjectAltName = IP:{0}".format(node_ip))
        # print file contents for easy debugging
        with open(temp_cert_extensions_file, "r") as fout:
            print(fout.read())

        if self.slave_host.ip != '127.0.0.1':
            dest_temp_cert_extensions_file = x509main.CONFIG_PATH + "server2.ext"
            self.copy_file_from_slave_to_server(self.slave_host, temp_cert_extensions_file,
                                                dest_temp_cert_extensions_file)
            temp_cert_extensions_file = dest_temp_cert_extensions_file

        shell = RemoteMachineShellConnection(self.slave_host)
        # create node CA private key
        self.log.info("Creating node {} private key".format(node_ip))
        output, error = shell.execute_command("openssl genrsa " +
                                              " -out " + node_ca_key_path +
                                              " " + str(self.key_length))
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))

        # create node CA csr
        self.log.info("Creating node {} certificate signing request".format(node_ip))
        output, error = shell.execute_command("openssl req -new -key " + node_ca_key_path +
                                              " -out " + node_ca_csr_path +
                                              " -subj '/C=UA/O=MyCompany/OU=Servers/CN=couchbase.node.svc'")
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))

        # create node CA pem
        self.log.info("Creating node {} certificate".format(node_ip))
        output, error = shell.execute_command("openssl x509 -req -in " + node_ca_csr_path +
                                              " -CA " + int_ca_path +
                                              " -CAkey " + int_ca_key_path +
                                              " -CAcreateserial -CAserial " +
                                              int_ca_dir + "intermediateCA.srl" +
                                              " -out " + node_ca_path +
                                              " -days 365 -sha256" +
                                              " -extfile " + temp_cert_extensions_file)
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))

        # concatenate node ca pem & int ca pem to give chain.pem
        output, error = shell.execute_command("cat " + node_ca_path +
                                              " " + int_ca_path +
                                              " > " + node_chain_ca_path)
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))

        if self.standard == "pkcs8":
            self.convert_to_pkcs8(node_ip=node_ip, key_path=node_ca_key_path,
                                  node_ca_dir=node_ca_dir)
        if self.standard == "pkcs12":
            self.convert_to_pkcs12(node_ip=node_ip, key_path=node_ca_key_path,
                                   node_ca_dir=node_ca_dir,
                                   node_chain_ca_path=node_chain_ca_path)

        if self.slave_host.ip == '127.0.0.1':
            os.remove(temp_cert_extensions_file)
        shell.disconnect()
        self.node_ca_map[str(node_ip)] = dict()
        self.node_ca_map[str(node_ip)]["signed_by"] = int_ca_name
        self.node_ca_map[str(node_ip)]["path"] = node_ca_dir
        self.manifest[root_ca_name]["intermediate"][int_ca_name]["nodes"][node_ip] = \
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
        temp_cert_extensions_file = "./pytests/security/x509_extension_files/client2.ext"
        copyfile(x509main.CLIENT_EXT, temp_cert_extensions_file)
        with open(temp_cert_extensions_file, "a+") as fin:
            # it must be noted that if SAN.DNS is used, it will always be used for auth
            # irrespective of other SANs in the certificate. So SAN.DNS must be a valid user
            if self.alt_names == 'default':
                fin.write("\nsubjectAltName = DNS:us.cbadminbucket.com")
                fin.write("\nsubjectAltName = URI:www.cbadminbucket.com")
            else:
                if self.dns is not None:
                    fin.write("\nsubjectAltName = DNS:{0}".format(self.dns))
                if self.uri is not None:
                    fin.write("\nsubjectAltName = URI:{0}".format(self.uri))

        # print file contents for easy debugging
        with open(temp_cert_extensions_file, "r") as fout:
            print(fout.read())

        if self.slave_host.ip != '127.0.0.1':
            dest_temp_cert_extensions_file = x509main.CONFIG_PATH + "client2.ext"
            self.copy_file_from_slave_to_server(self.slave_host, temp_cert_extensions_file,
                                                dest_temp_cert_extensions_file)
            temp_cert_extensions_file = dest_temp_cert_extensions_file

        shell = RemoteMachineShellConnection(self.slave_host)
        # create private key for client
        output, error = shell.execute_command("openssl genrsa " +
                                              " -out " + client_ca_key_path +
                                              " " + str(self.key_length))
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))

        # create client CA csr
        output, error = shell.execute_command("openssl req -new -key " + client_ca_key_path +
                                              " -out " + client_ca_csr_path +
                                              " -subj '/C=UA/O=MyCompany/OU=People/CN=clientuser'")
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))

        # create client CA pem
        output, error = shell.execute_command("openssl x509 -req -in " + client_ca_csr_path +
                                              " -CA " + int_ca_path +
                                              " -CAkey " + int_ca_key_path +
                                              " -CAcreateserial -CAserial " +
                                              client_ca_dir + "intermediateCA.srl" +
                                              " -out " + client_ca_path +
                                              " -days 365 -sha256" +
                                              " -extfile " + temp_cert_extensions_file)
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))

        # concatenate client ca pem & int ca pem to give client chain pem
        output, error = shell.execute_command("cat " + client_ca_path +
                                              " " + int_ca_path +
                                              " > " + client_chain_ca_path)
        self.log.info('Output message is {0} and error message is {1}'.format(output, error))
        if self.slave_host.ip == '127.0.0.1':
            os.remove(temp_cert_extensions_file)
        shell.disconnect()
        self.client_ca_map[str(client_ca_name)] = dict()
        self.client_ca_map[str(client_ca_name)]["signed_by"] = int_ca_name
        self.client_ca_map[str(client_ca_name)]["path"] = client_ca_dir
        self.manifest[root_ca_name]["intermediate"][int_ca_name]["clients"][self.client_ip] = \
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
        shell = RemoteMachineShellConnection(servers[0])
        if shell.extract_remote_info().distribution_type == "windows":
            self.windows_test = True
        shell.disconnect()

        self.create_directory(x509main.CACERTFILEPATH)
        if self.slave_host.ip != '127.0.0.1':
            config_dir = x509main.CACERTFILEPATH + "x509_extension_files/"
            x509main.CONFIG_PATH = config_dir
            self.create_directory(x509main.CONFIG_PATH)

        # Take care of creating certs from spec file
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
                    if node_ptr < max_ptr:
                        self.generate_node_certificate(root_ca_name, int_ca_name,
                                                       copy_servers[node_ptr].ip)
                        node_ptr = node_ptr + 1
                    if self.ca_count == spec["number_of_CAs"] and \
                            i == (number_of_int_ca - 1):
                        while node_ptr < max_ptr:
                            self.generate_node_certificate(root_ca_name, int_ca_name,
                                                           copy_servers[node_ptr].ip)
                            node_ptr = node_ptr + 1
        while self.ca_count < spec["number_of_CAs"]:
            root_ca_name = "r" + str(self.ca_count + 1)
            number_of_int_ca = spec["int_certs_per_CA"]
            self.generate_root_certificate(root_ca_name=root_ca_name)
            for i in range(number_of_int_ca):
                int_ca_name = "i" + str(i + 1) + "_" + root_ca_name
                self.generate_intermediate_certificate(root_ca_name, int_ca_name)
                if node_ptr < max_ptr:
                    self.generate_node_certificate(root_ca_name, int_ca_name,
                                                   copy_servers[node_ptr].ip)
                    node_ptr = node_ptr + 1
                if self.ca_count == spec["number_of_CAs"] and \
                        i == (number_of_int_ca - 1):
                    while node_ptr < max_ptr:
                        self.generate_node_certificate(root_ca_name, int_ca_name,
                                                       copy_servers[node_ptr].ip)
                        node_ptr = node_ptr + 1

        # first client
        int_ca_name = self.node_ca_map[servers[0].ip]["signed_by"]
        root_ca_name = int_ca_name.split("_")[1]
        self.generate_client_certificate(root_ca_name, int_ca_name)
        # second client
        int_ca_name = "iclient1_" + root_ca_name
        self.generate_intermediate_certificate(root_ca_name, int_ca_name)
        self.generate_client_certificate(root_ca_name, int_ca_name)
        # third client
        root_ca_name = "clientroot"
        int_ca_name = "iclient1_" + root_ca_name
        self.generate_root_certificate(root_ca_name)
        self.generate_intermediate_certificate(root_ca_name, int_ca_name)
        self.generate_client_certificate(root_ca_name, int_ca_name)

        self.write_client_cert_json_new()
        self.create_ca_bundle()

    def rotate_certs(self, all_servers, root_ca_names="all"):
        """
        Rotates x509(root, node, client) certs

        params
        :all_servers: - list of all servers objects involved/affected (self.servers)
        :root_ca_names: (optional) - list of root_ca_names. Defaults to all
        """
        if root_ca_names == "all":
            root_ca_names = copy.deepcopy(self.root_ca_names)
        old_ids = self.get_ids_from_ca_names(ca_names=root_ca_names,
                                             server=all_servers[0])
        nodes_affected_ips = list()
        for root_ca_name in root_ca_names:
            root_ca_manifest = copy.deepcopy(self.manifest[root_ca_name])
            del self.manifest[root_ca_name]
            self.remove_directory(root_ca_manifest['path'])
            self.root_ca_names.remove(root_ca_name)
            cn_name = root_ca_name + 'rotated'
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
                client_cas_manifest = int_ca_manifest["clients"]
                if self.client_ip in client_cas_manifest.keys():
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
        if len(servers):
            for server in servers:
                _ = self.upload_root_certs(server=server, root_ca_names=root_ca_names)
            self.upload_node_certs(servers=servers)
            self.create_ca_bundle()
            self.delete_trusted_CAs(server=servers[0], ids=old_ids,
                                    mark_deleted=False)

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
            root_ca_names = self.root_ca_names
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
        if not os.path.exists(x509main.CACERTFILEPATH):
            # Create the directory
            os.makedirs(x509main.CACERTFILEPATH)
            self.log.info(f"Directory {x509main.CACERTFILEPATH} created.")
        else:
            self.log.info(f"Directory {x509main.CACERTFILEPATH} already exists.")

        with open(template_path, 'r') as source_file:
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
        with open(config_json, 'w') as target_file:
            target_file.write(client_cert)

    def upload_client_cert_settings(self, server=None):
        """
        Upload client cert settings(that was initialized in init function) to CB server
        """
        if server is None:
            server = self.host
        with open(x509main.CACERTFILEPATH + x509main.CLIENT_CERT_AUTH_JSON,
                  'rb') as fh:
            data = fh.read()
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

    def load_trusted_CAs(self, server=None):
        if not server:
            server = self.host
        self.log.info("Loading root CA certificate on {}".format(server.ip))
        rest = RestConnection(server)
        status, content = rest.load_trusted_CAs()
        if not status:
            msg = "Could not load Trusted CAs on %s; Failed with error %s" \
                  % (server.ip, content)
            raise Exception(msg)
        return content

    def build_params(self, node):
        """
        Builds parameters for node certificate,key upload
        """
        params = dict()
        script_file = x509main.SCRIPTFILEPATH
        shell = RemoteMachineShellConnection(node)
        if shell.extract_remote_info().distribution_type == "windows":
            script_file = x509main.SCRIPTWINDOWSFILEPATH
        shell.disconnect()
        self.log.info("Building params for loading node cert")
        if self.encryption_type:
            params["privateKeyPassphrase"] = dict()
            params["privateKeyPassphrase"]["type"] = self.passphrase_type
            if self.passphrase_type == "script":
                params["privateKeyPassphrase"]["path"] = self.install_path + \
                                                         x509main.SCRIPTSPATH + \
                                                         script_file
                params["privateKeyPassphrase"]["timeout"] = self.passphrase_load_timeout
                params["privateKeyPassphrase"]["trim"] = 'true'
                if self.passphrase_script_args:
                    params["privateKeyPassphrase"]["args"] = self.passphrase_script_args
            elif self.passphrase_type == "rest":
                params["privateKeyPassphrase"]["url"] = self.passphrase_url
                params["privateKeyPassphrase"]["timeout"] = self.passphrase_load_timeout
                params["privateKeyPassphrase"]["httpsOpts"] = self.https_opts
            else:
                params["privateKeyPassphrase"]["type"] = "plain"
                params["privateKeyPassphrase"]["password"] = \
                    self.private_key_passphrase_map[str(node.ip)]
        params = json.dumps(params)
        return params

    def reload_node_certificates(self, servers):
        """
        reload node certificates from inbox folder

        params
        :servers: list of nodes
        """

        for server in servers:
            rest = RestConnection(server)
            params = ''
            if self.standard == "pkcs8" or self.standard == "pkcs12":
                params = self.build_params(server)
            self.log.info("Loading node certificate for {}".format(server.ip))
            status, content = rest.reload_certificate(params=params)
            if not status:
                msg = "Could not load reload node cert on %s; Failed with error %s" \
                      % (server.ip, content)
                raise Exception(msg)

    def get_trusted_CAs(self, server=None):
        if server is None:
            server = self.host
        rest = RestConnection(server)
        return rest.get_trusted_CAs()

    def get_ca_names_from_ids(self, ids, server=None):
        """
        Returns list of root ca_names,
        given a list of of CA IDs
        """
        ca_names = list()
        content = self.get_trusted_CAs(server=server)
        for ca_dict in content:
            if int(ca_dict["id"]) in ids:
                subject = ca_dict["subject"]
                root_ca_name = subject.split("CN=")[1]
                ca_names.append(root_ca_name)
        return ca_names

    def get_ids_from_ca_names(self, ca_names, server=None):
        """
        Returns list of CA IDs,
        given a list of string of CA names
        """
        ca_ids = list()
        content = self.get_trusted_CAs(server=server)
        for ca_dict in content:
            ca_id = ca_dict["id"]
            subject = ca_dict["subject"]
            root_ca_name = subject.split("CN=")[1]
            if root_ca_name in ca_names:
                ca_ids.append(int(ca_id))
        return ca_ids

    def delete_trusted_CAs(self, server=None, ids=None, mark_deleted=True):
        """
        Deletes trusted CAs from cluster

        :server: server object to make rest (defaults to self.host)
        :ids: list of CA IDs to delete. Defaults to all trusted CAs which
              haven't signed any node
        :mark_deleted: Boolean on whether to remove it from root_ca_names
                        global variable list. Defaults to True
        Returns None
        """
        if server is None:
            server = self.host
        rest = RestConnection(server)
        if ids is None:
            ids = list()
            content = self.get_trusted_CAs(server)
            for ca_dict in content:
                if len(ca_dict["nodes"]) == 0:
                    ca_id = ca_dict["id"]
                    ids.append(ca_id)
        ca_names = self.get_ca_names_from_ids(ids=ids, server=server)
        for ca_id in ids:
            status, content, response = rest.delete_trusted_CA(ca_id=ca_id)
            if not status:
                raise Exception("Could not delete trusted CA with id {0}. "
                                "Failed with content {1} response {2} ".format(ca_id, content, response))
        if mark_deleted:
            for ca_name in ca_names:
                ca_name = ca_name.rstrip("rotated")
                if ca_name in self.root_ca_names:
                    self.root_ca_names.remove(ca_name)

    def delete_unused_out_of_the_box_CAs(self, server=None):
        if server is None:
            server = self.host
        rest = RestConnection(server)
        content = self.get_trusted_CAs(server)
        for ca_dict in content:
            if len(ca_dict["nodes"]) == 0 and ca_dict["type"] == "generated":
                ca_id = ca_dict["id"]
                status, content, response = rest.delete_trusted_CA(ca_id=ca_id)
                if not status:
                    raise Exception("Could not delete trusted CA with id {0}. "
                                    "Failed with content {1} response {2} ".format(ca_id, content, response))

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
            self.log.info("Copying root certificate from {}:{} to {}:{}".
                          format(self.slave_host.ip, src_pem_path, server.ip,dest_pem_path))
            if self.slave_host.ip != '127.0.0.1':
                self.copy_file_from_host_to_server(server, src_pem_path, dest_pem_path)
            else:
                self.copy_file_from_slave_to_server(server, src_pem_path, dest_pem_path)

    def copy_node_cert(self, server):
        """
        copy chain.pem & pkey.key there to inbox of server
        """
        # self.create_inbox_folder_on_server(server=server)
        # self.create_scripts_folder_on_server(server=server)
        if self.standard == "pkcs12":
            node_ca_key_path = self.get_node_cert(server)
            dest_pem_path = self.install_path + x509main.CHAINFILEPATH + "/couchbase.p12"
            self.log.info("Copying node pkcs 12 key from {}:{} to {}:{}".
                          format(self.slave_host.ip, node_ca_key_path, server.ip, dest_pem_path))
            if self.slave_host.ip != '127.0.0.1':
                self.copy_file_from_host_to_server(server, node_ca_key_path, dest_pem_path)
            else:
                self.copy_file_from_slave_to_server(server, node_ca_key_path, dest_pem_path)
            dest_pem_folder = self.install_path + x509main.CHAINFILEPATH
            shell = RemoteMachineShellConnection(server)
            shell.execute_command("chown -R couchbase " + dest_pem_folder)
        else:
            node_ca_key_path, node_ca_path = self.get_node_cert(server)
            dest_pem_path = self.install_path + x509main.CHAINFILEPATH + "/chain.pem"
            self.copy_file_from_slave_to_server(server, node_ca_path, dest_pem_path)
            dest_pkey_path = self.install_path + x509main.CHAINFILEPATH + "/pkey.key"
            self.copy_file_from_slave_to_server(server, node_ca_key_path, dest_pkey_path)
        if (self.standard == "pkcs8" or self.standard == "pkcs12") and self.encryption_type and \
                self.passphrase_type == "script":
            node_key_passphrase_path = self.get_node_private_key_passphrase_script(server)
            shell = RemoteMachineShellConnection(server)
            if shell.extract_remote_info().distribution_type == "windows":
                dest_node_key_passphrase_path = self.install_path + x509main.SCRIPTSPATH + \
                                                x509main.SCRIPTWINDOWSFILEPATH
                if self.slave_host.ip != '127.0.0.1':
                    self.copy_file_from_host_to_server(server, node_key_passphrase_path,
                                                       dest_node_key_passphrase_path)
                else:
                    self.copy_file_from_slave_to_server(server, node_key_passphrase_path,
                                                        dest_node_key_passphrase_path)
                dest_node_key_passphrase_path = "/cygdrive/c/Program Files/Couchbase/Server/var/lib/couchbase/" + \
                                                x509main.SCRIPTSPATH + x509main.SCRIPTWINDOWSFILEPATH
                shell.execute_command("chmod 777 '" +
                                      dest_node_key_passphrase_path + "'")
            else:
                dest_node_key_passphrase_path = self.install_path + x509main.SCRIPTSPATH + \
                                                x509main.SCRIPTFILEPATH
                if self.slave_host.ip != '127.0.0.1':
                    self.copy_file_from_host_to_server(server, node_key_passphrase_path,
                                                       dest_node_key_passphrase_path)
                else:
                    self.copy_file_from_slave_to_server(server, node_key_passphrase_path,
                                                        dest_node_key_passphrase_path)
                output, error = shell.execute_command("chmod 777 '" +
                                                      dest_node_key_passphrase_path + "'")
                self.log.info('Output message is {0} and error message is {1}'.
                              format(output, error))
                output, error = shell.execute_command("chown couchbase:couchbase " +
                                                      dest_node_key_passphrase_path)
                self.log.info('Output message is {0} and error message is {1}'.
                              format(output, error))
            shell.disconnect()

    @staticmethod
    def regenerate_certs(server):
        rest = RestConnection(server)
        rest.regenerate_cluster_certificate()

    def teardown_certs(self, servers):
        """
        1. Remove dir from slave
        2. Delete all trusted CAs & regenerate certs
        """
        self.remove_directory(x509main.CACERTFILEPATH)
        for server in servers:
            self.delete_inbox_folder_on_server(server=server)
            self.delete_scripts_folder_on_server(server=server)
        for server in servers:
            self.regenerate_certs(server=server)
            self.delete_trusted_CAs(server=server)


class Validation:
    def __init__(self, server,
                 cacert=None,
                 client_cert_path_tuple=None):
        self.server = server
        self.cacert = cacert
        self.client_cert_path_tuple = client_cert_path_tuple
        self.log = logger.Logger.get_logger()

    def urllib_request(self, api, verb='GET', params='', headers=None, timeout=100, try_count=3):
        if headers is None:
            credentials = '{}:{}'.format(self.server.rest_username, self.server.rest_password)
            authorization = base64.encodebytes(credentials.encode('utf-8'))
            authorization = authorization.decode('utf-8').rstrip('\n')
            headers = {'Content-Type': 'application/x-www-form-urlencoded',
                       'Authorization': 'Basic %s' % authorization,
                       'Connection': 'close',
                       'Accept': '*/*'}
        if self.client_cert_path_tuple:
            self.log.info("Using client cert auth")
            del headers['Authorization']
        if self.cacert:
            verify = self.cacert
        else:
            verify = False
        tries = 0
        self.log.info("Making a rest request api={0} verb={1} params={2} "
                      "client_cert={3} verify={4}".
                      format(api, verb, params, self.client_cert_path_tuple,
                             verify))
        while tries < try_count:
            try:
                if verb == 'GET':
                    response = requests.get(api, params=params, headers=headers,
                                            timeout=timeout, cert=self.client_cert_path_tuple,
                                            verify=verify)
                elif verb == 'POST':
                    response = requests.post(api, data=params, headers=headers,
                                             timeout=timeout, cert=self.client_cert_path_tuple,
                                             verify=verify)
                elif verb == 'DELETE':
                    response = requests.delete(api, data=params, headers=headers,
                                               timeout=timeout, cert=self.client_cert_path_tuple,
                                               verify=verify)
                elif verb == "PUT":
                    response = requests.put(api, data=params, headers=headers,
                                            timeout=timeout, cert=self.client_cert_path_tuple,
                                            verify=verify)
                status = response.status_code
                content = response.content
                if status in [200, 201, 202]:
                    return True, content, response
                else:
                    self.log.error(response.reason)
                    return False, content, response
            except Exception as e:
                tries = tries + 1
                if tries >= try_count:
                    raise Exception(e)
                else:
                    self.log.error("Trying again ...")
                    time.sleep(5)

    @staticmethod
    def creates_sdk(client, start=0, end=10000):
        """
        Given a sdk client, performs creates
        TODO: Validate this creates ?
        """
        for i in range(start, end):
            key = 'document' + str(i)
            client.upsert(key, {'application': 'data'})

    def sdk_connection(self, bucket_name='default'):
        """
        opens bucket, returns sdk client connection
        """
        from couchbase.bucket import Bucket
        options = dict(certpath=self.client_cert_path_tuple[0],
                       truststorepath=self.cacert,
                       keypath=self.client_cert_path_tuple[1])
        self.log.info('couchbases://{hostname}/{bucketname}?certpath={certpath}'
                      '&truststorepath={truststorepath}&keypath={keypath}'.
                      format(hostname=self.server.ip, bucketname=bucket_name, **options))
        # TODo change after PYCBC-1179
        # bucket = Bucket('couchbases://{hostname}/{bucketname}?certpath={certpath}'
        #                 '&truststorepath={truststorepath}&keypath={keypath}'.
        #                 format(hostname=self.server.ip, bucketname=bucket_name, **options))
        authenticator = CertificateAuthenticator(cert_path=self.client_cert_path_tuple[0],
                                                 trust_store_path=self.cacert,
                                                 key_path=self.client_cert_path_tuple[1])
        cluster_ops = ClusterOptions(authenticator)
        cluster = Cluster.connect(f"couchbases://{self.server.ip}", cluster_ops)
        bucket = cluster.bucket(bucket_name)
        return bucket
