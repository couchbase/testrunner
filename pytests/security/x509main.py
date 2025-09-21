import logger

from lib.Cb_constants.CBServer import CbServer

log = logger.Logger.get_logger()
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
import httplib2
import base64
import requests
import urllib.request, urllib.parse, urllib.error
import random
import os
import uuid
import copy
import subprocess
import json
from pytests.security.ntonencryptionBase import ntonencryptionBase


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
    CHAINCERTFILE = 'chain.pem'
    NODECAKEYFILE = 'pkey.key'
    CACERTFILE = "root.crt"
    CAKEYFILE = "root.key"
    WININSTALLPATH = "C:/Program Files/Couchbase/Server/var/lib/couchbase/"
    LININSTALLPATH = "/opt/couchbase/var/lib/couchbase/"
    MACINSTALLPATH = "/Users/couchbase/Library/Application Support/Couchbase/var/lib/couchbase/"
    DOWNLOADPATH = "/tmp/"
    CACERTFILEPATH = "/tmp/newcerts" + str(uuid.uuid4()) + "/"
    CHAINFILEPATH = "inbox"
    GOCERTGENFILE = "gencert.go"
    INCORRECT_ROOT_CERT = "incorrect_root_cert.crt"
    SLAVE_HOST = ServerInfo('127.0.0.1', 22, 'root', 'couchbase')
    CLIENT_CERT_AUTH_JSON = 'client_cert_auth.json'
    CLIENT_CERT_AUTH_TEMPLATE = 'client_cert_config_template.txt'
    IP_ADDRESS = '172.16.1.174'
    KEY_FILE = CACERTFILEPATH + CAKEYFILE
    CERT_FILE = CACERTFILEPATH + CACERTFILE
    CLIENT_CERT_KEY = CACERTFILEPATH + IP_ADDRESS + ".key"
    CLIENT_CERT_PEM = CACERTFILEPATH + IP_ADDRESS + ".pem"
    SRC_CHAIN_FILE = CACERTFILEPATH + "long_chain" + IP_ADDRESS + ".pem"

    def __init__(self,
                 host=None,
                 method='REST'):

        if host is not None:
            self.host = host
            self.install_path = self._get_install_path(self.host)
        self.disable_ssl_certificate_validation = False
        if CbServer.use_https:
            self.disable_ssl_certificate_validation = True
        self.slave_host = x509main.SLAVE_HOST

    def getLocalIPAddress(self):
        """
        status, ipAddress = commands.getstatusoutput(
        "ifconfig en0 | grep 'inet addr:' | cut -d: -f2 |awk '{print $1}'")
        if '1' not in ipAddress:
            status, ipAddress = commands.getstatusoutput(
            "ifconfig eth0 | grep  -Eo 'inet (addr:)?([0-9]*.){3}[0-9]*'
            | awk '{print $2}'")
        return ipAddress
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('couchbase.com', 0))
        return s.getsockname()[0]

    def get_data_path(self,node):
        """Gets couchbase log directory, even for cluster_run
                """
        _, dir = RestConnection(node).diag_eval(
            'filename:absname(element(2, application:get_env(ns_server,path_config_datadir))).')
        dir = dir.strip('"')
        return str(dir)

    def _generate_cert(self, servers, root_cn='Root Authority', type='go', encryption="", key_length=1024, client_ip=None, alt_names='default', dns=None, uri=None,wildcard_dns=None):
        shell = RemoteMachineShellConnection(self.slave_host)
        shell.execute_command("rm -rf " + x509main.CACERTFILEPATH)
        shell.execute_command("mkdir -p " + x509main.CACERTFILEPATH)
        if type == 'go':
            files = []
            cert_file = "./pytests/security/" + x509main.GOCERTGENFILE
            output, error = shell.execute_command("go run " + cert_file + " -store-to=" + x509main.CACERTFILEPATH + "root -common-name=" + root_cn)
            log.info ('Output message is {0} and error message is {1}'.format(output, error))
            output, error = shell.execute_command("go run " + cert_file + " -store-to=" + x509main.CACERTFILEPATH + "interm -sign-with=" + x509main.CACERTFILEPATH + "root -common-name=Intemediate Authority")
            log.info ('Output message is {0} and error message is {1}'.format(output, error))
            for server in servers:
                if "[" in server.ip:
                    server.ip = server.ip.replace("[", "").replace("]", "")
                output, error = shell.execute_command("go run " + cert_file + " -store-to=" + x509main.CACERTFILEPATH + server.ip + " -sign-with=" + x509main.CACERTFILEPATH + "interm -common-name=" + server.ip + " -final=true")
                log.info ('Output message is {0} and error message is {1}'.format(output, error))
                output, error = shell.execute_command("cat " + x509main.CACERTFILEPATH + server.ip + ".crt " + x509main.CACERTFILEPATH + "interm.crt  > " + " " + x509main.CACERTFILEPATH + "long_chain" + server.ip + ".pem")
                log.info ('Output message is {0} and error message is {1}'.format(output, error))

            shell.execute_command("go run " + cert_file + " -store-to=" + x509main.CACERTFILEPATH + "incorrect_root_cert -common-name=Incorrect Authority")
            shell.disconnect()
        elif type == 'openssl':
            files = []
            v3_ca = "./pytests/security/v3_ca.crt"
            output, error = shell.execute_command("openssl genrsa " + encryption + " -out " + x509main.CACERTFILEPATH + "ca.key " + str(key_length))
            log.info ('Output message is {0} and error message is {1}'.format(output, error))
            output, error = shell.execute_command("openssl req -new -x509  -days 3650 -sha256 -key " + x509main.CACERTFILEPATH + "ca.key " + "-config ./pytests/security/v3_ca.conf -extensions v3_ca " + "-out " + x509main.CACERTFILEPATH + "ca.pem -subj '/C=UA/O=My Company/CN=My Company Root CA'")
            log.info ('Output message is {0} and error message is {1}'.format(output, error))
            output, error = shell.execute_command("openssl genrsa " + encryption + " -out " + x509main.CACERTFILEPATH + "int.key " + str(key_length))
            log.info ('Output message is {0} and error message is {1}'.format(output, error))
            output, error = shell.execute_command("openssl req -new -key " + x509main.CACERTFILEPATH + "int.key -out " + x509main.CACERTFILEPATH + "int.csr -subj '/C=UA/O=My Company/CN=My Company Intermediate CA'")
            log.info ('Output message is {0} and error message is {1}'.format(output, error))
            output, error = shell.execute_command("openssl x509 -req -in " + x509main.CACERTFILEPATH + "int.csr -CA " + x509main.CACERTFILEPATH + "ca.pem -CAkey " + x509main.CACERTFILEPATH + "ca.key -CAcreateserial -CAserial "
                            + x509main.CACERTFILEPATH + "rootCA.srl -extfile ./pytests/security/v3_ca.conf -extensions v3_ca -out " + x509main.CACERTFILEPATH + "int.pem -days 365 -sha256")
            log.info ('Output message is {0} and error message is {1}'.format(output, error))

            for server in servers:
                # check if the ip address is ipv6 raw ip address, remove [] brackets
                if "[" in server.ip:
                    server.ip = server.ip.replace("[", "").replace("]", "")
                from shutil import copyfile
                copyfile("./pytests/security/clientconf.conf", "./pytests/security/clientconf3.conf")
                fin = open("./pytests/security/clientconf3.conf", "a+")
                if ".com" in server.ip and wildcard_dns is None:
                    fin.write("\nDNS.0 = {0}".format(server.ip))
                elif wildcard_dns:
                    fin.write("\nDNS.0 = {0}".format(wildcard_dns))
                else:
                    fin.write("\nIP.0 = {0}".format(server.ip.replace('[', '').replace(']', '')))

                fin.close()

                import fileinput
                import sys
                for line in fileinput.input("./pytests/security/clientconf3.conf", inplace=1):
                    if "ip_address" in line:
                        line = line.replace("ip_address", server.ip)
                    sys.stdout.write(line)

                # print file contents for easy debugging
                fout = open("./pytests/security/clientconf3.conf", "r")
                print((fout.read()))
                fout.close()

                output, error = shell.execute_command("openssl genrsa " + encryption + " -out " + x509main.CACERTFILEPATH + server.ip + ".key " + str(key_length))
                log.info ('Output message is {0} and error message is {1}'.format(output, error))
                output, error = shell.execute_command("openssl req -new -key " + x509main.CACERTFILEPATH + server.ip + ".key -out " + x509main.CACERTFILEPATH + server.ip + ".csr -config ./pytests/security/clientconf3.conf")
                log.info ('Output message is {0} and error message is {1}'.format(output, error))
                output, error = shell.execute_command("openssl x509 -req -in " + x509main.CACERTFILEPATH + server.ip + ".csr -CA " + x509main.CACERTFILEPATH + "int.pem -CAkey " +
                                x509main.CACERTFILEPATH + "int.key -CAcreateserial -CAserial " + x509main.CACERTFILEPATH + "intermediateCA.srl -out " + x509main.CACERTFILEPATH + server.ip + ".pem -days 365 -sha256 -extfile ./pytests/security/clientconf3.conf -extensions req_ext")
                log.info ('Output message is {0} and error message is {1}'.format(output, error))
                output, error = shell.execute_command("cat " + x509main.CACERTFILEPATH + server.ip + ".pem " + x509main.CACERTFILEPATH + "int.pem " + x509main.CACERTFILEPATH + "ca.pem > " + x509main.CACERTFILEPATH + "long_chain" + server.ip + ".pem")
                log.info ('Output message is {0} and error message is {1}'.format(output, error))

            output, error = shell.execute_command("cp " + x509main.CACERTFILEPATH + "ca.pem " + x509main.CACERTFILEPATH + "root.crt")
            log.info ('Output message is {0} and error message is {1}'.format(output, error))

            os.remove("./pytests/security/clientconf3.conf")
            # Check if client_ip is ipv6, remove []

            if "[" in client_ip:
                client_ip = client_ip.replace("[", "").replace("]", "")

            from shutil import copyfile
            copyfile("./pytests/security/clientconf.conf", "./pytests/security/clientconf2.conf")
            fin = open("./pytests/security/clientconf2.conf", "a+")
            if alt_names == 'default':
                fin.write("\nDNS.1 = us.cbadminbucket.com")
                fin.write("\nURI.1 = www.cbadminbucket.com")
            elif alt_names == 'non_default':
                if dns is not None:
                    dns = "\nDNS.1 = " + dns
                    fin.write(dns)
                if uri is not None:
                    uri = "\nURI.1 = " + dns
                    fin.write(uri)
            if ".com" in server.ip:
                fin.write("\nDNS.0 = {0}".format(server.ip))
            else:
                fin.write("\nIP.0 = {0}".format(server.ip.replace('[', '').replace(']', '')))
            fin.close()

            # print file contents for easy debugging
            fout = open("./pytests/security/clientconf2.conf", "r")
            print((fout.read()))
            fout.close()

            # Generate Certificate for the client
            output, error = shell.execute_command("openssl genrsa " + encryption + " -out " + x509main.CACERTFILEPATH + client_ip + ".key " + str(key_length))
            log.info ('Output message is {0} and error message is {1}'.format(output, error))
            output, error = shell.execute_command("openssl req -new -key " + x509main.CACERTFILEPATH + client_ip + ".key -out " + x509main.CACERTFILEPATH + client_ip + ".csr -config ./pytests/security/clientconf2.conf")
            log.info ('Output message is {0} and error message is {1}'.format(output, error))
            output, error = shell.execute_command("openssl x509 -req -in " + x509main.CACERTFILEPATH + client_ip + ".csr -CA " + x509main.CACERTFILEPATH + "int.pem -CAkey " +
                                x509main.CACERTFILEPATH + "int.key -CAcreateserial -CAserial " + x509main.CACERTFILEPATH + "intermediateCA.srl -out " + x509main.CACERTFILEPATH + client_ip + ".pem -days 365 -sha256 -extfile ./pytests/security/clientconf2.conf -extensions req_ext")
            log.info ('Output message is {0} and error message is {1}'.format(output, error))
            output, error = shell.execute_command("cat " + x509main.CACERTFILEPATH + client_ip + ".pem " + x509main.CACERTFILEPATH + "int.pem " + x509main.CACERTFILEPATH + "ca.pem > " + x509main.CACERTFILEPATH + "long_chain" + client_ip + ".pem")
            log.info ('Output message is {0} and error message is {1}'.format(output, error))
            os.remove("./pytests/security/clientconf2.conf")

    # Top level method for setup of nodes in the cluster
    def setup_cluster_nodes_ssl(self, servers=[], reload_cert=False):
        # Make a copy of the servers not to change self.servers
        copy_servers = copy.deepcopy(servers)
        # For each server in cluster, setup a node certificates, create inbox folders and copy + chain cert
        for server in copy_servers:
            x509main(server)._setup_node_certificates(reload_cert=reload_cert, host=server)

    # Create inbox folder and copy node cert and chain cert
    def _setup_node_certificates(self, chain_cert=True, node_key=True, reload_cert=True, host=None):
        if host == None:
            host = self.host
        self._create_inbox_folder(host)
        if host.ip.count(':') > 0 and host.ip.count(']') > 0:
                # raw ipv6? enclose in square brackets
                host.ip = host.ip.replace('[', '').replace(']', '')
        src_chain_file = x509main.CACERTFILEPATH + "/long_chain" + host.ip + ".pem"
        dest_chain_file = self.install_path + x509main.CHAINFILEPATH + "/" + x509main.CHAINCERTFILE
        src_node_key = x509main.CACERTFILEPATH + "/" + host.ip + ".key"
        dest_node_key = self.install_path + x509main.CHAINFILEPATH + "/" + x509main.NODECAKEYFILE
        src_ca_file = x509main.CACERTFILEPATH + "ca.pem"
        dest_ca_file = self.install_path + x509main.CHAINFILEPATH + "/CA/" +  "ca.pem"
        self._copy_node_key_chain_cert(host, src_ca_file, dest_ca_file)
        if chain_cert:
            self._copy_node_key_chain_cert(host, src_chain_file, dest_chain_file)
        if node_key:
            self._copy_node_key_chain_cert(host, src_node_key, dest_node_key)
        if reload_cert:
            status, content = self._reload_node_certificate(host)
            return status, content


    # Reload cert for self signed certificate
    def _reload_node_certificate(self, host):
        rest = RestConnection(host)
        api = rest.baseUrl + "node/controller/reloadCertificate"
        http = httplib2.Http(disable_ssl_certificate_validation=self.disable_ssl_certificate_validation)
        status, content, header = rest._http_request(api, 'POST', headers=self._create_rest_headers('Administrator', 'password'))
        return status, content

    # Get the install path for different operating system
    def _get_install_path(self, host):
        shell = RemoteMachineShellConnection(host)
        os_type = shell.extract_remote_info().distribution_type
        log.info ("OS type is {0}".format(os_type))
        if os_type == 'windows':
            install_path = x509main.WININSTALLPATH
        elif os_type == 'Mac':
            install_path = x509main.MACINSTALLPATH
        else:
            #install_path = x509main.LININSTALLPATH
            install_path = str(self.get_data_path(host)) + "/"
        shell.disconnect()
        return install_path

    # create inbox folder for host
    def _create_inbox_folder(self, host):
        shell = RemoteMachineShellConnection(self.host)
        final_path = self.install_path + x509main.CHAINFILEPATH
        shell.create_directory(final_path)
        shell.create_directory(final_path+"/CA")
        shell.disconnect()

    # delete all file inbox folder and remove inbox folder
    def _delete_inbox_folder(self):
        shell = RemoteMachineShellConnection(self.host)
        final_path = self.install_path + x509main.CHAINFILEPATH
        shell = RemoteMachineShellConnection(self.host)
        os_type = shell.extract_remote_info().distribution_type
        log.info ("OS type is {0}".format(os_type))
        shell.delete_file(final_path, "root.crt")
        shell.delete_file(final_path, "chain.pem")
        shell.delete_file(final_path, "pkey.key")
        if os_type == 'windows':
            final_path = '/cygdrive/c/Program Files/Couchbase/Server/var/lib/couchbase/inbox'
            shell.execute_command('rm -rf ' + final_path)
        else:
            shell.execute_command('rm -rf ' + final_path)
        shell.disconnect()

    # Function to simply copy from source to destination
    def _copy_node_key_chain_cert(self, host, src_path, dest_path):
        shell = RemoteMachineShellConnection(host)
        shell.copy_file_local_to_remote(src_path, dest_path)
        shell.disconnect()

    def _create_rest_headers(self, username="Administrator", password="password"):
        authorization = base64.encodebytes(('%s:%s' % (username, password)).encode()).decode()
        return {'Content-Type': 'application/octet-stream',
            'Authorization': 'Basic %s' % authorization,
            'Accept': '*/*'}

    # Function that will upload file via rest
    def _rest_upload_file(self, URL, file_path_name, username=None, password=None):
        data = open(file_path_name, 'rb').read()
        http = httplib2.Http(disable_ssl_certificate_validation=self.disable_ssl_certificate_validation)
        status, content = http.request(URL, 'POST', headers=self._create_rest_headers(username, password), body=data)
        log.info (" Status from rest file upload command is {0}".format(status))
        log.info (" Content from rest file upload command is {0}".format(content))
        return status, content

    # Upload Cluster or root cert
    def _upload_cluster_ca_certificate(self, username, password):
        rest = RestConnection(self.host)
        url = "controller/uploadClusterCA"
        api = rest.baseUrl + url
        self._rest_upload_file(api, x509main.CACERTFILEPATH + x509main.CACERTFILE, "Administrator", 'password')

    # def _move_file_to_inbox(self, file_name):
    #     cmd  = f"scp {file_name} root@{self.host.ip}:{self.LININSTALLPATH}/{self.CACERTFILEPATH}/CA/{file_name}"
    #     return_code = subprocess.call(cmd, shell=True)
    #     if return_code != 0:
    #         raise Exception(f"Failed to move file to inbox: {return_code}")
    #     log.info(f"Moved file to inbox: {file_name}")

    def _new_upload_cluster_ca_certificate(self, username, password):
        # self._move_file_to_inbox(x509main.CACERTFILE)
        rest = RestConnection(self.host)
        rest.load_trusted_CAs()
        rest.reload_certificate()

    # def _move_file_to_inbox(self, file_name):
    #     cmd  = f"scp {file_name} root@{self.host.ip}:{self.LININSTALLPATH}/{self.CACERTFILEPATH}/CA/{file_name}"
    #     return_code = subprocess.call(cmd, shell=True)
    #     if return_code != 0:
    #         raise Exception(f"Failed to move file to inbox: {return_code}")
    #     log.info(f"Moved file to inbox: {file_name}")

    # Upload security setting for client cert
    def _upload_cluster_ca_settings(self, username, password):
        temp = self.host
        rest = RestConnection(temp)
        url = "settings/clientCertAuth"
        api = rest.baseUrl + url
        status, content = self._rest_upload_file(api, x509main.CACERTFILEPATH + x509main.CLIENT_CERT_AUTH_JSON, "Administrator", 'password')
        log.info (" --- Status from upload of client cert settings is {0} and Content is {1}".format(status, content))
        return status, content

    '''
    Use requests module to execute rest api's
    Steps:
    1. Check if client_cert is set or not. This will define rest of the parameters for client certificates to set for connections
    2. check what is the verb required for rest api, get, post, put and delete for each rest api
    3. Call request with client certs, data that is passed for each request and headers for each request
    4. Return text of the response to the calling function
    Capture any exception in the code and return error
    '''

    def _validate_ssl_login(self, final_url=None, header=None, client_cert=False, verb='GET', data='', plain_curl=False, username='Administrator', password='password', host=None, verify=True):
        if verify:
            verify = x509main.CERT_FILE
        if verb == 'GET' and plain_curl:
            r = requests.get(final_url, data=data)
            return r.status_code, r.text
        elif client_cert:
            try:
                if verb == 'GET':
                    r = requests.get(final_url, verify=verify, cert=(x509main.SRC_CHAIN_FILE, x509main.CLIENT_CERT_KEY), data=data)
                elif verb == 'POST':
                    r = requests.post(final_url, verify=verify, cert=(x509main.SRC_CHAIN_FILE, x509main.CLIENT_CERT_KEY), data=data)
                elif verb == 'PUT':
                    header = {'Content-type': 'Content-Type: application/json'}
                    r = requests.put(final_url, verify=verify, cert=(x509main.SRC_CHAIN_FILE, x509main.CLIENT_CERT_KEY), data=data, headers=header)
                elif verb == 'DELETE':
                    header = {'Content-type': 'Content-Type: application/json'}
                    r = requests.delete(final_url, verify=verify, cert=(x509main.SRC_CHAIN_FILE, x509main.CLIENT_CERT_KEY), headers=header)
                return r.status_code, r.text
            except Exception as ex:
                log.info ("into exception from validate_ssl_login with client cert")
                log.info (" Exception is {0}".format(ex))
                return 'error','error'
        else:
            try:
                r = requests.get("https://" + str(self.host.ip) + ":18091", verify=verify)
                if r.status_code == 200:
                    header = {'Content-type': 'application/x-www-form-urlencoded'}
                    params = urllib.parse.urlencode({'user':'{0}'.format(username), 'password':'{0}'.format(password)})
                    r = requests.post("https://" + str(self.host.ip) + ":18091/uilogin", data=params, headers=header, verify=verify)
                    return r.status_code
            except Exception as ex:
                log.info ("into exception from validate_ssl_login")
                log.info (" Exception is {0}".format(ex))
                return 'error','error'

    def _validate_mandatory_state_ssl_login(self):
        final_url = "https://" + str(self.host.ip) + ":18091/pools/default"
        status, text = self._validate_ssl_login(final_url=final_url, client_cert=True, verb='GET', verify=True)
        if status != 200:
            log.info ("Not able to login to /pools/default via SSL code")
            return status, text

        final_url = "https://" + str(self.host.ip) + ":18091/settings/clientCertAuth"
        status, text = self._validate_ssl_login(final_url=final_url, client_cert=True, verb='GET', verify=True)
        if status != 200:
            log.info ("Not able to GET /settings/clientCertAuth via SSL code")
            return status, text

        final_url = "https://" + str(self.host.ip) + ":18091/pools/default/"
        status, text = self._validate_ssl_login(final_url=final_url, client_cert=True, verb='POST', verify=True, data={"memoryQuota": "2000"})
        if status != 200:
            log.info ("Not able to POST to /pools/default via SSL code")
            return status, text

        final_url = "https://" + str(self.host.ip) + ":18091/pools/default/"
        status, text = self._validate_ssl_login(final_url=final_url, client_cert=True, verb='GET', verify=True)
        if status != 200:
            log.info ("Not able to GET /pools/default/ via SSL code")
            return status, text

        return status


    '''
    Call in curl requests to execute rest api's
    1. check for the verb that is GET or post or delete and decide which header to use
    2. check if the request is a simple curl to execute a rest api, no cert and no client auth
    3. Check if client cert is going to be used, in that case pass in the root cert, client key and cert
    4. Check the request is not for client cert, then pass in just the root cert
    5. Form the url, add url, header and ulr
    6. Add any data is there
    7. Execute the curl command
    retun the output of the curl command
    '''

    def _validate_curl(self, final_url=None, headers=None, client_cert=False, verb='GET', data='', plain_curl=False, username='Administrator', password='password'):

        if verb == 'GET':
            final_verb = 'curl -v'
        elif verb == 'POST':
            final_verb = 'curl -v -X POST'
        elif verb == 'DELETE':
            final_verb = 'curl -v -X DELETE'

        if plain_curl:
            main_url = final_verb
        elif client_cert:
            main_url = final_verb + " --cacert " + x509main.SRC_CHAIN_FILE + " --cert-type PEM --cert " + x509main.CLIENT_CERT_PEM + " --key-type PEM --key " + x509main.CLIENT_CERT_KEY
        else:
            main_url = final_verb + "  --cacert " + x509main.CERT_FILE

        main_url = main_url + " --tls-max 1.2"
        cmd = str(main_url) + " " + str(headers) + " " + str(final_url)
        if data is not None:
            cmd = cmd + " -d " + data
        log.info("Running command : {0}".format(cmd))
        output = subprocess.check_output(cmd, shell=True)
        return output

    '''
    Define what needs to be called for the authentication
    1. check if host is none, get it from the object
    2. check if the curl has to be plain execution, no cert at all else make it a https
    3. if the execution needs to be done via curl or via python requests
    Return the value of result.
    '''

    def _execute_command_clientcert(self, host=None, url=None, port=18091, headers=None, client_cert=False, curl=False, verb='GET', data=None,
                                    plain_curl=False, username='Administrator', password='password'):
        if host is None:
            host = self.host.ip

        if plain_curl:
            final_url = "http://" + str(host) + ":" + str(port) + str(url)
        else:
            final_url = "https://" + str(host) + ":" + str(port) + str(url)

        if curl:
            result = self._validate_curl(final_url, headers, client_cert, verb, data, plain_curl, username, password)
            return result
        else:
            status, result = self._validate_ssl_login(final_url, headers, client_cert, verb, data, plain_curl, username, password)
            return status, result

    # Setup master node
    # 1. Upload Cluster cert i.e
    # 2. Setup other nodes for certificates
    # 3. Create the cert.json file which contains state, path, prefixes and delimeters
    # 4. Upload the cert.json file
    def setup_master(self, state=None, paths=None, prefixs=None, delimeters=None,
                     mode='rest', user='Administrator', password='password',
                     non_local_CA_upload = True):
        level = ntonencryptionBase().get_encryption_level_cli(self.host)
        if level:
            ntonencryptionBase().disable_nton_cluster([self.host])
        copy_host = copy.deepcopy(self.host)
        if non_local_CA_upload:
            self.non_local_CA_upload(server=copy_host, allow=True)
        x509main(copy_host)._setup_node_certificates()
        x509main(copy_host)._new_upload_cluster_ca_certificate(user, password)
        if state is not None:
            self.write_client_cert_json_new(state, paths, prefixs, delimeters)
            if mode == 'rest':
                x509main(copy_host)._upload_cluster_ca_settings(user, password)
            elif mode == 'cli':
                x509main(copy_host)._upload_cert_file_via_cli(user, password)
        if level:
            ntonencryptionBase().setup_nton_cluster([self.host], clusterEncryptionLevel=level)
        if non_local_CA_upload:
            # Disable it back as uploading is done
            self.non_local_CA_upload(server=copy_host, allow=False)

    # write a new config json file based on state, paths, perfixes and delimeters
    def write_client_cert_json_new(self, state, paths, prefixs, delimeters):
        template_path = './pytests/security/' + x509main.CLIENT_CERT_AUTH_TEMPLATE
        config_json = x509main.CACERTFILEPATH + x509main.CLIENT_CERT_AUTH_JSON
        target_file = open(config_json, 'w')
        source_file = open(template_path, 'r')
        client_cert = '{"state" : ' + "'" + state + "'" + ", 'prefixes' : [ "
        for line in source_file:
            for path, prefix, delimeter in zip(paths, prefixs, delimeters):
                line1 = line.replace("@2", "'" + path + "'")
                line2 = line1.replace("@3", "'" + prefix + "'")
                line3 = line2.replace("@4", "'" + delimeter + "'")
                temp_client_cert = "{ " + line3 + " },"
                client_cert = client_cert + temp_client_cert
        client_cert = client_cert.replace("'", '"')
        client_cert = client_cert[:-1]
        client_cert = client_cert + " ]}"
        log.info ("-- Log current config json file ---{0}".format(client_cert))
        target_file.write(client_cert)

    #upload new config file via commandline.
    def _upload_cert_file_via_cli(self, user='Administrator', password='password'):
        src_cert_file =  x509main.CACERTFILEPATH + x509main.CLIENT_CERT_AUTH_JSON
        dest_cert_file = self.install_path + x509main.CHAINFILEPATH + "/" + x509main.CLIENT_CERT_AUTH_JSON
        self._copy_node_key_chain_cert(self.host, src_cert_file, dest_cert_file)
        cli_command = 'ssl-manage'
        options = "--set-client-auth "  + dest_cert_file
        remote_client = RemoteMachineShellConnection(self.host)
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                    options=options, cluster_host="localhost", user=user, password=password)
        log.info (" -- Output of command ssl-manage with --set-client-auth is {0} and erorr is {1}".format(output, error))

    def non_local_CA_upload(self, allow=False, server=None):
        """
        Changes whether or not the server should allow NonLocalCACertUpload
        allow: (bool) whether to allowNonLocalCACertUpload
        server: server object. If not given takes self.host
        """
        if server is None:
            server = self.host
        shell = RemoteMachineShellConnection(server)
        shell.non_local_CA_upload(allow=allow)
        shell.disconnect()
