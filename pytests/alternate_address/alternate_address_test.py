import copy
import json, ast, filecmp, itertools
import os, shutil, ast
from threading import Thread
from subprocess import Popen, PIPE, check_output, STDOUT, CalledProcessError

from TestInput import TestInputSingleton, TestInputServer
from alternate_address.alternate_address_base import AltAddrBaseTest
from membase.api.rest_client import RestConnection
from couchbase_helper.cluster import Cluster
from remote.remote_util import RemoteMachineShellConnection
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from couchbase_helper.documentgenerator import BlobGenerator, JsonDocGenerator
from pprint import pprint
from testconstants import CLI_COMMANDS, LINUX_COUCHBASE_BIN_PATH,\
                          WIN_COUCHBASE_BIN_PATH, COUCHBASE_FROM_MAD_HATTER,\
                          WIN_TMP_PATH_RAW
from __builtin__ import True
from Carbon.Aliases import true


class AlternateAddressTests(AltAddrBaseTest):
    def setUp(self):
        for server in TestInputSingleton.input.servers:
            remote = RemoteMachineShellConnection(server)
            remote.enable_diag_eval_on_non_local_hosts()
            remote.disconnect()
        super(AlternateAddressTests, self).setUp()
        self.remove_all_alternate_address_settings()
        self.cluster_helper = Cluster()
        self.ex_path = self.tmp_path + "export{0}/".format(self.master.ip)
        self.num_items = self.input.param("items", 1000)
        self.client_os = self.input.param("client_os", "linux")
        self.localhost = self.input.param("localhost", False)
        self.json_create_gen = JsonDocGenerator("altaddr", op_type="create",
                                       encoding="utf-8", start=0, end=self.num_items)
        self.json_delete_gen = JsonDocGenerator("imex", op_type="delete",
                                       encoding="utf-8", start=0, end=self.num_items)

    def tearDown(self):
        super(AlternateAddressTests, self).tearDown()
        ClusterOperationHelper.cleanup_cluster(self.servers, self.servers[0])
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        ClusterOperationHelper.wait_for_ns_servers_or_assert(self.servers, self)


    def test_setting_alternate_address(self):
        server1 = self.servers[0]
        url_format = ""
        secure_port = ""
        secure_conn = ""
        self.all_alt_addr_set = False
        shell = RemoteMachineShellConnection(server1)
        if self.secure_conn:
            cacert = self.get_cluster_certificate_info(server1)
            secure_port = "1"
            url_format = "s"
            if not self.no_cacert:
                secure_conn = "--cacert {0}".format(cacert)
            if self.no_ssl_verify:
                secure_conn = "--no-ssl-verify"
        output = self.list_alt_address(server=server1, url_format = url_format,
                                          secure_port = secure_port, secure_conn = secure_conn)
        if output:
            output, _ = self.remove_alt_address_setting(server=server1,
                                                        url_format = url_format,
                                                        secure_port = secure_port,
                                                        secure_conn = secure_conn)
            mesg = 'SUCCESS: Alternate address configuration deleted'
            if not self._check_output(mesg, output):
                self.fail("Fail to remove alternate address")
        output = self.list_alt_address(server=server1, url_format = url_format,
                                          secure_port = secure_port,
                                          secure_conn = secure_conn)
        if output and output[0] != "[]":
            self.fail("Fail to remove alternate address with remove command")

        self.log.info("Start to set alternate address")
        internal_IP = self.get_internal_IP(server1)
        setting_cmd = "{0}couchbase-cli{1} {2}"\
                       .format(self.cli_command_path, self.cmd_ext,
                               "setting-alternate-address")
        setting_cmd += " -c http{0}://{1}:{2}{3} --username {4} --password {5} {6}"\
                       .format(url_format, internal_IP , secure_port, server1.port,
                               server1.rest_username, server1.rest_password, secure_conn)
        setting_cmd = setting_cmd + "--set --hostname {0} ".format(server1.ip)
        shell.execute_command(setting_cmd)
        output = self.list_alt_address(server=server1, url_format = url_format,
                                                     secure_port = secure_port,
                                                     secure_conn = secure_conn)
        if output and output[0]:
            output = output[0]
            output = output[1:-1]
            output = ast.literal_eval(output)
            if output["hostname"] != server1.ip:
                self.fail("Fail to set correct hostname")
        else:
            self.fail("Fail to set alternate address")
        self.log.info("Start to add node to cluster use internal IP")
        services_in = self.alt_addr_services_in
        if "-" in services_in:
            set_services = services_in.split("-")
        else:
            set_services = services_in.split(",")
        i = 0
        for server in self.servers[1:]:
            internal_IP = self.get_internal_IP(server)
            node_services = "kv"
            if len(set_services) == 1:
                node_services = set_services[0]
            elif len(set_services) > 1:
                if len(set_services) == len(self.servers[1:]):
                    node_services = set_services[i]
                    i += 1
            try:
                shell.alt_addr_add_node(main_server=server1, internal_IP=internal_IP,
                                        server_add=server, services=node_services,
                                        cmd_ext=self.cmd_ext)
            except Exception as e:
                if e:
                    self.fail("Error: {0}".format(e))
        rest = RestConnection(self.master)
        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
        rest.monitorRebalance()

        if self.run_alt_addr_loader:
            if self.alt_addr_kv_loader:
                self.kv_loader(server1, client_os = self.client_os)
        alt_addr_status = []
        for server in self.servers[1:]:
            internal_IP = self.get_internal_IP(server)
            status = self.set_alternate_address(server, url_format = url_format,
                                       secure_port = secure_port, secure_conn = secure_conn,
                                       internal_IP = internal_IP)
            alt_addr_status.append(status)
        if False in alt_addr_status:
            self.fail("Fail to set alt address")
        else:
            self.all_alt_addr_set = True
            if self.run_alt_addr_loader:
                if self.alt_addr_kv_loader:
                    self.kv_loader(server1, self.client_os)
        remove_node = ""
        if self.alt_addr_rebalance_out:
            internal_IP = self.get_internal_IP(self.servers[-1])
            reject_node = "ns_1@{0}".format(internal_IP)
            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[reject_node])
            remove_node = internal_IP
        if self.alt_addr_rebalance_in:
            if remove_node:
                cmd = 'curl -X POST -d  "hostname={0}&user={1}&password={2}&services={3}" '\
                             .format(remove_node, server1.rest_username, server1.rest_password,
                                     self.alt_addr_rebalance_in_services)
                cmd += '-u Administrator:password http://{0}:8091/controller/addNode'\
                             .format(server1.ip)
                shell.execute_command(cmd)
                rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
                reb_status = rest.monitorRebalance()
                self.assertTrue(reb_status, "Rebalance failed")
            else:
                self.fail("We need a free node to add to cluster")
            if self.run_alt_addr_loader:
                if self.alt_addr_kv_loader:
                    self.kv_loader(server1, self.client_os)
        status = self.remove_all_alternate_address_settings()
        if not status:
            self.fail("Failed to remove all alternate address setting")

    def remove_all_alternate_address_settings(self):
        self.log.info("Remove alternate address setting in each node")
        remove_alt = []
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            cmd = "{0}couchbase-cli{1} {2} -c {3}:{4} --username {5} --password {6} {7}"\
                    .format(self.cli_command_path, self.cmd_ext,
                            "setting-alternate-address", server.ip,
                            server.port, server.rest_username, server.rest_password,
                            "--remove")
            output, error = shell.execute_command(cmd, debug=True)
            if error:
                remove_alt.append(error)
            shell.disconnect()
        if remove_alt:
            self.log.error("Remove alt address failed: {0}".format(remove_alt))
            return False
        else:
            return True

    def remove_alt_address_setting(self, server=None, url_format = "", secure_port = "",
                                   secure_conn = ""):
        sub_command = "setting-alternate-address"
        if server is None:
            server = self.master
        cmd = "{0}couchbase-cli{1} {2} -c http{3}://{4}:{5}{6} --username {7} --password {8} {9}"\
                    .format(self.cli_command_path, self.cmd_ext,
                            sub_command, url_format, server.ip, secure_port,
                            server.port, server.rest_username, server.rest_password,
                            secure_conn)
        remove_cmd = cmd + " --remove"
        shell = RemoteMachineShellConnection(server)
        output, error = shell.execute_command(remove_cmd)
        shell.disconnect()
        return output, error

    def list_alt_address(self, server=None, url_format = "", secure_port = "", secure_conn = ""):
        sub_command = "setting-alternate-address"
        if server is None:
            server = self.master
        cmd = "{0}couchbase-cli{1} {2} -c http{3}://{4}:{5}{6} --username {7} --password {8} {9}"\
                    .format(self.cli_command_path, self.cmd_ext,
                            sub_command, url_format, server.ip, secure_port,
                            server.port, server.rest_username, server.rest_password,
                            secure_conn)
        list_cmd = cmd + " --list"
        shell = RemoteMachineShellConnection(server)
        output, error = shell.execute_command(list_cmd)
        shell.disconnect()
        return output

    def set_alternate_address(self, server=None, url_format = "", secure_port = "",
                              secure_conn = "", internal_IP = ""):
        self.log.info("Start to set alternate address")
        if server is None:
            server = self.master
        shell = RemoteMachineShellConnection(server)
        internal_IP = self.get_internal_IP(server)
        setting_cmd = "{0}couchbase-cli{1} {2}"\
                       .format(self.cli_command_path, self.cmd_ext,
                               "setting-alternate-address")
        setting_cmd += " -c http{0}://{1}:{2}{3} --username {4} --password {5} {6}"\
                       .format(url_format, internal_IP , secure_port, server.port,
                               server.rest_username, server.rest_password, secure_conn)
        setting_cmd = setting_cmd + "--set --hostname {0} ".format(server.ip)
        shell.execute_command(setting_cmd)
        output = self.list_alt_address(server=server, url_format = url_format,
                                          secure_port = secure_port,
                                          secure_conn = secure_conn)
        if output:
            return True
        else:
            return False

    def get_cluster_certificate_info(self, server):
        """
            This will get certificate info from cluster
        """
        cert_file_location = self.root_path + "cert.pem"
        if self.os == "windows":
            cert_file_location = WIN_TMP_PATH_RAW + "cert.pem"
        shell = RemoteMachineShellConnection(server)
        cmd = "{0}couchbase-cli{1} ssl-manage "\
                     .format(self.cli_command_path, self.cmd_ext)
        cmd += "-c {0}:{1} -u Administrator -p password --cluster-cert-info > {2}"\
                                               .format(server.ip, server.port,
                                                       cert_file_location)
        output, _ = shell.execute_command(cmd)
        if output and "Error" in output[0]:
            self.fail("Failed to get CA certificate from cluster.")
        shell.disconnect()
        return cert_file_location

    def get_internal_IP(self, server):
        shell = RemoteMachineShellConnection(server)
        internal_IP = shell.get_ip_address()
        internal_IP = [x for x in internal_IP if x != "127.0.0.1"]
        shell.disconnect()
        if internal_IP:
            return internal_IP[0]
        else:
            self.fail("Fail to get internal IP")

    def kv_loader(self, server = None, client_os = "linux"):
        if server is None:
            server = self.master.ip
        base_path = "/opt/couchbase/bin/"
        if client_os == "mac":
            base_path = "/Applications/Couchbase\ Server.app/Contents/Resources/couchbase-core/bin/"
        loader_path = "{0}cbworkloadgen".format(base_path)
        cmd_load = " -n {0}:8091 -u Administrator -p password -j".format(server.ip)
        error_mesg = "No alternate address information found"
        try:
            self.log.info("Load kv doc to bucket from outside network")
            output = check_output("{0} {1}".format(loader_path, cmd_load), shell=True, stderr=STDOUT)
            if output:
                self.log.info("Output from kv loader: {0}".format(output))
        except CalledProcessError as e:
            print "Error return code: ", e.returncode
            if e.output:
                if self.all_alt_addr_set:
                    if "No alternate address information found" in e.output:
                        self.fail("Failed to set alternate address.")
                    else:
                        self.fail("Failed to load to remote cluster.{0}"\
                                  .format(e.output))
                else:
                    self.log.info("Error is expected due to alt addre not set yet")

    def _check_output(self, word_check, output):
        found = False
        if len(output) >=1 :
            if isinstance(word_check, list):
                for ele in word_check:
                    for x in output:
                        if ele.lower() in x.lower():
                            self.log.info("Found '{0} in CLI output".format(ele))
                            found = True
                            break
            elif isinstance(word_check, str):
                for x in output:
                    if word_check.lower() in x.lower():
                        self.log.info("Found '{0}' in CLI output".format(word_check))
                        found = True
                        break
            else:
                self.log.error("invalid {0}".format(word_check))
        return found
