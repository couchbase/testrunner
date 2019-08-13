import random
import time
import json
import urllib
from subprocess import Popen, PIPE, check_output, STDOUT, CalledProcessError

import logger
from basetestcase import BaseTestCase
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from testconstants import LINUX_COUCHBASE_LOGS_PATH, \
                          WIN_TMP_PATH, WIN_TMP_PATH_RAW, \
                          LINUX_COUCHBASE_BIN_PATH, LINUX_ROOT_PATH, LINUX_CB_PATH,\
                          MAC_COUCHBASE_BIN_PATH, WIN_COUCHBASE_BIN_PATH,\
                          WIN_ROOT_PATH, WIN_CB_PATH

from couchbase_helper.cluster import Cluster
from security.rbacmain import rbacmain
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper


log = logger.Logger.get_logger()


class AltAddrBaseTest(BaseTestCase):
    vbucketId = 0

    def setUp(self):
        self.times_teardown_called = 1
        super(AltAddrBaseTest, self).setUp()
        self.r = random.Random()
        self.cluster = Cluster()
        self.clusters_dic = self.input.clusters
        self.client_os = self.input.param("client_os", "linux")
        self.alt_addr_with_xdcr = self.input.param("alt_addr_with_xdcr", False)
        if self.clusters_dic:
            if len(self.clusters_dic) > 1:
                self.dest_nodes = self.clusters_dic[1]
                self.dest_master = self.dest_nodes[0]
            elif len(self.clusters_dic) == 1:
                self.log.error("=== need 2 cluster to setup xdcr in ini file ===")
            if self.alt_addr_with_xdcr:
                self.des_name = "des_cluster"
                self.delete_xdcr_reference(self.clusters_dic[0][0].ip, self.clusters_dic[1][0].ip)
                if self.skip_init_check_cbserver:
                    for key in self.clusters_dic.keys():
                        servers = self.clusters_dic[key]
                        try:
                            self.backup_reset_clusters(servers)
                        except:
                            self.log.error("was not able to cleanup cluster the first time")
                            self.backup_reset_clusters(servers)
        else:
            self.log.error("**** Cluster config is setup in ini file. ****")

        self.shell = RemoteMachineShellConnection(self.master)
        if not self.skip_init_check_cbserver:
            self.rest = RestConnection(self.master)
            self.cb_version = self.rest.get_nodes_version()

        self.key_gen = self.input.param("key-gen", True)
        self.secure_conn = self.input.param("secure-conn", False)
        self.no_cacert = self.input.param("no-cacert", False)
        self.no_ssl_verify = self.input.param("no-ssl-verify", False)
        self.verify_data = self.input.param("verify-data", False)
        self.debug_logs = self.input.param("debug-logs", False)
        self.should_fail = self.input.param("should-fail", False)
        self.add_hostname_node = self.input.param("add_hostname_node", False)
        self.add_hostname_node_at_src = self.input.param("add_hostname_node_at_src", False)
        self.add_hostname_node_at_des = self.input.param("add_hostname_node_at_des", False)
        self.num_hostname_add = self.input.param("num_hostname_add", 1)
        self.alt_addr_services_in = self.input.param("alt_addr_services_in", "kv")
        self.alt_addr_rebalance_out = self.input.param("alt_addr_rebalance_out", False)
        self.alt_addr_rebalance_in = self.input.param("alt_addr_rebalance_in", False)
        self.alt_addr_rebalance_in_services = self.input.param("alt_addr_rebalance_in_services", "kv")
        self.alt_addr_use_public_dns = self.input.param("alt_addr_use_public_dns", False)
        self.alt_addr_kv_loader = self.input.param("alt_addr_kv_loader", False)
        self.alt_addr_n1ql_query = self.input.param("alt_addr_n1ql_query", False)
        self.alt_addr_eventing_function = self.input.param("alt_addr_eventing_function", False)
        self.alt_addr_fts_loader = self.input.param("alt_addr_fts_loader", False)
        self.run_alt_addr_loader = self.input.param("run_alt_addr_loader", False)
        self.all_alt_addr_set = False

        info = self.shell.extract_remote_info()
        self.os_version = info.distribution_version.lower()
        self.deliverable_type = info.deliverable_type.lower()
        type = info.type.lower()
        self.excluded_commands = self.input.param("excluded_commands", None)
        self.os = 'linux'
        self.full_v = None
        self.short_v = None
        self.build_number = None
        cmd =  'curl -g {0}:8091/diag/eval -u {1}:{2} '.format(self.master.ip,
                                                              self.master.rest_username,
                                                              self.master.rest_password)
        cmd += '-d "path_config:component_path(bin)."'
        bin_path  = check_output(cmd, shell=True)
        if "bin" not in bin_path:
            self.fail("Check if cb server install on %s" % self.master.ip)
        else:
            self.cli_command_path = bin_path.replace('"','') + "/"
        self.root_path = LINUX_ROOT_PATH
        self.tmp_path = "/tmp/"
        self.tmp_path_raw = "/tmp/"
        self.cmd_ext = ""
        self.src_file = ""
        self.des_file = ""
        self.log_path = LINUX_COUCHBASE_LOGS_PATH
        self.base_cb_path = LINUX_CB_PATH
        """ non root path """
        if self.nonroot:
            self.log_path = "/home/%s%s" % (self.master.ssh_username,
                                            LINUX_COUCHBASE_LOGS_PATH)
            self.base_cb_path = "/home/%s%s" % (self.master.ssh_username,
                                                LINUX_CB_PATH)
            self.root_path = "/home/%s/" % self.master.ssh_username
        if type == 'windows':
            self.os = 'windows'
            self.cmd_ext = ".exe"
            self.root_path = WIN_ROOT_PATH
            self.tmp_path = WIN_TMP_PATH
            self.tmp_path_raw = WIN_TMP_PATH_RAW
            win_format = "C:/Program Files"
            cygwin_format = "/cygdrive/c/Program\ Files"
            if win_format in self.cli_command_path:
                self.cli_command_path = self.cli_command_path.replace(win_format,
                                                                      cygwin_format)
            self.base_cb_path = WIN_CB_PATH
        if info.distribution_type.lower() == 'mac':
            self.os = 'mac'
        self.full_v, self.short_v, self.build_number = self.shell.get_cbversion(type)
        self.couchbase_usrname = "%s" % (self.input.membase_settings.rest_username)
        self.couchbase_password = "%s" % (self.input.membase_settings.rest_password)
        self.cb_login_info = "%s:%s" % (self.couchbase_usrname,
                                        self.couchbase_password)
        self.path_type = self.input.param("path_type", None)
        if self.path_type is None:
            self.log.info("Test command with absolute path ")
        elif self.path_type == "local":
            self.log.info("Test command at %s dir " % self.cli_command_path)
            self.cli_command_path = "cd %s; ./" % self.cli_command_path
        self.cli_command = self.input.param("cli_command", None)

        self.start_with_cluster = self.input.param("start_with_cluster", True)
        if str(self.__class__).find('couchbase_clitest.CouchbaseCliTest') == -1:
            if len(self.servers) > 1 and int(self.nodes_init) == 1 and self.start_with_cluster:
                servers_in = [self.servers[i + 1] for i in range(self.num_servers - 1)]
                self.cluster.rebalance(self.servers[:1], servers_in, [])
        for bucket in self.buckets:
            testuser = [{'id': bucket.name, 'name': bucket.name, 'password': 'password'}]
            rolelist = [{'id': bucket.name, 'name': bucket.name, 'roles': 'admin'}]
            self.add_built_in_server_user(testuser=testuser, rolelist=rolelist)

    def tearDown(self):
        self.times_teardown_called += 1
        serverInfo = self.servers[0]
        rest = RestConnection(serverInfo)
        self.clusters_dic = self.input.clusters
        if self.clusters_dic:
            if len(self.clusters_dic) > 1:
                self.dest_nodes = self.clusters_dic[1]
                self.dest_master = self.dest_nodes[0]
                if self.dest_nodes and len(self.dest_nodes) > 1:
                    self.log.info("======== clean up destination cluster =======")
                    rest = RestConnection(self.dest_nodes[0])
                    rest.remove_all_remote_clusters()
                    rest.remove_all_replications()
                    BucketOperationHelper.delete_all_buckets_or_assert(self.dest_nodes, self)
                    ClusterOperationHelper.cleanup_cluster(self.dest_nodes)
            elif len(self.clusters_dic) == 1:
                self.log.error("=== need 2 cluster to setup xdcr in ini file ===")
        else:
            self.log.info("**** If run xdcr test, need cluster config is setup in ini file. ****")
        super(AltAddrBaseTest, self).tearDown()


    def _list_compare(self, list1, list2):
        if len(list1) != len(list2):
            return False
        for elem1 in list1:
            found = False
            for elem2 in list2:
                if elem1 == elem2:
                    found = True
                    break
            if not found:
                return False
        return True

    def get_internal_IP(self, server):
        shell = RemoteMachineShellConnection(server)
        internal_IP = shell.get_ip_address()
        internal_IP = [x for x in internal_IP if x != "127.0.0.1"]
        shell.disconnect()
        if internal_IP:
            return internal_IP[0]
        else:
            self.fail("Fail to get internal IP")

    def backup_reset_clusters(self, servers):
        BucketOperationHelper.delete_all_buckets_or_assert(servers, self)
        ClusterOperationHelper.cleanup_cluster(servers, master=servers[0])
        #ClusterOperationHelper.wait_for_ns_servers_or_assert(servers, self)

    def get_external_IP(self, internal_IP):
        found = False
        external_IP = ""
        for server in self.servers:
            internalIP = self.get_internal_IP(server)
            if internal_IP == internalIP:
                found = True
                external_IP = server.ip
                break
        if not found:
            self.fail("Could not find server which matches internal IP")
        else:
            return external_IP

    def setup_xdcr_cluster(self):
        if not self.input.clusters[0] and not self.input.clusters[1]:
            self.fail("This test needs ini set with cluster config")
        self.log.info("Create source cluster")
        self.create_xdcr_cluster(self.input.clusters[0])
        self.log.info("Create destination cluster")
        self.create_xdcr_cluster(self.input.clusters[1])

    def create_xdcr_cluster(self, cluster_servers):
        num_hostname_add = 1
        add_host_name = False
        if self.add_hostname_node_at_src:
            add_host_name = True
        if self.add_hostname_node_at_des:
            add_host_name = True
        shell = RemoteMachineShellConnection(cluster_servers[0])
        services_in = self.alt_addr_services_in
        if "-" in services_in:
            set_services = services_in.split("-")
        else:
            set_services = services_in.split(",")

        for server in cluster_servers[1:]:
            add_node_IP = self.get_internal_IP(server)
            node_services = "kv"
            if len(set_services) == 1:
                node_services = set_services[0]
            elif len(set_services) > 1:
                if len(set_services) == len(cluster_servers):
                    node_services = set_services[i]
                    i += 1
            if add_host_name and num_hostname_add <= self.num_hostname_add:
                add_node_IP = server.ip
                num_hostname_add += 1

            try:
                shell.alt_addr_add_node(main_server=cluster_servers[0], internal_IP=add_node_IP,
                                        server_add=server, services=node_services,
                                        cmd_ext=self.cmd_ext)
            except Exception as e:
                if e:
                    self.fail("Error: {0}".format(e))
        rest = RestConnection(cluster_servers[0])
        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
        rest.monitorRebalance()

    def create_xdcr_reference(self, src_IP, des_IP):
        cmd = "curl -u Administrator:password "
        cmd += "http://{0}:8091/pools/default/remoteClusters ".format(src_IP)
        cmd += "-d username=Administrator -d password=password "
        cmd += "-d name={0} -d demandEncryption=0 ".format(self.des_name)
        cmd += "-d hostname={0}:8091 ".format(des_IP)

        mesg = "\n **** Create XDCR cluster remote reference from cluster {0} ".format(src_IP)
        mesg += "to cluster {0}".format(des_IP)
        self.log.info(mesg)
        print("command to run: {0}".format(cmd))
        try:
            output = check_output(cmd, shell=True, stderr=STDOUT)
        except CalledProcessError as e:
            if e.output:
                self.fail("\n Error: ".format(e.output))

    def delete_xdcr_reference(self, src_IP, des_IP):
        cmd = "curl -X DELETE -u Administrator:password "
        cmd += "http://{0}:8091/pools/default/remoteClusters/{1}".format(src_IP, self.des_name)
        print("command to run: {0}".format(cmd))
        try:
            output = check_output(cmd, shell=True, stderr=STDOUT)
        except CalledProcessError as e:
            if e.output:
                self.fail("Error: ".format(e.output))

    def create_xdcr_replication(self, src_IP, des_IP, bucket_name):
        cmd = "curl -X POST -u Administrator:password "
        cmd += "http://{0}:8091/controller/createReplication ".format(src_IP)
        cmd += "-d fromBucket={0} ".format(bucket_name)
        cmd += "-d toCluster={0} ".format(self.des_name)
        cmd += "-d toBucket={0} ".format(bucket_name)
        cmd += "-d replicationType=continuous -d enableCompression=1 "
        print("command to run: {0}".format(cmd))
        try:
            output = check_output(cmd, shell=True, stderr=STDOUT)
            return output
        except CalledProcessError as e:
            if e.output:
                self.fail("Error: ".format(e.output))

    def delete_xdcr_replication(self, src_IP, replication_id):
        replication_id = urllib.quote(replication_id, safe='')
        cmd = "curl -X DELETE -u Administrator:password "
        cmd += " http://{0}:8091/controller/cancelXDCR/{1} ".format(src_IP, replication_id)
        print("command to run: {0}".format(cmd))
        try:
            output = check_output(cmd, shell=True, stderr=STDOUT)
        except CalledProcessError as e:
            if e.output:
                self.fail("Error: ".format(e.output))

    def set_xdcr_checkpoint(self, src_IP, check_time):
        cmd = "curl  -u Administrator:password "
        cmd += "http://{0}:8091/settings/replications ".format(src_IP)
        cmd += "-d goMaxProcs=10 "
        cmd += "-d checkpointInterval={0} ".format(check_time)
        print("command to run: {0}".format(cmd))
        try:
            self.log.info("Set xdcr checkpoint to {0} seconds".format(check_time))
            output = check_output(cmd, shell=True, stderr=STDOUT)
        except CalledProcessError as e:
            if e.output:
                self.fail("Error: ".format(e.output))

    def waitForItemCount(self, server, bucket_name, count, timeout=30):
        rest = RestConnection(server)
        for sec in range(timeout):
            items = int(
                rest.get_bucket_json(bucket_name)["basicStats"]["itemCount"])
            if items != count:
                time.sleep(1)
            else:
                return True
        log.info("Waiting for item count to be %d timed out", count)
        return False

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
