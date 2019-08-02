import random
import time
import json, subprocess

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
        if self.clusters_dic:
            if len(self.clusters_dic) > 1:
                self.dest_nodes = self.clusters_dic[1]
                self.dest_master = self.dest_nodes[0]
            elif len(self.clusters_dic) == 1:
                self.log.error("=== need 2 cluster to setup xdcr in ini file ===")
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
        self.num_hostname_add = self.input.param("num_hostname_add", 0)
        self.alt_addr_services_in = self.input.param("alt_addr_services_in", "kv")
        self.alt_addr_rebalance_out = self.input.param("alt_addr_rebalance_out", False)
        self.alt_addr_rebalance_in = self.input.param("alt_addr_rebalance_in", False)
        self.alt_addr_rebalance_in_services = self.input.param("alt_addr_rebalance_in_services", "kv")
        self.alt_addr_use_public_dns = self.input.param("alt_addr_use_public_dns", False)
        self.alt_addr_kv_loader = self.input.param("alt_addr_kv_loader", False)
        self.alt_addr_n1ql_query = self.input.param("alt_addr_n1ql_query", False)
        self.alt_addr_fts_loader = self.input.param("alt_addr_fts_loader", False)
        self.run_alt_addr_loader = self.input.param("run_alt_addr_loader", False)
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
        bin_path  = subprocess.check_output(cmd, shell=True)
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
