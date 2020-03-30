import time
import urllib.request, urllib.parse, urllib.error
import random
import testconstants

from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection, Bucket
from remote.remote_util import RemoteMachineShellConnection
from testconstants import LINUX_CW_LOG_PATH
from testconstants import MAC_CW_LOG_PATH
from testconstants import WINDOWS_CW_LOG_PATH
from testconstants import LINUX_COUCHBASE_BIN_PATH
from testconstants import WIN_COUCHBASE_BIN_PATH


class CWCBaseTest(BaseTestCase):

    def setUp(self):
        super(CWCBaseTest, self).setUp()
        self.product = self.input.param("product", "cb")
        self.vbuckets = self.input.param("vbuckets", 128)
        self.version = self.input.param("version", None)
        self.doc_ops = self.input.param("doc_ops", None)
        self.upload = self.input.param("upload", False)
        self.uploadHost = self.input.param("uploadHost", None)
        self.customer = self.input.param("customer", "")
        self.ticket = self.input.param("ticket", "")
        self.collect_nodes = self.input.param("collect_nodes", "*")
        self.cancel_collect = self.input.param("cancel_collect", False)
        self.cli_collect_nodes = self.input.param("cli_collect_nodes", "--all-nodes")
        self.cli_cancel_collect = self.input.param("cli_cancel_collect", False)
        self.cli_upload = self.input.param("cli_upload", False)
        self.shutdown_nodes = self.input.param("shutdown_nodes", None)
        self.add_services = self.input.param("add_services", None)
        if self.add_services is not None:
            if "-" in self.add_services:
                self.add_services = self.add_services.split("-")
            else:
                self.add_services = [self.add_services]
        if self.doc_ops is not None:
            self.doc_ops = self.doc_ops.split(";")
        self.defaul_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"

        #define the data that will be used to test
        self.blob_generator = self.input.param("blob_generator", True)
        server = self.servers[0]
        rest = RestConnection(server)
        if self.blob_generator:
            #gen_load data is used for upload before each test(1000 items by default)
            self.gen_load = BlobGenerator('test', 'test-', self.value_size, end=self.num_items)
            #gen_update is used for doing mutation for 1/2th of uploaded data
            self.gen_update = BlobGenerator('test', 'test-', self.value_size, end=(self.num_items // 2 - 1))
            #upload data before each test
            self._load_all_buckets(self.servers[0], self.gen_load, "create", 0)
        else:
            self._load_doc_data_all_buckets()
        shell = RemoteMachineShellConnection(self.master)
        type = shell.extract_remote_info().distribution_type
        shell.disconnect()
        self.log_path = ""
        self.bin_path = ""
        self.os_type = ""
        if type.lower() == 'windows':
            self.os_type = 'windows'
            self.log_path = WINDOWS_CW_LOG_PATH
            self.bin_path = WIN_COUCHBASE_BIN_PATH
        elif type.lower() in ["ubuntu", "centos", "red hat"]:
            self.os_type = "unix"
            self.log_path = LINUX_CW_LOG_PATH
            self.bin_path = LINUX_COUCHBASE_BIN_PATH
        elif type.lower() == "mac":
            self.os_type = 'mac'
            self.log_path = MAC_CW_LOG_PATH
            self.bin_path = MAC_COUCHBASE_BIN_PATH

    def tearDown(self):
        super(CWCBaseTest, self).tearDown()
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            info = shell.extract_remote_info()
            shell.stop_server(info.type.lower())
            shell.start_server(info.type.lower())
            shell.disconnect()

    """ convert output for CLI result to dict """
    def _get_dict(self, shell):
        command = "couchbase-cli collect-logs-status"
        output, e = shell.execute_command("{0}{1} -c {2}:8091 -u Administrator -p password " \
                                     .format(self.bin_path, command, self.master.ip))
        shell.log_command_output(output, e)
        result = {}
        if "runCmd" in output[0]:
            output = output[1:]
        if "Status" in output[0]:
            tmp = output[0].split(":")
            result["Status"] = tmp[1].strip()
        if "Details" in output[1]:
            result["perNode"] = {}
            for n in output[2:]:
                    if "Node" in n:
                        tmp_ip = n.split(":")
                        tmp_ip = tmp_ip[1].replace("ns_1@", "")
                        result["perNode"][tmp_ip] = {}
                    if "Status" in n:
                        tmp_stt = n.split(":")
                        result["perNode"][tmp_ip]["status"] = tmp_stt[1].strip()
                    if "path" in n:
                        tmp_p = n.split(":")
                        if self.os_type == 'windows':
                            result["perNode"][tmp_ip]["path"] = \
                                   ":".join((tmp_p[1].strip(), tmp_p[2].strip()))
                        elif self.os_type == 'unix':
                            result["perNode"][tmp_ip]["path"] = tmp_p[1].strip()
                        elif self.os_type == "mac":
                            result["perNode"][tmp_ip]["path"] = tmp_p[1].strip()
                    if "url" in n:
                        tmp_u = n.split(":")
                        result["perNode"][tmp_ip]["url"] = \
                                  ":".join((tmp_u[1].strip(), tmp_u[2].strip()))

            return result
        else:
            return None

    def _cli_get_cluster_logs_collection_status(self, shell):
        result = self._get_dict(shell)
        if result is not None:
            return result["Status"], result["perNode"]
        else:
            return None, None

    def _cli_monitor_collecting_log(self, shell, timeout):
        start_time = time.time()
        end_time = start_time + timeout
        collected = False
        uploaded = False
        cli_cancel_collect = False
        self.sleep(3)
        stt, perNode = self._cli_get_cluster_logs_collection_status(shell)
        while (stt == "running") and time.time() <= end_time :
            stt, perNode = self._cli_get_cluster_logs_collection_status(shell)
            if stt is not None and self.cli_cancel_collect:
                count = 0
                if "running" in stt and count == 0:
                    self.log.info("Start to cancel collect logs using CLI ")
                    status, content = self._cli_cancel_cluster_logs_collection(shell)
                    count += 1
                if "cancelled" in stt:
                    cli_cancel_collect = True
                    break
                elif count == 2:
                    self.fail("Failed to cancel log collection using CLI")

            self.log.info("CLI Cluster-wide collectinfo is running ...")
            if perNode is not None:
                for node in perNode:
                    self.log.info("Node: {0} **** Collect status: {1}" \
                                  .format(node, perNode[node]["status"]))
                    if "collected" in perNode[node]["status"]:
                        collected = True
                    elif "uploaded" in perNode[node]["status"]:
                        uploaded = True
            self.sleep(10)
        if time.time() > end_time:
            if self.cli_cancel_collect:
                self.log.error("Could not cancel log collection by CLI after {0} seconds ".format(timeout))
            elif self.upload:
                self.log.error("Log could not upload by CLI after {0} seconds ".format(timeout))
            else:
                self.log.error("Log could not collect by CLI after {0} seconds ".format(timeout))
            return collected, uploaded, cli_cancel_collect
        else:
            duration = time.time() - start_time
            self.log.info("log collection took {0} seconds ".format(duration))
            return collected, uploaded, cli_cancel_collect

    def _cli_verify_log_uploaded(self, shell):
        node_failed_to_uploaded = []
        status, perNode = self._cli_get_cluster_logs_collection_status(shell)
        for node in perNode:
            self.log.info("Verify log of node {0} uploaded to host: {1}" \
                          .format(node, self.uploadHost))
            uploaded = urllib.request.urlopen(perNode[node]["url"]).getcode()
            if uploaded == 200 and self.uploadHost in perNode[node]["url"]:
                self.log.info("Log of node {0} was uploaded to {1}" \
                              .format(node, perNode[node]["url"]))
            else:
                node_failed_to_uploaded.append(node)
        if not node_failed_to_uploaded:
            return True
        else:
            self.fail("Cluster-wide collectinfo using CLI failed to upload log at node(s) {0}" \
                           .format(node_failed_to_uploaded))

    def _cli_cancel_cluster_logs_collection(self, shell):
        cancel_status = False
        command = "couchbase-cli collect-logs-stop"
        o, e = shell.execute_command("{0}{1} -c {2}:8091 -u Administrator -p password " \
                                     .format(self.bin_path, command, self.master.ip))
        shell.log_command_output(o, e)
        if "SUCCESS" in o:
            cancel_status = True
        return cancel_status, o

    def _cli_verify_log_file(self, shell):
        stt, perNode = self._cli_get_cluster_logs_collection_status(shell)
        node_failed_to_collect = []
        for node in perNode:
            for server in self.servers[:self.nodes_init]:
                if server.ip in node or (self.nodes_init == 1 \
                                         and "127.0.0.1" in node):
                    shell = RemoteMachineShellConnection(server)
            file_name = perNode[node]["path"].replace(self.log_path, "")
            collected = False
            retry = 0
            while not collected and retry < 5:
                existed = shell.file_exists(self.log_path, file_name)
                if existed:
                    self.log.info("file {0} exists on node {1}"
                                  .format(perNode[node]["path"], node))
                    collected = True
                else:
                    self.log.info("retry {0} ".format(retry))
                    retry += 1
                    self.sleep(5)
                if retry == 5:
                    self.log.error("failed to collect log by CLI after {0} try at node {1}"
                                   .format(retry, node.replace("ns_1@", "")))
                    node_failed_to_collect.append(node)
        if not node_failed_to_collect:
            return True
        else:
            self.fail("CLI Cluster-wide collectinfo failed to collect log at {0}" \
                           .format(node_failed_to_collect))

    def _cli_generate_random_collecting_node(self, rest):
        random_nodes = []
        nodes = rest.get_nodes()
        for k in random.sample(list(range(int(self.nodes_init))), int(self.cli_collect_nodes)):
            node_ip = nodes[k].id.replace("ns_1@", "")
            random_nodes.append(node_ip)
        random_nodes =";".join(random_nodes)
        self.log.info("nodes randomly selected to do CWC {0}".format(random_nodes))
        return random_nodes
