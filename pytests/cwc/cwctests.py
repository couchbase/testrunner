import time
import unittest
import random
import testconstants
from TestInput import TestInputSingleton

from cwc.cwc_base import CWCBaseTest
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection



class CWCTests(CWCBaseTest):
    def setUp(self):
        super(CWCTests, self).setUp()
        self.command = self.input.param("command", "")
        self.command_options = self.input.param("command_options", '')
        self.item_size = self.input.param("item_size", 128)
        self.shutdown_node = self.input.param("shutdown_node", 1)
        self.do_verify = self.input.param("do-verify", True)
        self.timeout = 6000


    def tearDown(self):
        super(CWCTests, self).tearDown()

    def test_start_collect_log_without_upload(self):
        rest = RestConnection(self.master)
        shell = RemoteMachineShellConnection(self.master)
        if "*" not in str(self.collect_nodes) and self.nodes_init > 1:
            self.collect_nodes = self._generate_random_collecting_node(rest)
        status, content = rest.start_cluster_logs_collection(nodes=self.collect_nodes)
        if status:
            collected = self._monitor_collecting_log(rest, timeout=1200)
            if collected:
                self._verify_log_file(rest)
        shell.disconnect()

    def _monitor_collecting_log(self, rest, timeout):
        start_time = time.time()
        end_time = start_time + timeout
        progress = 0
        progress, stt, perNode = rest.get_cluster_logs_collection_status()
        while (progress != 100 or stt == "running") and time.time() <= end_time :
            progress, stt, perNode = rest.get_cluster_logs_collection_status()
            self.log.info("Cluster-wide collectinfo progress: {0}".format(progress))
            if perNode is not None:
                for node in perNode:
                    self.log.info("Node: {0} **** Collect status: {1}" \
                                  .format(node, perNode[node]["status"]))
            self.sleep(10)
        if time.time() > end_time:
            self.log.error("log could not collect after {0} seconds ".format(timeout))
            return False
        else:
            duration = time.time() - start_time
            self.log.info("log collection took {0} seconds ".format(duration))
            return True


    def _verify_log_file(self, rest):
        progress, status, perNode = rest.get_cluster_logs_collection_status()
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
                                  .format(perNode[node]["path"], node.replace("ns_1@", "")))
                    collected = True
                else:
                    self.log.info("retry {0} ".format(retry))
                    retry += 1
                    self.sleep(5)
                if retry == 5:
                    self.log.error("failed to collect log after {0} at node {1}"
                                   .format(retry, node.replace("ns_1@", "")))
                    node_failed_to_collect.append(node)
        if not node_failed_to_collect:
            return True
        else:
            self.fail("Cluster-wide collectinfo failed to collect log at {0}" \
                           .format(node_failed_to_collect))

    def _generate_random_collecting_node(self, rest):
        random_nodes = []
        nodes = rest.get_nodes()
        for k in random.sample(range(int(self.nodes_init)), int(self.collect_nodes)):
            random_nodes.append(nodes[k].id)
        random_nodes =",".join(random_nodes)
        self.log.info("nodes randomly selected to do CWC {0}".format(random_nodes))
        return random_nodes
