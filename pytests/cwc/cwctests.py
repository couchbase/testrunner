import time
import unittest
import urllib.request, urllib.parse, urllib.error
import random, copy
import testconstants
from TestInput import TestInputSingleton

from cwc.cwc_base import CWCBaseTest
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from testconstants import COUCHBASE_FROM_WATSON



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

    def test_start_collect_log(self):
        rest = RestConnection(self.master)
        shell = RemoteMachineShellConnection(self.master)
        """ add service nodes to cluster.  Test will add with or without data service in """
        if self.add_services:
            self.add_services = [x.replace(":", ",")  or  x for x in self.add_services]
            if len(self.servers) > 1:
                nodes = rest.get_nodes()
                cwc_servers = copy.deepcopy(self.servers)
                if len(self.add_services) == 1:
                    self._add_service_node(cwc_servers, len(self.add_services),
                                                    services=self.add_services)
                elif len(self.add_services) > 1 and \
                             (len(nodes) + len(self.add_services)) <= len(cwc_servers):
                    for service in self.add_services:
                        nodes = rest.get_nodes()
                        self._add_service_node(cwc_servers, len(self.add_services),
                                                                services=[service])
                elif (len(nodes) + len(self.add_services)) > len(cwc_servers):
                    self.fail("Not enough servers to add services nodes")
            else:
                self.log.error("Need more than 2 servers to run this test")

        if "*" not in str(self.collect_nodes) and self.nodes_init > 1:
            self.collect_nodes = self._generate_random_collecting_node(rest)
        status, content = rest.start_cluster_logs_collection(nodes=self.collect_nodes, \
                                upload=self.upload, uploadHost=self.uploadHost, \
                                customer=self.customer, ticket=self.ticket)
        if status:
            collected, uploaded, cancel_collect  = \
                       self._monitor_collecting_log(rest, timeout=1200)
            if collected:
                self._verify_log_file(rest)
            if self.upload and uploaded:
                self._verify_log_uploaded(rest)
            if self.cancel_collect:
                if cancel_collect:
                    self.log.info("Logs collection were cancelled")
                else:
                    self.fail("Failed to cancel log collection")
            shell.disconnect()
        else:
            self.fail("ERROR:  {0}".format(content))

    """
        Add node to cluster with different services
    """
    def _add_service_node(self, cwc_servers, serices_add, services="kv"):
        if serices_add == 1:
            total_servers = cwc_servers[:3]
        elif services_add > 1:
            total_servers = cwc_servers[:len(nodes) + 1]
        add_node_rest = RestConnection(cwc_servers[len(nodes)])
        try:
            add_node_rest.force_eject_node()
            status = add_node_rest.init_node_services(
                                        hostname=cwc_servers[len(nodes)].ip,
                                        services=services)
            init_node = self.cluster.async_init_node(cwc_servers[len(nodes)])
            self.log.info("===== add node %s with service %s =====" \
                                                       % (cwc_servers[len(nodes)].ip,
                                                          services))
            self.cluster.rebalance(total_servers, [cwc_servers[len(nodes)]], \
                                                        [], services=services)
        except Exception as e:
            if e:
                print(e)
        if status:
            if self.cb_version[:5] in COUCHBASE_FROM_WATSON:
                add_node_rest.set_indexer_storage_mode(storageMode="memory_optimized")

    def _monitor_collecting_log(self, rest, timeout):
        start_time = time.time()
        end_time = start_time + timeout
        collected = False
        uploaded = False
        cancel_collect = False
        progress = 0
        progress, stt, perNode = rest.get_cluster_logs_collection_status()
        while (progress != 100 or stt == "running") and time.time() <= end_time :
            progress, stt, perNode = rest.get_cluster_logs_collection_status()
            if stt is not None and self.cancel_collect:
                count = 0
                if "running" in stt and count == 0:
                    self.log.info("Start to cancel collect logs ")
                    status, content = rest.cancel_cluster_logs_collection()
                    count += 1
                if "cancelled" in stt:
                    cancel_collect = True
                    break
                elif count == 2:
                    self.fail("Failed to cancel log collection")
            self.log.info("Cluster-wide collectinfo progress: {0}".format(progress))
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
            if self.cancel_collect:
                self.log.error("Could not cancel log collection after {0} seconds ".format(timeout))
            elif self.upload:
                self.log.error("Log could not upload after {0} seconds ".format(timeout))
            else:
                self.log.error("Log could not collect after {0} seconds ".format(timeout))
            return collected, uploaded, cancel_collect
        else:
            duration = time.time() - start_time
            self.log.info("log collection took {0} seconds ".format(duration))
            return collected, uploaded, cancel_collect


    def _verify_log_file(self, rest):
        progress, status, perNode = rest.get_cluster_logs_collection_status()
        node_failed_to_collect = []
        nodes = rest.get_nodes()
        for node in perNode:
            for server in self.servers[:len(nodes)]:
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
                    self.log.error("failed to collect log after {0} try at node {1}"
                                   .format(retry, node.replace("ns_1@", "")))
                    node_failed_to_collect.append(node)
        if not node_failed_to_collect:
            return True
        else:
            self.fail("Cluster-wide collectinfo failed to collect log at {0}" \
                           .format(node_failed_to_collect))

    def _verify_log_uploaded(self, rest):
        node_failed_to_uploaded = []
        progress, status, perNode = rest.get_cluster_logs_collection_status()
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
            self.fail("Cluster-wide collectinfo failed to upload log at node(s) {0}" \
                           .format(node_failed_to_uploaded))

    def test_cli_start_collect_log(self):
        command = "couchbase-cli collect-logs-start"
        rest = RestConnection(self.master)
        shell = RemoteMachineShellConnection(self.master)
        num_node_collect = self.cli_collect_nodes
        if "--all-nodes" not in str(self.cli_collect_nodes) and self.nodes_init > 1:
            self.cli_collect_nodes = self._cli_generate_random_collecting_node(rest)
            num_node_collect = '--nodes="{0}"'.format(self.cli_collect_nodes)
        if not self.cli_upload:
            o, e = shell.execute_command("{0}{1} -c {2}:8091 -u Administrator -p password {3} " \
                             .format(self.bin_path, command, self.master.ip, num_node_collect))
        else:
            o, e = shell.execute_command("{0}{1} -c {2}:8091 -u Administrator -p password {3} --upload \
                           --upload-host='{4}' --customer='{5}' --ticket='{6}' " .format(self.bin_path, \
                           command, self.master.ip, num_node_collect, self.uploadHost, self.customer, \
                           self.ticket))
        self.log.info("Command output is {0} {1}".format(o, e) )
        shell.log_command_output(o, e)
        if "runCmd" in o[0]:
            o = o[1:]
        """ output when --nodes is used
                ['NODES: ns_1@12,ns_1@11,ns_1@10', 'SUCCESS: Log collection started']
            output when --all-nodes is used
                'SUCCESS: Log collection started' """
        status_check = o[0]
        if "--all-nodes" not in str(self.cli_collect_nodes):
            status_check = o[1]
        if "SUCCESS" in status_check:
            self.log.info("start monitoring cluster-wide collectinfo using CLI ...")
            collected, uploaded, cancel_collect = \
                   self._cli_monitor_collecting_log(shell, timeout=1200)
            if collected:
                self._cli_verify_log_file(shell)
            if self.cli_upload and uploaded:
                self._cli_verify_log_uploaded(shell)
            if self.cli_cancel_collect:
                if cancel_collect:
                    self.log.info("Logs collection were cancelled by CLI")
                else:
                    self.fail("Failed to cancel log collection by CLI")
            shell.disconnect()
        elif o and o[0] and "ERROR" in o[0]:
                self.fail("ERROR:  {0}".format(o[0]))

    def _generate_random_collecting_node(self, rest):
        random_nodes = []
        nodes = rest.get_nodes()
        for k in random.sample(list(range(int(self.nodes_init))), int(self.collect_nodes)):
            random_nodes.append(nodes[k].id)
        random_nodes =",".join(random_nodes)
        self.log.info("nodes randomly selected to do CWC {0}".format(random_nodes))
        return random_nodes
