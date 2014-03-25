import logger
import copy
import json
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.rebalance_helper import RebalanceHelper
from remote.remote_util import RemoteMachineShellConnection
from remote.remote_util import RemoteUtilHelper
from failoverbasetests import FailoverBaseTest



class FailoverTests(FailoverBaseTest):
    def setUp(self):
        super(FailoverTests, self).setUp(self)

    def tearDown(self):
        super(FailoverTests, self).tearDown(self)

    def test_failover_firewall(self):
        self.common_test_body(self.num_items, 'firewall')

    def test_failover_normal(self):
        self.common_test_body(self.num_items, 'normal')

    def test_failover_stop_server(self):
        self.common_test_body(self.num_items, 'stop_server')

    def test_failover_then_add_back(self):
        self.add_back_flag = True
        self.common_test_body(self.num_items, 'normal')

    def common_test_body(self, keys_count, failover_reason):
        log = logger.Logger.get_logger()
        log.info("keys_count : {0}".format(keys_count))
        log.info("replicas : {0}".format(self.num_replicas))
        log.info("failover_reason : {0}".format(failover_reason))
        log.info('picking server : {0} as the master'.format(self.master))

        rest = RestConnection(self.master)
        versions = rest.get_nodes_versions()
        for version in versions:
            if "3" > version and self.graceful:
                log.error("Graceful failover can't be applied to nodes with version less then 3.*")
                log.error("Please check configuration parameters: SKIPPING TEST.")
                return

        self._load_all_buckets(self.master, self.gen_create, "create", 0,
                               batch_size=10000, pause_secs=5, timeout_secs=180)
        self._wait_for_stats_all_buckets(self.servers)

        nodes = rest.node_statuses()

        RebalanceHelper.wait_for_replication(self.servers, self.cluster)
        chosen = RebalanceHelper.pick_nodes(self.master, howmany=self.num_replicas)
        for node in chosen:
            # let's do op
            if failover_reason == 'stop_server':
                self.stop_server(node)
                log.info("10 seconds delay to wait for membase-server to shutdown")
                # wait for 5 minutes until node is down
                self.assertTrue(RestHelper(rest).wait_for_node_status(node, "unhealthy", 300),
                                    msg="node status is not unhealthy even after waiting for 5 minutes")
            elif failover_reason == "firewall":
                server = [srv for srv in self.servers if node.ip == srv.ip][0]
                RemoteUtilHelper.enable_firewall(server, bidirectional=self.bidirectional)
                status = RestHelper(rest).wait_for_node_status(node, "unhealthy", 300)
                if status:
                    log.info("node {0}:{1} is 'unhealthy' as expected".format(node.ip, node.port))
                else:
                    # verify iptables on the node if something wrong
                    for server in self.servers:
                        if server.ip == node.ip:
                            shell = RemoteMachineShellConnection(server)
                            info = shell.extract_remote_info()
                            if info.type.lower() == "windows":
                                o, r = shell.execute_command("netsh advfirewall show allprofiles")
                            else:
                                o, r = shell.execute_command("/sbin/iptables --list")
                            shell.log_command_output(o, r)
                            shell.disconnect()
                    rest.print_UI_logs()
                    api = rest.baseUrl + 'nodeStatuses'
                    status, content, header = rest._http_request(api)
                    json_parsed = json.loads(content)
                    self.log.info("nodeStatuses: {0}".format(json_parsed))
                    self.fail("node status is not unhealthy even after waiting for 5 minutes")

            failed_over = rest.fail_over(node.id, graceful=self.graceful)
            if self.graceful and (failover_reason in ['stop_server', 'firewall']):
                if failed_over:
                    # MB-10479
                    rest.print_UI_logs()
                self.assertFalse(failed_over, "Graceful Falover was started for unhealthy node!!! ")
                #perform general Failover
                failed_over = rest.fail_over(node.id)
            if not failed_over:
                self.log.info("unable to failover the node the first time. try again in  60 seconds..")
                # try again in 75 seconds
                self.sleep(75)
                failed_over = rest.fail_over(node.id, graceful=self.graceful)
            if self.graceful and (failover_reason not in ['stop_server', 'firewall']):
                reached = RestHelper(rest).rebalance_reached()
                self.assertTrue(reached, "rebalance failed for Graceful Failover, stuck or did not completed")

            self.assertTrue(failed_over, "unable to failover node after {0}".format(failover_reason))
            log.info("failed over node : {0}".format(node.id))
            self._failed_nodes.append(node)

        if self.add_back_flag:
            for node in self._failed_nodes:
                rest.add_back_node(node.id)
                self.sleep(5)
            self.sleep(10, "after failover before invoking rebalance...")
            rest.rebalance(otpNodes=[node.id for node in nodes],
                               ejectedNodes=[])
            msg = "rebalance failed while removing failover nodes {0}".format(chosen)
            self.assertTrue(rest.monitorRebalance(stop_if_loop=True), msg=msg)
        else:
            # Need a delay > min because MB-7168
            self.sleep(60, "after failover before invoking rebalance...")
            rest.rebalance(otpNodes=[node.id for node in nodes],
                               ejectedNodes=[node.id for node in chosen])
            if self.during_ops:
                self.sleep(5, "Wait for some progress in rebalance")
                if self.during_ops == "change_password":
                    old_pass = self.master.rest_password
                    self.change_password(new_password=self.input.param("new_password", "new_pass"))
                    rest = RestConnection(self.master)
                elif self.during_ops == "change_port":
                    self.change_port(new_port=self.input.param("new_port", "9090"))
                    rest = RestConnection(self.master)
            _servers_ = copy.deepcopy(self.servers)
            try:
                msg = "rebalance failed while removing failover nodes {0}".format(chosen)
                self.assertTrue(rest.monitorRebalance(stop_if_loop=True), msg=msg)
                for failed in chosen:
                    for server in _servers_:
                        if server.ip == failed.ip:
                             _servers_.remove(server)
                             self._cleanup_nodes.append(server)

                log.info("Begin VERIFICATION ...")
                RebalanceHelper.wait_for_replication(_servers_, self.cluster)
                self.verify_cluster_stats(_servers_, self.master)
            finally:
                if self.during_ops:
                     if self.during_ops == "change_password":
                         self.change_password(new_password=old_pass)
                     elif self.during_ops == "change_port":
                         self.change_port(new_port='8091',
                                          current_port=self.input.param("new_port", "9090"))


    def stop_server(self, node):
        log = logger.Logger.get_logger()
        for server in self.servers:
            if server.ip == node.ip:
                shell = RemoteMachineShellConnection(server)
                if shell.is_couchbase_installed():
                    shell.stop_couchbase()
                    log.info("Couchbase stopped")
                else:
                    shell.stop_membase()
                    log.info("Membase stopped")
                shell.disconnect()
                break
