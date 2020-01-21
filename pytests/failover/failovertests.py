import copy
import json
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.rebalance_helper import RebalanceHelper
from remote.remote_util import RemoteMachineShellConnection
from remote.remote_util import RemoteUtilHelper
from .failoverbasetests import FailoverBaseTest

GRACEFUL = "graceful"

class FailoverTests(FailoverBaseTest):
    def setUp(self):
        super(FailoverTests, self).setUp()
        self.server_map = self.get_server_map(self.servers)

    def tearDown(self):
        super(FailoverTests, self).tearDown()

    def test_failover_firewall(self):
        self.common_test_body('firewall')

    def test_failover_normal(self):
        self.common_test_body('normal')

    def test_failover_stop_server(self):
        self.common_test_body('stop_server')

    def test_failover_then_add_back(self):
        self.add_back_flag = True
        self.common_test_body('normal')

    def common_test_body(self, failover_reason):
        """
            Main Test body which contains the flow of the failover basic steps
            1. Starts Operations if programmed into the test case(before/after)
            2. Start View and Index Building operations
            3. Failover K out of N nodes (failover can be HARD/GRACEFUL)
            4.1 Rebalance the cluster is failover of K nodeStatuses
            4.2 Run Add-Back operation with recoveryType = (full/delta) with rebalance
            5. Verify all expected operations completed by checking stats,
               replication, views, data correctness
        """
        # Pick the reference node for communication
        # We pick a node in the cluster which will NOT be failed over
        self.filter_list = []
        if self.failoverMaster:
            self.master = self.servers[1]
        self.log.info("Picking node {0} as reference node for test case"
                      .format(self.master.ip))
        self.print_test_params(failover_reason)
        self.rest = RestConnection(self.master)
        self.nodes = self.rest.node_statuses()
        # Set the data path for the cluster
        self.data_path = self.rest.get_data_path()

        # Check if the test case has to be run for 3.0.0
        versions = self.rest.get_nodes_versions()
        self.version_greater_than_2_5 = True
        for version in versions:
            if "3" > version:
                self.version_greater_than_2_5 = False

        # Do not run this this test if graceful category is being used
        if not self.version_greater_than_2_5 and (self.graceful or (self.recoveryType != None)):
            self.log.error("Graceful failover can't be applied to nodes with version less then 3.*")
            self.log.error("Please check configuration parameters: SKIPPING TEST.")
            return

        # Find nodes that will under go failover
        if self.failoverMaster:
            self.chosen = RebalanceHelper.pick_nodes(
                self.master, howmany=1, target_node=self.servers[0])
        else:
            self.chosen = RebalanceHelper.pick_nodes(
                self.master, howmany=self.num_failed_nodes)

        # Perform operations - Create/Update/Delete
        # self.withMutationOps = True => Run Operations in parallel to failover
        # self.withMutationOps = False => Run Operations Before failover
        self.load_initial_data()
        if not self.withMutationOps:
            self.run_mutation_operations()
        # Perform view creation tasks and wait for completion before failover
        if self.withViewsOps:
            self.run_view_creation_operations(self.servers)
            if not self.createIndexesDuringFailover:
                self.query_and_monitor_view_tasks(self.servers)

        # Validate seq_no snap_start/stop values
        self.check_snap_start_corruption()

        # Take snap-shot of data set used for validaiton
        record_static_data_set = dict()
        prev_vbucket_stats = dict()
        prev_failover_stats = dict()
        if not self.withMutationOps:
            record_static_data_set = self.get_data_set_all(
                self.servers, self.buckets, path=None)

        # Capture  vbucket and failover stats if test version >= 2.5.*
        if self.version_greater_than_2_5 and self.upr_check:
            prev_vbucket_stats = self.get_vbucket_seqnos(self.servers,
                                                         self.buckets)
            prev_failover_stats = self.get_failovers_logs(self.servers,
                                                          self.buckets)

        # Perform Operations related to failover
        if self.withMutationOps or self.withViewsOps or self.compact:
            self.run_failover_operations_with_ops(self.chosen, failover_reason)
        else:
            self.run_failover_operations(self.chosen, failover_reason)

        # TODO: Enable this even when 'flusher_batch_split_trigger' is not set
        if self.flusher_batch_split_trigger and \
                self.num_replicas >= self.num_failed_nodes:
            tasks = self._async_load_all_buckets(
                self.master, self.gen_update, "update", 0)
            for task in tasks:
                task.result()

        if self.graceful:
            # Validate seq_no snap_start/stop values
            self.check_snap_start_corruption()

        # Add back + rebalance // only rebalance with verification
        if not self.gracefulFailoverFail and self.runRebalanceAfterFailover:
            if self.add_back_flag:
                self.run_add_back_operation_and_verify(
                    self.chosen, prev_vbucket_stats, record_static_data_set,
                    prev_failover_stats)
            else:
                self.run_rebalance_after_failover_and_verify(
                    self.chosen, prev_vbucket_stats, record_static_data_set,
                    prev_failover_stats)

        if self.graceful:
            # Validate seq_no snap_start/stop values
            self.check_snap_start_corruption()

        if self.during_ops is None:
            self.verify_unacked_bytes_all_buckets(filter_list=self.filter_list,
                                                  master_node=self.master)

    def run_rebalance_after_failover_and_verify(self, chosen, prev_vbucket_stats, record_static_data_set, prev_failover_stats):
        """ Method to run rebalance after failover and verify """
        # Need a delay > min because MB-7168
        _servers_ = self.filter_servers(self.servers, chosen)
        self._wait_for_stats_all_buckets(_servers_, check_ep_items_remaining=True)
        self.sleep(5, "after failover before invoking rebalance...")
        # Rebalance after Failover operation
        self.rest.rebalance(otpNodes=[node.id for node in self.nodes], ejectedNodes=[node.id for node in chosen])
        if self.during_ops:
            self.sleep(5, "Wait for some progress in rebalance")
            if self.during_ops == "change_password":
                old_pass = self.master.rest_password
                self.change_password(new_password=self.input.param("new_password", "new_pass"))
                self.rest = RestConnection(self.master)
            elif self.during_ops == "change_port":
                self.change_port(new_port=self.input.param("new_port", "9090"))
                self.rest = RestConnection(self.master)
        # Perform Compaction
        if self.compact:
            for bucket in self.buckets:
                self.cluster.compact_bucket(self.master, bucket)
        # Peform View Validation if Supported
        nodes = self.filter_servers(self.servers, chosen)
        if self.withViewsOps:
            self.query_and_monitor_view_tasks(nodes)

        # Run operations if required during rebalance after failover
        if self.withMutationOps:
            self.run_mutation_operations_after_failover()

        # Kill or restart operations
        if self.killNodes or self.stopNodes or self.firewallOnNodes:
            self.victim_node_operations(node=chosen[0])
            self.sleep(60)
            self.log.info(" Start Rebalance Again !")
            self.rest.rebalance(otpNodes=[node.id for node in self.nodes], ejectedNodes=[node.id for node in chosen])

        # Rebalance Monitoring
        msg = "rebalance failed while removing failover nodes {0}".format([node.id for node in chosen])
        self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg=msg)

        # Reset password or port
        if self.during_ops:
            if self.during_ops == "change_password":
                self.change_password(new_password=old_pass)
            elif self.during_ops == "change_port":
                self.change_port(new_port='8091',
                current_port=self.input.param("new_port", "9090"))
            return

        #  Drain Queue and make sure intra-cluster replication is complete
        self.log.info("Begin VERIFICATION for Rebalance after Failover Only")
        self.verify_cluster_stats(_servers_, self.master, check_bucket_stats=True, check_ep_items_remaining=True)
        # Verify all data set with meta data if failover happens after failover
        if not self.withMutationOps:
            self.sleep(60)
            self.data_analysis_all(record_static_data_set, _servers_, self.buckets, path=None, addedItems=None)

        # Check Cluster Stats and Data as well if max_verify > 0
        # Check Failover logs :: Not sure about this logic, currently not checking, will update code once confirmed
        # Currently, only  for checking case where we  have graceful failover
        if self.version_greater_than_2_5 and self.graceful and self.upr_check:
            new_failover_stats = self.compare_failovers_logs(prev_failover_stats, _servers_, self.buckets)
            new_vbucket_stats = self.compare_vbucket_seqnos(prev_vbucket_stats, _servers_, self.buckets)
            self.compare_vbucketseq_failoverlogs(new_vbucket_stats, new_failover_stats)
        # Verify Active and Replica Bucket Count
        if self.num_replicas > 0:
            nodes = self.get_nodes_in_cluster(self.master)
            self.vb_distribution_analysis(servers=nodes, buckets=self.buckets, std=20.0, total_vbuckets=self.total_vbuckets)
        self.log.info("End VERIFICATION for Rebalance after Failover Only")

    def run_add_back_operation_and_verify(self, chosen, prev_vbucket_stats, record_static_data_set, prev_failover_stats):
        """
            Method to run add-back operation with recovery type = (delta/full)
            It also verifies if the operations are correct with data verificaiton steps
        """
        _servers_ = self.filter_servers(self.servers, chosen)
        self._wait_for_stats_all_buckets(_servers_, check_ep_items_remaining=True)
        recoveryTypeMap = self.define_maps_during_failover(self.recoveryType)
        fileMapsForVerification = self.create_file(chosen, self.buckets, self.server_map)
        index = 0
        for node in chosen:
            self.sleep(5)
            if self.recoveryType:
                # define precondition for recoverytype
                self.rest.set_recovery_type(otpNode=node.id, recoveryType=self.recoveryType[index])
                index += 1
            else:
                self.rest.add_back_node(node.id)

        # Doc_mutation before triggering rebalance
        tasks = self._async_load_all_buckets(
            self.master, self.gen_update, "update", 0)
        for task in tasks:
            task.result()

        self.sleep(20, "After failover before invoking rebalance...")
        self.rest.rebalance(otpNodes=[node.id for node in self.nodes],
                            ejectedNodes=[],
                            deltaRecoveryBuckets=self.deltaRecoveryBuckets)

        # Perform Compaction
        if self.compact:
            for bucket in self.buckets:
                self.cluster.compact_bucket(self.master, bucket)

        # Peform View Validation if Supported
        nodes = self.filter_servers(self.servers, chosen)
        if self.withViewsOps:
            self.query_and_monitor_view_tasks(nodes)

        # Run operations if required during rebalance after failover
        if self.withMutationOps:
            self.run_mutation_operations_after_failover()

        # Kill or restart operations
        if self.killNodes or self.stopNodes or self.firewallOnNodes:
            self.victim_node_operations(node=chosen[0])
            self.sleep(60)
            self.log.info("Start Rebalance Again!")
            self.rest.rebalance(otpNodes=[node.id for node in self.nodes],
                                ejectedNodes=[],
                                deltaRecoveryBuckets=self.deltaRecoveryBuckets)

        # Check if node has to be killed or restarted during rebalance
        # Monitor Rebalance
        msg = "rebalance failed while removing failover nodes {0}".format(chosen)
        self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg=msg)

        # Drain ep_queue and make sure that intra-cluster replication is complete
        self._wait_for_stats_all_buckets(self.servers, check_ep_items_remaining=True)

        self.log.info("Begin VERIFICATION for Add-back and rebalance")

        # Verify Stats of cluster and Data is max_verify > 0
        self.verify_cluster_stats(self.servers, self.master, check_bucket_stats=True, check_ep_items_remaining=True)

        # Verify recovery Type succeeded if we added-back nodes
        self.verify_for_recovery_type(chosen, self.server_map, self.buckets, recoveryTypeMap, fileMapsForVerification, self.deltaRecoveryBuckets)

        # Comparison of all data if required
        if not self.withMutationOps:
            self.sleep(60)
            self.data_analysis_all(record_static_data_set, self.servers, self.buckets, path=None, addedItems=None)

        # Verify if vbucket sequence numbers and failover logs are as expected
        # We will check only for version  > 2.5.* and if the failover is graceful
        if self.version_greater_than_2_5 and self.graceful and self.upr_check:
            new_vbucket_stats = self.compare_vbucket_seqnos(prev_vbucket_stats, self.servers, self.buckets, perNode=False)
            new_failover_stats = self.compare_failovers_logs(prev_failover_stats, self.servers, self.buckets)
            self.compare_vbucketseq_failoverlogs(new_vbucket_stats, new_failover_stats)

        # Verify Active and Replica Bucket Count
        if self.num_replicas > 0:
            nodes = self.get_nodes_in_cluster(self.master)
            self.vb_distribution_analysis(servers=nodes, buckets=self.buckets, std=20.0, total_vbuckets=self.total_vbuckets)

        self.log.info("End VERIFICATION for Add-back and rebalance")

    def print_test_params(self, failover_reason):
        """ Method to print test parameters """
        self.log.info("num_replicas : {0}".format(self.num_replicas))
        self.log.info("recoveryType : {0}".format(self.recoveryType))
        self.log.info("failover_reason : {0}".format(failover_reason))
        self.log.info("num_failed_nodes : {0}".format(self.num_failed_nodes))
        self.log.info('picking server : {0} as the master'.format(self.master))

    def run_failover_operations(self, chosen, failover_reason):
        """ Method to run fail over operations used in the test scenario based on failover reason """
        # Perform Operations relalted to failover
        graceful_count = 0
        graceful_failover = True
        failed_over = True
        for node in chosen:
            unreachable = False
            if failover_reason == 'stop_server':
                unreachable = True
                self.stop_server(node)
                self.log.info("10 seconds delay to wait for membase-server to shutdown")
                # wait for 5 minutes until node is down
                self.assertTrue(RestHelper(self.rest).wait_for_node_status(node, "unhealthy", self.wait_timeout * 10),
                                    msg="node status is not unhealthy even after waiting for 5 minutes")
            elif failover_reason == "firewall":
                unreachable = True
                self.filter_list.append (node.ip)
                server = [srv for srv in self.servers if node.ip == srv.ip][0]
                RemoteUtilHelper.enable_firewall(server, bidirectional=self.bidirectional)
                status = RestHelper(self.rest).wait_for_node_status(node, "unhealthy", self.wait_timeout * 10)
                if status:
                    self.log.info("node {0}:{1} is 'unhealthy' as expected".format(node.ip, node.port))
                else:
                    # verify iptables on the node if something wrong
                    for server in self.servers:
                        if server.ip == node.ip:
                            shell = RemoteMachineShellConnection(server)
                            info = shell.extract_remote_info()
                            if info.type.lower() == "windows":
                                o, r = shell.execute_command("netsh advfirewall show allprofiles")
                                shell.log_command_output(o, r)
                            else:
                                o, r = shell.execute_command("/sbin/iptables --list")
                                shell.log_command_output(o, r)
                            shell.disconnect()
                    self.rest.print_UI_logs()
                    api = self.rest.baseUrl + 'nodeStatuses'
                    status, content, header = self.rest._http_request(api)
                    json_parsed = json.loads(content)
                    self.log.info("nodeStatuses: {0}".format(json_parsed))
                    self.fail("node status is not unhealthy even after waiting for 5 minutes")
            # verify the failover type
            if self.check_verify_failover_type:
                graceful_count, graceful_failover = self.verify_failover_type(node, graceful_count, self.num_replicas, unreachable)
            # define precondition check for failover
            success_failed_over = self.rest.fail_over(node.id, graceful=(self.graceful and graceful_failover))
            if self.graceful and graceful_failover:
                if self.stopGracefulFailover or self.killNodes or self.stopNodes or self.firewallOnNodes:
                    self.victim_node_operations(node)
                    # Start Graceful Again
                    self.log.info(" Start Graceful Failover Again !")
                    self.sleep(60)
                    success_failed_over = self.rest.fail_over(node.id, graceful=(self.graceful and graceful_failover))
                    msg = "graceful failover failed for nodes {0}".format(node.id)
                    self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg=msg)
                else:
                    msg = "rebalance failed while removing failover nodes {0}".format(node.id)
                    self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg=msg)
            failed_over = failed_over and success_failed_over

        # Check for negative cases
        if self.graceful and (failover_reason in ['stop_server', 'firewall']):
            if failed_over:
                # MB-10479
                self.rest.print_UI_logs()
            self.assertFalse(failed_over, "Graceful Falover was started for unhealthy node!!! ")
            return
        elif self.gracefulFailoverFail and not failed_over:
            """ Check if the fail_over fails as expected """
            self.assertFalse(failed_over, """ Graceful failover should fail due to not enough replicas """)
            return

        # Check if failover happened as expected or re-try one more time
        if not failed_over:
            self.log.info("unable to failover the node the first time. try again in  60 seconds..")
            # try again in 75 seconds
            self.sleep(75)
            failed_over = self.rest.fail_over(node.id, graceful=(self.graceful and graceful_failover))
        if self.graceful and (failover_reason not in ['stop_server', 'firewall']):
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed for Graceful Failover, stuck or did not completed")

        # Verify Active and Replica Bucket Count
        if self.num_replicas > 0:
            nodes = self.filter_servers(self.servers, chosen)
            self.vb_distribution_analysis(servers=nodes, buckets=self.buckets, std=20.0, total_vbuckets=self.total_vbuckets, type="failover", graceful=(self.graceful and graceful_failover))

    def run_failover_operations_with_ops(self, chosen, failover_reason):
        """ Method to run fail over operations used in the test scenario based on failover reason """
        # Perform Operations relalted to failover
        failed_over = True
        for node in chosen:
            unreachable = False
            if failover_reason == 'stop_server':
                unreachable = True
                self.stop_server(node)
                self.log.info("10 seconds delay to wait for membase-server to shutdown")
                # wait for 5 minutes until node is down
                self.assertTrue(RestHelper(self.rest).wait_for_node_status(node, "unhealthy", 300),
                                    msg="node status is not unhealthy even after waiting for 5 minutes")
            elif failover_reason == "firewall":
                unreachable = True
                self.filter_list.append (node.ip)
                server = [srv for srv in self.servers if node.ip == srv.ip][0]
                RemoteUtilHelper.enable_firewall(server, bidirectional=self.bidirectional)
                status = RestHelper(self.rest).wait_for_node_status(node, "unhealthy", 300)
                if status:
                    self.log.info("node {0}:{1} is 'unhealthy' as expected".format(node.ip, node.port))
                else:
                    # verify iptables on the node if something wrong
                    for server in self.servers:
                        if server.ip == node.ip:
                            shell = RemoteMachineShellConnection(server)
                            info = shell.extract_remote_info()
                            if info.type.lower() == "windows":
                                o, r = shell.execute_command("netsh advfirewall show allprofiles")
                                shell.log_command_output(o, r)
                            else:
                                o, r = shell.execute_command("/sbin/iptables --list")
                                shell.log_command_output(o, r)
                            shell.disconnect()
                    self.rest.print_UI_logs()
                    api = self.rest.baseUrl + 'nodeStatuses'
                    status, content, header = self.rest._http_request(api)
                    json_parsed = json.loads(content)
                    self.log.info("nodeStatuses: {0}".format(json_parsed))
                    self.fail("node status is not unhealthy even after waiting for 5 minutes")
        nodes = self.filter_servers(self.servers, chosen)
        failed_over = self.cluster.async_failover([self.master], failover_nodes=chosen, graceful=self.graceful)
        # Perform Compaction
        compact_tasks = []
        if self.compact:
            for bucket in self.buckets:
                compact_tasks.append(self.cluster.async_compact_bucket(self.master, bucket))
        # Run View Operations
        if self.withViewsOps:
            self.query_and_monitor_view_tasks(nodes)
        # Run mutation operations
        if self.withMutationOps:
            self.run_mutation_operations()
        failed_over.result()
        for task in compact_tasks:
            task.result()
        msg = "rebalance failed while removing failover nodes {0}".format(node.id)
        self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg=msg)


    def load_initial_data(self):
        """ Method to run operations Update/Delete/Create """
        # Load All Buckets if num_items > 0
        tasks = []
        tasks += self._async_load_all_buckets(self.master, self.gen_initial_create, "create", 0, flag=2, batch_size=20000)
        for task in tasks:
            task.result()
        self._wait_for_stats_all_buckets(self.servers, check_ep_items_remaining=True)
        self._verify_stats_all_buckets(self.servers, timeout=120)

    def run_mutation_operations(self):
        mutation_ops_tasks = []
        if "create" in self.doc_ops:
            mutation_ops_tasks += self._async_load_all_buckets(
                self.master, self.gen_create, "create", 0)
        if "update" in self.doc_ops:
            mutation_ops_tasks += self._async_load_all_buckets(
                self.master, self.gen_update, "update", 0)
        if "delete" in self.doc_ops:
            mutation_ops_tasks += self._async_load_all_buckets(
                self.master, self.gen_delete, "delete", 0)
        try:
            for task in mutation_ops_tasks:
                task.result()
        except Exception as ex:
            self.log.info(ex)

    def run_mutation_operations_after_failover(self):
        mutation_ops_tasks = []
        if "create" in self.doc_ops:
            mutation_ops_tasks += self._async_load_all_buckets(
                self.master, self.afterfailover_gen_create, "create", 0)
        if "update" in self.doc_ops:
            mutation_ops_tasks += self._async_load_all_buckets(
                self.master, self.afterfailover_gen_update, "update", 0)
        if "delete" in self.doc_ops:
            mutation_ops_tasks += self._async_load_all_buckets(
                self.master, self.afterfailover_gen_delete, "delete", 0)
        try:
            for task in mutation_ops_tasks:
                task.result()
        except Exception as ex:
            self.log.info(ex)

    def define_maps_during_failover(self, recoveryType=[]):
        """ Method to define nope ip, recovery type map """
        recoveryTypeMap = {}
        index = 0
        for server in self.chosen:
            if recoveryType:
                recoveryTypeMap[server.ip] = recoveryType[index]
            index += 1
        return recoveryTypeMap

    def filter_servers(self, original_servers, filter_servers):
        """ Filter servers that have not failed over """
        _servers_ = copy.deepcopy(original_servers)
        for failed in filter_servers:
            for server in _servers_:
                if server.ip == failed.ip:
                    _servers_.remove(server)
                    self._cleanup_nodes.append(server)
        return _servers_

    def verify_for_recovery_type(self, chosen=[], serverMap={}, buckets=[], recoveryTypeMap={}, fileMap={}, deltaRecoveryBuckets=[]):
        """ Verify recovery type is delta or full """
        summary = ""
        logic = True
        for server in self.chosen:
            shell = RemoteMachineShellConnection(serverMap[server.ip])
            os_type = shell.extract_remote_info()
            if os_type.type.lower() == 'windows':
                return
            for bucket in buckets:
                path = fileMap[server.ip][bucket.name]
                exists = shell.file_exists(path, "check.txt")
                if deltaRecoveryBuckets != None:
                    if recoveryTypeMap[server.ip] == "delta" and (bucket.name in deltaRecoveryBuckets) and not exists:
                        logic = False
                        summary += "\n Failed Condition :: node {0}, bucket {1} :: Expected Delta, Actual Full".format(server.ip, bucket.name)
                    elif recoveryTypeMap[server.ip] == "delta" and (bucket.name not in deltaRecoveryBuckets) and exists:
                        summary += "\n Failed Condition :: node {0}, bucket {1} :: Expected Full, Actual Delta".format(server.ip, bucket.name)
                        logic = False
                else:
                    if recoveryTypeMap[server.ip] == "delta"  and not exists:
                        logic = False
                        summary += "\n Failed Condition :: node {0}, bucket {1} :: Expected Delta, Actual Full".format(server.ip, bucket.name)
                    elif recoveryTypeMap[server.ip] == "full" and exists:
                        logic = False
                        summary += "\n Failed Condition :: node {0}, bucket {1}  :: Expected Full, Actual Delta".format(server.ip, bucket.name)
            shell.disconnect()
        self.assertTrue(logic, summary)

    def run_view_creation_operations(self, servers):
        """" Run view Creation and indexing building tasks on servers """
        num_views = self.input.param("num_views", 5)
        is_dev_ddoc = self.input.param("is_dev_ddoc", True)
        num_tries = self.input.param("num_tries", 10)
        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[is_dev_ddoc]

        query = {}
        query["connectionTimeout"] = 60000;
        query["full_set"] = "true"

        views = []
        tasks = []
        for bucket in self.buckets:
            temp = self.make_default_views(self.default_view_name, num_views,
                                           is_dev_ddoc, different_map=False)
            temp_tasks = self.async_create_views(self.master, ddoc_name, temp, bucket)
            views += temp
            tasks += temp_tasks

        timeout = max(self.wait_timeout * 4, len(self.buckets) * self.wait_timeout * self.num_items // 50000)

        for task in tasks:
            task.result(self.wait_timeout * 20)

    def query_and_monitor_view_tasks(self, servers):
        """ Monitor Query Tasks for their completion """
        num_views = self.input.param("num_views", 5)
        is_dev_ddoc = self.input.param("is_dev_ddoc", True)
        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[is_dev_ddoc]
        self.verify_query_task()
        active_tasks = self.cluster.async_monitor_active_task(servers, "indexer", "_design/" + prefix + ddoc_name, wait_task=False)
        for active_task in active_tasks:
            result = active_task.result()
            self.assertTrue(result)

    def verify_query_task(self):
        """ Verify Query Results """
        num_views = self.input.param("num_views", 5)
        is_dev_ddoc = self.input.param("is_dev_ddoc", True)
        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[is_dev_ddoc]
        query = {}
        query["connectionTimeout"] = 60000;
        query["full_set"] = "true"
        expected_rows = None
        timeout = None
        if self.active_resident_threshold == 0:
            timeout = 2400
        if self.max_verify:
            expected_rows = self.max_verify
            query["limit"] = expected_rows
        query["stale"] = "false"
        for bucket in self.buckets:
            self.perform_verify_queries(num_views, prefix, ddoc_name, query, bucket=bucket, wait_time=timeout, expected_rows=expected_rows)

    def create_file(self, chosen, buckets, serverMap):
        """ Created files in data paths for checking if delta/full recovery occured """
        fileMap = {}
        for server in self.chosen:
            shell = RemoteMachineShellConnection(serverMap[server.ip])
            type = shell.extract_remote_info().distribution_type
            map = {}
            for bucket in buckets:
                if type.lower() == 'windows':
                    self.data_path = 'c:/Program\ Files/Couchbase/Server/var/lib/couchbase/data'
                bucket_data_path = self.data_path + "/" + bucket.name + "/" + "check.txt"
                full_path = self.data_path + "/" + bucket.name + "/"
                map[bucket.name] = full_path
                shell.create_file(bucket_data_path, "check")
            fileMap[server.ip] = map
            shell.disconnect()
        return fileMap

    def verify_failover_type(self, chosen=None, graceful_count=0, replica_count=0, unreachable=False):
        logic = True
        summary = ""
        nodes = self.rest.node_statuses()
        node_count = len(nodes)
        change_graceful_count = graceful_count
        graceful_failover = True
        if unreachable:
            node_count -= 1
        else:
            change_graceful_count += 1
        if replica_count != 0:
            for node in nodes:
                if unreachable and node.ip == chosen.ip:
                    graceful_failover = node.gracefulFailoverPossible
                    if node.gracefulFailoverPossible:
                        logic = False
                        summary += "\n failover type for unreachable node {0} Expected :: Hard, Actual :: Graceful".format(node.ip)
                elif node.ip == chosen.ip:
                    graceful_failover = node.gracefulFailoverPossible
                    if replica_count > graceful_count and (node_count - 1) + graceful_count >= replica_count:
                        if not node.gracefulFailoverPossible:
                            logic = False
                            summary += "\n failover type for node {0} Expected :: Graceful, Actual :: Hard".format(node.ip)
                    else:
                        if node.gracefulFailoverPossible:
                            logic = False
                            summary += "\n failover type for  {0} Expected :: Hard, Actual :: Graceful".format(node.ip)
        else:
            for node in nodes:
                if node.ip == chosen.ip:
                    graceful_failover = node.gracefulFailoverPossible
                    if node.gracefulFailoverPossible:
                        logic = False
                        summary += "\n failover type for node {0} Expected :: Hard, Actual :: Graceful".format(node.ip)
        self.assertTrue(logic, summary)
        return change_graceful_count, graceful_failover

    def get_server_map(self, node):
        """ Map of ips and server information """
        map = {}
        for server in self.servers:
            map[server.ip] = server
        return map

    def victim_node_operations(self, node=None):
        if self.stopGracefulFailover:
            self.log.info(" Stopping Graceful Failover ")
            stopped = self.rest.stop_rebalance(wait_timeout=self.wait_timeout // 3)
            self.assertTrue(stopped, msg="unable to stop rebalance")
        if self.killNodes:
            self.log.info(" Killing Memcached ")
            kill_nodes = self.get_victim_nodes(self.servers, self.master, node, self.victim_type, self.victim_count)
            for kill_node in kill_nodes:
                self.kill_server_memcached(kill_node)
        if self.stopNodes:
            self.log.info(" Stopping Node")
            stop_nodes = self.get_victim_nodes(self.servers, self.master, node, self.victim_type, self.victim_count)
            for stop_node in stop_nodes:
                self.stop_server(stop_node)
            self.sleep(10)
            for start_node in stop_nodes:
                self.start_server(start_node)
        if self.firewallOnNodes:
            self.log.info(" Enabling Firewall for Node ")
            stop_nodes = self.get_victim_nodes(self.servers, self.master, node, self.victim_type, self.victim_count)
            for stop_node in stop_nodes:
                self.start_firewall_on_node(stop_node)
            self.sleep(30)
            self.log.info(" Disable Firewall for Node ")
            for start_node in stop_nodes:
                self.stop_firewall_on_node(start_node)
        self.sleep(60)
