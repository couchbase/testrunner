import copy
import json
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.rebalance_helper import RebalanceHelper
from remote.remote_util import RemoteMachineShellConnection
from remote.remote_util import RemoteUtilHelper
from failoverbasetests import FailoverBaseTest

GRACEFUL = "graceful"

class FailoverTests(FailoverBaseTest):
    def setUp(self):
        super(FailoverTests, self).setUp(self)

    def tearDown(self):
        super(FailoverTests, self).tearDown(self)

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
            1. Starts Operations if programmed into the test case (before/after)
            2. Start View and Index Building operations
            3. Failover K out of N nodes (failover can be HARDFAILOVER/GRACEFUL)
            4.1 Rebalance the cluster is failover of K nodeStatuses
            4.2 Run Add-Back operation with recoveryType = (full/delta) with rebalance
            5. Verify all expected operations completed by checking stats, replicaiton, views, data correctness
        """
        # Pick the reference node for communication
        # We pick a node in the cluster which will NOT be failed over
        self.referenceNode = self.master
        if self.failoverMaster:
            self.referenceNode = self.servers[1]
        self.log.info(" Picking node {0} as reference node for test case".format(self.referenceNode.ip))
        self.print_test_params(failover_reason)
        self.rest = RestConnection(self.referenceNode)
        self.nodes = self.rest.node_statuses()

        # Set the data path for the cluster
        node_info = self.rest.get_nodes_self()
        self.data_path = node_info.storage[0].get_data_path()
        # Check if the test case has to be run for 3.0.0
        versions = self.rest.get_nodes_versions()
        for version in versions:
            if "3" > version and (self.graceful or (self.recoveryType != None)):
                self.log.error("Graceful failover can't be applied to nodes with version less then 3.*")
                self.log.error("Please check configuration parameters: SKIPPING TEST.")
                return

        # Find nodes that will under go failover
        self.chosen = RebalanceHelper.pick_nodes(self.referenceNode, howmany=self.num_failed_nodes)

        # Perform operations - Create/Update/Delete
        # self.withOps = True => Run Operations in parallel to failover
        # self.withOps = False => Run Operations Before failover
        self.ops_tasks = self.run_operation_tasks()

        # Perform View Creation Tasks and check for completion if required before failover
        if self.runViews:
            self.run_view_creation_operations(self.servers)
            if not self.runViewsDuringFailover:
                self.run_view_creation_operations(self.servers)
                self.monitor_view_tasks(self.servers)

        # Take snap-shot of data set used for validaiton
        prev_vbucket_stats = self.get_vbucket_seqnos(self.servers, self.buckets)
        record_static_data_set = self.get_data_set_all(self.servers, self.buckets)
        prev_failover_stats = self.get_failovers_logs(self.servers, self.buckets)

        # Perform Operations relalted to failover
        self.run_failover_operations(self.chosen, failover_reason)

        # Perform Add Back Operation with Rebalance Or only Rebalance with Verificaitons
        if not self.gracefulFailoverFail:
            if self.add_back_flag:
                self.run_add_back_operation_and_verify(self.chosen, prev_vbucket_stats, record_static_data_set, prev_failover_stats)
            else:
                self.run_rebalance_after_failover_and_verify(self.chosen, prev_vbucket_stats, record_static_data_set, prev_failover_stats)

    def run_rebalance_after_failover_and_verify(self, chosen, prev_vbucket_stats, record_static_data_set, prev_failover_stats):
        """ Method to run rebalance after failover and verify """
        # Need a delay > min because MB-7168
        self.sleep(60, "after failover before invoking rebalance...")
        _servers_ = self.filter_servers(self.servers, chosen)
        # Rebalance after Failover operation
        self.rest.rebalance(otpNodes=[node.id for node in self.nodes],
                               ejectedNodes=[node.id for node in chosen])
        if self.during_ops:
            self.sleep(5, "Wait for some progress in rebalance")
            if self.during_ops == "change_password":
                old_pass = self.referenceNode.rest_password
                self.change_password(new_password=self.input.param("new_password", "new_pass"))
                self.rest = RestConnection(self.referenceNode)
            elif self.during_ops == "change_port":
                self.change_port(new_port=self.input.param("new_port", "9090"))
                self.rest = RestConnection(self.referenceNode)
        try:
            msg = "rebalance failed while removing failover nodes {0}".format([node.id for node in chosen])
            self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg=msg)
            self.log.info("Begin VERIFICATION for Rebalance after Failover Only")
            RebalanceHelper.wait_for_replication(_servers_, self.cluster)
            # Verify all data set with meta data if failover happens after failover
            if not self.withOps:
                self.data_analysis_all(record_static_data_set, _servers_, self.buckets)
            # Check Cluster Stats and Data as well if max_verify > 0
            self.verify_cluster_stats(_servers_, self.referenceNode)
            # If views were created they can be verified
            if self.runViews:
                if self.runViewsDuringFailover:
                    self.monitor_view_tasks(_servers_)
                self.verify_query_task()
            # Check Failover logs :: Not sure about this logic, currently not checking, will update code once confirmed
            # Currently, only  for checking case where we  have graceful failover
            if self.graceful:
                new_failover_stats = self.compare_failovers_logs(prev_failover_stats, _servers_, self.buckets)
                new_vbucket_stats =  self.compare_vbucket_seqnos(prev_vbucket_stats, _servers_, self.buckets)
                self.compare_vbucketseq_failoverlogs(new_vbucket_stats, new_failover_stats)
            self.log.info("End VERIFICATION for Rebalance after Failover Only")
        finally:
            if self.during_ops:
                if self.during_ops == "change_password":
                    self.change_password(new_password=old_pass)
                elif self.during_ops == "change_port":
                    self.change_port(new_port='8091',
                    current_port=self.input.param("new_port", "9090"))

    def run_add_back_operation_and_verify(self, chosen, prev_vbucket_stats, record_static_data_set, prev_failover_stats):
        """
            Method to run add-back operation with recovery type = (delta/full)
            It also verifies if the operations are correct with data verificaiton steps
        """
        serverMap =  self.get_server_map(self.servers)
        recoveryTypeMap = self.define_maps_during_failover(self.recoveryType)
        fileMapsForVerification = self.create_file(chosen, self.buckets, serverMap)
        index = 0
        for node in chosen:
            self.rest.add_back_node(node.id)
            self.sleep(5)
            if self.recoveryType:
                # define precondition for recoverytype
                self.rest.set_recovery_type(otpNode=node.id, recoveryType=self.recoveryType[index])
                index += 1
        self.sleep(20, "After failover before invoking rebalance...")
        self.rest.rebalance(otpNodes=[node.id for node in self.nodes],
                               ejectedNodes=[])
        msg = "rebalance failed while removing failover nodes {0}".format(chosen)
        self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg=msg)
        RebalanceHelper.wait_for_replication(self.servers, self.cluster)
        self.log.info("Begin VERIFICATION for Add-back and rebalance")
        # Verify recovery Type succeeded if we added-back nodes
        self.verify_for_recovery_type(chosen, serverMap, self.buckets,
                recoveryTypeMap, fileMapsForVerification)
        # Comparison of all data if required
        if not self.withOps:
            self.data_analysis_all(record_static_data_set,self.servers, self.buckets)
       # Verify if vbucket sequence numbers are as expected
        new_vbucket_stats = self.compare_vbucket_seqnos(prev_vbucket_stats, self.servers, self.buckets,perNode= False)
        # Verify Stats of cluster and Data is max_verify > 0
        self.verify_cluster_stats(self.servers, self.referenceNode)
        # Check Failover logs
        new_failover_stats = self.compare_failovers_logs(prev_failover_stats,self.servers,self.buckets)
        # Compare Failover logs seq numbers and  Vbucket seq numbers
        self.compare_vbucketseq_failoverlogs(new_vbucket_stats, new_failover_stats)
        # Peform View Validation if Supported
        if self.runViews:
            if self.runViewsDuringFailover:
                self.monitor_view_tasks(self.servers)
            self.verify_query_task()
        self.log.info("End VERIFICATION for Add-back and rebalance")

    def print_test_params(self, failover_reason):
        """ Method to print test parameters """
        self.log.info("num_replicas : {0}".format(self.num_replicas))
        self.log.info("recoveryType : {0}".format(self.recoveryType))
        self.log.info("failover_reason : {0}".format(failover_reason))
        self.log.info("num_failed_nodes : {0}".format(self.num_failed_nodes))
        self.log.info('picking server : {0} as the master'.format(self.referenceNode))

    def run_failover_operations(self, chosen, failover_reason):
        """ Method to run fail over operations used in the test scenario based on failover reason """
        # Perform Operations relalted to failover
        for node in chosen:
            if failover_reason == 'stop_server':
                self.stop_server(node)
                self.log.info("10 seconds delay to wait for membase-server to shutdown")
                # wait for 5 minutes until node is down
                self.assertTrue(RestHelper(self.rest).wait_for_node_status(node, "unhealthy", 300),
                                    msg="node status is not unhealthy even after waiting for 5 minutes")
            elif failover_reason == "firewall":
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

        # define precondition check for failover
        failed_over = self.rest.fail_over(node.id, graceful=self.graceful)

        # Check for negative cases
        if self.graceful and (failover_reason in ['stop_server', 'firewall']):
            if failed_over:
                # MB-10479
                self.rest.print_UI_logs()
            self.assertFalse(failed_over, "Graceful Falover was started for unhealthy node!!! ")
            return
        elif self.gracefulFailoverFail and failed_over:
            """ Check if the fail_over fails as expected """
            self.assertTrue(not failed_over,""" Graceful failover should fail due to not enough replicas """)
            return

        # Check if failover happened as expected or re-try one more time
        if not failed_over:
            self.log.info("unable to failover the node the first time. try again in  60 seconds..")
            # try again in 75 seconds
            self.sleep(75)
            failed_over = self.rest.fail_over(node.id, graceful=self.graceful)
        if self.graceful and (failover_reason not in ['stop_server', 'firewall']):
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed for Graceful Failover, stuck or did not completed")

    def run_operation_tasks(self):
        """ Method to run operations Update/Delete/Create """
        # Load All Buckets if num_items > 0
        ops_tasks =  []
        ops_tasks += self._async_load_all_buckets(self.referenceNode, self.gen_initial_create, "create", 0)
        for task in ops_tasks:
            task.result()
        # Update or Delete buckets if items > 0 and options are passed in tests
        # These can run in parallel (withOps = True), or before (withOps = True)
        if(self.doc_ops is not None):
            ops_tasks = []
            if("create" in self.doc_ops):
                ops_tasks += self._async_load_all_buckets(self.referenceNode, self.gen_update, "create", 0)
            if("update" in self.doc_ops):
                ops_tasks += self._async_load_all_buckets(self.referenceNode, self.gen_update, "update", 0)
            if("delete" in self.doc_ops):
                ops_tasks += self._async_load_all_buckets(self.referenceNode, self.gen_delete, "delete", 0)
            for task in ops_tasks:
                task.result()
            if self.withOps:
                self._wait_for_stats_all_buckets(self.servers)
                RebalanceHelper.wait_for_replication(self.servers, self.cluster)
        return ops_tasks

    def define_maps_during_failover(self, recoveryType = []):
        """ Method to define nope ip, recovery type map """
        recoveryTypeMap={}
        index=0
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

    def verify_for_recovery_type(self, chosen = [], serverMap = {}, buckets = [], recoveryTypeMap = {}, fileMap = {}):
        """ Verify recovery type is delta or full """
        logic = True
        summary = ""
        for server in self.chosen:
            shell = RemoteMachineShellConnection(serverMap[server.ip])
            for bucket in buckets:
                path = fileMap[server.ip][bucket.name]
                exists = shell.file_exists(path,"check.txt")
                if recoveryTypeMap[server.ip] == "delta" and not exists:
                    logic = False
                    summary += "\n Failed Condition :: node {0}, bucket {1} :: Expected Delta, Actual Full".format(server.ip,bucket.name)
                elif recoveryTypeMap[server.ip] == "full" and exists:
                    logic = False
                    summary += "\n Failed Condition :: node {0}, bucket {1}  :: Expected Full, Actual Delta".format(server.ip,bucket.name)
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
                                           is_dev_ddoc, different_map= False)
            temp_tasks = self.async_create_views(self.master, ddoc_name, temp, bucket)
            views += temp
            tasks += temp_tasks

        timeout = max(self.wait_timeout * 4, len(self.buckets) * self.wait_timeout * self.num_items / 50000)

        for task in tasks:
            task.result(self.wait_timeout * 20)

        for bucket in self.buckets:
                for view in views:
                    # run queries to create indexes
                    self.cluster.query_view(self.master, prefix + ddoc_name, view.name, query)
        self.verify_query_task()
        active_tasks = self.cluster.async_monitor_active_task(servers, "indexer", "_design/" + prefix + ddoc_name, wait_task=False)
        for active_task in active_tasks:
            result = active_task.result()
            self.assertTrue(result)

    def monitor_view_tasks(self, servers):
        """ Monitor Query Tasks for their completion """
        num_views = self.input.param("num_views", 5)
        is_dev_ddoc = self.input.param("is_dev_ddoc", True)
        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[is_dev_ddoc]

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
        if self.max_verify:
            expected_rows = self.max_verify
            query["limit"] = expected_rows
        query["stale"] = "false"
        for bucket in self.buckets:
            self.perform_verify_queries(num_views, prefix, ddoc_name, query, bucket=bucket, wait_time=2400, expected_rows=expected_rows)

    def create_file(self,chosen,buckets,serverMap):
        """ Created files in data paths for checking if delta/full recovery occured """
        fileMap={}
        for server in self.chosen:
            shell = RemoteMachineShellConnection(serverMap[server.ip])
            map = {}
            for bucket in buckets:
                bucket_data_path=self.data_path+"/"+bucket.name+"/"+"check.txt"
                full_path=self.data_path+"/"+bucket.name+"/"
                map[bucket.name] = full_path
                shell.create_file(bucket_data_path,"check")
            fileMap[server.ip] = map
            shell.disconnect()
        return fileMap

    def get_server_map(self,node):
        """ Map of ips and server information """
        map = {}
        for server in self.servers:
            map[server.ip] = server
        return map

    def stop_server(self, node):
        """ Method to stop a server which is subject to failover """
        for server in self.servers:
            if server.ip == node.ip:
                shell = RemoteMachineShellConnection(server)
                if shell.is_couchbase_installed():
                    shell.stop_couchbase()
                    self.log.info("Couchbase stopped")
                else:
                    shell.stop_membase()
                    self.log.info("Membase stopped")
                shell.disconnect()
                break
