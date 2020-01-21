import time
import datetime
import unittest
from TestInput import TestInputSingleton
import logger
from couchbase_helper.cluster import Cluster
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import LoadWithMcsoda
from threading import Thread
from remote.remote_util import RemoteMachineShellConnection
from memcached.helper.data_helper import MemcachedClientHelper
from membase.api.exception import RebalanceFailedException
from basetestcase import BaseTestCase
from security.rbac_base import RbacBase

class SwapRebalanceBase(unittest.TestCase):

    @staticmethod
    def common_setup(self):
        self.cluster_helper = Cluster()
        self.log = logger.Logger.get_logger()
        self.cluster_run = False
        self.input = TestInputSingleton.input
        self.servers = self.input.servers
        serverInfo = self.servers[0]
        rest = RestConnection(serverInfo)
        if len({server.ip for server in self.servers}) == 1:
            ip = rest.get_nodes_self().ip
            for server in self.servers:
                server.ip = ip
            self.cluster_run = True
        self.case_number = self.input.param("case_number", 0)
        self.replica = self.input.param("replica", 1)
        self.keys_count = self.input.param("keys-count", 1000)
        self.load_ratio = self.input.param("load-ratio", 1)
        self.ratio_expiry = self.input.param("ratio-expiry", 0.03)
        self.ratio_deletes = self.input.param("ratio-deletes", 0.13)
        self.num_buckets = self.input.param("num-buckets", 1)
        self.failover_factor = self.num_swap = self.input.param("num-swap", 1)
        self.num_initial_servers = self.input.param("num-initial-servers", 3)
        self.fail_orchestrator = self.swap_orchestrator = self.input.param("swap-orchestrator", False)
        self.do_access = self.input.param("do-access", True)
        self.load_started = False
        self.loaders = []
        try:
            # Clear the state from Previous invalid run
            if rest._rebalance_progress_status() == 'running':
                self.log.warning("rebalancing is still running, previous test should be verified")
                stopped = rest.stop_rebalance()
                self.assertTrue(stopped, msg="unable to stop rebalance")
            self.log.info("==============  SwapRebalanceBase setup was started for test #{0} {1}=============="\
                      .format(self.case_number, self._testMethodName))
            SwapRebalanceBase.reset(self)

            # Make sure the test is setup correctly
            min_servers = int(self.num_initial_servers) + int(self.num_swap)
            msg = "minimum {0} nodes required for running swap rebalance"
            self.assertTrue(len(self.servers) >= min_servers, msg=msg.format(min_servers))

            self.log.info('picking server : {0} as the master'.format(serverInfo))
            node_ram_ratio = BucketOperationHelper.base_bucket_ratio(self.servers)
            info = rest.get_nodes_self()
            rest.init_cluster(username=serverInfo.rest_username, password=serverInfo.rest_password)
            rest.init_cluster_memoryQuota(memoryQuota=int(info.mcdMemoryReserved * node_ram_ratio))
            SwapRebalanceBase.enable_diag_eval_on_non_local_hosts(self, serverInfo)
            # Add built-in user
            testuser = [{'id': 'cbadminbucket', 'name': 'cbadminbucket', 'password': 'password'}]
            RbacBase().create_user_source(testuser, 'builtin', self.servers[0])

            # Assign user to role
            role_list = [{'id': 'cbadminbucket', 'name': 'cbadminbucket', 'roles': 'admin'}]
            RbacBase().add_user_role(role_list, RestConnection(self.servers[0]), 'builtin')

            if self.num_buckets > 10:
                BaseTestCase.change_max_buckets(self, self.num_buckets)
            self.log.info("==============  SwapRebalanceBase setup was finished for test #{0} {1} =============="
                      .format(self.case_number, self._testMethodName))
            SwapRebalanceBase._log_start(self)
        except Exception as e:
            self.cluster_helper.shutdown()
            self.fail(e)

    @staticmethod
    def common_tearDown(self):
        self.cluster_helper.shutdown()

        test_failed = (hasattr(self, '_resultForDoCleanups') and len(self._resultForDoCleanups.failures or self._resultForDoCleanups.errors)) \
                   or (hasattr(self, '_exc_info') and self._exc_info()[1] is not None)
        if test_failed and TestInputSingleton.input.param("stop-on-failure", False)\
                        or self.input.param("skip_cleanup", False):
                    self.log.warn("CLEANUP WAS SKIPPED")
        else:
            SwapRebalanceBase.reset(self)
            SwapRebalanceBase._log_finish(self)

        # Remove rbac user in teardown
        try:
            role_del = ['cbadminbucket']
            RbacBase().remove_user_role(role_del, RestConnection(
                self.servers[0]))
        except:
            pass

    @staticmethod
    def reset(self):
        self.log.info("==============  SwapRebalanceBase cleanup was started for test #{0} {1} =============="\
                          .format(self.case_number, self._testMethodName))
        self.log.info("Stopping load in Teardown")
        SwapRebalanceBase.stop_load(self.loaders)
        for server in self.servers:
            rest = RestConnection(server)
            if rest._rebalance_progress_status() == 'running':
                self.log.warning("rebalancing is still running, test should be verified")
                stopped = rest.stop_rebalance()
                self.assertTrue(stopped, msg="unable to stop rebalance")
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        for server in self.servers:
            ClusterOperationHelper.cleanup_cluster([server])
            if server.data_path:
                rest = RestConnection(server)
                rest.set_data_path(data_path=server.data_path)
        ClusterOperationHelper.wait_for_ns_servers_or_assert(self.servers, self)
        self.log.info("==============  SwapRebalanceBase cleanup was finished for test #{0} {1} =============="\
                          .format(self.case_number, self._testMethodName))

    @staticmethod
    def enable_diag_eval_on_non_local_hosts(self, master):
        """
        Enable diag/eval to be run on non-local hosts.
        :param master: Node information of the master node of the cluster
        :return: Nothing
        """
        remote = RemoteMachineShellConnection(master)
        output, error = remote.enable_diag_eval_on_non_local_hosts()
        if "ok" not in output:
            self.log.error("Error in enabling diag/eval on non-local hosts on {}. {}".format(master.ip, output))
            raise Exception("Error in enabling diag/eval on non-local hosts on {}".format(master.ip))
        else:
            self.log.info("Enabled diag/eval for non-local hosts from {}".format(master.ip))

    @staticmethod
    def _log_start(self):
        try:
            msg = "{0} : {1} started ".format(datetime.datetime.now(), self._testMethodName)
            RestConnection(self.servers[0]).log_client_error(msg)
        except:
            pass

    @staticmethod
    def _log_finish(self):
        try:
            msg = "{0} : {1} finished ".format(datetime.datetime.now(), self._testMethodName)
            RestConnection(self.servers[0]).log_client_error(msg)
        except:
            pass

    @staticmethod
    def sleep(self, timeout=1, message=""):
        self.log.info("sleep for {0} secs. {1} ...".format(timeout, message))
        time.sleep(timeout)

    @staticmethod
    def _create_default_bucket(self, replica=1):
        name = "default"
        master = self.servers[0]
        rest = RestConnection(master)
        helper = RestHelper(RestConnection(master))
        if not helper.bucket_exists(name):
            node_ram_ratio = BucketOperationHelper.base_bucket_ratio(self.servers)
            info = rest.get_nodes_self()
            available_ram = info.memoryQuota * node_ram_ratio
            rest.create_bucket(bucket=name, ramQuotaMB=int(available_ram), replicaNumber=replica)
            ready = BucketOperationHelper.wait_for_memcached(master, name)
            self.assertTrue(ready, msg="wait_for_memcached failed")
        self.assertTrue(helper.bucket_exists(name),
            msg="unable to create {0} bucket".format(name))

    @staticmethod
    def _create_multiple_buckets(self, replica=1):
        master = self.servers[0]
        created = BucketOperationHelper.create_multiple_buckets(master, replica, howmany=self.num_buckets)
        self.assertTrue(created, "unable to create multiple buckets")

        rest = RestConnection(master)
        buckets = rest.get_buckets()
        for bucket in buckets:
            ready = BucketOperationHelper.wait_for_memcached(master, bucket.name)
            self.assertTrue(ready, msg="wait_for_memcached failed")

    # Used for items verification active vs. replica
    @staticmethod
    def items_verification(test, master):
        rest = RestConnection(master)
        # Verify items count across all node
        timeout = 600
        for bucket in rest.get_buckets():
            verified = RebalanceHelper.wait_till_total_numbers_match(master, bucket.name, timeout_in_seconds=timeout)
            test.assertTrue(verified, "Lost items!!.. failing test in {0} secs".format(timeout))

    @staticmethod
    def start_load_phase(self, master):
        loaders = []
        rest = RestConnection(master)
        for bucket in rest.get_buckets():
            loader = dict()
            loader["mcsoda"] = LoadWithMcsoda(master, self.keys_count, bucket=bucket.name,
                                rest_password=master.rest_password, prefix=str(bucket.name), port=8091)
            loader["mcsoda"].cfg["exit-after-creates"] = 1
            loader["mcsoda"].cfg["json"] = 0
            loader["thread"] = Thread(target=loader["mcsoda"].load_data, name='mcloader_' + bucket.name)
            loader["thread"].daemon = True
            loaders.append(loader)
        for loader in loaders:
            loader["thread"].start()
        return loaders

    @staticmethod
    def start_access_phase(self, master):
        loaders = []
        rest = RestConnection(master)
        for bucket in rest.get_buckets():
            loader = dict()
            loader["mcsoda"] = LoadWithMcsoda(master, self.keys_count // 2, bucket=bucket.name,
                    rest_password=master.rest_password, prefix=str(bucket.name), port=8091)
            loader["mcsoda"].cfg["ratio-sets"] = 0.8
            loader["mcsoda"].cfg["ratio-hot"] = 0.2
            loader["mcsoda"].cfg["ratio-creates"] = 0.5
            loader["mcsoda"].cfg["ratio-deletes"] = self.ratio_deletes
            loader["mcsoda"].cfg["ratio-expirations"] = self.ratio_expiry
            loader["mcsoda"].cfg["json"] = 0
            loader["thread"] = Thread(target=loader["mcsoda"].load_data, name='mcloader_' + bucket.name)
            loader["thread"].daemon = True
            loaders.append(loader)
        for loader in loaders:
            loader["thread"].start()
        return loaders

    @staticmethod
    def stop_load(loaders, do_stop=True):
        if do_stop:
            for loader in loaders:
                loader["mcsoda"].load_stop()
        for loader in loaders:
            if do_stop:
                loader["thread"].join(300)
            else:
                loader["thread"].join()

    @staticmethod
    def create_buckets(self):
        if self.num_buckets == 1:
            SwapRebalanceBase._create_default_bucket(self, replica=self.replica)
        else:
            SwapRebalanceBase._create_multiple_buckets(self, replica=self.replica)

    @staticmethod
    def verification_phase(test, master):
        # Stop loaders
        SwapRebalanceBase.stop_load(test.loaders)
        test.log.info("DONE DATA ACCESS PHASE")

        test.log.info("VERIFICATION PHASE")
        rest = RestConnection(master)
        servers_in_cluster = []
        nodes = rest.get_nodes()
        for server in test.servers:
            for node in nodes:
                if node.ip == server.ip and node.port == server.port:
                    servers_in_cluster.append(server)
        time.sleep(60)
        SwapRebalanceBase.items_verification(test, master)

    @staticmethod
    def _common_test_body_swap_rebalance(self, do_stop_start=False):
        master = self.servers[0]
        rest = RestConnection(master)
        num_initial_servers = self.num_initial_servers
        creds = self.input.membase_settings
        intial_severs = self.servers[:num_initial_servers]

        self.log.info("CREATE BUCKET PHASE")
        SwapRebalanceBase.create_buckets(self)

        # Cluster all starting set of servers
        self.log.info("INITIAL REBALANCE PHASE")
        status, servers_rebalanced = RebalanceHelper.rebalance_in(intial_severs, len(intial_severs) - 1)
        self.assertTrue(status, msg="Rebalance was failed")

        self.log.info("DATA LOAD PHASE")
        self.loaders = SwapRebalanceBase.start_load_phase(self, master)

        # Wait till load phase is over
        SwapRebalanceBase.stop_load(self.loaders, do_stop=False)
        self.log.info("DONE LOAD PHASE")

        # Start the swap rebalance
        current_nodes = RebalanceHelper.getOtpNodeIds(master)
        self.log.info("current nodes : {0}".format(current_nodes))
        toBeEjectedNodes = RebalanceHelper.pick_nodes(master, howmany=self.num_swap)
        optNodesIds = [node.id for node in toBeEjectedNodes]

        if self.swap_orchestrator:
            status, content = ClusterOperationHelper.find_orchestrator(master)
            self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}".\
                format(status, content))
            if self.num_swap is len(current_nodes):
                optNodesIds.append(content)
            else:
                optNodesIds[0] = content

        for node in optNodesIds:
            self.log.info("removing node {0} and rebalance afterwards".format(node))

        new_swap_servers = self.servers[num_initial_servers:num_initial_servers + self.num_swap]
        for server in new_swap_servers:
            otpNode = rest.add_node(creds.rest_username, creds.rest_password, server.ip, server.port)
            msg = "unable to add node {0} to the cluster"
            self.assertTrue(otpNode, msg.format(server.ip))

        if self.swap_orchestrator:
            rest = RestConnection(new_swap_servers[0])
            master = new_swap_servers[0]

        if self.do_access:
            self.log.info("DATA ACCESS PHASE")
            self.loaders = SwapRebalanceBase.start_access_phase(self, master)

        self.log.info("SWAP REBALANCE PHASE")
        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],
                       ejectedNodes=optNodesIds)

        if do_stop_start:
            # Rebalance is stopped at 20%, 40% and 60% completion
            retry = 0
            for expected_progress in (20, 40, 60):
                self.log.info("STOP/START SWAP REBALANCE PHASE WITH PROGRESS {0}%".
                              format(expected_progress))
                while True:
                    progress = rest._rebalance_progress()
                    if progress < 0:
                        self.log.error("rebalance progress code : {0}".format(progress))
                        break
                    elif progress == 100:
                        self.log.warn("Rebalance has already reached 100%")
                        break
                    elif progress >= expected_progress:
                        self.log.info("Rebalance will be stopped with {0}%".format(progress))
                        stopped = rest.stop_rebalance()
                        self.assertTrue(stopped, msg="unable to stop rebalance")
                        SwapRebalanceBase.sleep(self, 20)
                        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],
                                       ejectedNodes=optNodesIds)
                        break
                    elif retry > 100:
                        break
                    else:
                        retry += 1
                        SwapRebalanceBase.sleep(self, 1)
        self.assertTrue(rest.monitorRebalance(),
            msg="rebalance operation failed after adding node {0}".format(optNodesIds))
        SwapRebalanceBase.verification_phase(self, master)

    @staticmethod
    def _common_test_body_failed_swap_rebalance(self):
        master = self.servers[0]
        rest = RestConnection(master)
        num_initial_servers = self.num_initial_servers
        creds = self.input.membase_settings
        intial_severs = self.servers[:num_initial_servers]

        self.log.info("CREATE BUCKET PHASE")
        SwapRebalanceBase.create_buckets(self)

        # Cluster all starting set of servers
        self.log.info("INITIAL REBALANCE PHASE")
        status, servers_rebalanced = RebalanceHelper.rebalance_in(intial_severs, len(intial_severs) - 1)
        self.assertTrue(status, msg="Rebalance was failed")

        self.log.info("DATA LOAD PHASE")
        self.loaders = SwapRebalanceBase.start_load_phase(self, master)

        # Wait till load phase is over
        SwapRebalanceBase.stop_load(self.loaders, do_stop=False)
        self.log.info("DONE LOAD PHASE")

        # Start the swap rebalance
        current_nodes = RebalanceHelper.getOtpNodeIds(master)
        self.log.info("current nodes : {0}".format(current_nodes))
        toBeEjectedNodes = RebalanceHelper.pick_nodes(master, howmany=self.num_swap)
        optNodesIds = [node.id for node in toBeEjectedNodes]
        if self.swap_orchestrator:
            status, content = ClusterOperationHelper.find_orchestrator(master)
            self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}".\
            format(status, content))
            # When swapping all the nodes
            if self.num_swap is len(current_nodes):
                optNodesIds.append(content)
            else:
                optNodesIds[0] = content

        for node in optNodesIds:
            self.log.info("removing node {0} and rebalance afterwards".format(node))

        new_swap_servers = self.servers[num_initial_servers:num_initial_servers + self.num_swap]
        for server in new_swap_servers:
            otpNode = rest.add_node(creds.rest_username, creds.rest_password, server.ip, server.port)
            msg = "unable to add node {0} to the cluster"
            self.assertTrue(otpNode, msg.format(server.ip))

        if self.swap_orchestrator:
            rest = RestConnection(new_swap_servers[0])
            master = new_swap_servers[0]

        self.log.info("DATA ACCESS PHASE")
        self.loaders = SwapRebalanceBase.start_access_phase(self, master)

        self.log.info("SWAP REBALANCE PHASE")
        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],
            ejectedNodes=optNodesIds)
        SwapRebalanceBase.sleep(self, 10, "Rebalance should start")
        self.log.info("FAIL SWAP REBALANCE PHASE @ {0}".format(self.percentage_progress))
        reached = RestHelper(rest).rebalance_reached(self.percentage_progress)
        if reached and RestHelper(rest).is_cluster_rebalanced():
            # handle situation when rebalance failed at the beginning
            self.log.error('seems rebalance failed!')
            rest.print_UI_logs()
            self.fail("rebalance failed even before killing memcached")
        bucket = rest.get_buckets()[0].name
        pid = None
        if self.swap_orchestrator and not self.cluster_run:
            # get PID via remote connection if master is a new node
            shell = RemoteMachineShellConnection(master)
            pid = shell.get_memcache_pid()
            shell.disconnect()
        else:
            times = 2
            if self.cluster_run:
                times = 20
            for i in range(times):
                try:
                    _mc = MemcachedClientHelper.direct_client(master, bucket)
                    pid = _mc.stats()["pid"]
                    break
                except (EOFError, KeyError) as e:
                    self.log.error("{0}.Retry in 2 sec".format(e))
                    SwapRebalanceBase.sleep(self, 2)
        if pid is None:
            # sometimes pid is not returned by mc.stats()
            shell = RemoteMachineShellConnection(master)
            pid = shell.get_memcache_pid()
            shell.disconnect()
            if pid is None:
                self.fail("impossible to get a PID")
        command = "os:cmd(\"kill -9 {0} \")".format(pid)
        self.log.info(command)
        killed = rest.diag_eval(command)
        self.log.info("killed {0}:{1}??  {2} ".format(master.ip, master.port, killed))
        self.log.info("sleep for 10 sec after kill memcached")
        SwapRebalanceBase.sleep(self, 10)
        # we can't get stats for new node when rebalance falls
        if not self.swap_orchestrator:
            ClusterOperationHelper._wait_warmup_completed(self, [master], bucket, wait_time=600)
        i = 0
        # we expect that rebalance will be failed
        try:
            rest.monitorRebalance()
        except RebalanceFailedException:
            # retry rebalance if it failed
            self.log.warn("Rebalance failed but it's expected")
            SwapRebalanceBase.sleep(self, 30)
            self.assertFalse(RestHelper(rest).is_cluster_rebalanced(), msg="cluster need rebalance")
            knownNodes = rest.node_statuses();
            self.log.info("nodes are still in cluster: {0}".format([(node.ip, node.port) for node in knownNodes]))
            ejectedNodes = list(set(optNodesIds) & {node.id for node in knownNodes})
            rest.rebalance(otpNodes=[node.id for node in knownNodes], ejectedNodes=ejectedNodes)
            self.assertTrue(rest.monitorRebalance(),
                            msg="rebalance operation failed after adding node {0}".format(toBeEjectedNodes))
        else:
            self.log.info("rebalance completed successfully")
        SwapRebalanceBase.verification_phase(self, master)

    @staticmethod
    def _add_back_failed_node(self, do_node_cleanup=False):
        master = self.servers[0]
        rest = RestConnection(master)
        creds = self.input.membase_settings

        self.log.info("CREATE BUCKET PHASE")
        SwapRebalanceBase.create_buckets(self)

        # Cluster all servers
        self.log.info("INITIAL REBALANCE PHASE")
        status, servers_rebalanced = RebalanceHelper.rebalance_in(self.servers, len(self.servers) - 1)
        self.assertTrue(status, msg="Rebalance was failed")

        self.log.info("DATA LOAD PHASE")
        self.loaders = SwapRebalanceBase.start_load_phase(self, master)

        # Wait till load phase is over
        SwapRebalanceBase.stop_load(self.loaders, do_stop=False)
        self.log.info("DONE LOAD PHASE")

        # Start the swap rebalance
        current_nodes = RebalanceHelper.getOtpNodeIds(master)
        self.log.info("current nodes : {0}".format(current_nodes))
        toBeEjectedNodes = RebalanceHelper.pick_nodes(master, howmany=self.failover_factor)
        optNodesIds = [node.id for node in toBeEjectedNodes]

        # List of servers that will not be failed over
        not_failed_over = []
        for server in self.servers:
            if self.cluster_run:
                if server.port not in [node.port for node in toBeEjectedNodes]:
                    not_failed_over.append(server)
                    self.log.info("Node {0}:{1} not failed over".format(server.ip, server.port))
            else:
                if server.ip not in [node.ip for node in toBeEjectedNodes]:
                    not_failed_over.append(server)
                    self.log.info("Node {0}:{1} not failed over".format(server.ip, server.port))

        if self.fail_orchestrator:
            status, content = ClusterOperationHelper.find_orchestrator(master)
            self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}".\
                format(status, content))
            # When swapping all the nodes
            if self.num_swap is len(current_nodes):
                optNodesIds.append(content)
            else:
                optNodesIds[0] = content
            master = not_failed_over[-1]

        self.log.info("DATA ACCESS PHASE")
        self.loaders = SwapRebalanceBase.start_access_phase(self, master)

        # Failover selected nodes
        for node in optNodesIds:
            self.log.info("failover node {0} and rebalance afterwards".format(node))
            rest.fail_over(node)

        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], \
            ejectedNodes=optNodesIds)

        self.assertTrue(rest.monitorRebalance(),
            msg="rebalance operation failed after adding node {0}".format(optNodesIds))

        # Add back the same failed over nodes

        # Cleanup the node, somehow
        # TODO: cluster_run?
        if do_node_cleanup:
            pass

        # Make rest connection with node part of cluster
        rest = RestConnection(master)

        # Given the optNode, find ip
        add_back_servers = []
        nodes = rest.get_nodes()
        for server in nodes:
            if isinstance(server.ip, str):
                add_back_servers.append(server)
        final_add_back_servers = []
        for server in self.servers:
            if self.cluster_run:
                if server.port not in [serv.port for serv in add_back_servers]:
                    final_add_back_servers.append(server)
            else:
                if server.ip not in [serv.ip for serv in add_back_servers]:
                    final_add_back_servers.append(server)
        for server in final_add_back_servers:
            otpNode = rest.add_node(creds.rest_username, creds.rest_password, server.ip, server.port)
            msg = "unable to add node {0} to the cluster"
            self.assertTrue(otpNode, msg.format(server.ip))

        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])

        self.assertTrue(rest.monitorRebalance(),
            msg="rebalance operation failed after adding node {0}".format(add_back_servers))

        SwapRebalanceBase.verification_phase(self, master)

    @staticmethod
    def _failover_swap_rebalance(self):
        master = self.servers[0]
        rest = RestConnection(master)
        creds = self.input.membase_settings
        num_initial_servers = self.num_initial_servers
        intial_severs = self.servers[:num_initial_servers]

        self.log.info("CREATE BUCKET PHASE")
        SwapRebalanceBase.create_buckets(self)

        # Cluster all starting set of servers
        self.log.info("INITIAL REBALANCE PHASE")
        status, servers_rebalanced = RebalanceHelper.rebalance_in(intial_severs, len(intial_severs) - 1)
        self.assertTrue(status, msg="Rebalance was failed")

        self.log.info("DATA LOAD PHASE")
        self.loaders = SwapRebalanceBase.start_load_phase(self, master)

        # Wait till load phase is over
        SwapRebalanceBase.stop_load(self.loaders, do_stop=False)
        self.log.info("DONE LOAD PHASE")

        # Start the swap rebalance
        self.log.info("current nodes : {0}".format(RebalanceHelper.getOtpNodeIds(master)))
        toBeEjectedNodes = RebalanceHelper.pick_nodes(master, howmany=self.failover_factor)
        optNodesIds = [node.id for node in toBeEjectedNodes]
        if self.fail_orchestrator:
            status, content = ClusterOperationHelper.find_orchestrator(master)
            self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}".\
            format(status, content))
            optNodesIds[0] = content

        self.log.info("FAILOVER PHASE")
        # Failover selected nodes
        for node in optNodesIds:
            self.log.info("failover node {0} and rebalance afterwards".format(node))
            rest.fail_over(node)

        new_swap_servers = self.servers[num_initial_servers:num_initial_servers + self.failover_factor]
        for server in new_swap_servers:
            otpNode = rest.add_node(creds.rest_username, creds.rest_password, server.ip, server.port)
            msg = "unable to add node {0} to the cluster"
            self.assertTrue(otpNode, msg.format(server.ip))

        if self.fail_orchestrator:
            rest = RestConnection(new_swap_servers[0])
            master = new_swap_servers[0]

        self.log.info("DATA ACCESS PHASE")
        self.loaders = SwapRebalanceBase.start_access_phase(self, master)

        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], \
            ejectedNodes=optNodesIds)

        self.assertTrue(rest.monitorRebalance(),
            msg="rebalance operation failed after adding node {0}".format(new_swap_servers))

        SwapRebalanceBase.verification_phase(self, master)

class SwapRebalanceBasicTests(unittest.TestCase):

    def setUp(self):
        SwapRebalanceBase.common_setup(self)

    def tearDown(self):
        SwapRebalanceBase.common_tearDown(self)

    def do_test(self):
        SwapRebalanceBase._common_test_body_swap_rebalance(self, do_stop_start=False)

class SwapRebalanceStartStopTests(unittest.TestCase):

    def setUp(self):
        SwapRebalanceBase.common_setup(self)

    def tearDown(self):
        SwapRebalanceBase.common_tearDown(self)

    def do_test(self):
        SwapRebalanceBase._common_test_body_swap_rebalance(self, do_stop_start=True)

class SwapRebalanceFailedTests(unittest.TestCase):

    def setUp(self):
        SwapRebalanceBase.common_setup(self)

    def tearDown(self):
        SwapRebalanceBase.common_tearDown(self)

    def test_failed_swap_rebalance(self):
        self.percentage_progress = self.input.param("percentage_progress", 50)
        SwapRebalanceBase._common_test_body_failed_swap_rebalance(self)

    # Not cluster_run friendly, yet
    def test_add_back_failed_node(self):
        SwapRebalanceBase._add_back_failed_node(self, do_node_cleanup=False)

    def test_failover_swap_rebalance(self):
        SwapRebalanceBase._failover_swap_rebalance(self)
