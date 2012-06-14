import time
import unittest
from TestInput import TestInputSingleton
import logger
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper as ClusterHelper, ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import LoadWithMcsoda
from threading import Thread

class SwapRebalanceBase(unittest.TestCase):

    @staticmethod
    def common_setup(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.servers = self.input.servers
        serverInfo = self.servers[0]
        rest = RestConnection(serverInfo)

        # Clear the state from Previous invalid run
        rest.stop_rebalance()
        self.load_started = False
        self.loaders = []
        SwapRebalanceBase.reset(self)

        # Initialize test params
        self.replica  = self.input.param("replica", 1)
        self.keys_count = self.input.param("keys-count", 100000)
        self.load_ratio = self.input.param("load-ratio", 1)
        self.ratio_expiry = self.input.param("ratio-expiry", 0.03)
        self.ratio_deletes = self.input.param("ratio-deletes", 0.13)
        self.num_buckets = self.input.param("num-buckets", 1)
        self.failover_factor = self.num_swap = self.input.param("num-swap", 1)
        self.num_initial_servers = self.input.param("num-initial-servers", 3)
        self.fail_orchestrator = self.swap_orchestrator = self.input.param("swap-orchestrator", False)
        self.skip_cleanup = self.input.param("skip-cleanup", False)
        self.do_access = self.input.param("do-access", True)

        # Make sure the test is setup correctly
        min_servers = int(self.num_initial_servers) + int(self.num_swap)
        msg = "minimum {0} nodes required for running swap rebalance"
        self.assertTrue(len(self.servers) >= min_servers,
            msg=msg.format(min_servers))

        self.log.info('picking server : {0} as the master'.format(serverInfo))
        node_ram_ratio = BucketOperationHelper.base_bucket_ratio(self.servers)
        info = rest.get_nodes_self()
        rest.init_cluster(username=serverInfo.rest_username, password=serverInfo.rest_password)
        rest.init_cluster_memoryQuota(memoryQuota=int(info.mcdMemoryReserved * node_ram_ratio))

    @staticmethod
    def common_tearDown(self):
        if not self.skip_cleanup:
            SwapRebalanceBase.reset(self)

    @staticmethod
    def reset(self):
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        for server in self.servers:
            ClusterOperationHelper.cleanup_cluster([server])
            rest = RestConnection(server)
            if server.data_path:
                rest.set_data_path(data_path=server.data_path)
        self.log.info("Stopping load in Teardown")
        SwapRebalanceBase.stop_load(self.loaders)
        ClusterHelper.wait_for_ns_servers_or_assert(self.servers, self)

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
    def items_verification(master, test):
        rest = RestConnection(master)
        #Verify items count across all node
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
                password=bucket.saslPassword, prefix=str(bucket.name), port=8091)
            loader["mcsoda"].cfg["exit-after-creates"] = 1
            loader["mcsoda"].cfg["json"] = 0
            loader["thread"] = Thread(target=loader["mcsoda"].load_data, name='mcloader_'+bucket.name)
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
            loader["mcsoda"] = LoadWithMcsoda(master, self.keys_count/2, bucket=bucket.name,
                    password=bucket.saslPassword, prefix=str(bucket.name), port=8091)
            loader["mcsoda"].cfg["ratio-sets"] = 0.8
            loader["mcsoda"].cfg["ratio-hot"] = 0.2
            loader["mcsoda"].cfg["ratio-creates"] = 0.5
            loader["mcsoda"].cfg["ratio-deletes"] = self.ratio_deletes
            loader["mcsoda"].cfg["ratio-expirations"] = self.ratio_expiry
            loader["mcsoda"].cfg["json"] = 0
            loader["thread"] = Thread(target=loader["mcsoda"].load_data, name='mcloader_'+bucket.name)
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
        if self.num_buckets==1:
            SwapRebalanceBase._create_default_bucket(self, replica=self.replica)
        else:
            SwapRebalanceBase._create_multiple_buckets(self, replica=self.replica)

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
        RebalanceHelper.rebalance_in(intial_severs, len(intial_severs)-1)

        self.log.info("DATA LOAD PHASE")
        loaders = SwapRebalanceBase.start_load_phase(self, master)

        # Wait till load phase is over
        SwapRebalanceBase.stop_load(loaders, do_stop=False)
        self.log.info("DONE LOAD PHASE")

        # Start the swap rebalance
        current_nodes = RebalanceHelper.getOtpNodeIds(master)
        self.log.info("current nodes : {0}".format(current_nodes))
        toBeEjectedNodes = RebalanceHelper.pick_nodes(master, howmany=self.num_swap)
        optNodesIds = [node.id for node in toBeEjectedNodes]

        if self.swap_orchestrator:
            status, content = ClusterHelper.find_orchestrator(master)
            self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}".\
                format(status, content))
            if self.num_swap is len(current_nodes):
                optNodesIds.append(content)
            else:
                optNodesIds[0] = content

        for node in optNodesIds:
            self.log.info("removing node {0} and rebalance afterwards".format(node))

        new_swap_servers = self.servers[num_initial_servers:num_initial_servers+self.num_swap]
        for server in new_swap_servers:
            otpNode = rest.add_node(creds.rest_username, creds.rest_password, server.ip)
            msg = "unable to add node {0} to the cluster"
            self.assertTrue(otpNode, msg.format(server.ip))

        if self.swap_orchestrator:
            rest = RestConnection(new_swap_servers[0])
            master = new_swap_servers[0]

        if self.do_access:
            self.log.info("DATA ACCESS PHASE")
            loaders = SwapRebalanceBase.start_access_phase(self, master)

        self.log.info("SWAP REBALANCE PHASE")
        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],
            ejectedNodes=optNodesIds)

        if do_stop_start:
            # Rebalance is stopped at 20%, 40% and 60% completion
            self.log.info("STOP/START SWAP REBALANCE PHASE")
            retry = 0
            for expected_progress in (20, 40, 60):
                while True:
                    progress = rest._rebalance_progress()
                    if progress < 0:
                        self.log.error("rebalance progress code : {0}".format(progress))
                        break
                    elif progress == 100:
                        self.log.warn("Rebalance is already reached")
                        break
                    elif progress >= expected_progress:
                        self.log.info("Rebalance will be stopped with {0}%".format(progress))
                        stopped = rest.stop_rebalance()
                        self.assertTrue(stopped, msg="unable to stop rebalance")
                        time.sleep(20)
                        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],
                                       ejectedNodes=optNodesIds)
                        break
                    elif retry > 100:
                        break
                    else:
                        retry += 1
                        time.sleep(1)
                #self.assertTrue(reached, "rebalance failed or did not reach {0}%".format(expected_progress))
        self.assertTrue(rest.monitorRebalance(),
            msg="rebalance operation failed after adding node {0}".format(optNodesIds))

        # Stop loaders
        SwapRebalanceBase.stop_load(loaders)

        self.log.info("DONE DATA ACCESS PHASE")
        #for bucket in rest.get_buckets():
        #    SwapRebalanceBase.verify_data(new_swap_servers[0], bucket_data[bucket.name].get('inserted_keys'),\
        #        bucket.name, self)
            #RebalanceHelper.wait_for_persistence(master, bucket.name)

        self.log.info("VERIFICATION PHASE")
        SwapRebalanceBase.items_verification(master, self)

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
        RebalanceHelper.rebalance_in(intial_severs, len(intial_severs)-1)

        self.log.info("DATA LOAD PHASE")
        loaders = SwapRebalanceBase.start_load_phase(self, master)

        # Wait till load phase is over
        SwapRebalanceBase.stop_load(loaders, do_stop=False)
        self.log.info("DONE LOAD PHASE")

        # Start the swap rebalance
        current_nodes = RebalanceHelper.getOtpNodeIds(master)
        self.log.info("current nodes : {0}".format(current_nodes))
        toBeEjectedNodes = RebalanceHelper.pick_nodes(master, howmany=self.num_swap)
        optNodesIds = [node.id for node in toBeEjectedNodes]
        if self.swap_orchestrator:
            status, content = ClusterHelper.find_orchestrator(master)
            self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}".\
            format(status, content))
            # When swapping all the nodes
            if self.num_swap is len(current_nodes):
                optNodesIds.append(content)
            else:
                optNodesIds[0] = content

        for node in optNodesIds:
            self.log.info("removing node {0} and rebalance afterwards".format(node))

        new_swap_servers = self.servers[num_initial_servers:num_initial_servers+self.num_swap]
        for server in new_swap_servers:
            otpNode = rest.add_node(creds.rest_username, creds.rest_password, server.ip)
            msg = "unable to add node {0} to the cluster"
            self.assertTrue(otpNode, msg.format(server.ip))

        if self.swap_orchestrator:
            rest = RestConnection(new_swap_servers[0])
            master = new_swap_servers[0]

        self.log.info("DATA ACCESS PHASE")
        loaders = SwapRebalanceBase.start_access_phase(self, master)

        self.log.info("SWAP REBALANCE PHASE")
        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],
            ejectedNodes=optNodesIds)

        # Rebalance is failed at 20%, 40% and 60% completion
        for i in [1, 2, 3]:
            expected_progress = 20*i
            self.log.info("FAIL SWAP REBALANCE PHASE @ {0}".format(expected_progress))
            RestHelper(rest).rebalance_reached(expected_progress)
            command = "[erlang:exit(element(2, X), kill) || X <- supervisor:which_children(ns_port_sup)]."
            memcached_restarted = rest.diag_eval(command)
            self.assertTrue(memcached_restarted, "unable to restart memcached/moxi process through diag/eval")
            time.sleep(5)

            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],
                ejectedNodes=optNodesIds)

        self.assertTrue(rest.monitorRebalance(),
            msg="rebalance operation failed after adding node {0}".format(toBeEjectedNodes))

        # Stop loaders
        SwapRebalanceBase.stop_load(loaders)

        self.log.info("DONE DATA ACCESS PHASE")
        #for bucket in rest.get_buckets():
        #    SwapRebalanceBase.verify_data(new_swap_servers[0], bucket_data[bucket.name].get('inserted_keys'),\
        #        bucket.name, self)
        #    RebalanceHelper.wait_for_persistence(master, bucket.name)

        self.log.info("VERIFICATION PHASE")
        SwapRebalanceBase.items_verification(master, self)

    @staticmethod
    def _add_back_failed_node(self, do_node_cleanup=False):
        master = self.servers[0]
        rest = RestConnection(master)
        creds = self.input.membase_settings

        self.log.info("CREATE BUCKET PHASE")
        SwapRebalanceBase.create_buckets(self)

        # Cluster all servers
        self.log.info("INITIAL REBALANCE PHASE")
        RebalanceHelper.rebalance_in(self.servers, len(self.servers)-1)

        self.log.info("DATA LOAD PHASE")
        loaders = SwapRebalanceBase.start_load_phase(self, master)

        # Wait till load phase is over
        SwapRebalanceBase.stop_load(loaders, do_stop=False)
        self.log.info("DONE LOAD PHASE")

        # Start the swap rebalance
        current_nodes = RebalanceHelper.getOtpNodeIds(master)
        self.log.info("current nodes : {0}".format(current_nodes))
        toBeEjectedNodes = RebalanceHelper.pick_nodes(master, howmany=self.failover_factor)
        optNodesIds = [node.id for node in toBeEjectedNodes]

        # List of servers that will not be failed over
        not_failed_over = []
        for server in self.servers:
            if server.ip not in [node.ip for node in toBeEjectedNodes]:
                not_failed_over.append(server)
                self.log.info("Node %s not failed over" % server.ip)

        if self.fail_orchestrator:
            status, content = ClusterHelper.find_orchestrator(master)
            self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}".\
                format(status, content))
            # When swapping all the nodes
            if self.num_swap is len(current_nodes):
                optNodesIds.append(content)
            else:
                optNodesIds[0] = content
            master = not_failed_over[-1]

        self.log.info("DATA ACCESS PHASE")
        loaders = SwapRebalanceBase.start_access_phase(self, master)

        #Failover selected nodes
        for node in optNodesIds:
            self.log.info("failover node {0} and rebalance afterwards".format(node))
            rest.fail_over(node)

        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],\
            ejectedNodes=optNodesIds)

        self.assertTrue(rest.monitorRebalance(),
            msg="rebalance operation failed after adding node {0}".format(optNodesIds))

        # Add back the same failed over nodes

        #Cleanup the node, somehow
        #TODO: cluster_run?
        if do_node_cleanup:
            pass

        # Make rest connection with node part of cluster
        rest = RestConnection(master)

        # Given the optNode, find ip
        add_back_servers = []
        nodes = rest.get_nodes()
        for server in [node.ip for node in nodes]:
            if isinstance(server, unicode):
                add_back_servers.append(server)
        final_add_back_servers = []
        for server in self.servers:
            if server.ip not in add_back_servers:
                final_add_back_servers.append(server)

        for server in final_add_back_servers:
            otpNode = rest.add_node(creds.rest_username, creds.rest_password, server.ip)
            msg = "unable to add node {0} to the cluster"
            self.assertTrue(otpNode, msg.format(server.ip))

        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])

        self.assertTrue(rest.monitorRebalance(),
            msg="rebalance operation failed after adding node {0}".format(add_back_servers))

        # Stop loaders
        SwapRebalanceBase.stop_load(loaders)

        self.log.info("DONE DATA ACCESS PHASE")
        #for bucket in rest.get_buckets():
        #    SwapRebalanceBase.verify_data(new_swap_servers[0], bucket_data[bucket.name].get('inserted_keys'),\
        #        bucket.name, self)
        #    RebalanceHelper.wait_for_persistence(master, bucket.name)

        self.log.info("VERIFICATION PHASE")
        SwapRebalanceBase.items_verification(master, self)

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
        RebalanceHelper.rebalance_in(intial_severs, len(intial_severs)-1)

        self.log.info("DATA LOAD PHASE")
        loaders = SwapRebalanceBase.start_load_phase(self, master)

        # Wait till load phase is over
        SwapRebalanceBase.stop_load(loaders, do_stop=False)
        self.log.info("DONE LOAD PHASE")

        # Start the swap rebalance
        self.log.info("current nodes : {0}".format(RebalanceHelper.getOtpNodeIds(master)))
        toBeEjectedNodes = RebalanceHelper.pick_nodes(master, howmany=self.failover_factor)
        optNodesIds = [node.id for node in toBeEjectedNodes]
        if self.fail_orchestrator:
            status, content = ClusterHelper.find_orchestrator(master)
            self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}".\
            format(status, content))
            optNodesIds[0] = content

        self.log.info("FAILOVER PHASE")
        #Failover selected nodes
        for node in optNodesIds:
            self.log.info("failover node {0} and rebalance afterwards".format(node))
            rest.fail_over(node)

        new_swap_servers = self.servers[num_initial_servers:num_initial_servers+self.failover_factor]
        for server in new_swap_servers:
            otpNode = rest.add_node(creds.rest_username, creds.rest_password, server.ip)
            msg = "unable to add node {0} to the cluster"
            self.assertTrue(otpNode, msg.format(server.ip))

        if self.fail_orchestrator:
            rest = RestConnection(new_swap_servers[0])
            master = new_swap_servers[0]

        self.log.info("DATA ACCESS PHASE")
        loaders = SwapRebalanceBase.start_access_phase(self, master)

        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],\
            ejectedNodes=optNodesIds)

        self.assertTrue(rest.monitorRebalance(),
            msg="rebalance operation failed after adding node {0}".format(new_swap_servers))

        # Stop loaders
        SwapRebalanceBase.stop_load(loaders)

        self.log.info("DONE DATA ACCESS PHASE")
        #for bucket in rest.get_buckets():
        #    SwapRebalanceBase.verify_data(new_swap_servers[0], bucket_data[bucket.name].get('inserted_keys'),\
        #        bucket.name, self)
        #    RebalanceHelper.wait_for_persistence(master, bucket.name)

        self.log.info("VERIFICATION PHASE")
        SwapRebalanceBase.items_verification(master, self)

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
        SwapRebalanceBase._common_test_body_failed_swap_rebalance(self)

    # Not cluster_run friendly, yet
    def test_add_back_failed_node(self):
        SwapRebalanceBase._add_back_failed_node(self, do_node_cleanup=False)

    def test_failover_swap_rebalance(self):
        SwapRebalanceBase._failover_swap_rebalance(self)
