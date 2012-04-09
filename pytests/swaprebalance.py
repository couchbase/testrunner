import time
import unittest
from TestInput import TestInputSingleton
import logger
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper as ClusterHelper, ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import MemcachedClientHelper, MutationThread, VBucketAwareMemcached, LoadWithMcsoda
from membase.helper.failover_helper import FailoverHelper

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
        SwapRebalanceBaseTest.common_tearDown(self)

        # Initialize test params
        self.replica  = self.input.param("replica", 1)
        self.failover_factor = self.input.param("failover-factor", 1)
        self.keys_count = self.input.param("keys_count", 10000)
        self.load_ratio = self.input.param("load-ratio", 1)
        self.num_buckets = self.input.param("num-buckets", 1)
        self.num_swap = self.input.param("num-swap", 1)
        self.num_initial_servers = self.input.param("num-initial-servers", 3)
        self.fail_orchestrator = self.swap_orchestrator = self.input.param("swap-orchestrator", False)

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
        if self.num_buckets==1:
            SwapRebalanceBaseTest._create_default_bucket(self, replica=self.replica)
        else:
            SwapRebalanceBaseTest._create_multiple_buckets(self, replica=self.replica)

    @staticmethod
    def common_tearDown(self):
        for server in self.servers:
            ClusterOperationHelper.cleanup_cluster([server])
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        ClusterHelper.wait_for_ns_servers_or_assert(self.servers, self)
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)

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

    @staticmethod
    def load_data_for_buckets(rest, load_ratio, distribution, rebalanced_servers, bucket_data, test):
        buckets = rest.get_buckets()
        for bucket in buckets:
            inserted_count, rejected_count =\
            MemcachedClientHelper.load_bucket(name=bucket.name,
                servers=rebalanced_servers,
                ram_load_ratio=load_ratio,
                value_size_distribution=distribution,
                number_of_threads=1,
                write_only=True,
                moxi=True)
            test.log.info('inserted {0} keys'.format(inserted_count))
            bucket_data[bucket.name]["items_inserted_count"] += inserted_count

    @staticmethod
    def threads_for_buckets(rest, load_ratio, distribution, rebalanced_servers, bucket_data, delete_ratio=0,
                            expiry_ratio=0):
        buckets = rest.get_buckets()
        for bucket in buckets:
            threads = MemcachedClientHelper.create_threads(servers=rebalanced_servers,
                name=bucket.name,
                ram_load_ratio=load_ratio,
                value_size_distribution=distribution,
                number_of_threads=4,
                delete_ratio=delete_ratio,
                expiry_ratio=expiry_ratio)
            [t.start() for t in threads]
            bucket_data[bucket.name]["threads"] = threads
        return bucket_data

    @staticmethod
    def bucket_data_init(rest):
        bucket_data = {}
        buckets = rest.get_buckets()
        for bucket in buckets:
            bucket_data[bucket.name] = {}
            bucket_data[bucket.name]['items_inserted_count'] = 0
            bucket_data[bucket.name]['inserted_keys'] = []
        return bucket_data


    @staticmethod
    def load_data(master, bucket, keys_count=-1, load_ratio=-1, delete_ratio=0, \
                  expiry_ratio=0, test=None, wait_to_drain=True):
        inserted_keys, rejected_keys =\
        MemcachedClientHelper.load_bucket_and_return_the_keys(servers=[master],
            name=bucket,
            ram_load_ratio=load_ratio,
            number_of_items=keys_count,
            number_of_threads=2,
            write_only=True,
            delete_ratio=delete_ratio,
            expiry_ratio=expiry_ratio,
            moxi=True)
        if wait_to_drain:
            test.log.info("wait until data is completely persisted on the disk")
            ready = RebalanceHelper.wait_for_stats_on_all(master, bucket, 'ep_queue_size', 0, timeout_in_seconds=120)
            test.assertTrue(ready, "wait_for ep_queue_size == 0 failed")
            ready = RebalanceHelper.wait_for_stats_on_all(master, bucket, 'ep_flusher_todo', 0, timeout_in_seconds=120)
            test.assertTrue(ready, "wait_for ep_flusher_todo == 0 failed")
        return inserted_keys

    @staticmethod
    def verify_data(master, inserted_keys, bucket, test):
        test.log.info("Verifying data")
        ready = RebalanceHelper.wait_for_stats_on_all(master, bucket, 'ep_queue_size', 0)
        test.assertTrue(ready, "wait_for ep_queue_size == 0 failed")
        ready = RebalanceHelper.wait_for_stats_on_all(master, bucket, 'ep_flusher_todo', 0)
        test.assertTrue(ready, "wait_for ep_queue_size == 0 failed")
        BucketOperationHelper.keys_exist_or_assert_in_parallel(keys=inserted_keys, server=master, \
            bucket_name=bucket, test=test, concurrency=4)

    @staticmethod
    def verification(master, test):
        rest = RestConnection(master)
        #Verify items count across all node
        for bucket in rest.get_buckets():
            verified = RebalanceHelper.verify_items_count(master, bucket.name)
            test.assertTrue(verified, "Lost items!!.. failing test")

class SwapRebalanceTests(unittest.TestCase):

    def setUp(self):
        SwapRebalanceBaseTest.common_setup(self)

    def tearDown(self):
        SwapRebalanceBaseTest.common_tearDown(self)

    def _common_test_body_swap_rebalance(self, do_stop_start=False):
        master = self.servers[0]
        rest = RestConnection(master)
        num_initial_servers = self.num_initial_servers
        creds = self.input.membase_settings
        intial_severs = self.servers[:num_initial_servers]

        # Cluster all starting set of servers
        RebalanceHelper.rebalance_in(intial_severs, len(intial_severs)-1)

        self.log.info("inserting some items in the master before adding any nodes")
        bucket_data = SwapRebalanceBaseTest.bucket_data_init(rest)
        for bucket in rest.get_buckets():
            inserted_keys = SwapRebalanceBaseTest.load_data(master, bucket.name, self.keys_count, \
                self.load_ratio, delete_ratio=0, expiry_ratio=0, test=self, wait_to_drain=False)
            self.log.info("inserted {0} keys".format(len(inserted_keys)))
            bucket_data[bucket.name]['inserted_keys'].extend(inserted_keys)
            bucket_data[bucket.name]["items_inserted_count"] += len(inserted_keys)

        # Start the swap rebalance
        self.log.info("Starting swap rebalance")
        self.log.info("current nodes : {0}".format(RebalanceHelper.getOtpNodeIds(master)))

        toBeEjectedNodes = RebalanceHelper.pick_nodes(master, howmany=self.num_swap)
        optNodesIds = [node.id for node in toBeEjectedNodes]

        if self.swap_orchestrator:
            status, content = ClusterHelper.find_orchestrator(master)
            self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}".\
                format(status, content))
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

        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],\
            ejectedNodes=optNodesIds)

        if do_stop_start:
            # Rebalance is stopped at 20%, 40% and 60% completion
            for i in [1, 2, 3]:
                expected_progress = 20*i
                reached = RestHelper(rest).rebalance_reached(expected_progress)
                self.assertTrue(reached, "rebalance failed or did not reach {0}%".format(expected_progress))
                stopped = rest.stop_rebalance()
                self.assertTrue(stopped, msg="unable to stop rebalance")
                time.sleep(20)
                rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],\
                    ejectedNodes=optNodesIds)

        self.assertTrue(rest.monitorRebalance(),
            msg="rebalance operation failed after adding node {0}".format(optNodesIds))

        SwapRebalanceBaseTest.verification(master, self)

    def _common_test_body_failed_swap_rebalance(self):
        master = self.servers[0]
        rest = RestConnection(master)
        num_initial_servers = self.num_initial_servers
        creds = self.input.membase_settings
        intial_severs = self.servers[:num_initial_servers]

        # Cluster all starting set of servers
        RebalanceHelper.rebalance_in(intial_severs, len(intial_severs)-1)

        self.log.info("inserting some items in the master before adding any nodes")
        bucket_data = SwapRebalanceBaseTest.bucket_data_init(rest)
        for bucket in rest.get_buckets():
            inserted_keys = SwapRebalanceBaseTest.load_data(master, bucket.name, self.keys_count,\
                self.load_ratio, delete_ratio=0, expiry_ratio=0, test=self, wait_to_drain=True)
            self.log.info("inserted {0} keys".format(len(inserted_keys)))
            bucket_data[bucket.name]['inserted_keys'].extend(inserted_keys)
            bucket_data[bucket.name]["items_inserted_count"] += len(inserted_keys)

        # Start the swap rebalance
        self.log.info("Starting swap rebalance")
        self.log.info("current nodes : {0}".format(RebalanceHelper.getOtpNodeIds(master)))
        toBeEjectedNodes = RebalanceHelper.pick_nodes(master, howmany=self.num_swap)
        optNodesIds = [node.id for node in toBeEjectedNodes]
        if self.swap_orchestrator:
            status, content = ClusterHelper.find_orchestrator(master)
            self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}".\
            format(status, content))
            optNodesIds[0] = content
        for node in optNodesIds:
            self.log.info("removing node {0} and rebalance afterwards".format(node))

        new_swap_servers = self.servers[num_initial_servers:num_initial_servers+self.num_swap]
        for server in new_swap_servers:
            otpNode = rest.add_node(creds.rest_username, creds.rest_password, server.ip)
            msg = "unable to add node {0} to the cluster"
            self.assertTrue(otpNode, msg.format(server.ip))

        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],\
            ejectedNodes=optNodesIds)

        if self.swap_orchestrator:
            rest = RestConnection(new_swap_servers[0])

        # Rebalance is failed at 20%, 40% and 60% completion
        for i in [1, 2, 3]:
            expected_progress = 20*i
            reached = RestHelper(rest).rebalance_reached(expected_progress)
            command = "[erlang:exit(element(2, X), kill) || X <- supervisor:which_children(ns_port_sup)]."
            memcached_restarted = rest.diag_eval(command)
            self.assertTrue(memcached_restarted, "unable to restart memcached/moxi process through diag/eval")
            time.sleep(20)

            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],\
                ejectedNodes=optNodesIds)

        self.assertTrue(rest.monitorRebalance(),
            msg="rebalance operation failed after adding node {0}".format(toBeEjectedNodes))

        SwapRebalanceBaseTest.verification(master, self)

    def _add_back_failed_node(self, do_node_cleanup=False):
        master = self.servers[0]
        rest = RestConnection(master)
        creds = self.input.membase_settings

        # Cluster all servers
        RebalanceHelper.rebalance_in(self.servers, len(self.servers)-1)

        self.log.info("inserting some items in the master before adding any nodes")
        bucket_data = SwapRebalanceBaseTest.bucket_data_init(rest)
        for bucket in rest.get_buckets():
            inserted_keys = SwapRebalanceBaseTest.load_data(master, bucket.name, self.keys_count,\
                self.load_ratio, delete_ratio=0, expiry_ratio=0, test=self, wait_to_drain=True)
            self.log.info("inserted {0} keys".format(len(inserted_keys)))
            bucket_data[bucket.name]['inserted_keys'].extend(inserted_keys)
            bucket_data[bucket.name]["items_inserted_count"] += len(inserted_keys)

        # Start the swap rebalance
        self.log.info("Failing over a node")
        self.log.info("current nodes : {0}".format(RebalanceHelper.getOtpNodeIds(master)))
        toBeEjectedNodes = RebalanceHelper.pick_nodes(master, howmany=self.failover_factor)
        optNodesIds = [node.id for node in toBeEjectedNodes]
        if self.fail_orchestrator:
            status, content = ClusterHelper.find_orchestrator(master)
            self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}".\
            format(status, content))
            optNodesIds[0] = content

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
        rest = RestConnection(self.servers[-1])

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

        SwapRebalanceBaseTest.verification(master, self)


class SwapRebalanceBasicTests(unittest.TestCase):

    def setUp(self):
        self.common_setup(self)

    def tearDown(self):
        self.common_tearDown(self)

    def do_test(self):
        SwapRebalanceBase._common_test_body_swap_rebalance(do_stop_start=True)

class SwapRebalabanceStartStopTests(unittest.TestCase):

    def setUp(self):
        self.common_setup(self)

    def tearDown(self):
        self.common_tearDown(self)

    def do_test(self):
        SwapRebalanceBase._common_test_body_swap_rebalance(do_stop_start=True)

class SwapRebalanceFailedTests(unittest.TestCase):

    def setUp(self):
        self.common_setup(self)

    def tearDown(self):
        self.common_tearDown(self)

    def test_failed_swap_rebalance(self):
        SwapRebalanceBase._common_test_body_failed_swap_rebalance()

    # Not cluster_run friendly, yet
    def test_add_back_failed_node_swap_rebalance(self):
        SwapRebalanceBase._add_back_failed_node(do_node_cleanup=False)
