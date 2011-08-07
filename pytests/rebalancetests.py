from random import shuffle
import time
import unittest
import uuid
from TestInput import TestInputSingleton
import logger
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper as ClusterHelper, ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import MemcachedClientHelper, MutationThread, VBucketAwareMemcached

class RebalanceBaseTest(unittest.TestCase):
    @staticmethod
    def common_setup(input, testcase, bucket_ram_ratio=(2.0 / 3.0), replica=0):
        log = logger.Logger.get_logger()
        servers = input.servers
        BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)
        ClusterHelper.cleanup_cluster(servers)
        ClusterHelper.wait_for_ns_servers_or_assert(servers, testcase)
        serverInfo = servers[0]

        log.info('picking server : {0} as the master'.format(serverInfo))
        #if all nodes are on the same machine let's have the bucket_ram_ratio as bucket_ram_ratio * 1/len(servers)
        node_ram_ratio = BucketOperationHelper.base_bucket_ratio(servers)
        rest = RestConnection(serverInfo)
        info = rest.get_nodes_self()
        rest.init_cluster(username=serverInfo.rest_username,
                          password=serverInfo.rest_password)
        rest.init_cluster_memoryQuota(memoryQuota=info.mcdMemoryReserved * node_ram_ratio)
        BucketOperationHelper.create_multiple_buckets(serverInfo, replica, node_ram_ratio * bucket_ram_ratio, howmany=1)

    @staticmethod
    def common_tearDown(servers, testcase):
        BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)
        ClusterOperationHelper.cleanup_cluster(servers)
        ClusterHelper.wait_for_ns_servers_or_assert(servers, testcase)
        BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)

    @staticmethod
    def replication_verification(master, bucket_data, replica, test, failed_over=False):
        asserts = []
        rest = RestConnection(master)
        buckets = rest.get_buckets()
        nodes = rest.node_statuses()
        test.log.info("expect {0} / {1} replication ? {2}".format(len(nodes),
            (1.0 + replica), len(nodes) / (1.0 + replica)))
        if len(nodes) / (1.0 + replica) >= 1:
            final_replication_state = RestHelper(rest).wait_for_replication(300)
            msg = "replication state after waiting for up to 5 minutes : {0}"
            test.log.info(msg.format(final_replication_state))

            for bucket in buckets:
                replica_match = RebalanceHelper.wait_till_total_numbers_match(bucket=bucket.name,
                                                                              master=master,
                                                                              timeout_in_seconds=300)
                if not replica_match:
                    asserts.append("replication was completed but sum(curr_items) dont match the curr_items_total")
                if not failed_over:
                    stats = rest.get_bucket_stats(bucket=bucket.name)
                    RebalanceHelper.print_taps_from_all_nodes(rest, bucket.name)
                    msg = "curr_items : {0} is not equal to actual # of keys inserted : {1}"
                    active_items_match = stats["curr_items"] == bucket_data[bucket.name]["items_inserted_count"]
                    if not active_items_match:
                        asserts.append(
                            msg.format(stats["curr_items"], bucket_data[bucket.name]["items_inserted_count"]))

        if len(asserts) > 0:
            for msg in asserts:
                test.log.error(msg)
            test.assertTrue(len(asserts) == 0, msg=asserts)


    @staticmethod
    def get_distribution(load_ratio):
        distribution = {10: 0.2, 20: 0.5, 30: 0.25, 40: 0.05}
        if load_ratio == 10:
            distribution = {1024: 0.4, 2 * 1024: 0.5, 10 * 1024: 0.1}
        elif load_ratio > 10:
            distribution = {5 * 1024: 0.4, 10 * 1024: 0.5, 20 * 1024: 0.1}
        return distribution


    @staticmethod
    def rebalance_in(servers, how_many):
        servers_rebalanced = []
        log = logger.Logger.get_logger()
        rest = RestConnection(servers[0])
        nodes = rest.node_statuses()
        #choose how_many nodes from self._servers which are not part of
        # nodes
        nodeIps = [node.ip for node in nodes]
        log.info("current nodes : {0}".format(nodeIps))
        toBeAdded = []
        selection = servers[1:]
        shuffle(selection)
        for server in selection:
            if not server.ip in nodeIps:
                toBeAdded.append(server)
                servers_rebalanced.append(server)
            if len(toBeAdded) == how_many:
                break

        for server in toBeAdded:
            rest.add_node('Administrator', 'password', server.ip)
            #check if its added ?
        otpNodes = [node.id for node in rest.node_statuses()]
        started = rest.rebalance(otpNodes, [])
        msg = "rebalance operation started ? {0}"
        log.info(msg.format(started))
        if started:
            result = rest.monitorRebalance()
            msg = "successfully rebalanced out selected nodes from the cluster ? {0}"
            log.info(msg.format(result))
            return result, servers_rebalanced
        return False, servers_rebalanced

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
                                              moxi=False)
            test.log.info('inserted {0} keys'.format(inserted_count))
            bucket_data[bucket.name]["items_inserted_count"] += inserted_count

    @staticmethod
    def threads_for_buckets(rest, load_ratio, distribution, rebalanced_servers, bucket_data):
        buckets = rest.get_buckets()
        for bucket in buckets:
            threads = MemcachedClientHelper.create_threads(servers=rebalanced_servers,
                                                           name=bucket.name,
                                                           ram_load_ratio=load_ratio,
                                                           value_size_distribution=distribution,
                                                           number_of_threads=20,
                                                           moxi=False)
            [t.start() for t in threads]
            bucket_data[bucket.name]["threads"] = threads
        return bucket_data

    @staticmethod
    def bucket_data_init(rest):
        bucket_data = {}
        buckets = rest.get_buckets()
        for bucket in buckets:
            bucket_data[bucket.name] = {}
            bucket_data[bucket.name]["items_inserted_count"] = 0
        return bucket_data

    @staticmethod
    def getOtpNodeIds(master):
        rest = RestConnection(master)
        nodes = rest.node_statuses()
        otpNodeIds = [node.id for node in nodes]
        #        if 'ns_1@127.0.0.1' in otpNodeIds:
        #            otpNodeIds.remove('ns_1@127.0.0.1')
        #            otpNodeIds.append('ns_1@{0}'.format(master.ip))
        return otpNodeIds


    @staticmethod
    def pick_node(master):
        rest = RestConnection(master)
        nodes = rest.node_statuses()
        picked = None
        for node_for_stat in nodes:
            if node_for_stat.id.find('127.0.0.1') == -1 and node_for_stat.id.find(master.ip) == -1:
                picked = node_for_stat
                break
        return picked


#load data. add one node rebalance , rebalance out
class IncrementalRebalanceInTests(unittest.TestCase):
    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self._servers, self)

    #load data add one node , rebalance add another node rebalance
    def _common_test_body(self, load_ratio, replica=1):
        master = self._servers[0]
        rest = RestConnection(master)
        creds = self._input.membase_settings
        rebalanced_servers = [master]
        bucket_data = RebalanceBaseTest.bucket_data_init(rest)

        for server in self._servers[1:]:
            otpNodeIds = RebalanceBaseTest.getOtpNodeIds(master)
            self.log.info("current nodes : {0}".format(otpNodeIds))
            self.log.info("adding node {0} and rebalance afterwards".format(server.ip))
            otpNode = rest.add_node(creds.rest_username, creds.rest_password, server.ip, server.port)
            msg = "unable to add node {0} to the cluster {1}"
            self.assertTrue(otpNode, msg.format(server.ip, master.ip))
            otpNodeIds.append(otpNode.id)
            distribution = RebalanceBaseTest.get_distribution(load_ratio)
            RebalanceBaseTest.load_data_for_buckets(rest, load_ratio, distribution, rebalanced_servers, bucket_data,
                                                    self)
            rest.rebalance(otpNodes=otpNodeIds, ejectedNodes=[])
            self.assertTrue(rest.monitorRebalance(),
                            msg="rebalance operation failed after adding node {0}".format(server.ip))
            rebalanced_servers.append(server)
            RebalanceBaseTest.replication_verification(master, bucket_data, replica, self)

        BucketOperationHelper.delete_all_buckets_or_assert(self._servers, self)

    def test_small_load_replica_0(self):
        RebalanceBaseTest.common_setup(self._input, self)
        self._common_test_body(0.1)


    def test_small_load(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=1)
        self._common_test_body(0.1)

    def test_small_load_2_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=2)
        self._common_test_body(0.1, replica=2)

    def test_small_load_3_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=3)
        self._common_test_body(0.1, replica=3)

    def test_medium_load(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=1)
        self._common_test_body(10.0)

    def test_medium_load_2_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=2)
        self._common_test_body(10.0, replica=2)

    def test_medium_load_3_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=3)
        self._common_test_body(10.0, replica=3)

    def test_heavy_load(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=1)
        self._common_test_body(20.0)

    def test_heavy_load_2_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=2)
        self._common_test_body(20.0, replica=2)

    def test_heavy_load_3_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=3)
        self._common_test_body(20.0, replica=3)

    def test_dgm_150(self):
        RebalanceBaseTest.common_setup(self._input, self)
        self._common_test_body(150.0)

    def test_dgm_300(self):
        RebalanceBaseTest.common_setup(self._input, self)
        self._common_test_body(300.0)

    def test_dgm_150_2_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=2)
        self._common_test_body(150.0, replica=2)

    def test_dgm_150_3_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=3)
        self._common_test_body(150.0, replica=3)

    def test_dgm_300_2_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=2)
        self._common_test_body(300.0, replica=2)

    def test_dgm_300_3_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=3)
        self._common_test_body(300.0, replica=3)


#disk greater than memory
#class IncrementalRebalanceInAndOutTests(unittest.TestCase):
#    pass
class IncrementalRebalanceInWithParallelLoad(unittest.TestCase):
    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self._servers, self)

    #load data add one node , rebalance add another node rebalance
    def _common_test_body(self, load_ratio, replica):
        master = self._servers[0]
        rest = RestConnection(master)
        bucket_data = RebalanceBaseTest.bucket_data_init(rest)
        creds = self._input.membase_settings
        rebalanced_servers = [master]

        for server in self._servers[1:]:
            otpNodeIds = RebalanceBaseTest.getOtpNodeIds(master)
            self.log.info("current nodes : {0}".format(otpNodeIds))
            self.log.info("adding node {0} and rebalance afterwards".format(server.ip))
            otpNode = rest.add_node(creds.rest_username,
                                    creds.rest_password,
                                    server.ip)
            msg = "unable to add node {0} to the cluster"
            self.assertTrue(otpNode, msg.format(server.ip))
            otpNodeIds.append(otpNode.id)
            distribution = RebalanceBaseTest.get_distribution(load_ratio)
            bucket_data = RebalanceBaseTest.threads_for_buckets(rest, load_ratio, distribution, rebalanced_servers,
                                                                bucket_data)

            rest.rebalance(otpNodes=otpNodeIds, ejectedNodes=[])
            self.assertTrue(rest.monitorRebalance(),
                            msg="rebalance operation failed after adding node {0}".format(server.ip))
            rebalanced_servers.append(server)

            for name in bucket_data:
                for thread in bucket_data[name]["threads"]:
                    bucket_data[name]["items_inserted_count"] += thread.inserted_keys_count()

            RebalanceBaseTest.replication_verification(master, bucket_data, replica, self)

        BucketOperationHelper.delete_all_buckets_or_assert(self._servers, self)

    def test_small_load(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=1)
        self._common_test_body(1.0, replica=1)

    def test_small_load_2_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=2)
        self._common_test_body(1.0, replica=2)

    def test_small_load_3_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=3)
        self._common_test_body(1.0, replica=3)

    def test_medium_load(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=1)
        self._common_test_body(10.0, replica=1)

    def test_medium_load_2_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=2)
        self._common_test_body(10.0, replica=2)

    def test_medium_load_3_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=3)
        self._common_test_body(10.0, replica=3)

    def test_heavy_load(self):
        self._common_test_body(40.0, replica=1)
        self._common_test_body(40.0, replica=1)

    def test_heavy_load_2_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=2)
        self._common_test_body(40.0, replica=2)

    def test_heavy_load_3_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=3)
        self._common_test_body(40.0, replica=3)

    def test_dgm_150(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=1)
        self._common_test_body(150.0, replica=1)

    def test_dgm_150_2_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=2)
        self._common_test_body(150.0, replica=2)

    def test_dgm_150_3_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=3)
        self._common_test_body(150.0, replica=3)

    def test_dgm_300(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=1)
        self._common_test_body(300.0, replica=1)

    def test_dgm_300_2_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=2)
        self._common_test_body(300.0, replica=2)

    def test_dgm_300_3_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=3)
        self._common_test_body(300.0, replica=3)

#this test case will add all the nodes and then start removing them one by one
#
class IncrementalRebalanceOut(unittest.TestCase):
    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()


    def test_rebalance_out_small_load(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=1)
        self.test_rebalance_out(1.0, replica=1)

    def test_rebalance_out_small_load_2_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=2)
        self.test_rebalance_out(1.0, replica=2)

    def test_rebalance_out_small_load_3_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=3)
        self.test_rebalance_out(1.0, replica=3)

    def test_rebalance_out_medium_load(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=1)
        self.test_rebalance_out(15.0, replica=1)

    def test_rebalance_out_medium_load_2_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=2)
        self.test_rebalance_out(15.0, replica=2)

    def test_rebalance_out_medium_load_3_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=3)
        self.test_rebalance_out(15.0, replica=3)


    def test_rebalance_out_dgm(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=1)
        self.test_rebalance_out(200.0, replica=1)

    def test_rebalance_out_dgm_2_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=2)
        self.test_rebalance_out(200.0, replica=2)

    def test_rebalance_out_dgm_3_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=3)
        self.test_rebalance_out(200.0, replica=3)

    def test_rebalance_out(self, ratio, replica):
        ram_ratio = (ratio / (len(self._servers)))
        #the ratio is relative to the number of nodes ?
        master = self._servers[0]
        rest = RestConnection(master)
        creds = self._input.membase_settings
        bucket_data = RebalanceBaseTest.bucket_data_init(rest)

        distribution = RebalanceBaseTest.get_distribution(ram_ratio)

        ClusterHelper.add_all_nodes_or_assert(master, self._servers, creds, self)
        #remove nodes one by one and rebalance , add some item and
        #rebalance ?
        rebalanced_servers = []
        rebalanced_servers.extend(self._servers)

        nodes = rest.node_statuses()

        #dont rebalance out the current node
        while len(nodes) > 1:
            #pick a node that is not the master node
            toBeEjectedNode = RebalanceBaseTest.pick_node(master)

            RebalanceBaseTest.load_data_for_buckets(rest, ram_ratio, distribution, [master], bucket_data, self)

            otpNodeIds = RebalanceBaseTest.getOtpNodeIds(master)
            self.log.info("current nodes : {0}".format(otpNodeIds))
            self.log.info("removing node {0} and rebalance afterwards".format(toBeEjectedNode.id))
            rest.rebalance(otpNodes=otpNodeIds, ejectedNodes=[toBeEjectedNode.id])
            self.assertTrue(rest.monitorRebalance(),
                            msg="rebalance operation failed after adding node {0}".format(toBeEjectedNode.id))

            for node in nodes:
                for rebalanced_server in rebalanced_servers:
                    if rebalanced_server.ip.find(node.ip) != -1:
                        rebalanced_servers.remove(rebalanced_server)
                        break
            RebalanceBaseTest.replication_verification(master, bucket_data, replica, self)
            nodes = rest.node_statuses()

        BucketOperationHelper.delete_all_buckets_or_assert(self._servers, self)


class StopRebalanceAfterFailoverTests(unittest.TestCase):
    # this test will create cluster of n nodes
    # stops rebalance
    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()


    def stop_rebalance_1_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=1)
        self._common_test_body(1.0, replica=1)

    def stop_rebalance_2_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=2)
        self._common_test_body(1.0, replica=2)

    def stop_rebalance_3_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=3)
        self._common_test_body(1.0, replica=3)


    def _common_test_body(self, ratio, replica):
        ram_ratio = (ratio / (len(self._servers)))
        #the ratio is relative to the number of nodes ?
        master = self._servers[0]
        rest = RestConnection(master)
        creds = self._input.membase_settings
        bucket_data = RebalanceBaseTest.bucket_data_init(rest)

        distribution = RebalanceBaseTest.get_distribution(ram_ratio)

        ClusterHelper.add_all_nodes_or_assert(master, self._servers, creds, self)
        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
        self.assertTrue(rest.monitorRebalance(),
                        msg="rebalance operation failed after adding nodes")

        nodes = rest.node_statuses()

        #dont rebalance out the current node
        while len(nodes) > 1:
            #pick a node that is not the master node
            toBeEjectedNode = RebalanceBaseTest.pick_node(master)
            RebalanceBaseTest.load_data_for_buckets(rest, ram_ratio, distribution, [master], bucket_data, self)
            self.log.info("current nodes : {0}".format([node.id for node in rest.node_statuses()]))
            #let's start/step rebalance three times
            self.log.info("removing node {0} and rebalance afterwards".format(toBeEjectedNode.id))
            rest.fail_over(toBeEjectedNode.id)
            self.log.info("failed over {0}".format(toBeEjectedNode.id))
            time.sleep(10)
            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],
                           ejectedNodes=[toBeEjectedNode.id])
            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[toBeEjectedNode.id])
            expected_progress = 30
            reached = RestHelper(rest).rebalance_reached(expected_progress)
            self.assertTrue(reached, "rebalance failed or did not reach {0}%".format(expected_progress))
            stopped = rest.stop_rebalance()
            self.assertTrue(stopped, msg="unable to stop rebalance")
            time.sleep(20)
            RebalanceBaseTest.replication_verification(master, bucket_data, replica, self)
            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[toBeEjectedNode.id])
            self.assertTrue(rest.monitorRebalance(),
                            msg="rebalance operation failed after adding node {0}".format(toBeEjectedNode.id))
            time.sleep(20)

            RebalanceBaseTest.replication_verification(master, bucket_data, replica, self)
            nodes = rest.node_statuses()

        BucketOperationHelper.delete_all_buckets_or_assert(self._servers, self)


class StopRebalanceTests(unittest.TestCase):
    # this test will create cluster of n nodes
    # stops rebalance
    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()


    def stop_rebalance_1_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=1)
        self._common_test_body(5.0, replica=1)

    def stop_rebalance_2_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=2)
        self._common_test_body(5.0, replica=2)

    def stop_rebalance_3_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=3)
        self._common_test_body(5.0, replica=3)


    def _common_test_body(self, ratio, replica):
        ram_ratio = (ratio / (len(self._servers)))
        #the ratio is relative to the number of nodes ?
        master = self._servers[0]
        rest = RestConnection(master)
        creds = self._input.membase_settings
        bucket_data = RebalanceBaseTest.bucket_data_init(rest)

        distribution = RebalanceBaseTest.get_distribution(ram_ratio)

        ClusterHelper.add_all_nodes_or_assert(master, self._servers, creds, self)
        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
        self.assertTrue(rest.monitorRebalance(),
                        msg="rebalance operation failed after adding nodes")

        nodes = rest.node_statuses()

        #dont rebalance out the current node
        while len(nodes) > 1:
            #pick a node that is not the master node
            toBeEjectedNode = RebalanceBaseTest.pick_node(master)
            RebalanceBaseTest.load_data_for_buckets(rest, ram_ratio, distribution, [master], bucket_data, self)
            self.log.info("current nodes : {0}".format([node.id for node in rest.node_statuses()]))
            #let's start/step rebalance three times
            for i in range(0, 3):
                self.log.info("removing node {0} and rebalance afterwards".format(toBeEjectedNode.id))
                rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[toBeEjectedNode.id])
                expected_progress = 30
                reached = RestHelper(rest).rebalance_reached(expected_progress)
                self.assertTrue(reached, "rebalance failed or did not reach {0}%".format(expected_progress))
                stopped = rest.stop_rebalance()
                self.assertTrue(stopped, msg="unable to stop rebalance")
                time.sleep(20)
                RebalanceBaseTest.replication_verification(master, bucket_data, replica, self)
            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[toBeEjectedNode.id])
            self.assertTrue(rest.monitorRebalance(),
                            msg="rebalance operation failed after adding node {0}".format(toBeEjectedNode.id))
            time.sleep(20)

            RebalanceBaseTest.replication_verification(master, bucket_data, replica, self)
            nodes = rest.node_statuses()

        BucketOperationHelper.delete_all_buckets_or_assert(self._servers, self)


class IncrementalRebalanceWithParallelReadTests(unittest.TestCase):
    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self._servers, self)

    #load data add one node , rebalance add another node rebalance
    def _common_test_body(self, items, replica=1, moxi=False):
        master = self._servers[0]
        rest = RestConnection(master)
        creds = self._input.membase_settings
        bucket_data = RebalanceBaseTest.bucket_data_init(rest)

        for server in self._servers[1:]:
            otpNodeIds = RebalanceBaseTest.getOtpNodeIds(master)
            self.log.info("current nodes : {0}".format(otpNodeIds))
            self.log.info("adding node {0}:{1} and rebalance afterwards".format(server.ip,server.port))
            otpNode = rest.add_node(creds.rest_username, creds.rest_password, server.ip, server.port)
            msg = "unable to add node {0} to the cluster {1}"
            self.assertTrue(otpNode, msg.format(server.ip, master.ip))
            otpNodeIds.append(otpNode.id)
            distribution = RebalanceBaseTest.get_distribution(items)
            for name in bucket_data:
                inserted_keys, rejected_keys =\
                MemcachedClientHelper.load_bucket_and_return_the_keys(servers=[self._servers[0]],
                                                                      name=name,
                                                                      ram_load_ratio=-1,
                                                                      number_of_items=items,
                                                                      value_size_distribution=distribution,
                                                                      number_of_threads=1)
                rest.rebalance(otpNodes=otpNodeIds, ejectedNodes=[])
                self._reader_thread(inserted_keys, bucket_data, moxi=moxi)
                self.assertTrue(rest.monitorRebalance(),
                                msg="rebalance operation failed after adding node {0}".format(server.ip))
                break

        BucketOperationHelper.delete_all_buckets_or_assert(self._servers, self)

    def _reader_thread(self, inserted_keys, bucket_data, moxi=False):
        errors = []
        rest = RestConnection(self._servers[0])
        smartclient = None
        for name in bucket_data:
            for key in inserted_keys:
                if moxi:
                    moxi = MemcachedClientHelper.proxy_client(self._servers[0],name)
                else:
                    smartclient = VBucketAwareMemcached(rest, name)
                try:
                    if moxi:
                        moxi.get(key)
                    else:
                        smartclient.memcached(key).get(key)
                except Exception as ex:
                    errors.append({"error": ex, "key": key})
                    self.log.info(ex)
                    if not moxi:
                        smartclient.done()
                        smartclient = VBucketAwareMemcached(rest, name)

    def test_10k_moxi(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=1)
        self._common_test_body(10 * 1000, moxi=True)

    def test_10k_memcached(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=1)
        self._common_test_body(10 * 1000)



class FailoverRebalanceRepeatTests(unittest.TestCase):
    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self._servers, self)

    #load data add one node , rebalance add another node rebalance
    def _common_test_body(self, load_ratio, replica=1):
        master = self._servers[0]
        rest = RestConnection(master)
        creds = self._input.membase_settings
        rebalanced_servers = [master]
        bucket_data = RebalanceBaseTest.bucket_data_init(rest)

        distribution = RebalanceBaseTest.get_distribution(load_ratio)
        bucket_data = RebalanceBaseTest.threads_for_buckets(rest, load_ratio, distribution, rebalanced_servers,
                                                            bucket_data)

        for name in bucket_data:
            for thread in bucket_data[name]["threads"]:
                bucket_data[name]["items_inserted_count"] += thread.inserted_keys_count()

        for server in self._servers[1:]:
            otpNodeIds = RebalanceBaseTest.getOtpNodeIds(master)
            self.log.info("current nodes : {0}".format(otpNodeIds))
            #do this 3 times , start rebalance , failover the node , remove the node and rebalance
            for i in range(0, 5):
                distribution = RebalanceBaseTest.get_distribution(load_ratio)

                RebalanceBaseTest.load_data_for_buckets(rest, load_ratio, distribution, [master], bucket_data, self)
                self.log.info("adding node {0} and rebalance afterwards".format(server.ip))
                otpNode = rest.add_node(creds.rest_username, creds.rest_password, server.ip, server.port)
                msg = "unable to add node {0} to the cluster {1}"
                self.assertTrue(otpNode, msg.format(server.ip, master.ip))
                rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
                self.assertTrue(rest.monitorRebalance(),
                                msg="rebalance operation failed after adding node {0}".format(server.ip))
                rebalanced_servers.append(server)
                RebalanceBaseTest.replication_verification(master, bucket_data, replica, self, True)
                rest.fail_over(otpNode.id)
                self.log.info("failed over {0}".format(otpNode.id))
                time.sleep(10)
                rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],
                               ejectedNodes=[otpNode.id])
                msg = "rebalance failed while removing failover nodes {0}".format(otpNode.id)
                self.assertTrue(rest.monitorRebalance(), msg=msg)
                #now verify the numbers again ?
                RebalanceBaseTest.replication_verification(master, bucket_data, replica, self, True)
                #wait 6 minutes
                time.sleep(6 * 60)

            self.log.info("adding node {0} and rebalance afterwards".format(server.ip))
            otpNode = rest.add_node(creds.rest_username, creds.rest_password, server.ip, server.port)
            msg = "unable to add node {0} to the cluster {1}"
            self.assertTrue(otpNode, msg.format(server.ip, master.ip))
            distribution = RebalanceBaseTest.get_distribution(load_ratio)
            RebalanceBaseTest.load_data_for_buckets(rest, load_ratio, distribution, rebalanced_servers, bucket_data,
                                                    self)
            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
            self.assertTrue(rest.monitorRebalance(),
                            msg="rebalance operation failed after adding node {0}".format(server.ip))
            rebalanced_servers.append(server)
            RebalanceBaseTest.replication_verification(master, bucket_data, replica, self, True)

        BucketOperationHelper.delete_all_buckets_or_assert(self._servers, self)

    def test_small_load_replica_1(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=1)
        self._common_test_body(1)


class RebalanceTestsWithMutationLoadTests(unittest.TestCase):
    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self._servers, self)

    #load data add one node , rebalance add another node rebalance
    def _common_test_body(self, load_ratio, replica):
        self.log.info(load_ratio)
        master = self._servers[0]
        rest = RestConnection(master)
        creds = self._input.membase_settings
        items_inserted_count = 0

        self.log.info("inserting some items in the master before adding any nodes")
        distribution = {10: 0.2, 20: 0.5, 30: 0.25, 40: 0.05}
        if load_ratio == 10:
            distribution = {1024: 0.4, 2 * 1024: 0.5, 10 * 1024: 0.1}
        elif load_ratio > 10:
            distribution = {5 * 1024: 0.4, 10 * 1024: 0.5, 20 * 1024: 0.1}
        rebalanced_servers = [master]
        inserted_keys, rejected_keys =\
        MemcachedClientHelper.load_bucket_and_return_the_keys(servers=rebalanced_servers,
                                                              ram_load_ratio=load_ratio,
                                                              number_of_items=-1,
                                                              value_size_distribution=distribution,
                                                              number_of_threads=20)
        items_inserted_count += len(inserted_keys)

        #let's mutate all those keys
        for server in self._servers[1:]:
            nodes = rest.node_statuses()
            otpNodeIds = [node.id for node in nodes]
            #            if 'ns_1@127.0.0.1' in otpNodeIds:
            #                otpNodeIds.remove('ns_1@127.0.0.1')
            #                otpNodeIds.append('ns_1@{0}'.format(master.ip))
            self.log.info("current nodes : {0}".format(otpNodeIds))
            self.log.info("adding node {0} and rebalance afterwards".format(server.ip))
            otpNode = rest.add_node(creds.rest_username,
                                    creds.rest_password,
                                    server.ip)
            msg = "unable to add node {0} to the cluster"
            self.assertTrue(otpNode, msg.format(server.ip))
            otpNodeIds.append(otpNode.id)

            mutation = "{0}".format(uuid.uuid4())
            thread = MutationThread(serverInfo=server, keys=inserted_keys, seed=mutation, op="set")
            thread.start()

            rest.rebalance(otpNodes=otpNodeIds, ejectedNodes=[])

            self.assertTrue(rest.monitorRebalance(),
                            msg="rebalance operation failed after adding node {0}".format(server.ip))
            rebalanced_servers.append(server)

            self.log.info("waiting for mutation thread....")
            thread.join()

            rejected = thread._rejected_keys
            client = MemcachedClientHelper.direct_client(master, "default")
            self.log.info("printing tap stats before verifying mutations...")
            RebalanceHelper.print_taps_from_all_nodes(rest)
            did_not_mutate_keys = []
            for key in inserted_keys:
                if key not in rejected:
                    try:
                        flag, keyx, value = client.get(key)
                        if not value.endswith(mutation):
                            did_not_mutate_keys.append({'key': key, 'value': value})
                    except Exception as ex:
                        self.log.info(ex)
                        #                    self.log.info(value)
            if len(did_not_mutate_keys) > 0:
                for item in did_not_mutate_keys:
                    self.log.error(
                        "mutation did not replicate for key : {0} value : {1}".format(item['key'], item['value']))
                    self.fail("{0} mutations were not replicated during rebalance..".format(len(did_not_mutate_keys)))

            nodes = rest.node_statuses()
            self.log.info("expect {0} / {1} replication ? {2}".format(len(nodes),
                (1.0 + replica), len(nodes) / (1.0 + replica)))

            if len(nodes) / (1.0 + replica) >= 1:
                final_replication_state = RestHelper(rest).wait_for_replication(600)
                msg = "replication state after waiting for up to 10 minutes : {0}"
                self.log.info(msg.format(final_replication_state))
                self.assertTrue(RebalanceHelper.wait_till_total_numbers_match(master=master,
                                                                              bucket='default',
                                                                              timeout_in_seconds=600),
                                msg="replication was completed but sum(curr_items) dont match the curr_items_total")

            start_time = time.time()
            stats = rest.get_bucket_stats()
            while time.time() < (start_time + 120) and stats["curr_items"] != items_inserted_count:
                self.log.info("curr_items : {0} versus {1}".format(stats["curr_items"], items_inserted_count))
                time.sleep(5)
                stats = rest.get_bucket_stats()
                #loop over all keys and verify

            RebalanceHelper.print_taps_from_all_nodes(rest)
            self.log.info("curr_items : {0} versus {1}".format(stats["curr_items"], items_inserted_count))
            stats = rest.get_bucket_stats()
            msg = "curr_items : {0} is not equal to actual # of keys inserted : {1}"
            self.assertEquals(stats["curr_items"], items_inserted_count,
                              msg=msg.format(stats["curr_items"], items_inserted_count))
        BucketOperationHelper.delete_all_buckets_or_assert(self._servers, self)

    def test_small_load(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=1)
        self._common_test_body(1.0, replica=1)

    def test_small_load_2_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=2)
        self._common_test_body(1.0, replica=2)

    def test_small_load_3_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=3)
        self._common_test_body(1.0, replica=3)


    def test_medium_load(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=1)
        self._common_test_body(10.0, replica=1)

    def test_medium_load_2_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=2)
        self._common_test_body(10.0, replica=2)

    def test_medium_load_3_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=3)
        self._common_test_body(10.0, replica=3)


    def test_heavy_load(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=1)
        self._common_test_body(40.0, replica=1)

    def test_heavy_load_2_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=2)
        self._common_test_body(40.0, replica=2)

    def test_heavy_load_3_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, replica=3)
        self._common_test_body(401.0, replica=3)


# fill bucket len(nodes)X and then rebalance nodes one by one
class IncrementalRebalanceInDgmTests(unittest.TestCase):
    pass

    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self._servers, self)

    #load data add one node , rebalance add another node rebalance
    def _common_test_body(self, dgm_ratio, distribution, rebalance_in=1, replica=1):
        master = self._servers[0]
        rest = RestConnection(master)
        bucket_data = RebalanceBaseTest.bucket_data_init(rest)

        rebalanced_servers = [master]

        nodes = rest.node_statuses()
        while len(nodes) <= len(self._servers):
            rebalanced_in, which_servers = RebalanceBaseTest.rebalance_in(self._servers, rebalance_in)
            self.assertTrue(rebalanced_in, msg="unable to add and rebalance more nodes")
            rebalanced_servers.extend(which_servers)
            RebalanceBaseTest.load_data_for_buckets(rest, dgm_ratio * 50, distribution, rebalanced_servers, bucket_data,
                                                    self)

            nodes = rest.node_statuses()
            RebalanceBaseTest.replication_verification(master, bucket_data, replica, self)
        BucketOperationHelper.delete_all_buckets_or_assert(self._servers, self)

    def test_1_5x(self):
        RebalanceBaseTest.common_setup(self._input, 'default', self, 1.0 / 3.0)
        distribution = {1 * 512: 0.4, 1 * 1024: 0.5, 2 * 1024: 0.1}
        self._common_test_body(1.5, distribution)

    def test_1_5x_2_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, 1.0 / 3.0, replica=2)
        distribution = {1 * 512: 0.4, 1 * 1024: 0.5, 2 * 1024: 0.1}
        self._common_test_body(1.5, distribution, replica=2)

    def test_1_5x_3_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, 1.0 / 3.0, replica=3)
        distribution = {1 * 512: 0.4, 1 * 1024: 0.5, 2 * 1024: 0.1}
        self._common_test_body(1.5, distribution, replica=3)

    def test_1_5x_cluster_half_2_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, 1.0 / 3.0, replica=2)
        distribution = {2 * 1024: 0.9}
        self._common_test_body(1.5, distribution, len(self._servers) / 2, replica=2)

    def test_1_5x_cluster_half_3_replica(self):
        RebalanceBaseTest.common_setup(self._input, self, 1.0 / 3.0, replica=3)
        distribution = {2 * 1024: 0.9}
        self._common_test_body(1.5, distribution, len(self._servers) / 2, replica=3)


    def test_1_5x_cluster_half(self):
        RebalanceBaseTest.common_setup(self._input, self, 1.0 / 3.0)
        distribution = {2 * 1024: 0.9}
        self._common_test_body(1.5, distribution, len(self._servers) / 2)

    def test_1_5x_cluster_one_third(self):
        RebalanceBaseTest.common_setup(self._input, self, 1.0 / 3.0)
        distribution = {2 * 1024: 0.9}
        self._common_test_body(1.5, distribution, len(self._servers) / 3)

    def test_5x_cluster_half(self):
        RebalanceBaseTest.common_setup(self._input, self, 1.0 / 3.0)
        distribution = {2 * 1024: 0.9}
        self._common_test_body(5, distribution, len(self._servers) / 2)

    def test_5x_cluster_one_third(self):
        RebalanceBaseTest.common_setup(self._input, self, 1.0 / 3.0)
        distribution = {2 * 1024: 0.9}
        self._common_test_body(5, distribution, len(self._servers) / 3)


    def test_1_5x_large_values(self):
        RebalanceBaseTest.common_setup(self._input, 'default', self, 1.0 / 3.0)
        distribution = {2 * 1024: 0.9}
        self._common_test_body(1.5, distribution)

    def test_2_x(self):
        RebalanceBaseTest.common_setup(self._input, 'default', self, 1.0 / 3.0)
        distribution = {2 * 1024: 0.9}
        self._common_test_body(2, distribution)

    def test_3_x(self):
        RebalanceBaseTest.common_setup(self._input, 'default', self, 1.0 / 3.0)
        distribution = {2 * 1024: 0.9}
        self._common_test_body(3, distribution)

    def test_5_x(self):
        RebalanceBaseTest.common_setup(self._input, 'default', self, 1.0 / 3.0)
        distribution = {2 * 1024: 0.9}
        self._common_test_body(5, distribution)


class RebalanceSwapTests(unittest.TestCase):
    pass

    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()
        RebalanceBaseTest.common_setup(self._input, 'default', self)

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self._servers, self)

    def rebalance_in(self, how_many):
        rest = RestConnection(self._servers[0])
        nodes = rest.node_statuses()
        #choose how_many nodes from self._servers which are not part of
        # nodes
        nodeIps = [node.ip for node in nodes]
        self.log.info("current nodes : {0}".format(nodeIps))
        toBeAdded = []
        selection = self._servers[1:]
        shuffle(selection)
        for server in selection:
            if server.ip in nodeIps:
                toBeAdded.append(server)
            if len(toBeAdded) == how_many:
                break

        for server in toBeAdded:
            rest.add_node('Administrator', 'password', server.ip)
            #check if its added ?
        otpNodes = [node.id for node in nodes]
        started = rest.rebalance(otpNodes, [])
        msg = "rebalance operation started ? {0}"
        self.log.info(msg.format(started))
        if started:
            result = rest.monitorRebalance()
            msg = "successfully rebalanced out selected nodes from the cluster ? {0}"
            self.log.info(msg.format(result))
            return result
        return False

    #load data add one node , rebalance add another node rebalance
    #swap one node with another node
    #swap two nodes with two another nodes
    def _common_test_body(self, swap_count):
        msg = "minimum {0} nodes required for running rebalance swap of {1} nodes"
        self.assertTrue(len(self._servers) >= (2 * swap_count),
                        msg=msg.format((2 * swap_count), swap_count))
        master = self._servers[0]
        rest = RestConnection(master)
        creds = self._input.membase_settings
        items_inserted_count = 0

        self.log.info("inserting some items in the master before adding any nodes")
        distribution = {10: 0.5, 20: 0.5}
        inserted_count, rejected_count =\
        MemcachedClientHelper.load_bucket(servers=[master],
                                          ram_load_ratio=0.1,
                                          value_size_distribution=distribution,
                                          number_of_threads=20)
        items_inserted_count += inserted_count

        self.assertTrue(self.rebalance_in(swap_count - 1))
        #now add nodes for being swapped
        #eject the current nodes and add new ones
        # add 2 * swap -1 nodes
        nodeIps = [node.ip for node in rest.node_statuses()]
        self.log.info("current nodes : {0}".format(nodeIps))
        toBeAdded = []
        selection = self._servers[1:]
        shuffle(selection)
        for server in selection:
            if not server.ip in nodeIps:
                toBeAdded.append(server)
            if len(toBeAdded) == swap_count:
                break
        toBeEjected = [node.id for node in rest.node_statuses()]
        for server in toBeAdded:
            rest.add_node(creds.rest_username, creds.rest_password, server.ip)

        currentNodes = [node.id for node in rest.node_statuses()]
        started = rest.rebalance(currentNodes, toBeEjected)
        msg = "rebalance operation started ? {0}"
        self.log.info(msg.format(started))
        self.assertTrue(started, "rebalance operation did not start for swapping out the nodes")
        result = rest.monitorRebalance()
        msg = "successfully swapped out {0} nodes from the cluster ? {0}"
        self.assertTrue(result, msg.format(swap_count))
        #rest will have to change now?
        rest = RestConnection(toBeAdded[0])
        final_replication_state = RestHelper(rest).wait_for_replication()
        msg = "replication state after waiting for up to 2 minutes : {0}"
        self.log.info(msg.format(final_replication_state))
        start_time = time.time()
        stats = rest.get_bucket_stats()
        while time.time() < (start_time + 120) and stats["curr_items"] != items_inserted_count:
            self.log.info("curr_items : {0} versus {1}".format(stats["curr_items"], items_inserted_count))
            time.sleep(5)
            stats = rest.get_bucket_stats()
        RebalanceHelper.print_taps_from_all_nodes(rest)
        self.log.info("curr_items : {0} versus {1}".format(stats["curr_items"], items_inserted_count))
        stats = rest.get_bucket_stats()
        msg = "curr_items : {0} is not equal to actual # of keys inserted : {1}"
        self.assertEquals(stats["curr_items"], items_inserted_count,
                          msg=msg.format(stats["curr_items"], items_inserted_count))


    def test_swap_1(self):
        self._common_test_body(1)

    def test_swap_2(self):
        self._common_test_body(2)

    def test_swap_4(self):
        self._common_test_body(4)