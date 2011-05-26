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
from memcached.helper.data_helper import MemcachedClientHelper, MutationThread

class RebalanceBaseTest(unittest.TestCase):
    @staticmethod
    def common_setup(input, bucket, testcase):
        log = logger.Logger.get_logger()
        servers = input.servers
        ClusterHelper.cleanup_cluster(servers)
        ClusterHelper.wait_for_ns_servers_or_assert(servers, testcase)
        BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)
        serverInfo = servers[0]
        log.info('picking server : {0} as the master'.format(serverInfo))
        rest = RestConnection(serverInfo)
        info = rest.get_nodes_self()
        rest.init_cluster(username=serverInfo.rest_username,
                          password=serverInfo.rest_password)
        rest.init_cluster_memoryQuota(memoryQuota=info.mcdMemoryReserved)
        bucket_ram = info.mcdMemoryReserved * 2 / 3
        rest.create_bucket(bucket=bucket, ramQuotaMB=bucket_ram, replicaNumber=1, proxyPort=11211)
        BucketOperationHelper.wait_till_memcached_is_ready_or_assert(servers=[serverInfo],
                                                                     bucket_port=11211,
                                                                     test=testcase)

    @staticmethod
    def common_tearDown(servers, testcase):
        BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)
        ClusterOperationHelper.cleanup_cluster(servers)
        ClusterHelper.wait_for_ns_servers_or_assert(servers, testcase)
        BucketOperationHelper.delete_all_buckets_or_assert(servers, testcase)


    @staticmethod
    def rebalance_in(servers,how_many):
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
            if len(toBeAdded) == how_many:
                break

        for server in toBeAdded:
            rest.add_node('Administrator','password',server.ip)
            #check if its added ?
        otpNodes = [node.id for node in rest.node_statuses()]
        started = rest.rebalance(otpNodes,[])
        msg = "rebalance operation started ? {0}"
        log.info(msg.format(started))
        if started:
            result = rest.monitorRebalance()
            msg = "successfully rebalanced out selected nodes from the cluster ? {0}"
            log.info(msg.format(result))
            return result
        return False

#load data. add one node rebalance , rebalance out
class IncrementalRebalanceInTests(unittest.TestCase):
    pass

    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()
        RebalanceBaseTest.common_setup(self._input, 'default', self)

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self._servers, self)

    #load data add one node , rebalance add another node rebalance
    def _common_test_body(self, load_ratio):
        master = self._servers[0]
        rest = RestConnection(master)
        creds = self._input.membase_settings
        items_inserted_count = 0

        self.log.info("inserting some items in the master before adding any nodes")
        distribution = {10: 0.5, 20: 0.5}
        inserted_count, rejected_count =\
        MemcachedClientHelper.load_bucket(serverInfo=master,
                                          ram_load_ratio=0.1,
                                          value_size_distribution=distribution,
                                          number_of_threads=20)
        items_inserted_count += inserted_count

        for server in self._servers[1:]:
            nodes = rest.node_statuses()
            otpNodeIds = [node.id for node in nodes]
            if 'ns_1@127.0.0.1' in otpNodeIds:
                otpNodeIds.remove('ns_1@127.0.0.1')
                otpNodeIds.append('ns_1@{0}'.format(master.ip))
            self.log.info("current nodes : {0}".format(otpNodeIds))
            self.log.info("adding node {0} and rebalance afterwards".format(server.ip))
            otpNode = rest.add_node(creds.rest_username,
                                    creds.rest_password,
                                    server.ip)
            msg = "unable to add node {0} to the cluster"
            self.assertTrue(otpNode, msg.format(server.ip))
            otpNodeIds.append(otpNode.id)
            distribution = {10: 0.2, 20: 0.5, 30: 0.25, 40: 0.05}
            if load_ratio == 10:
                distribution = {1024: 0.4, 2 * 1024: 0.5, 10 * 1024: 0.1}
            elif load_ratio > 10:
                distribution = {5 * 1024: 0.4, 10 * 1024: 0.5, 20 * 1024: 0.1}

            inserted_count, rejected_count =\
            MemcachedClientHelper.load_bucket(serverInfo=master,
                                              ram_load_ratio=load_ratio,
                                              value_size_distribution=distribution,
                                              number_of_threads=5)
            self.log.info('inserted {0} keys'.format(inserted_count))
            rest.rebalance(otpNodes=otpNodeIds, ejectedNodes=[])
            self.assertTrue(rest.monitorRebalance(),
                            msg="rebalance operation failed after adding node {0}".format(server.ip))
            items_inserted_count += inserted_count
            final_replication_state = RestHelper(rest).wait_for_replication(120)
            msg = "replication state after waiting for up to 2 minutes : {0}"
            self.log.info(msg.format(final_replication_state))
            start_time = time.time()
            stats = rest.get_bucket_stats()
            while time.time() < (start_time + 120) and stats["curr_items"] != items_inserted_count:
                self.log.info("curr_items : {0} versus {1}".format(stats["curr_items"], items_inserted_count))
                time.sleep(5)
                stats = rest.get_bucket_stats()
            RebalanceHelper.print_taps_from_all_nodes(rest,'default')
            self.log.info("curr_items : {0} versus {1}".format(stats["curr_items"], items_inserted_count))
            stats = rest.get_bucket_stats()
            msg = "curr_items : {0} is not equal to actual # of keys inserted : {1}"
            self.assertEquals(stats["curr_items"], items_inserted_count,
                              msg=msg.format(stats["curr_items"], items_inserted_count))
        BucketOperationHelper.delete_all_buckets_or_assert(self._servers, self)

    def test_small_load(self):
        self._common_test_body(0.1)

    def test_medium_load(self):
        self._common_test_body(10.0)

    def test_heavy_load(self):
        self._common_test_body(20.0)

    def test_dgm_150(self):
        self._common_test_body(150.0)

    def test_dgm_300(self):
        self._common_test_body(300.0)


#disk greater than memory
#class IncrementalRebalanceInAndOutTests(unittest.TestCase):
#    pass
class IncrementalRebalanceInWithParallelLoad(unittest.TestCase):
    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()
        RebalanceBaseTest.common_setup(self._input, 'default', self)

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self._servers, self)

    #load data add one node , rebalance add another node rebalance
    def _common_test_body(self, load_ratio):
        master = self._servers[0]
        rest = RestConnection(master)
        creds = self._input.membase_settings
        items_inserted_count = 0

        self.log.info("inserting some items in the master before adding any nodes")
        distribution = {10: 0.5, 20: 0.5}
        inserted_count, rejected_count =\
        MemcachedClientHelper.load_bucket(serverInfo=master,
                                          ram_load_ratio=0.1,
                                          value_size_distribution=distribution,
                                          number_of_threads=20)
        items_inserted_count += inserted_count

        for server in self._servers[1:]:
            nodes = rest.node_statuses()
            otpNodeIds = [node.id for node in nodes]
            if 'ns_1@127.0.0.1' in otpNodeIds:
                otpNodeIds.remove('ns_1@127.0.0.1')
                otpNodeIds.append('ns_1@{0}'.format(master.ip))
            self.log.info("current nodes : {0}".format(otpNodeIds))
            self.log.info("adding node {0} and rebalance afterwards".format(server.ip))
            otpNode = rest.add_node(creds.rest_username,
                                    creds.rest_password,
                                    server.ip)
            msg = "unable to add node {0} to the cluster"
            self.assertTrue(otpNode, msg.format(server.ip))
            otpNodeIds.append(otpNode.id)
            #let's just start the load thread
            #better if we do sth like . start rebalance after 20 percent of the data is set
            distribution = {10: 0.2, 20: 0.5, 30: 0.25, 40: 0.05}
            if load_ratio == 10:
                distribution = {1024: 0.4, 2 * 1024: 0.5, 10 * 1024: 0.1}
            elif load_ratio > 10:
                distribution = {5 * 1024: 0.4, 10 * 1024: 0.5, 20 * 1024: 0.1}
            threads = MemcachedClientHelper.create_threads_for_load_bucket(serverInfo=master,
                                                                           ram_load_ratio=load_ratio,
                                                                           value_size_distribution=distribution,
                                                                           number_of_threads=20)
            for thread in threads:
                thread.start()

            rest.rebalance(otpNodes=otpNodeIds, ejectedNodes=[])
            self.assertTrue(rest.monitorRebalance(),
                            msg="rebalance operation failed after adding node {0}".format(server.ip))

            for thread in threads:
                thread.join()
                items_inserted_count += thread.inserted_keys_count()

            final_replication_state = RestHelper(rest).wait_for_replication(120)
            msg = "replication state after waiting for up to 2 minutes : {0}"
            self.log.info(msg.format(final_replication_state))
            start_time = time.time()
            stats = rest.get_bucket_stats()
            while time.time() < (start_time + 120) and stats["curr_items"] != items_inserted_count:
                self.log.info("curr_items : {0} versus {1}".format(stats["curr_items"], items_inserted_count))
                time.sleep(5)
                stats = rest.get_bucket_stats()
            RebalanceHelper.print_taps_from_all_nodes(rest, 'default')
            self.log.info("curr_items : {0} versus {1}".format(stats["curr_items"], items_inserted_count))
            stats = rest.get_bucket_stats()
            msg = "curr_items : {0} is not equal to actual # of keys inserted : {1}"
            self.assertEquals(stats["curr_items"], items_inserted_count,
                              msg=msg.format(stats["curr_items"], items_inserted_count))
        BucketOperationHelper.delete_all_buckets_or_assert(self._servers, self)

    def test_small_load(self):
        self._common_test_body(1.0)

    def test_medium_load(self):
        self._common_test_body(10.0)

    def test_heavy_load(self):
        self._common_test_body(40.0)

    def test_dgm_150(self):
        self._common_test_body(150.0)

    def test_dgm_300(self):
        self._common_test_body(300.0)


#this test case will add all the nodes and then start removing them one by one
#
class IncrementalRebalanceOut(unittest.TestCase):
    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()
        RebalanceBaseTest.common_setup(self._input, 'default', self)


    def test_rebalance_out_small_load(self):
        self.test_rebalance_out(1.0)

    def test_rebalance_out_medium_load(self):
        self.test_rebalance_out(10)

    def test_rebalance_out(self, ratio):
        ram_ratio = (ratio / (len(self._servers)))
        #the ratio is relative to the number of nodes ?
        master = self._servers[0]
        rest = RestConnection(master)
        creds = self._input.membase_settings
        items_inserted_count = 0

        self.log.info("inserting some items in the master before adding any nodes")
        distribution = {10: 0.2, 20: 0.5, 30: 0.25, 40: 0.05}
        if ram_ratio == 10:
            distribution = {1024: 0.4, 2 * 1024: 0.5, 10 * 1024: 0.1}
        elif ram_ratio > 10:
            distribution = {5 * 1024: 0.4, 10 * 1024: 0.5, 20 * 1024: 0.1}
        inserted_count, rejected_count =\
        MemcachedClientHelper.load_bucket(serverInfo=master,
                                          ram_load_ratio=ram_ratio,
                                          value_size_distribution=distribution,
                                          number_of_threads=20)
        self.log.info('inserted {0} keys'.format(inserted_count))
        items_inserted_count += inserted_count

        ClusterHelper.add_all_nodes_or_assert(master, self._servers, creds, self)
        #remove nodes one by one and rebalance , add some item and
        #rebalance ?

        nodes = rest.node_statuses()

        #dont rebalance out the current node
        while len(nodes) > 1:
            #pick a node that is not the master node
            toBeEjectedNode = None
            for node_for_stat in nodes:
                if node_for_stat.id.find('127.0.0.1') == -1 and node_for_stat.id.find(master.ip) == -1:
                    toBeEjectedNode = node_for_stat
                    break

            if not toBeEjectedNode:
                break
            distribution = {10: 0.2, 20: 0.5, 30: 0.25, 40: 0.05}
            if ram_ratio == 10:
                distribution = {1024: 0.4, 2 * 1024: 0.5, 10 * 1024: 0.1}
            elif ram_ratio > 10:
                distribution = {5 * 1024: 0.4, 10 * 1024: 0.5, 20 * 1024: 0.1}
            inserted_keys_count, rejected_keys_count =\
            MemcachedClientHelper.load_bucket(serverInfo=master,
                                              ram_load_ratio=ram_ratio,
                                              value_size_distribution=distribution,
                                              number_of_threads=20)
            self.log.info('inserted {0} keys'.format(inserted_keys_count))
            items_inserted_count += inserted_keys_count
            nodes = rest.node_statuses()
            otpNodeIds = [node.id for node in nodes]
            self.log.info("current nodes : {0}".format(otpNodeIds))
            self.log.info("removing node {0} and rebalance afterwards".format(toBeEjectedNode.id))
            rest.rebalance(otpNodes=otpNodeIds, ejectedNodes=[toBeEjectedNode.id])
            self.assertTrue(rest.monitorRebalance(),
                            msg="rebalance operation failed after adding node {0}".format(toBeEjectedNode.id))
            final_replication_state = RestHelper(rest).wait_for_replication(240)
            msg = "replication state after waiting for up to 4 minutes : {0}"
            self.log.info(msg.format(final_replication_state))
            start_time = time.time()
            stats = rest.get_bucket_stats()
            while time.time() < (start_time + 120) and stats["curr_items"] != items_inserted_count:
                self.log.info("curr_items : {0} versus {1}".format(stats["curr_items"], items_inserted_count))
                time.sleep(5)
                stats = rest.get_bucket_stats()
            self.log.info("curr_items : {0} versus {1}".format(stats["curr_items"], items_inserted_count))

            #visit all nodes ?
            RebalanceHelper.print_taps_from_all_nodes(rest,'default')
            stats = rest.get_bucket_stats()
            msg = "curr_items : {0} is not equal to actual # of keys inserted : {1}"
            #print
            self.assertEquals(stats["curr_items"], items_inserted_count,
                              msg=msg.format(stats["curr_items"], items_inserted_count))
            nodes = rest.node_statuses()
        BucketOperationHelper.delete_all_buckets_or_assert(self._servers, self)


class RebalanceTestsWithMutationLoadTests(unittest.TestCase):
    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()
        RebalanceBaseTest.common_setup(self._input, 'default', self)

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self._servers, self)

    #load data add one node , rebalance add another node rebalance
    def _common_test_body(self, load_ratio):
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
        inserted_keys, rejected_keys =\
        MemcachedClientHelper.load_bucket_and_return_the_keys(serverInfo=master,
                                                              ram_load_ratio=load_ratio,
                                                              number_of_items=-1,
                                                              value_size_distribution=distribution,
                                                              number_of_threads=20)
        items_inserted_count += len(inserted_keys)

        #let's mutate all those keys
        for server in self._servers[1:]:
            nodes = rest.node_statuses()
            otpNodeIds = [node.id for node in nodes]
            if 'ns_1@127.0.0.1' in otpNodeIds:
                otpNodeIds.remove('ns_1@127.0.0.1')
                otpNodeIds.append('ns_1@{0}'.format(master.ip))
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

            self.log.info("waiting for mutation thread....")
            thread.join()

            rejected = thread._rejected_keys
            client = MemcachedClientHelper.create_memcached_client(master.ip)
            self.log.info("printing tap stats before verifying mutations...")
            RebalanceHelper.print_taps_from_all_nodes(rest,'default')
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

            final_replication_state = RestHelper(rest).wait_for_replication(120)
            msg = "replication state after waiting for up to 2 minutes : {0}"
            self.log.info(msg.format(final_replication_state))
            start_time = time.time()
            stats = rest.get_bucket_stats()
            while time.time() < (start_time + 120) and stats["curr_items"] != items_inserted_count:
                self.log.info("curr_items : {0} versus {1}".format(stats["curr_items"], items_inserted_count))
                time.sleep(5)
                stats = rest.get_bucket_stats()
                #loop over all keys and verify

            RebalanceHelper.print_taps_from_all_nodes(rest,'default')
            self.log.info("curr_items : {0} versus {1}".format(stats["curr_items"], items_inserted_count))
            stats = rest.get_bucket_stats()
            msg = "curr_items : {0} is not equal to actual # of keys inserted : {1}"
            self.assertEquals(stats["curr_items"], items_inserted_count,
                              msg=msg.format(stats["curr_items"], items_inserted_count))
        BucketOperationHelper.delete_all_buckets_or_assert(self._servers, self)

    def test_small_load(self):
        self._common_test_body(1.0)

    def test_medium_load(self):
        self._common_test_body(10.0)

    def test_heavy_load(self):
        self._common_test_body(40.0)

# fill bucket len(nodes)X and then rebalance nodes one by one
class IncrementalRebalanceInDgmTests(unittest.TestCase):
    pass

    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()
        RebalanceBaseTest.common_setup(self._input, 'default', self)

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self._servers, self)

    #load data add one node , rebalance add another node rebalance
    def _common_test_body(self, dgm_ratio):
        master = self._servers[0]
        rest = RestConnection(master)
        items_inserted_count = 0
        self.log.info("inserting some items in the master before adding any nodes")
        distribution = {10: 0.5, 20: 0.5}
        inserted_count, rejected_count =\
        MemcachedClientHelper.load_bucket(serverInfo=master,
                                          ram_load_ratio=0.1,
                                          value_size_distribution=distribution,
                                          number_of_threads=20)
        items_inserted_count += inserted_count

        nodes = rest.node_statuses()
        while len(nodes) < len(self._servers):
            self.assertTrue(RebalanceBaseTest.rebalance_in(self._servers, 1),
                            msg="unable to add and rebalance another node")
            distribution = {1 * 512: 0.4, 1 * 1024: 0.5, 2 * 1024: 0.1}
            inserted_count, rejected_count =\
            MemcachedClientHelper.load_bucket(serverInfo=master,
                                              ram_load_ratio=dgm_ratio * 100,
                                              value_size_distribution=distribution,
                                              number_of_threads=40)
            self.log.info('inserted {0} keys'.format(inserted_count))
            items_inserted_count += inserted_count
            final_replication_state = RestHelper(rest).wait_for_replication(120)
            msg = "replication state after waiting for up to 2 minutes : {0}"
            self.log.info(msg.format(final_replication_state))
            start_time = time.time()
            stats = rest.get_bucket_stats()
            while time.time() < (start_time + 120) and stats["curr_items"] != items_inserted_count:
                self.log.info("curr_items : {0} versus {1}".format(stats["curr_items"], items_inserted_count))
                time.sleep(5)
                stats = rest.get_bucket_stats()
            RebalanceHelper.print_taps_from_all_nodes(rest, 'default')
            self.log.info("curr_items : {0} versus {1}".format(stats["curr_items"], items_inserted_count))
            stats = rest.get_bucket_stats()
            msg = "curr_items : {0} is not equal to actual # of keys inserted : {1}"
            self.assertEquals(stats["curr_items"], items_inserted_count,
                              msg=msg.format(stats["curr_items"], items_inserted_count))
        BucketOperationHelper.delete_all_buckets_or_assert(self._servers, self)

    def test_1_5x(self):
        self._common_test_body(1.5)

    def test_2_x(self):
        self._common_test_body(2)

    def test_3_x(self):
        self._common_test_body(3)

    def test_5_x(self):
        self._common_test_body(5)


class RebalanceSwapTests(unittest.TestCase):
    pass

    def setUp(self):
        self._input = TestInputSingleton.input
        self._servers = self._input.servers
        self.log = logger.Logger().get_logger()
        RebalanceBaseTest.common_setup(self._input, 'default', self)

    def tearDown(self):
        RebalanceBaseTest.common_tearDown(self._servers, self)

    def rebalance_in(self,how_many):
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
            rest.add_node('Administrator','password',server.ip)
            #check if its added ?
        otpNodes = [node.id for node in nodes]
        started = rest.rebalance(otpNodes,[])
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
        MemcachedClientHelper.load_bucket(serverInfo=master,
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
        started = rest.rebalance(currentNodes,toBeEjected)
        msg = "rebalance operation started ? {0}"
        self.log.info(msg.format(started))
        self.assertTrue(started, "rebalance operation did not start for swapping out the nodes")
        result = rest.monitorRebalance()
        msg = "successfully swapped out {0} nodes from the cluster ? {0}"
        self.assertTrue(result,msg.format(swap_count))
        #rest will have to change now?
        rest = RestConnection(toBeAdded[0])
        final_replication_state = RestHelper(rest).wait_for_replication(120)
        msg = "replication state after waiting for up to 2 minutes : {0}"
        self.log.info(msg.format(final_replication_state))
        start_time = time.time()
        stats = rest.get_bucket_stats()
        while time.time() < (start_time + 120) and stats["curr_items"] != items_inserted_count:
            self.log.info("curr_items : {0} versus {1}".format(stats["curr_items"], items_inserted_count))
            time.sleep(5)
            stats = rest.get_bucket_stats()
        RebalanceHelper.print_taps_from_all_nodes(rest,'default')
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