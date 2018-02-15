import copy
import json

from base_2i import BaseSecondaryIndexingTests
from membase.api.rest_client import RestConnection, RestHelper
import random
from lib import testconstants
from lib.memcached.helper.data_helper import MemcachedClientHelper
from lib.remote.remote_util import RemoteMachineShellConnection
from threading import Thread
from pytests.query_tests_helper import QueryHelperTests
from couchbase_helper.documentgenerator import JsonDocGenerator
from couchbase_helper.cluster import Cluster
from gsi_replica_indexes import GSIReplicaIndexesTests


class GSIIndexPartitioningTests(GSIReplicaIndexesTests):
    def setUp(self):
        super(GSIIndexPartitioningTests, self).setUp()
        self.index_servers = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True)
        self.rest = RestConnection(self.index_servers[0])
        self.node_list = []
        for server in self.index_servers:
            self.node_list.append(server.ip + ":" + server.port)

        self.num_queries = self.input.param("num_queries", 100)
        self.num_index_partitions = self.input.param("num_index_partitions", 16)
        self.recover_failed_node = self.input.param("recover_failed_node", False)

    def tearDown(self):
        super(GSIIndexPartitioningTests, self).tearDown()

    # Test that generates n number of create index statements with various permutations and combinations
    # of different clauses used in the create index statement.
    def test_create_partitioned_indexes(self):
        self._load_emp_dataset()

        create_index_queries = self.generate_random_create_index_statements(
            bucketname=self.buckets[0].name, idx_node_list=self.node_list,
            num_statements=self.num_queries)

        failed_index_creation = 0
        for create_index_query in create_index_queries:

            try:
                self.n1ql_helper.run_cbq_query(
                    query=create_index_query["index_definition"],
                    server=self.n1ql_node)
            except Exception, ex:
                self.log.info(str(ex))

            self.sleep(10)

            index_metadata = self.rest.get_indexer_metadata()

            self.log.info("output from /getIndexStatus")
            self.log.info(index_metadata)

            self.log.info("Index Map")
            index_map = self.get_index_map()
            self.log.info(index_map)

            if index_metadata:
                status = self.validate_partitioned_indexes(create_index_query,
                                                           index_map,
                                                           index_metadata)

                if not status:
                    failed_index_creation += 1
                    self.log.info(
                        "** Following query failed validation : {0}".format(
                            create_index_query["index_definition"]))
            else:
                failed_index_creation += 1
                self.log.info(
                    "** Following index did not get created : {0}".format(
                        create_index_query["index_definition"]))

            drop_index_query = "DROP INDEX default.{0}".format(
                create_index_query["index_name"])
            try:
                self.n1ql_helper.run_cbq_query(
                    query=drop_index_query,
                    server=self.n1ql_node)
            except Exception, ex:
                self.log.info(str(ex))

            self.sleep(5)

        self.log.info(
            "Total Create Index Statements Run: {0}, Passed : {1}, Failed : {2}".format(
                self.num_queries, self.num_queries - failed_index_creation,
                failed_index_creation))
        self.assertTrue(failed_index_creation == 0,
                        "Some create index statements failed validations. Pls see the test log above for details.")

    def test_partition_index_with_excluded_nodes(self):
        self._load_emp_dataset()

        # Setting to exclude a node for planner
        self.rest.set_index_planner_settings("excludeNode=in")
        # Create partitioned index
        create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name)"

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_index_statement,
                server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))

        # Validate index created and check the hosts on which partitions are hosted.
        expected_hosts = self.node_list[1:]
        expected_hosts.sort()
        validated = False
        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata :::")
        self.log.info(index_metadata)

        for index in index_metadata["status"]:
            if index["name"] == "idx1":
                self.log.info("Expected Hosts : {0}".format(expected_hosts))
                self.log.info("Actual Hosts   : {0}".format(index["hosts"]))
                self.assertEqual(index["hosts"], expected_hosts,
                                 "Planner did not ignore excluded node during index creation")
                validated = True

        if not validated:
            self.fail("Looks like index was not created.")

    def test_partitioned_index_with_replica(self):
        self._load_emp_dataset()

        # Create partitioned index
        create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'num_replica':{0}, 'num_partition':{1}}}".format(
            self.num_index_replicas, self.num_index_partitions)

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_index_statement,
                server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata :::")
        self.log.info(index_metadata)

        self.assertTrue(self.validate_partition_map(index_metadata, "idx1",
                                                    self.num_index_replicas,
                                                    self.num_index_partitions),
                        "Partition map validation failed")

    def test_partitioned_index_with_replica_with_server_groups(self):
        self._load_emp_dataset()
        self._create_server_groups()

        # Create partitioned index
        create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'num_replica':{0}}}".format(
            self.num_index_replicas)

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_index_statement,
                server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))

        index_metadata = self.rest.get_indexer_metadata()

        index_hosts_list = []
        for index in index_metadata["status"]:
            index_hosts_list.append(index["hosts"])

        self.log.info("Index Host List : {0}".format(index_hosts_list))

        #Need to change the validation logic here. Between index and its replicas, they should have a full set of partitions in both the server groups.
        #idx11 - .101, .102: 3, 4, 5, 10, 11, 15, 16
        #idx11 - .103, .104: 1, 2, 6, 7, 8, 9, 12, 13, 14

        #idx12 - .101, .102: 1, 2, 6, 7, 8, 9, 12, 13, 14
        #idx12 - .103, .104: 3, 4, 5, 10, 11, 15, 16

        validation = True
        for i in range(0,len(index_hosts_list)):
            for j in range(i+1,len(index_hosts_list)):
                if (index_hosts_list[i].sort() != index_hosts_list[j].sort()):
                    continue
                else:
                    validation &= False

        self.assertTrue(validation, "Partitions of replica indexes do not honour server grouping")

    def test_create_partitioned_index_one_node_already_down(self):
        self._load_emp_dataset()

        node_out = self.servers[self.node_out]
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            self.graceful, wait_for_pending=60)

        failover_task.result()

        # Create partitioned index
        create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name)"

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_index_statement,
                server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            self.fail("Failed to create index with one node failed")

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata :::")
        self.log.info(index_metadata)

        hosts = index_metadata["status"][0]["hosts"]
        self.log.info("Actual nodes : {0}".format(hosts))
        node_out_str = node_out.ip + ":" + node_out.port
        self.assertTrue(node_out_str not in hosts, "Partitioned index not created on expected hosts")

    def test_create_partitioned_index_one_node_network_partitioned(self):
        self._load_emp_dataset()

        node_out = self.servers[self.node_out]
        self.start_firewall_on_node(node_out)

        # Create partitioned index
        create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name)"

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_index_statement,
                server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            self.fail("Failed to create index with one node failed")

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata :::")
        self.log.info(index_metadata)

        self.stop_firewall_on_node(node_out)

        hosts = index_metadata["status"][0]["hosts"]
        self.log.info("Actual nodes : {0}".format(hosts))
        node_out_str = node_out.ip + ":" + node_out.port
        self.assertTrue(node_out_str not in hosts,
                        "Partitioned index not created on expected hosts")

    def test_node_fails_during_create_partitioned_index(self):
        self._load_emp_dataset()

        node_out = self.servers[self.node_out]

        # Create partitioned index
        create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name)"

        threads = []
        threads.append(
            Thread(target=self.n1ql_helper.run_cbq_query, name="run_query",
                   args=(create_index_statement, 10, self.n1ql_node)))
        threads.append(
            Thread(target=self.cluster.failover, name="failover", args=(
                self.servers[:self.nodes_init], [node_out], self.graceful, False, 60)))

        for thread in threads:
            thread.start()
        self.sleep(5)
        for thread in threads:
            thread.join()

        self.sleep(30)

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata :::")
        self.log.info(index_metadata)

    def test_node_nw_partitioned_during_create_partitioned_index(self):
        self._load_emp_dataset()

        node_out = self.servers[self.node_out]

        # Create partitioned index
        create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name)"

        threads = []
        threads.append(
            Thread(target=self.start_firewall_on_node,
                   name="network_partitioning", args=(node_out,)))
        threads.append(
            Thread(target=self.n1ql_helper.run_cbq_query, name="run_query",
                   args=(create_index_statement, 10, self.n1ql_node)))

        for thread in threads:
            thread.start()
        self.sleep(5)
        for thread in threads:
            thread.join()

        self.sleep(10)

        try:
            index_metadata = self.rest.get_indexer_metadata()
            self.log.info("Indexer Metadata :::")
            self.log.info(index_metadata)
            if index_metadata != {}:
                hosts = index_metadata["status"][0]["hosts"]
                self.log.info("Actual nodes : {0}".format(hosts))
                node_out_str = node_out.ip + ":" + node_out.port
                self.assertTrue(node_out_str not in hosts,
                        "Partitioned index not created on expected hosts")
            else:
                self.log.info("Cannot retrieve index metadata since one node is down")
        except Exception, ex:
            self.log.info(str(ex))
        finally:
            self.stop_firewall_on_node(node_out)
            self.sleep(30)
            index_metadata = self.rest.get_indexer_metadata()
            self.log.info("Indexer Metadata :::")
            self.log.info(index_metadata)

            hosts = index_metadata["status"][0]["hosts"]
            node_out_str = node_out.ip + ":" + node_out.port
            self.assertTrue(node_out_str in hosts,
                            "Partitioned index not created on all hosts")

    def test_node_nw_partitioned_during_create_partitioned_index_with_node_list(self):
        self._load_emp_dataset()

        node_out = self.servers[self.node_out]
        node_list_str = "[\"" + "\",\"".join(self.node_list) + "\"]"

        # Create partitioned index
        create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'nodes' : {0}}}".format(node_list_str)

        threads = []

        threads.append(
            Thread(target=self.start_firewall_on_node,
                   name="network_partitioning", args=(node_out,)))
        threads.append(
            Thread(target=self.n1ql_helper.run_cbq_query, name="run_query",
                   args=(create_index_statement, 10, self.n1ql_node)))

        for thread in threads:
            thread.start()
        self.sleep(5)
        for thread in threads:
            thread.join()

        self.sleep(10)

        try:
            index_metadata = self.rest.get_indexer_metadata()
            self.log.info("Indexer Metadata :::")
            self.log.info(index_metadata)
            if index_metadata != {}:
                hosts = index_metadata["status"][0]["hosts"]
                self.log.info("Actual nodes : {0}".format(hosts))
                node_out_str = node_out.ip + ":" + node_out.port
                self.assertTrue(node_out_str not in hosts,
                                "Partitioned index not created on expected hosts")
            else:
                self.log.info(
                    "Cannot retrieve index metadata since one node is down")
        except Exception, ex:
            self.log.info(str(ex))
        finally:
            self.stop_firewall_on_node(node_out)
            self.sleep(30)
            index_metadata = self.rest.get_indexer_metadata()
            self.log.info("Indexer Metadata :::")
            self.log.info(index_metadata)

            hosts = index_metadata["status"][0]["hosts"]
            node_out_str = node_out.ip + ":" + node_out.port
            self.assertTrue(node_out_str in hosts,
                            "Partitioned index not created on all hosts")

    def test_build_partitioned_index(self):
        self._load_emp_dataset()

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        if self.num_index_replicas > 0:
            create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(name,dept,salary) partition by hash(name) USING GSI  WITH {{'num_partition': {0}, 'defer_build': true, 'num_replica':{1}}};".format(
            self.num_index_partitions, self.num_index_replicas)
        else:
            create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(name,dept,salary) partition by hash(name) USING GSI  WITH {{'num_partition': {0}, 'defer_build': true}};".format(
                self.num_index_partitions)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            self.fail("index creation failed with error : {0}".format(str(ex)))

        self.sleep(10)
        index_map = self.get_index_map()
        self.log.info(index_map)

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata Before Build:")
        self.log.info(index_metadata)

        index_details = {}
        index_details["index_name"] = index_name_prefix
        index_details["num_partitions"] = self.num_index_partitions
        index_details["defer_build"] = True

        self.assertTrue(self.validate_partitioned_indexes(index_details,index_map,index_metadata),"Deferred Partitioned index created not as expected")

        #Validation for replica indexes
        if self.num_index_replicas > 0:
            for i in range(1,self.num_index_replicas+1):
                index_details["index_name"] = index_name_prefix + " (replica {0})".format(str(i))
                self.assertTrue(
                    self.validate_partitioned_indexes(index_details, index_map,
                                                      index_metadata),
                    "Deferred Partitioned index created not as expected")

        build_index_query = "BUILD INDEX on `default`(" + index_name_prefix + ")"

        try:
            self.n1ql_helper.run_cbq_query(query=build_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            self.fail("index building failed with error : {0}".format(str(ex)))

        self.sleep(30)
        index_map = self.get_index_map()
        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata After Build:")
        self.log.info(index_metadata)

        index_details["index_name"] = index_name_prefix
        index_details["defer_build"] = False

        self.assertTrue(
            self.validate_partitioned_indexes(index_details, index_map,
                                              index_metadata),
            "Deferred Partitioned index created not as expected")
        # Validation for replica indexes
        if self.num_index_replicas > 0:
            for i in range(1, self.num_index_replicas + 1):
                index_details[
                    "index_name"] = index_name_prefix + " (replica {0})".format(
                    str(i))
                self.assertTrue(
                    self.validate_partitioned_indexes(index_details, index_map,
                                                      index_metadata),
                    "Deferred Partitioned index created not as expected")

    def test_build_partitioned_index_one_failed_node(self):
        self._load_emp_dataset()

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        node_list_str = "[\"" + "\",\"".join(self.node_list) + "\"]"
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(name,dept,salary) partition by hash(name) USING GSI  WITH {{'num_partition': {0}, 'nodes': {1}, 'defer_build': true}};".format(
            self.num_index_partitions, node_list_str)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            self.fail("index creation failed with error : {0}".format(str(ex)))

        self.sleep(10)
        index_map = self.get_index_map()
        self.log.info(index_map)

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata Before Build:")
        self.log.info(index_metadata)

        index_details = {}
        index_details["index_name"] = index_name_prefix
        index_details["num_partitions"] = self.num_index_partitions
        index_details["defer_build"] = True

        self.assertTrue(
            self.validate_partitioned_indexes(index_details, index_map,
                                              index_metadata),
            "Deferred Partitioned index created not as expected")

        node_out = self.servers[self.node_out]
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            self.graceful, wait_for_pending=180)

        failover_task.result()

        build_index_query = "BUILD INDEX on `default`(" + index_name_prefix + ")"

        try:
            self.n1ql_helper.run_cbq_query(query=build_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            self.fail("index building failed with error : {0}".format(str(ex)))

        self.sleep(30)
        index_map = self.get_index_map()
        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata After Build:")
        self.log.info(index_metadata)

        index_details["defer_build"] = False

        self.assertTrue(
            self.validate_partitioned_indexes(index_details, index_map,
                                              index_metadata),
            "Deferred Partitioned index created not as expected")

        if self.recover_failed_node:
            nodes_all = self.rest.node_statuses()
            for node in nodes_all:
                if node.ip == node_out.ip:
                    break

            self.rest.set_recovery_type(node.id, self.recovery_type)
            self.rest.add_back_node(node.id)

            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init],
                [], [])
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached,
                            "rebalance failed, stuck or did not complete")
            rebalance.result()
            self.sleep(180)

            index_map = self.get_index_map()
            index_metadata = self.rest.get_indexer_metadata()
            self.log.info("Indexer Metadata After Build:")
            self.log.info(index_metadata)

            index_details["defer_build"] = False

            self.assertTrue(
                self.validate_partitioned_indexes(index_details, index_map,
                                                  index_metadata),
                "Deferred Partitioned index created not as expected")

    def test_failover_during_build_partitioned_index(self):
        self._load_emp_dataset()

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        node_list_str = "[\"" + "\",\"".join(self.node_list) + "\"]"
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(name,dept,salary) partition by hash(name) USING GSI  WITH {{'num_partition': {0}, 'nodes': {1}, 'defer_build': true}};".format(
            self.num_index_partitions, node_list_str)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            self.fail("index creation failed with error : {0}".format(str(ex)))

        self.sleep(10)
        index_map = self.get_index_map()
        self.log.info(index_map)

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata Before Build:")
        self.log.info(index_metadata)

        index_details = {}
        index_details["index_name"] = index_name_prefix
        index_details["num_partitions"] = self.num_index_partitions
        index_details["defer_build"] = True

        self.assertTrue(
            self.validate_partitioned_indexes(index_details, index_map,
                                              index_metadata),
            "Deferred Partitioned index created not as expected")

        node_out = self.servers[self.node_out]
        build_index_query = "BUILD INDEX on `default`(" + index_name_prefix + ")"
        threads = []
        threads.append(
            Thread(target=self.n1ql_helper.run_cbq_query, name="run_query",
                   args=(build_index_query, 10, self.n1ql_node)))
        threads.append(
            Thread(target=self.cluster.async_failover, name="failover", args=(
                self.servers[:self.nodes_init], [node_out], self.graceful)))
        for thread in threads:
            thread.start()
            thread.join()
        self.sleep(30)

        index_map = self.get_index_map()
        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata After Build:")
        self.log.info(index_metadata)

        index_details["defer_build"] = False

        self.assertTrue(
            self.validate_partitioned_indexes(index_details, index_map,
                                              index_metadata),
            "Deferred Partitioned index created not as expected")

    def test_build_partitioned_index_with_network_partitioning(self):
        self._load_emp_dataset()

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        node_list_str = "[\"" + "\",\"".join(self.node_list) + "\"]"
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(name,dept,salary) partition by hash(name) USING GSI  WITH {{'num_partition': {0}, 'nodes': {1}, 'defer_build': true}};".format(
            self.num_index_partitions, node_list_str)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            self.fail("index creation failed with error : {0}".format(str(ex)))

        self.sleep(10)
        index_map = self.get_index_map()
        self.log.info(index_map)

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata Before Build:")
        self.log.info(index_metadata)

        index_details = {}
        index_details["index_name"] = index_name_prefix
        index_details["num_partitions"] = self.num_index_partitions
        index_details["defer_build"] = True

        self.assertTrue(
            self.validate_partitioned_indexes(index_details, index_map,
                                              index_metadata),
            "Deferred Partitioned index created not as expected")

        node_out = self.servers[self.node_out]
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            self.graceful, wait_for_pending=180)

        failover_task.result()

        build_index_query = "BUILD INDEX on `default`(" + index_name_prefix + ")"

        try:
            self.start_firewall_on_node(node_out)
            self.sleep(10)
            self.n1ql_helper.run_cbq_query(query=build_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            self.fail("index building failed with error : {0}".format(str(ex)))

        finally:
            # Heal network partition and wait for some time to allow indexes
            # to get built automatically on that node
            self.stop_firewall_on_node(node_out)
            self.sleep(360)

            index_map = self.get_index_map()
            index_metadata = self.rest.get_indexer_metadata()
            self.log.info("Indexer Metadata After Build:")
            self.log.info(index_metadata)

            index_details["defer_build"] = False

            self.assertTrue(
                self.validate_partitioned_indexes(index_details, index_map,
                                                  index_metadata),
                "Deferred Partitioned index created not as expected")

    def test_drop_partitioned_index(self):
        self._load_emp_dataset()

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))

        with_clause = "WITH {{'num_partition': {0} ".format(self.num_index_partitions)
        if self.num_index_replicas > 0:
            with_clause += ", 'num_replica':{0}".format(self.num_index_replicas)
        if self.defer_build:
            with_clause += ", 'defer_build':True"
        with_clause += " }"

        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(name,dept,salary) partition by hash(name) USING GSI  {0}".format(with_clause)

        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        self.sleep(10)
        index_map = self.get_index_map()
        self.log.info(index_map)

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata Before Build:")
        self.log.info(index_metadata)

        index_details = {}
        index_details["index_name"] = index_name_prefix
        index_details["num_partitions"] = self.num_index_partitions
        index_details["defer_build"] = self.defer_build

        self.assertTrue(
            self.validate_partitioned_indexes(index_details, index_map,
                                              index_metadata),
            "Deferred Partitioned index created not as expected")

        # Validation for replica indexes
        if self.num_index_replicas > 0:
            for i in range(1, self.num_index_replicas + 1):
                index_details[
                    "index_name"] = index_name_prefix + " (replica {0})".format(
                    str(i))
                self.assertTrue(
                    self.validate_partitioned_indexes(index_details,
                                                      index_map,
                                                      index_metadata),
                    "Deferred Partitioned index created not as expected")

        drop_index_query = "DROP INDEX `default`." + index_name_prefix

        try:
            self.n1ql_helper.run_cbq_query(query=drop_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            self.fail(
                    "Drop index failed with error : {0}".format(str(ex)))

        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info("Index map after drop index: %s", index_map)
        if not index_map == {}:
            self.fail("Indexes not dropped correctly")

    def test_drop_partitioned_index_one_failed_node(self):
        self._load_emp_dataset()

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        node_list_str = "[\"" + "\",\"".join(self.node_list) + "\"]"
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(name,dept,salary) partition by hash(name) USING GSI  WITH {{'num_partition': {0}, 'nodes': {1}}};".format(
            self.num_index_partitions, node_list_str)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        self.sleep(10)
        index_map = self.get_index_map()
        self.log.info(index_map)

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata:")
        self.log.info(index_metadata)

        index_details = {}
        index_details["index_name"] = index_name_prefix
        index_details["num_partitions"] = self.num_index_partitions
        index_details["defer_build"] = False

        self.assertTrue(
            self.validate_partitioned_indexes(index_details, index_map,
                                              index_metadata),
            "Partitioned index created not as expected")

        node_out = self.servers[self.node_out]
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            self.graceful, wait_for_pending=180)

        failover_task.result()

        drop_index_query = "DROP INDEX `default`." + index_name_prefix

        try:
            self.n1ql_helper.run_cbq_query(query=drop_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            self.fail(
                "Drop index failed with error : {0}".format(str(ex)))

        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info("Index map after drop index: %s", index_map)
        if not index_map == {}:
            self.fail("Indexes not dropped correctly")

        if self.recover_failed_node:
            nodes_all = self.rest.node_statuses()
            for node in nodes_all:
                if node.ip == node_out.ip:
                    break

            self.rest.set_recovery_type(node.id, self.recovery_type)
            self.rest.add_back_node(node.id)

            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init],
                [], [])
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached,
                            "rebalance failed, stuck or did not complete")
            rebalance.result()
            self.sleep(180)

            index_map = self.get_index_map()
            self.log.info("Index map after drop index: %s", index_map)
            if not index_map == {}:
                self.fail("Indexes not dropped correctly")

    def test_failover_during_drop_partitioned_index(self):
        self._load_emp_dataset()

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        node_list_str = "[\"" + "\",\"".join(self.node_list) + "\"]"
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(name,dept,salary) partition by hash(name) USING GSI  WITH {{'num_partition': {0}, 'nodes': {1}}};".format(
            self.num_index_partitions, node_list_str)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            self.fail("index creation failed with error : {0}".format(
                str(ex)))

        self.sleep(10)
        index_map = self.get_index_map()
        self.log.info(index_map)

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata:")
        self.log.info(index_metadata)

        index_details = {}
        index_details["index_name"] = index_name_prefix
        index_details["num_partitions"] = self.num_index_partitions
        index_details["defer_build"] = False

        self.assertTrue(
            self.validate_partitioned_indexes(index_details, index_map,
                                              index_metadata),
            "Partitioned index created not as expected")

        node_out = self.servers[self.node_out]
        drop_index_query = "DROP INDEX `default`." + index_name_prefix
        threads = []
        threads.append(
            Thread(target=self.n1ql_helper.run_cbq_query,
                   name="run_query",
                   args=(drop_index_query, 10, self.n1ql_node)))
        threads.append(
            Thread(target=self.cluster.async_failover, name="failover",
                   args=(
                       self.servers[:self.nodes_init], [node_out],
                       self.graceful)))
        for thread in threads:
            thread.start()
            thread.join()
        self.sleep(30)

        index_map = self.get_index_map()
        self.log.info("Index map after drop index: %s", index_map)
        if not index_map == {}:
            self.fail("Indexes not dropped correctly")

    def test_drop_partitioned_index_with_network_partitioning(self):
        self._load_emp_dataset()

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        node_list_str = "[\"" + "\",\"".join(self.node_list) + "\"]"
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(name,dept,salary) partition by hash(name) USING GSI  WITH {{'num_partition': {0}, 'nodes': {1}}};".format(
            self.num_index_partitions, node_list_str)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        self.sleep(10)
        index_map = self.get_index_map()
        self.log.info(index_map)

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata Before Build:")
        self.log.info(index_metadata)

        index_details = {}
        index_details["index_name"] = index_name_prefix
        index_details["num_partitions"] = self.num_index_partitions
        index_details["defer_build"] = False

        self.assertTrue(
            self.validate_partitioned_indexes(index_details, index_map,
                                              index_metadata),
            "Partitioned index created not as expected")

        node_out = self.servers[self.node_out]
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            self.graceful, wait_for_pending=180)

        failover_task.result()

        drop_index_query = "DROP INDEX `default`." + index_name_prefix

        try:
            self.start_firewall_on_node(node_out)
            self.sleep(10)
            self.n1ql_helper.run_cbq_query(query=drop_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            self.fail(
                "index drop failed with error : {0}".format(str(ex)))

        finally:
            # Heal network partition and wait for some time to allow indexes
            # to get built automatically on that node
            self.stop_firewall_on_node(node_out)
            self.sleep(360)

            index_map = self.get_index_map()
            self.log.info("Index map after drop index: %s", index_map)
            if not index_map == {}:
                self.fail("Indexes not dropped correctly")

    # Description : Validate index metadata : num_partitions, index status, index existence
    def validate_partitioned_indexes(self, index_details, index_map,
                                     index_metadata):

        isIndexPresent = False
        isNumPartitionsCorrect = False
        isDeferBuildCorrect = False

        # Check if index exists
        for index in index_metadata["status"]:
            if index["name"] == index_details["index_name"]:
                isIndexPresent = True
                # If num-partitions are set, check no. of partitions
                expected_num_partitions = 16
                if index_details["num_partitions"] > 0:
                    expected_num_partitions = index_details["num_partitions"]

                if index["partitioned"] and index[
                    "numPartition"] == expected_num_partitions:
                    isNumPartitionsCorrect = True
                else:
                    self.log.info(
                        "Index {0} on /getIndexStatus : Partitioned={1}, num_partition={2}.. Expected numPartitions={3}".format(
                            index["name"], index["partitioned"],
                            index["numPartition"],
                            index_details["num_partitions"]))

                if index_details["defer_build"] == True and index[
                    "status"] == "Created":
                    isDeferBuildCorrect = True
                elif index_details["defer_build"] == False and index[
                    "status"] == "Ready":
                    isDeferBuildCorrect = True
                else:
                    self.log.info(
                        "Incorrect build status for index created with defer_build=True. Status for {0} is {1}".format(
                            index["name"], index["status"]))

        if not isIndexPresent:
            self.log.info("Index not listed in /getIndexStatus")

        return isIndexPresent and isNumPartitionsCorrect and isDeferBuildCorrect

    # Description : Checks if same host contains same partitions from different replica, and also if for each replica, if the partitions are distributed across nodes
    def validate_partition_map(self, index_metadata, index_name, num_replica,
                               num_partitions):
        index_names = []
        index_names.append(index_name)

        hosts = index_metadata["status"][0]["hosts"]

        for i in range(1, num_replica+1):
            index_names.append(index_name + " (replica {0})".format(str(i)))

        partition_validation_per_host = True
        for host in hosts:
            pmap_host = []
            for idx_name in index_names:
                for index in index_metadata["status"]:
                    if index["name"] == idx_name:
                        pmap_host += index["partitionMap"][host]

            self.log.info("List of partitions on {0} : {1}".format(host, pmap_host))
            if len(set(pmap_host)) != num_partitions:
                partition_validation_per_host &= False
                self.log.info(
                    "Partitions on {0} for all replicas are not correct".format(host))

        partitions_distributed_for_index = True
        for idx_name in index_names:
            for index in index_metadata["status"]:
                if index["name"] == idx_name:
                    totalPartitions = 0
                    for host in hosts:
                        if not index["partitionMap"][host]:
                            partitions_distributed_for_index &= False
                        totalPartitions += len(index["partitionMap"][host])

                    partitions_distributed_for_index &= (totalPartitions == num_partitions)


        return partition_validation_per_host & partitions_distributed_for_index

    # Description : Returns a list of create index statements generated randomly for emp dataset.
    #               The create index statements are generated by randomizing various parts of the statements like list of
    #               index keys, partition keys, primary/secondary indexes, deferred index, partial index, replica index, etc.
    def generate_random_create_index_statements(self, bucketname="default",
                                                idx_node_list=None,
                                                num_statements=1):
        num_idx_nodes = len(idx_node_list)

        emp_fields = {
            'text': ["name", "dept", "languages_known", "email", "meta().id"],
            'number': ["mutated", "salary"],
            'boolean': ["is_manager"],
            'datetime': ["join_date"],
            'object': ["manages"]  # denote nested fields
        }

        emp_nested_fields = {
            'manages': {
                'text': ["reports"],
                'number': ["team_size"]
            }
        }

        index_variations_list = ["num_partitions", "num_replica", "defer_build",
                                 "partial_index", "primary_index", "nodes",
                                 "sizing_estimates"]

        all_emp_fields = ["name", "dept", "languages_known", "email", "mutated",
                          "salary", "is_manager", "join_date", "reports",
                          "team_size"]

        partition_key_type_list = ["leading_key", "trailing_key",
                                   "function_applied_key",
                                   "document_id", "function_applied_doc_id"]

        index_details = []

        for i in range(num_statements):

            random.seed()

            # 1. Generate a random no. of fields to be indexed
            num_index_keys = random.randint(1, len(all_emp_fields) - 1)

            # 2. Generate random fields
            index_fields = []
            for index in range(0, num_index_keys):
                index_field_list_idx = random.randint(0, len(
                    all_emp_fields) - 1)
                if all_emp_fields[
                    index_field_list_idx] not in index_fields:
                    index_fields.append(
                        all_emp_fields[index_field_list_idx])
                else:
                    # Generate a random index again
                    index_field_list_idx = random.randint(0,
                                                          len(
                                                              all_emp_fields) - 1)
                    if all_emp_fields[
                        index_field_list_idx] not in index_fields:
                        index_fields.append(
                            all_emp_fields[index_field_list_idx])

            # 3. Generate a random no. for no. of partition keys (this should be < #1)
            if num_index_keys > 1:
                num_partition_keys = random.randint(1, num_index_keys - 1)
            else:
                num_partition_keys = num_index_keys

            # 4. For each partition key, randomly select a partition key type from the list and generate a partition key with it
            partition_keys = []
            for index in range(num_partition_keys):
                key = None
                partition_key_type = partition_key_type_list[
                    random.randint(0, len(partition_key_type_list) - 1)]

                if partition_key_type == partition_key_type_list[0]:
                    key = index_fields[0]

                if partition_key_type == partition_key_type_list[1]:
                    if len(index_fields) > 1:
                        randval = random.randint(1, len(index_fields) - 1)
                        key = index_fields[randval]

                if partition_key_type == partition_key_type_list[2]:
                    idx_key = index_fields[
                        random.randint(0, len(index_fields) - 1)]

                    if idx_key in emp_fields["text"]:
                        key = ("LOWER({0})".format(idx_key))
                    elif idx_key in emp_fields["number"]:
                        key = ("({0} % 10) + ({0} * 2) ").format(idx_key)
                    elif idx_key in emp_fields["boolean"]:
                        key = ("NOT {0}".format(idx_key))
                    elif idx_key in emp_fields["datetime"]:
                        key = ("DATE_ADD_STR({0},-1,'year')".format(idx_key))
                    elif idx_key in emp_nested_fields["manages"]["text"]:
                        key = ("LOWER({0})".format(idx_key))
                    elif idx_key in emp_nested_fields["manages"]["number"]:
                        key = ("({0} % 10) + ({0} * 2)").format(idx_key)

                if partition_key_type == partition_key_type_list[3]:
                    key = "meta.id"

                if partition_key_type == partition_key_type_list[4]:
                    key = "SUBSTR(meta.id, POSITION(meta.id, '__')+2)"

                if key is not None and key not in partition_keys:
                    partition_keys.append(key)

            # 6. Choose other variation in queries from the list.
            num_index_variations = random.randint(0, len(
                index_variations_list) - 1)
            index_variations = []
            for index in range(num_index_variations):
                index_variation = index_variations_list[
                    random.randint(0, len(index_variations_list) - 1)]
                if index_variation not in index_variations:
                    index_variations.append(index_variation)

            # Primary indexes cannot be partial, so remove partial index if primary index is in the list
            if ("primary_index" in index_variations) and (
                        "partial_index" in index_variations):
                index_variations.remove("partial_index")

            # 7. Build create index queries.
            index_name = "idx" + str(random.randint(0, 1000000))
            if "primary_index" in index_variations:
                create_index_statement = "CREATE PRIMARY INDEX {0} on {1}".format(
                    index_name, bucketname)
            else:
                create_index_statement = "CREATE INDEX {0} on {1}(".format(
                    index_name, bucketname)
                create_index_statement += ",".join(index_fields) + ")"

            create_index_statement += " partition by hash("
            create_index_statement += ",".join(partition_keys) + ")"

            if "partial_index" in index_variations:
                create_index_statement += " where meta().id > 10"

            with_list = ["num_partitions", "num_replica", "defer_build",
                         "nodes", "sizing_estimates"]

            num_partitions = 0
            num_replica = 0
            defer_build = False
            nodes = []
            if (any(x in index_variations for x in with_list)):
                with_statement = []
                create_index_statement += " with {"
                if "num_partitions" in index_variations:
                    num_partitions = random.randint(4, 100)
                    with_statement.append(
                        "'num_partition':{0}".format(num_partitions))
                if "num_replica" in index_variations:
                    # We do not want 'num_replica' and 'nodes' both in the with clause, as it can cause errors if they do not match.
                    if "nodes" in index_variations:
                        index_variations.remove("nodes")

                    num_replica = random.randint(1, num_idx_nodes - 1)
                    with_statement.append(
                        "'num_replica':{0}".format(num_replica))
                if "defer_build" in index_variations:
                    defer_build = True
                    with_statement.append("'defer_build':true")
                if "sizing_estimates" in index_variations:
                    with_statement.append("'secKeySize':20")
                    with_statement.append("'docKeySize':20")
                    with_statement.append("'arrSize':10")
                if "nodes" in index_variations:
                    num_nodes = random.randint(1, num_idx_nodes - 1)
                    for i in range(0, num_nodes):
                        node = idx_node_list[
                            random.randint(0, num_idx_nodes - 1)]
                        if node not in nodes:
                            nodes.append(node)

                    node_list_str = ""
                    if nodes is not None and len(nodes) > 1:
                        node_list_str = "\"" + "\",\"".join(nodes) + "\""
                    else:
                        node_list_str = "\"" + nodes[0] + "\""
                    with_statement.append("'nodes':[{0}]".format(node_list_str))

                create_index_statement += ",".join(with_statement) + "}"

            index_detail = {}
            index_detail["index_name"] = index_name
            index_detail["num_partitions"] = num_partitions
            index_detail["num_replica"] = num_replica
            index_detail["defer_build"] = defer_build
            index_detail["index_definition"] = create_index_statement
            index_detail["nodes"] = nodes

            index_details.append(index_detail)

        return index_details

    def _load_emp_dataset(self):
        # Load Emp Dataset
        self.cluster.bucket_flush(self.master)

        self._kv_gen = JsonDocGenerator("emp_",
                                        encoding="utf-8",
                                        start=0,
                                        end=self.num_items)
        gen = copy.deepcopy(self._kv_gen)

        self._load_bucket(self.buckets[0], self.servers[0], gen, "create", 0)
