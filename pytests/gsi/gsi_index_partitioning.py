import copy
import json
import threading
import time

from .base_gsi import BaseSecondaryIndexingTests
from membase.api.rest_client import RestConnection, RestHelper
import random
from lib import testconstants
from lib.Cb_constants.CBServer import CbServer
from lib.couchbase_helper.tuq_generators import TuqGenerators
from lib.memcached.helper.data_helper import MemcachedClientHelper
from lib.remote.remote_util import RemoteMachineShellConnection
from threading import Thread
from pytests.query_tests_helper import QueryHelperTests
from couchbase_helper.documentgenerator import JsonDocGenerator
from couchbase_helper.cluster import Cluster
from .gsi_replica_indexes import GSIReplicaIndexesTests
from lib.membase.helper.cluster_helper import ClusterOperationHelper


class GSIIndexPartitioningTests(GSIReplicaIndexesTests):
    def setUp(self):
        super(GSIIndexPartitioningTests, self).setUp()
        self.num_items = self.input.param("items", 5000)
        self.log.info("No. of items: {0}".format(str(self.num_items)))
        self.index_servers = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True)
        self.rest = RestConnection(self.index_servers[0])
        self.node_list = []
        for server in self.index_servers:
            self.node_list.append(server.ip + ":" + self.node_port)

        self.num_queries = self.input.param("num_queries", 100)
        self.num_index_partitions = self.input.param("num_index_partitions", 8)
        self.recover_failed_node = self.input.param("recover_failed_node",
                                                    False)
        self.op_type = self.input.param("op_type", "create")
        self.node_operation = self.input.param("node_op", "reboot")
        self.implicit_use_index = self.input.param("implicit_use_index", False)
        self.use_replica_index = self.input.param("use_replica_index", False)
        self.failover_index = self.input.param("failover_index", False)
        self.index_partitioned = self.input.param('index_partitioned', False)

    def tearDown(self):
        super(GSIIndexPartitioningTests, self).tearDown()

    '''Test that checks if hte last_known_scan_time stat is being set properly
        - Test explicitly calling a specific index to see if it is updated
        - Test implicitly calling a specific index to see if it is updated
        - Test if the stat persists after an indexer crash'''
    def test_index_last_query_stat(self):
        index_node = self.get_nodes_from_services_map(service_type="index",
                                                         get_all_nodes=False)
        rest = RestConnection(index_node)
        doc = {"indexer.statsPersistenceInterval": 60}
        rest.set_index_settings_internal(doc)

        shell = RemoteMachineShellConnection(index_node)
        output1, error1 = shell.execute_command("killall -9 indexer")
        self.sleep(30)

        if self.index_partitioned:
            create_index_query = "CREATE INDEX idx on default(age) partition by hash(name) USING GSI"
        else:
            create_index_query = "CREATE INDEX idx ON default(age) USING GSI"
        create_index_query2 = "CREATE INDEX idx2 ON default(name) USING GSI"
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index_query2,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        self.wait_until_indexes_online()

        indexer_nodes = self.get_nodes_from_services_map(service_type="index",
                                                         get_all_nodes=True)
        self.assertTrue(indexer_nodes, "There are no indexer nodes in the cluster!")
        # Ensure last_known_scan_time starts at default value
        for node in indexer_nodes:
            rest = RestConnection(node)
            indexes = rest.get_index_stats()
            self.log.info(indexes)
            self.assertTrue(indexes, "There are no indexes on the node!")
            if 'default' in indexes:
                for index in indexes['default']:
                    self.assertEqual(indexes['default'][index]['last_known_scan_time'], 0)
            else:
                continue

        # Implicitly or Explicitly use the index in question
        if self.implicit_use_index:
            use_index_query = 'select * from default where age > 50'
        else:
            use_index_query = 'select * from default USE INDEX (idx using GSI) where age > 50'

        self.n1ql_helper.run_cbq_query(query=use_index_query, server= self.n1ql_node)

        current_time = int(time.time())
        self.log.info(current_time)

        used_index = 'idx'

        for index_node in indexer_nodes:
            rest = RestConnection(index_node)
            indexes = rest.get_index_stats()
            self.log.info(indexes)
            self.assertTrue(indexes, "There are no indexes on the node!")
            if 'default' in indexes:
                for index in indexes['default']:
                    if index == used_index:
                        self.log.info(int(str(indexes['default'][index]['last_known_scan_time'])[:10]))
                        self.assertTrue(current_time - int(str(indexes['default'][index]['last_known_scan_time'])[:10]) < 60, 'The timestamp is more than a minute off')
                        if self.failover_index:
                            self.sleep(60)
                            shell = RemoteMachineShellConnection(index_node)
                            output1, error1 = shell.execute_command("killall -9 indexer")
                            self.sleep(30)
                            break
                    else:
                        self.assertTrue(indexes['default'][index]['last_known_scan_time'] == 0)
            else:
                continue

        if self.failover_index:
            for index_node in indexer_nodes:
                rest = RestConnection(index_node)
                indexes = rest.get_index_stats()
                self.log.info(indexes)
                self.assertTrue(indexes, "There are no indexes on the node!")
                if 'default' in indexes:
                    for index in indexes['default']:
                        if index == used_index:
                            self.log.info(int(str(indexes['default'][index]['last_known_scan_time'])[:10]))
                            self.assertTrue(
                                current_time - int(str(indexes['default'][index]['last_known_scan_time'])[:10]) < 180,
                                'The timestamp is more than a minute off')
                        else:
                            self.assertTrue(indexes['default'][index]['last_known_scan_time'] == 0)
                else:
                    continue

    '''Same as  the test above for partitioned indexes'''
    def test_index_last_query_stat_partitioned(self):
        create_index_query = "CREATE INDEX idx on default(age) partition by hash(name) USING GSI"
        create_index_query2 = "CREATE INDEX idx2 ON default(name) USING GSI"
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index_query2,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        self.wait_until_indexes_online()

        indexer_nodes = self.get_nodes_from_services_map(service_type="index",
                                                         get_all_nodes=True)
        self.assertTrue(indexer_nodes, "There are no indexer nodes in the cluster!")
        # Ensure last_known_scan_time starts at default value
        for node in indexer_nodes:
            rest = RestConnection(node)
            indexes = rest.get_index_stats()
            self.log.info(indexes)
            self.assertTrue(indexes, "There are no indexes on the node!")
            if 'default' in indexes:
                for index in indexes['default']:
                    self.assertEqual(indexes['default'][index]['last_known_scan_time'], 0)
            else:
                continue

        # Implicitly or Explicitly use the index in question
        if self.implicit_use_index:
            use_index_query = 'select * from default where age > 50'
        else:
            use_index_query = 'select * from default USE INDEX (idx using GSI) where age > 50'

        self.n1ql_helper.run_cbq_query(query=use_index_query, server= self.n1ql_node)

        current_time = int(time.time())
        self.log.info(current_time)

        used_index = 'idx'

        for index_node in indexer_nodes:
            rest = RestConnection(index_node)
            indexes = rest.get_index_stats()
            self.log.info(indexes)
            self.assertTrue(indexes, "There are no indexes on the node!")
            if 'default' in indexes:
                for index in indexes['default']:
                    if index == used_index:
                        self.assertTrue(current_time - int(str(indexes['default'][index]['last_known_scan_time'])[:10]) < 60, 'The timestamp is more than a minute off')
                    else:
                        self.assertTrue(indexes['default'][index]['last_known_scan_time'] == 0)
            else:
                continue

    '''Test that equivalent indexes/replicas are being updated properly, if you specifically use an index any of 
       its equivalent indexes can be used, however both should not be used'''
    def test_index_last_query_stat_equivalent_indexes(self):
        if not self.use_replica_index:
            create_index_query = "CREATE INDEX idx ON default(age) USING GSI"
            create_index_query2 = "CREATE INDEX idx2 ON default(name) USING GSI"
            create_index_query3 = "CREATE INDEX idx3 ON default(age) USING GSI"

            try:
                self.n1ql_helper.run_cbq_query(query=create_index_query,
                                               server=self.n1ql_node)
                self.n1ql_helper.run_cbq_query(query=create_index_query2,
                                               server=self.n1ql_node)
                self.n1ql_helper.run_cbq_query(query=create_index_query3,
                                               server=self.n1ql_node)
            except Exception as ex:
                self.log.info(str(ex))
                self.fail(
                    "index creation failed with error : {0}".format(str(ex)))
        else:
            create_index_query = "CREATE INDEX idx ON default(age) USING GSI  WITH {'num_replica': 1};"
            create_index_query2 = "CREATE INDEX idx2 ON default(name) USING GSI"
            try:
                self.n1ql_helper.run_cbq_query(query=create_index_query,
                                               server=self.n1ql_node)
                self.n1ql_helper.run_cbq_query(query=create_index_query2,
                                               server=self.n1ql_node)
            except Exception as ex:
                self.log.info(str(ex))
                self.fail(
                    "index creation failed with error : {0}".format(str(ex)))


        self.wait_until_indexes_online()

        indexer_nodes = self.get_nodes_from_services_map(service_type="index",
                                                         get_all_nodes=True)
        self.assertTrue(indexer_nodes, "There are no indexer nodes in the cluster!")
        # Ensure last_known_scan_time starts at default value
        for node in indexer_nodes:
            rest = RestConnection(node)
            indexes = rest.get_index_stats()
            self.log.info(indexes)
            self.assertTrue(indexes, "There are no indexes on the node!")
            if 'default' in indexes:
                for index in indexes['default']:
                    self.assertEqual(indexes['default'][index]['last_known_scan_time'], 0)
            else:
                continue

        use_index_query = 'select * from default USE INDEX (idx using GSI) where age > 50'

        self.n1ql_helper.run_cbq_query(query=use_index_query, server= self.n1ql_node)

        current_time = int(time.time())
        self.log.info(current_time)

        check_idx = False
        check_idx3 = False

        for node in indexer_nodes:
            rest = RestConnection(node)
            indexes = rest.get_index_stats()
            self.log.info(indexes)
            self.assertTrue(indexes, "There are no indexes on the node!")
            if 'default' in indexes:
                for index in indexes['default']:
                    if self.use_replica_index:
                        if index == 'idx':
                            if current_time - int(str(indexes['default'][index]['last_known_scan_time'])[:10]) < 60:
                                check_idx = True
                        elif index == 'idx (replica 1)':
                            if current_time - int(str(indexes['default'][index]['last_known_scan_time'])[:10]) < 60:
                                check_idx3 = True
                        else:
                            self.assertTrue(indexes['default'][index]['last_known_scan_time'] == 0)
                    else:
                        if index == 'idx':
                            if current_time - int(str(indexes['default'][index]['last_known_scan_time'])[:10]) < 60:
                                check_idx = True
                        elif index == 'idx3':
                            if current_time - int(str(indexes['default'][index]['last_known_scan_time'])[:10]) < 60:
                                check_idx3 = True
                        else:
                            self.assertTrue(indexes['default'][index]['last_known_scan_time'] == 0)
            else:
                continue

        # One or the other should have been used, not both
        self.assertTrue(check_idx or check_idx3)
        self.assertFalse(check_idx and check_idx3)

    '''Run a query that uses two different indexes at once and make sure both are properly updated'''
    def test_index_last_query_multiple_indexes(self):
        create_index_query = "CREATE INDEX idx ON default(age) USING GSI"
        create_index_query2 = "CREATE INDEX idx2 ON default(name) USING GSI"
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index_query2,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        self.wait_until_indexes_online()

        indexer_nodes = self.get_nodes_from_services_map(service_type="index",
                                                         get_all_nodes=True)
        self.assertTrue(indexer_nodes, "There are no indexer nodes in the cluster!")

        # Ensure last_known_scan_time starts at default value
        for node in indexer_nodes:
            rest = RestConnection(node)
            indexes = rest.get_index_stats()
            self.log.info(indexes)
            self.assertTrue(indexes, "There are no indexes on the node!")
            if 'default' in indexes:
                for index in indexes['default']:
                    self.assertEqual(indexes['default'][index]['last_known_scan_time'], 0)
            else:
                continue

        # Construct a query that uses both created indexes and ensure they both have a last used timestamp
        use_index_query = 'select * from default where age > 50 and name = "Caryssa"'

        self.n1ql_helper.run_cbq_query(query=use_index_query, server= self.n1ql_node)

        current_time = int(time.time())
        self.log.info(current_time)

        # All indexes that were created should be used
        for node in indexer_nodes:
            rest = RestConnection(node)
            indexes = rest.get_index_stats()
            self.log.info(indexes)
            self.assertTrue(indexes, "There are no indexes on the node!")
            if 'default' in indexes:
                for index in indexes['default']:
                        self.assertTrue(current_time - int(str(indexes['default'][index]['last_known_scan_time'])[:10]) < 60, 'The timestamp is more than a minute off')
            else:
                continue
    '''Make sure that two indexes with the same name on two different buckets does not cause an incorrect update of stat'''
    def test_index_last_query_stat_multiple_buckets(self):
        create_index_query = "CREATE INDEX idx ON default(age) USING GSI"
        create_index_query2 = "CREATE INDEX idx ON standard_bucket0(age) USING GSI"
        create_index_query3 = "CREATE INDEX idx2 ON default(name) USING GSI"

        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index_query2,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index_query3,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        self.wait_until_indexes_online()

        indexer_nodes = self.get_nodes_from_services_map(service_type="index",
                                                         get_all_nodes=True)
        self.assertTrue(indexer_nodes, "There are no indexer nodes in the cluster!")
        # Ensure last_known_scan_time starts at default value
        for node in indexer_nodes:
            rest = RestConnection(node)
            indexes = rest.get_index_stats()
            self.log.info(indexes)
            self.assertTrue(indexes, "There are no indexes on the node!")
            self.fail_if_no_buckets()
            for bucket in self.buckets:
                if bucket.name in indexes:
                    for index in indexes[bucket.name]:
                        self.assertEqual(indexes[bucket.name][index]['last_known_scan_time'], 0)
                else:
                    continue

        # Implicitly or Explicitly use the index in question
        if self.implicit_use_index:
            use_index_query = 'select * from default where age > 50'
        else:
            use_index_query = 'select * from default USE INDEX (idx using GSI) where age > 50'

        self.n1ql_helper.run_cbq_query(query=use_index_query, server= self.n1ql_node)

        current_time = int(time.time())
        self.log.info(current_time)

        used_index = 'idx'
        used_bucket = 'default'

        for node in indexer_nodes:
            rest = RestConnection(node)
            indexes = rest.get_index_stats()
            self.log.info(indexes)
            self.assertTrue(indexes, "There are no indexes on the node!")
            self.fail_if_no_buckets()
            for bucket in self.buckets:
                if bucket.name in indexes:
                    for index in indexes[bucket.name]:
                        if index == used_index and used_bucket == bucket.name:
                            self.assertTrue(current_time - int(str(indexes['default'][index]['last_known_scan_time'])[:10]) < 60, 'The timestamp is more than a minute off')
                        else:
                            self.assertTrue(indexes[bucket.name][index]['last_known_scan_time'] == 0)
                else:
                    continue

    # Test that generates n number of create index statements with various permutations and combinations
    # of different clauses used in the create index statement.
    def test_create_partitioned_indexes(self):
        self._load_emp_dataset(end=self.num_items)

        create_index_queries = self.generate_random_create_index_statements(
            bucketname=self.buckets[0].name, idx_node_list=self.node_list,
            num_statements=self.num_queries)

        failed_index_creation = 0
        for create_index_query in create_index_queries:

            try:
                self.n1ql_helper.run_cbq_query(
                    query=create_index_query["index_definition"],
                    server=self.n1ql_node)
            except Exception as ex:
                self.log.info(str(ex))

            self.sleep(10)

            index_metadata = self.rest.get_indexer_metadata()
            index_map = self.get_index_map()

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
                self.log.info("output from /getIndexStatus")
                self.log.info(index_metadata)
                self.log.info("Index Map")
                self.log.info(index_map)

            drop_index_query = "DROP INDEX default.{0}".format(
                create_index_query["index_name"])
            try:
                self.n1ql_helper.run_cbq_query(
                    query=drop_index_query,
                    server=self.n1ql_node)
            except Exception as ex:
                self.log.info(str(ex))

        self.log.info(
            "Total Create Index Statements Run: {0}, Passed : {1}, Failed : {2}".format(
                self.num_queries, self.num_queries - failed_index_creation,
                failed_index_creation))
        self.assertTrue(failed_index_creation == 0,
                        "Some create index statements failed validations. Pls see the test log above for details.")

    def test_partition_index_with_excluded_nodes(self):
        self._load_emp_dataset(end=self.num_items)

        # Setting to exclude a node for planner
        self.rest.set_index_planner_settings("excludeNode=in")
        # Create partitioned index
        create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name)"

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_index_statement,
                server=self.n1ql_node)
        except Exception as ex:
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
                self.assertNotIn(self.node_list[0], index["hosts"],
                                 "Planner did not ignore excluded node during index creation")
                #self.assertEqual(index["hosts"], expected_hosts,
                #                 "Planner did not ignore excluded node during index creation")
                validated = True

        if not validated:
            self.fail("Looks like index was not created.")

    def test_replica_partition_index_with_excluded_nodes(self):
        self._load_emp_dataset(end=self.num_items)

        # Setting to exclude a node for planner
        self.rest.set_index_planner_settings("excludeNode=in")
        # Create partitioned index
        create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'num_replica':{0}}}".format(
            self.num_index_replica)
        try:
            self.n1ql_helper.run_cbq_query(
                query=create_index_statement,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))

        index_names = []
        index_names.append("idx1")
        for i in range(1, self.num_index_replica + 1):
            index_names.append("idx1 (replica {0})".format(str(i)))

        # Need to see if the indexes get created in the first place

        # Validate index created and check the hosts on which partitions are hosted.
        expected_hosts = self.node_list[1:]
        expected_hosts.sort()
        validated = False
        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata :::")
        self.log.info(index_metadata)

        index_validated = 0
        for index_name in index_names:
            for index in index_metadata["status"]:
                if index["name"] == index_name:
                    self.log.info("Expected Hosts : {0}".format(expected_hosts))
                    self.log.info("Actual Hosts   : {0}".format(index["hosts"]))
                    self.assertEqual(index["hosts"], expected_hosts,
                                     "Planner did not ignore excluded node during index creation for {0}".format(
                                         index_name))
                    index_validated += 1

        self.assertEqual(index_validated, (self.num_index_replica + 1),
                         "All index replicas not created")

    def test_partition_index_by_non_indexed_field(self):
        self._load_emp_dataset(end=self.num_items)

        create_index_statement = "CREATE INDEX idx1 on default(name,dept) partition by hash(salary) USING GSI"
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_statement,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("index creation failed with error : {0}".format(str(ex)))

        self.sleep(10)
        index_map = self.get_index_map()
        self.log.info(index_map)

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata Before Build:")
        self.log.info(index_metadata)

        index_details = {}
        index_details["index_name"] = "idx1"
        index_details["num_partitions"] = self.num_index_partitions
        index_details["defer_build"] = False

        self.assertTrue(
            self.validate_partitioned_indexes(index_details, index_map,
                                              index_metadata),
            "Partitioned index created not as expected")

    def test_default_num_partitions(self):
        self._load_emp_dataset(end=self.num_items)

        self.rest.set_index_settings(
            {"indexer.numPartitions": 6})

        create_index_statement = "CREATE INDEX idx1 on default(name,dept) partition by hash(salary) USING GSI"
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_statement,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("index creation failed with error : {0}".format(str(ex)))

        self.sleep(10)
        index_map = self.get_index_map()
        self.log.info(index_map)

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata Before Build:")
        self.log.info(index_metadata)

        index_details = {}
        index_details["index_name"] = "idx1"
        index_details["num_partitions"] = 6
        index_details["defer_build"] = False

        self.assertTrue(
            self.validate_partitioned_indexes(index_details, index_map,
                                              index_metadata),
            "Partitioned index created not as expected")

    def test_change_default_num_partitions_after_create_index(self):
        self._load_emp_dataset(end=self.num_items)

        self.rest.set_index_settings(
            {"indexer.numPartitions": 16})

        create_index_statement = "CREATE INDEX idx1 on default(name,dept) partition by hash(salary) USING GSI"
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_statement,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("index creation failed with error : {0}".format(str(ex)))

        self.sleep(10)
        index_map = self.get_index_map()
        self.log.info(index_map)

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata Before Build:")
        self.log.info(index_metadata)

        index_details = {}
        index_details["index_name"] = "idx1"
        index_details["num_partitions"] = 16
        index_details["defer_build"] = False

        self.assertTrue(
            self.validate_partitioned_indexes(index_details, index_map,
                                              index_metadata),
            "Partitioned index created not as expected")

        self.rest.set_index_settings(
            {"indexer.numPartitions": 32})

        create_index_statement = "CREATE INDEX idx2 on default(namesalary) partition by hash(salary) USING GSI"
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_statement,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("index creation failed with error : {0}".format(str(ex)))

        self.sleep(10)
        index_map = self.get_index_map()
        self.log.info(index_map)

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata Before Build:")
        self.log.info(index_metadata)

        index_details = {}
        index_details["index_name"] = "idx2"
        index_details["num_partitions"] = 32
        index_details["defer_build"] = False

        self.assertTrue(
            self.validate_partitioned_indexes(index_details, index_map,
                                              index_metadata),
            "Partitioned index created not as expected")

        # Validate num_partitions for idx1 doesnt change
        index_details = {}
        index_details["index_name"] = "idx1"
        index_details["num_partitions"] = 16
        index_details["defer_build"] = False

        self.assertTrue(
            self.validate_partitioned_indexes(index_details, index_map,
                                              index_metadata),
            "Num partitions for existing indexes changed after updating default value")

    def test_default_num_partitions_negative(self):
        self._load_emp_dataset(end=self.num_items)

        self.rest.set_index_settings(
            {"indexer.numPartitions": 8})

        numpartition_values_str = ["abc", "2018-03-04 18:02:37"]
        numpartition_values_num = [0, -5, 46.6789]

        for value in numpartition_values_str:

            indexname = "index_" + str(random.randint(1, 100))
            try:
                self.rest.set_index_settings(
                    {"indexer.numPartitions": '{0}'.format(value)})

                create_index_statement = "CREATE INDEX {0} on default(name,dept) partition by hash(salary) USING GSI".format(
                    indexname)

                self.n1ql_helper.run_cbq_query(query=create_index_statement,
                                               server=self.n1ql_node)
            except Exception as ex:
                self.log.info(str(ex))

            self.sleep(10)
            index_map = self.get_index_map()
            self.log.info(index_map)

            index_metadata = self.rest.get_indexer_metadata()
            self.log.info("Indexer Metadata Before Build:")
            self.log.info(index_metadata)

            index_details = {}
            index_details["index_name"] = indexname
            if (not isinstance(value, str)) and int(value) > 0:
                index_details["num_partitions"] = int(value)
            else:
                index_details["num_partitions"] = 8
            index_details["defer_build"] = False

            self.assertTrue(
                self.validate_partitioned_indexes(index_details, index_map,
                                                  index_metadata),
                "Partitioned index created not as expected")

        for value in numpartition_values_num:
            indexname = "index_" + str(random.randint(101, 200))
            try:
                self.rest.set_index_settings(
                    {"indexer.numPartitions": value})

                create_index_statement = "CREATE INDEX {0} on default(name,dept) partition by hash(salary) USING GSI".format(
                    indexname)

                self.n1ql_helper.run_cbq_query(query=create_index_statement,
                                               server=self.n1ql_node)
            except Exception as ex:
                self.log.info(str(ex))

            self.sleep(10)
            index_map = self.get_index_map()
            self.log.info(index_map)

            index_metadata = self.rest.get_indexer_metadata()
            self.log.info("Indexer Metadata Before Build:")
            self.log.info(index_metadata)

            index_details = {}
            index_details["index_name"] = indexname
            if (not isinstance(value, str)) and int(value) > 0:
                index_details["num_partitions"] = int(value)
            else:
                index_details["num_partitions"] = 8
            index_details["defer_build"] = False

            self.assertTrue(
                self.validate_partitioned_indexes(index_details, index_map,
                                                  index_metadata),
                "Partitioned index created not as expected")

    def test_numpartitions_negative(self):
        self._load_emp_dataset(end=self.num_items)

        create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'num_partition':null}}"
        try:
            self.n1ql_helper.run_cbq_query(
                query=create_index_statement,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))

        numpartition_values_str = ["abc", "2018-03-04 18:02:37"]
        for value in numpartition_values_str:
            # Create partitioned index
            create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'num_partition':'{0}'}}".format(
                value)
            try:
                self.n1ql_helper.run_cbq_query(
                    query=create_index_statement,
                    server=self.n1ql_node)
            except Exception as ex:
                self.log.info(str(ex))
            else:
                self.fail(
                    "Index got created with an invalid num_partition value : {0}".format(
                        value))

        numpartition_values_num = [0, -5]
        for value in numpartition_values_num:
            # Create partitioned index
            create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'num_partition':{0}}}".format(
                value)
            try:
                self.n1ql_helper.run_cbq_query(
                    query=create_index_statement,
                    server=self.n1ql_node)
            except Exception as ex:
                self.log.info(str(ex))
            else:
                self.fail(
                    "Index got created with an invalid num_partition value : {0}".format(
                        value))

        create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {'num_partition':47.6789}"
        try:
            self.n1ql_helper.run_cbq_query(
                query=create_index_statement,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "Index did not get created with an double value for num_partition value : 47.6789")
        else:
            self.log.info("Index got created successfully with num_partition being a double value : 47.6789")


    def test_partitioned_index_with_replica(self):
        self._load_emp_dataset(end=self.num_items)

        # Create partitioned index
        create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'num_replica':{0}, 'num_partition':{1}}}".format(
            self.num_index_replica, self.num_index_partitions)

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_index_statement,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata :::")
        self.log.info(index_metadata)

        self.assertTrue(self.validate_partition_map(index_metadata, "idx1",
                                                    self.num_index_replica,
                                                    self.num_index_partitions),
                        "Partition map validation failed")

    def test_partitioned_index_with_replica_with_server_groups(self):
        self._load_emp_dataset(end=self.num_items)
        self._create_server_groups()

        # Create partitioned index
        create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'num_replica':{0}}}".format(
            self.num_index_replica)

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_index_statement,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))

        index_metadata = self.rest.get_indexer_metadata()

        index_hosts_list = []
        for index in index_metadata["status"]:
            index_hosts_list.append(index["hosts"])

        self.log.info("Index Host List : {0}".format(index_hosts_list))

        # Need to change the validation logic here. Between index and its replicas, they should have a full set of partitions in both the server groups.
        # idx11 - .101, .102: 3, 4, 5, 10, 11, 15, 16
        # idx11 - .103, .104: 1, 2, 6, 7, 8, 9, 12, 13, 14

        # idx12 - .101, .102: 1, 2, 6, 7, 8, 9, 12, 13, 14
        # idx12 - .103, .104: 3, 4, 5, 10, 11, 15, 16

        validation = True
        for i in range(0, len(index_hosts_list)):
            for j in range(i + 1, len(index_hosts_list)):
                if (index_hosts_list[i].sort() != index_hosts_list[j].sort()):
                    continue
                else:
                    validation &= False

        self.assertTrue(validation,
                        "Partitions of replica indexes do not honour server grouping")

    def test_create_partitioned_index_one_node_already_down(self):
        self._load_emp_dataset(end=self.num_items)

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
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("Failed to create index with one node failed")

        if node_out == self.index_servers[0]:
            rest = RestConnection(self.index_servers[1])
        else:
            rest = self.rest

        index_metadata = rest.get_indexer_metadata()
        self.log.info("Indexer Metadata :::")
        self.log.info(index_metadata)

        hosts = index_metadata["status"][0]["hosts"]
        self.log.info("Actual nodes : {0}".format(hosts))
        node_out_str = node_out.ip + ":" + self.node_port
        self.assertTrue(node_out_str not in hosts,
                        "Partitioned index not created on expected hosts")

    def test_create_partitioned_index_one_node_network_partitioned(self):
        self._load_emp_dataset(end=self.num_items)

        node_out = self.servers[self.node_out]
        self.start_firewall_on_node(node_out)
        self.sleep(10)

        # Create partitioned index
        create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name)"

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_index_statement,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("Failed to create index with one node failed")
        finally:
            # Heal network partition and wait for some time to allow indexes
            # to get built automatically on that node
            self.stop_firewall_on_node(node_out)
            self.sleep(120)

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata :::")
        self.log.info(index_metadata)

        hosts = index_metadata["status"][0]["hosts"]
        self.log.info("Actual nodes : {0}".format(hosts))
        node_out_str = node_out.ip + ":" + self.node_port
        self.assertTrue(node_out_str not in hosts,
                        "Partitioned index not created on expected hosts")

    def test_node_fails_during_create_partitioned_index(self):
        self._load_emp_dataset(end=self.num_items)

        node_out = self.servers[self.node_out]

        # Create partitioned index
        create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name)"

        threads = []
        threads.append(
            Thread(target=self.n1ql_helper.run_cbq_query, name="run_query",
                   args=(create_index_statement, 10, self.n1ql_node)))
        threads.append(
            Thread(target=self.cluster.failover, name="failover", args=(
                self.servers[:self.nodes_init], [node_out], self.graceful,
                False, 60)))

        for thread in threads:
            thread.start()
        self.sleep(5)
        for thread in threads:
            thread.join()

        self.sleep(30)

        if node_out == self.index_servers[0]:
            rest = RestConnection(self.index_servers[1])
        else:
            rest = self.rest

        index_metadata = rest.get_indexer_metadata()
        self.log.info("Indexer Metadata :::")
        self.log.info(index_metadata)

    def test_node_nw_partitioned_during_create_partitioned_index(self):
        self._load_emp_dataset(end=self.num_items)

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
                node_out_str = node_out.ip + ":" + self.node_port
                self.assertTrue(node_out_str not in hosts,
                                "Partitioned index not created on expected hosts")
            else:
                self.log.info(
                    "Cannot retrieve index metadata since one node is down")
        except Exception as ex:
            self.log.info(str(ex))
        finally:
            self.stop_firewall_on_node(node_out)
            self.sleep(30)
            index_metadata = self.rest.get_indexer_metadata()
            self.log.info("Indexer Metadata :::")
            self.log.info(index_metadata)

            hosts = index_metadata["status"][0]["hosts"]
            node_out_str = node_out.ip + ":" + self.node_port
            self.assertTrue(node_out_str in hosts,
                            "Partitioned index not created on all hosts")

    def test_node_nw_partitioned_during_create_partitioned_index_with_node_list(
            self):
        self._load_emp_dataset(end=self.num_items)

        node_out = self.servers[self.node_out]
        node_list_str = "[\"" + "\",\"".join(self.node_list) + "\"]"

        # Create partitioned index
        create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'nodes' : {0}}}".format(
            node_list_str)

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
                node_out_str = node_out.ip + ":" + self.node_port
                self.assertTrue(node_out_str not in hosts,
                                "Partitioned index not created on expected hosts")
            else:
                self.log.info(
                    "Cannot retrieve index metadata since one node is down")
        except Exception as ex:
            self.log.info(str(ex))
        finally:
            self.stop_firewall_on_node(node_out)
            self.sleep(30)
            index_metadata = self.rest.get_indexer_metadata()
            self.log.info("Indexer Metadata :::")
            self.log.info(index_metadata)

            hosts = index_metadata["status"][0]["hosts"]
            node_out_str = node_out.ip + ":" + self.node_port
            self.assertTrue(node_out_str in hosts,
                            "Partitioned index not created on all hosts")

    def test_build_partitioned_index(self):
        self._load_emp_dataset(end=self.num_items)

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        if self.num_index_replica > 0:
            create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(name,dept,salary) partition by hash(name) USING GSI  WITH {{'num_partition': {0}, 'defer_build': true, 'num_replica':{1}}};".format(
                self.num_index_partitions, self.num_index_replica)
        else:
            create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(name,dept,salary) partition by hash(name) USING GSI  WITH {{'num_partition': {0}, 'defer_build': true}};".format(
                self.num_index_partitions)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
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

        # Validation for replica indexes
        if self.num_index_replica > 0:
            for i in range(1, self.num_index_replica + 1):
                index_details[
                    "index_name"] = index_name_prefix + " (replica {0})".format(
                    str(i))
                self.assertTrue(
                    self.validate_partitioned_indexes(index_details, index_map,
                                                      index_metadata),
                    "Deferred Partitioned index created not as expected")

        build_index_query = "BUILD INDEX on `default`(" + index_name_prefix + ")"

        try:
            self.n1ql_helper.run_cbq_query(query=build_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
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
        if self.num_index_replica > 0:
            for i in range(1, self.num_index_replica + 1):
                index_details[
                    "index_name"] = index_name_prefix + " (replica {0})".format(
                    str(i))
                self.assertTrue(
                    self.validate_partitioned_indexes(index_details, index_map,
                                                      index_metadata),
                    "Deferred Partitioned index created not as expected")

    def test_build_partitioned_index_one_failed_node(self):
        self._load_emp_dataset(end=self.num_items)

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        node_list_str = "[\"" + "\",\"".join(self.node_list) + "\"]"
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(name,dept,salary) partition by hash(name) USING GSI  WITH {{'num_partition': {0}, 'nodes': {1}, 'defer_build': true}};".format(
            self.num_index_partitions, node_list_str)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
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
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("index building failed with error : {0}".format(str(ex)))

        self.sleep(30)
        index_map = self.get_index_map()
        if node_out == self.index_servers[0]:
            rest = RestConnection(self.index_servers[1])
        else:
            rest = self.rest
        index_metadata = rest.get_indexer_metadata()
        self.log.info("Indexer Metadata After Build:")
        self.log.info(index_metadata)

        index_details["defer_build"] = False

        # At this point, since one node is in a failed state, all partitions would not be built.
        self.assertTrue(
            self.validate_partitioned_indexes(index_details, index_map,
                                              index_metadata, skip_numpartitions_check=True),
            "Deferred Partitioned index created not as expected")

        # Recover the failed node and check if after recovery, all partitions are built.
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
        self._load_emp_dataset(end=self.num_items)

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        node_list_str = "[\"" + "\",\"".join(self.node_list) + "\"]"
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(name,dept,salary) partition by hash(name) USING GSI  WITH {{'num_partition': {0}, 'nodes': {1}, 'defer_build': true}};".format(
            self.num_index_partitions, node_list_str)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
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
        self.log.info(f"index_details is {index_details}. Index map is {index_map}. Index metadata is {index_metadata}")
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
        if node_out == self.index_servers[0]:
            rest = RestConnection(self.index_servers[1])
        else:
            rest = self.rest
        index_metadata = rest.get_indexer_metadata()
        self.log.info("Indexer Metadata After Build:")
        self.log.info(index_metadata)

        index_details["defer_build"] = False

        # At this point, since one node is in a failed state, all partitions would not be built.
        self.assertTrue(
            self.validate_partitioned_indexes(index_details, index_map,
                                              index_metadata, skip_numpartitions_check=True),
            "Deferred Partitioned index created not as expected")

    def test_build_partitioned_index_with_network_partitioning(self):
        self._load_emp_dataset(end=self.num_items)

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        node_list_str = "[\"" + "\",\"".join(self.node_list) + "\"]"
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(name,dept,salary) partition by hash(name) USING GSI  WITH {{'num_partition': {0}, 'nodes': {1}, 'defer_build': true}};".format(
            self.num_index_partitions, node_list_str)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
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

        try:
            self.start_firewall_on_node(node_out)
            self.sleep(10)
            self.n1ql_helper.run_cbq_query(query=build_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if not ("Index build will be retried in background" in str(ex) or "Terminate Request during cleanup" in str(ex)):
                self.fail("index building failed with error : {0}".format(str(ex)))
            else:
                self.log.info("Index build failed with expected error")

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
        self._load_emp_dataset(end=self.num_items)

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))

        with_clause = "WITH {{'num_partition': {0} ".format(
            self.num_index_partitions)
        if self.num_index_replica > 0:
            with_clause += ", 'num_replica':{0}".format(self.num_index_replica)
        if self.defer_build:
            with_clause += ", 'defer_build':True"
        with_clause += " }"

        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(name,dept,salary) partition by hash(name) USING GSI  {0}".format(
            with_clause)

        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
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
        if self.num_index_replica > 0:
            for i in range(1, self.num_index_replica + 1):
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
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "Drop index failed with error : {0}".format(str(ex)))

        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info("Index map after drop index: %s", index_map)
        if not index_map == {}:
            self.fail("Indexes not dropped correctly")

    def test_delete_bucket_cascade_drop_partitioned_index(self):
        self._load_emp_dataset(end=self.num_items)

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))

        with_clause = "WITH {{'num_partition': {0} ".format(
            self.num_index_partitions)
        if self.num_index_replica > 0:
            with_clause += ", 'num_replica':{0}".format(self.num_index_replica)
        if self.defer_build:
            with_clause += ", 'defer_build':True"
        with_clause += " }"

        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(name,dept,salary) partition by hash(name) USING GSI  {0}".format(
            with_clause)
        create_primary_index_statement = "CREATE PRIMARY INDEX pidx1 on default partition by hash(meta().id) with {{'num_partition':{0}}}".format(
                self.num_index_partitions)

        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_primary_index_statement,
                                           server=self.n1ql_node)
        except Exception as ex:
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
        if self.num_index_replica > 0:
            for i in range(1, self.num_index_replica + 1):
                index_details[
                    "index_name"] = index_name_prefix + " (replica {0})".format(
                    str(i))
                self.assertTrue(
                    self.validate_partitioned_indexes(index_details,
                                                      index_map,
                                                      index_metadata),
                    "Deferred Partitioned index created not as expected")

        self.cluster.bucket_delete(server=self.master, bucket='default')

        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info("Index map after drop index: %s", index_map)
        if not index_map == {}:
            self.fail("Indexes not dropped correctly")

    def test_drop_partitioned_index_one_failed_node(self):
        self._load_emp_dataset(end=self.num_items)

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        node_list_str = "[\"" + "\",\"".join(self.node_list) + "\"]"
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(name,dept,salary) partition by hash(name) USING GSI  WITH {{'num_partition': {0}, 'nodes': {1}}};".format(
            self.num_index_partitions, node_list_str)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
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
        except Exception as ex:
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
        self._load_emp_dataset(end=self.num_items)

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        node_list_str = "[\"" + "\",\"".join(self.node_list) + "\"]"
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(name,dept,salary) partition by hash(name) USING GSI  WITH {{'num_partition': {0}, 'nodes': {1}}};".format(
            self.num_index_partitions, node_list_str)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
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
        self._load_emp_dataset(end=self.num_items)

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        node_list_str = "[\"" + "\",\"".join(self.node_list) + "\"]"
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(name,dept,salary) partition by hash(name) USING GSI  WITH {{'num_partition': {0}, 'nodes': {1}}};".format(
            self.num_index_partitions, node_list_str)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
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
        self.start_firewall_on_node(node_out)

        drop_index_query = "DROP INDEX `default`." + index_name_prefix

        try:
            self.start_firewall_on_node(node_out)
            self.sleep(10)
            self.n1ql_helper.run_cbq_query(query=drop_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if not "the operation will automatically retry after cluster is back to normal" in str(ex):
                self.fail(
                    "index drop failed with error : {0}".format(str(ex)))
            else:
                self.log.info("Index drop failed with expected error")

        finally:
            # Heal network partition and wait for some time to allow indexes
            # to get built automatically on that node
            self.stop_firewall_on_node(node_out)
            self.sleep(360)

            index_map = self.get_index_map()
            self.log.info("Index map after drop index: %s", index_map)
            if not index_map == {}:
                self.fail("Indexes not dropped correctly")

    def test_partitioned_index_warmup_behaviour(self):
        node_out = self.servers[self.node_out]

        self._load_emp_dataset(end=self.num_items)

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))

        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(name,dept,salary) partition by hash(name) USING GSI"
        if self.defer_build:
            create_index_query += " WITH {'defer_build':true}"

        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        self.sleep(10)
        index_map = self.get_index_map()
        self.log.info(index_map)

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata Before:")
        self.log.info(index_metadata)

        index_details = {}
        index_details["index_name"] = index_name_prefix
        index_details["num_partitions"] = self.num_index_partitions
        index_details["defer_build"] = self.defer_build

        self.assertTrue(
            self.validate_partitioned_indexes(index_details, index_map,
                                              index_metadata),
            "Partitioned index created not as expected")

        remote_client = RemoteMachineShellConnection(node_out)
        if self.node_operation == "kill_indexer":
            remote_client.terminate_process(process_name="indexer")
            remote_client.disconnect()
        else:
            self.reboot_node(node_out)

        # wait for restart and warmup on all node
        self.sleep(self.wait_timeout * 3)
        # disable firewall on these nodes
        self.stop_firewall_on_node(node_out)
        # wait till node is ready after warmup
        ClusterOperationHelper.wait_for_ns_servers_or_assert([node_out], self,
                                                             wait_if_warmup=True)

        index_map = self.get_index_map()
        self.log.info(index_map)

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata After:")
        self.log.info(index_metadata)

        self.assertTrue(
            self.validate_partitioned_indexes(index_details, index_map,
                                              index_metadata),
            "Partitioned index warmup behavior not as expected")

    def test_mutations_on_partitioned_indexes(self):
        self.run_async_index_operations(operation_type="create_index")
        self.run_doc_ops()
        self.sleep(30)

        # Get item counts
        bucket_item_count, total_item_count, total_num_docs_processed = self.get_stats_for_partitioned_indexes()

        self.assertEqual(bucket_item_count, total_item_count,
                         "# Items indexed {0} do not match bucket items {1}".format(
                             total_item_count, bucket_item_count))

    def test_update_mutations_on_indexed_keys_partitioned_indexes(self):
        create_index_query = "CREATE INDEX idx1 ON default(name,mutated) partition by hash(name) USING GSI;"
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))
        self.run_doc_ops()
        self.sleep(30)

        # Get item counts
        bucket_item_count, total_item_count, total_num_docs_processed = self.get_stats_for_partitioned_indexes(
            index_name="idx1")

        self.assertEqual(bucket_item_count, total_item_count,
                         "# Items indexed {0} do not match bucket items {1}".format(
                             total_item_count, bucket_item_count))

    def test_kv_full_rollback_on_partitioned_indexes(self):
        self.run_async_index_operations(operation_type="create_index")
        self.sleep(30)

        self.cluster.bucket_flush(self.master)
        self.sleep(60)

        # Get item counts
        bucket_item_count, total_item_count, total_num_docs_processed = self.get_stats_for_partitioned_indexes()

        self.assertEqual(total_item_count, 0, "Rollback to zero fails")

    def test_kv_partial_rollback_on_partitioned_indexes(self):
        self.run_async_index_operations(operation_type="create_index")

        # Stop Persistence on Node A & Node B
        self.log.info("Stopping persistence on NodeA & NodeB")
        mem_client = MemcachedClientHelper.direct_client(self.servers[0],
                                                         "default")
        mem_client.stop_persistence()
        mem_client = MemcachedClientHelper.direct_client(self.servers[1],
                                                         "default")
        mem_client.stop_persistence()

        self.run_doc_ops()

        self.sleep(10)

        # Get count before rollback
        bucket_count_before_rollback, item_count_before_rollback, num_docs_processed_before_rollback = self.get_stats_for_partitioned_indexes()

        # Kill memcached on Node A so that Node B becomes master
        self.log.info("Kill Memcached process on NodeA")
        shell = RemoteMachineShellConnection(self.master)
        shell.kill_memcached()

        # Start persistence on Node B
        self.log.info("Starting persistence on NodeB")
        mem_client = MemcachedClientHelper.direct_client(
            self.input.servers[1], "default")
        mem_client.start_persistence()

        # Failover Node B
        self.log.info("Failing over NodeB")
        self.sleep(10)
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init], [self.servers[1]], self.graceful,
            wait_for_pending=120)

        failover_task.result()

        # Wait for a couple of mins to allow rollback to complete
        self.sleep(120)

        # Get count after rollback
        bucket_count_after_rollback, item_count_after_rollback, num_docs_processed_after_rollback = self.get_stats_for_partitioned_indexes()

        self.assertEqual(bucket_count_after_rollback, item_count_after_rollback,
                         "Partial KV Rollback not processed by Partitioned indexes")

    def test_scan_availability(self):
        create_index_query = "CREATE INDEX idx1 ON default(name,mutated) partition by hash(BASE64(meta().id)) USING GSI"
        if self.num_index_replica:
            create_index_query += " with {{'num_replica':{0}}};".format(
                self.num_index_replica)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        node_out = self.servers[self.node_out]
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            self.graceful, wait_for_pending=60)

        failover_task.result()

        self.sleep(30)

        # Run query
        scan_query = "select name,mutated from default where name > 'a' and mutated >=0;"
        try:
            self.n1ql_helper.run_cbq_query(query=scan_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.num_index_replica == 0:
                if self.expected_err_msg in str(ex):
                    pass
                else:
                    self.fail(
                        "Scan failed with unexpected error message".format(
                            str(ex)))
            else:
                self.fail("Scan failed")

    def test_scan_availability_with_network_partitioning(self):
        create_index_query = "CREATE INDEX idx1 ON default(name,mutated) partition by hash(BASE64(meta().id)) USING GSI"
        if self.num_index_replica:
            create_index_query += " with {{'num_replica':{0}}};".format(
                self.num_index_replica)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        # Induce network partitioning on one of the nodes
        node_out = self.servers[self.node_out]
        self.start_firewall_on_node(node_out)

        # Run query
        scan_query = "select name,mutated from default where name > 'a' and mutated >=0;"
        try:
            self.n1ql_helper.run_cbq_query(query=scan_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(
                "Scan failed as one indexer node was experiencing network partititioning. Error : %s",
                str(ex))

        # Heal Network Partitioning
        self.stop_firewall_on_node(node_out)

        # Re-run query
        scan_query = "select name,mutated from default where name > 'a' and mutated >=0;"
        try:
            self.n1ql_helper.run_cbq_query(query=scan_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.num_index_replica:
                if self.expected_err_msg in str(ex):
                    pass
                else:
                    self.fail(
                        "Scan failed with unexpected error message".format(
                            str(ex)))
            else:
                self.fail("Scan failed")

    def test_index_scans(self):
        self._load_emp_dataset(end=self.num_items)

        # Create Partitioned and non-partitioned indexes

        if self.num_index_partitions > 0:
            self.rest.set_index_settings(
                {"indexer.numPartitions": self.num_index_partitions})

        create_partitioned_index1_query = "CREATE INDEX partitioned_idx1 ON default(name,dept,salary) partition by hash(name,dept,salary) USING GSI;"
        create_index1_query = "CREATE INDEX non_partitioned_idx1 ON default(name,dept,salary) USING GSI;"
        create_partitioned_index2_query = "create index partitioned_idx2 on default(name,manages.team_size) partition by hash(manages.team_size) USING GSI;"
        create_index2_query = "create index non_partitioned_idx2 on default(name,manages.team_size) USING GSI;"
        create_partitioned_index3_query = "create index partitioned_idx3 on default(name,manages.team_size) partition by hash(name,manages.team_size) USING GSI;"

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_partitioned_index1_query,
                server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index1_query,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(
                query=create_partitioned_index2_query,
                server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index2_query,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(
                query=create_partitioned_index3_query,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        # Scans

        queries = []

        # 1. Small lookup query with equality predicate on the partition key
        query_details = {}
        query_details[
            "query"] = "select name,dept,salary from default USE INDEX (indexname USING GSI) where name='Safiya Palmer'"
        query_details["partitioned_idx_name"] = "partitioned_idx1"
        query_details["non_partitioned_idx_name"] = "non_partitioned_idx1"
        queries.append(query_details)

        # 2. Pagination query with equality predicate on the partition key
        query_details = {}
        query_details[
            "query"] = "select name,dept,salary from default USE INDEX (indexname USING GSI) where name is not missing AND dept='HR' offset 0 limit 10"
        query_details["partitioned_idx_name"] = "partitioned_idx1"
        query_details["non_partitioned_idx_name"] = "non_partitioned_idx1"
        queries.append(query_details)

        # 3. Large aggregated query
        query_details = {}
        query_details[
            "query"] = "select count(name), dept from default USE INDEX (indexname USING GSI) where name is not missing group by dept"
        query_details["partitioned_idx_name"] = "partitioned_idx1"
        query_details["non_partitioned_idx_name"] = "non_partitioned_idx1"
        queries.append(query_details)

        # 4. Scan with large result sets
        query_details = {}
        query_details[
            "query"] = "select name,dept,salary from default USE INDEX (indexname USING GSI) where name is not missing AND salary > 10000"
        query_details["partitioned_idx_name"] = "partitioned_idx1"
        query_details["non_partitioned_idx_name"] = "non_partitioned_idx1"
        queries.append(query_details)

        # 5. Scan that does not require sorted data
        query_details = {}
        query_details[
            "query"] = "select name,dept,salary from default USE INDEX (indexname USING GSI) where name is not missing AND salary > 100000"
        query_details["partitioned_idx_name"] = "partitioned_idx1"
        query_details["non_partitioned_idx_name"] = "non_partitioned_idx1"
        queries.append(query_details)

        # 6. Scan that requires sorted data
        query_details = {}
        query_details[
            "query"] = "select name,dept,salary from default USE INDEX (indexname USING GSI) where name is not missing AND salary > 10000 order by dept asc,salary desc"
        query_details["partitioned_idx_name"] = "partitioned_idx1"
        query_details["non_partitioned_idx_name"] = "non_partitioned_idx1"
        queries.append(query_details)

        # 7. Scan with predicate on a dataset that has some values for the partition key missing, and present for some
        query_details = {}
        query_details[
            "query"] = "select name from default USE INDEX (indexname USING GSI) where name is not missing AND manages.team_size > 3"
        query_details["partitioned_idx_name"] = "partitioned_idx2"
        query_details["non_partitioned_idx_name"] = "non_partitioned_idx2"
        queries.append(query_details)

        # 8. Index partitioned on multiple keys. Scan with predicate on multiple keys with a dataset that has some values for the partition keys missing, and present for some
        query_details = {}
        query_details[
            "query"] = "select name from default USE INDEX (indexname USING GSI) where manages.team_size >= 3 and manages.team_size <= 7 and name like 'A%'"
        query_details["partitioned_idx_name"] = "partitioned_idx3"
        query_details["non_partitioned_idx_name"] = "non_partitioned_idx2"
        queries.append(query_details)

        # 9. Overlap scans on partition keys
        query_details = {}
        query_details[
            "query"] = "select name from default USE INDEX (indexname USING GSI) where name is not missing AND (manages.team_size >= 3 or manages.team_size >= 7)"
        query_details["partitioned_idx_name"] = "partitioned_idx2"
        query_details["non_partitioned_idx_name"] = "non_partitioned_idx2"
        queries.append(query_details)

        total_scans = 0
        failures = 0
        for query_details in queries:
            total_scans += 1

            try:
                query_partitioned_index = query_details["query"].replace(
                    "indexname", query_details["partitioned_idx_name"])
                query_non_partitioned_index = query_details["query"].replace(
                    "indexname", query_details["non_partitioned_idx_name"])

                result_partitioned_index = \
                    self.n1ql_helper.run_cbq_query(
                        query=query_partitioned_index,
                        min_output_size=10000000,
                        server=self.n1ql_node)["results"]
                result_non_partitioned_index = self.n1ql_helper.run_cbq_query(
                    query=query_non_partitioned_index, min_output_size=10000000,
                    server=self.n1ql_node)["results"]

                self.log.info("Partitioned : {0}".format(
                    str(result_partitioned_index.sort())))
                self.log.info("Non Partitioned : {0}".format(
                    str(result_non_partitioned_index.sort())))

                if result_partitioned_index.sort() != result_non_partitioned_index.sort():
                    failures += 1
                    self.log.info(
                        "*** This query does not return same results for partitioned and non-partitioned indexes.")
            except Exception as ex:
                self.log.info(str(ex))

        self.log.info(
            "Total scans : {0}, Matching results : {1}, Non-matching results : {2}".format(
                total_scans, total_scans - failures, failures))
        self.assertEqual(failures, 0,
                         "Some scans did not yield the same results for partitioned index and non-partitioned indexes. Details above.")

    def test_load_balancing_amongst_partitioned_index_replicas(self):
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) partition by hash (meta().id) USING GSI  WITH {{'num_replica': {0},'num_partition':{1}}};".format(
            self.num_index_replica, self.num_index_partitions)
        select_query = "SELECT count(age) from default"
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
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

        self.assertTrue(self.validate_partition_map(index_metadata, index_name_prefix,
                                                    self.num_index_replica,
                                                    self.num_index_partitions),
                        "Partition map validation failed")

        # Run select query 100 times
        for i in range(0, 100):
            self.n1ql_helper.run_cbq_query(query=select_query,
                                           server=self.n1ql_node)

        index_stats = self.get_index_stats(perNode=True)
        load_balanced = True
        for i in range(0, self.num_index_replica + 1):
            if i == 0:
                index_name = index_name_prefix
            else:
                index_name = index_name_prefix + " (replica {0})".format(str(i))

            hosts, _ = self.n1ql_helper.get_index_details_using_index_name(
                index_name, index_map)
            for hostname in hosts:
                num_request_served = index_stats[hostname]['default'][index_name][
                    "num_completed_requests"]
                self.log.info("# Requests served by %s on %s = %s" % (
                    index_name, hostname, num_request_served))
                if num_request_served == 0:
                    load_balanced = False

        if not load_balanced:
            self.fail("Load is not balanced amongst index replicas")

    def test_indexer_pushdowns_multiscan(self):
        self._load_emp_dataset(end=self.num_items)

        # Create Partitioned indexes
        create_partitioned_index_query = "CREATE INDEX partitioned_idx1 ON default(name,dept,salary) partition by hash(meta().id) USING GSI with {{'num_partition':{0}}};".format(
            self.num_index_partitions)

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_partitioned_index_query,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        explain_query1 = "EXPLAIN select name from default where name is not missing and dept='HR' and salary > 120000 and salary < 150000"
        results = self.n1ql_helper.run_cbq_query(query=explain_query1,
                                                 server=self.n1ql_node)

        self.log.info("Explain plan for query 1 : {0}".format(results))

        span_pushdown, _, _, _, _ = self.validate_query_plan(
            plan=results["results"][0]["plan"], index_name="partitioned_idx1",
            num_spans=3)

        self.assertTrue(span_pushdown, "Operators not pushed down to indexer")

        explain_query2 = "EXPLAIN select name from default where name is not missing and dept='HR' and salary BETWEEN 120000 and 150000"
        results = self.n1ql_helper.run_cbq_query(query=explain_query2,
                                                 server=self.n1ql_node)

        self.log.info("Explain plan for query 2 : {0}".format(results))

        span_pushdown, _, _, _, _ = self.validate_query_plan(
            plan=results["results"][0]["plan"], index_name="partitioned_idx1",
            num_spans=3)

        self.assertTrue(span_pushdown, "Operators not pushed down to indexer")

        explain_query3 = "EXPLAIN select name from default where name is not missing and dept='HR' and (salary > 120000 or salary > 180000)"
        results = self.n1ql_helper.run_cbq_query(query=explain_query3,
                                                 server=self.n1ql_node)

        self.log.info("Explain plan for query 3 : {0}".format(results))

        span_pushdown, _, _, _, _ = self.validate_query_plan(
            plan=results["results"][0]["plan"], index_name="partitioned_idx1",
            num_spans=3)

        self.assertTrue(span_pushdown, "Operators not pushed down to indexer")

    def test_indexer_pushdowns_offset_limit(self):
        self._load_emp_dataset(end=self.num_items)

        # Create Partitioned indexes
        create_partitioned_index_query = "CREATE INDEX partitioned_idx1 ON default(name,dept,salary) partition by hash(meta().id) USING GSI with {{'num_partition':{0}}};".format(
            self.num_index_partitions)

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_partitioned_index_query,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        explain_query1 = "EXPLAIN select name from default where name is not missing and dept='HR' and salary > 120000 and salary < 150000 OFFSET 10 LIMIT 10"
        results = self.n1ql_helper.run_cbq_query(query=explain_query1,
                                                 server=self.n1ql_node)

        self.log.info("Explain plan for query 1 : {0}".format(results))

        _, limit_pushdown, offset_pushdown, _, _ = self.validate_query_plan(
            plan=results["results"][0]["plan"], index_name="partitioned_idx1",
            offset=10, limit=10)

        self.assertTrue(limit_pushdown & offset_pushdown,
                        "Operators not pushed down to indexer")

    def test_indexer_pushdowns_projection(self):
        self._load_emp_dataset(end=self.num_items)

        # Create Partitioned indexes
        create_partitioned_index_query = "CREATE INDEX partitioned_idx1 ON default(name,dept,salary) partition by hash(meta().id) USING GSI with {{'num_partition':{0}}};".format(
            self.num_index_partitions)

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_partitioned_index_query,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        explain_query1 = "EXPLAIN select name from default where name is not missing and lower(dept) > 'accounts'"
        results = self.n1ql_helper.run_cbq_query(query=explain_query1,
                                                 server=self.n1ql_node)

        self.log.info("Explain plan for query 1 : {0}".format(results))

        self.sleep(30)

        _, _, _, projection_pushdown, _ = self.validate_query_plan(
            plan=results["results"][0]["plan"], index_name="partitioned_idx1",
            projection_list=[0, 1])

        self.assertTrue(projection_pushdown,
                        "Operators not pushed down to indexer")

        explain_query2 = "EXPLAIN select name,dept,salary from default where name is not missing and lower(dept) > 'accounts'"
        results = self.n1ql_helper.run_cbq_query(query=explain_query2,
                                                 server=self.n1ql_node)

        self.log.info("Explain plan for query 1 : {0}".format(results))

        _, _, _, projection_pushdown, _ = self.validate_query_plan(
            plan=results["results"][0]["plan"], index_name="partitioned_idx1",
            projection_list=[0, 1, 2])

        self.assertTrue(projection_pushdown,
                        "Operators not pushed down to indexer")

        explain_query3 = "EXPLAIN select meta().id from default where name is not missing and lower(dept) > 'accounts'"
        results = self.n1ql_helper.run_cbq_query(query=explain_query3,
                                                 server=self.n1ql_node)

        self.log.info("Explain plan for query 1 : {0}".format(results))

        _, _, _, projection_pushdown, _ = self.validate_query_plan(
            plan=results["results"][0]["plan"], index_name="partitioned_idx1",
            projection_list=[0, 1])

        self.assertTrue(projection_pushdown,
                        "Operators not pushed down to indexer")

    def test_indexer_pushdowns_sorting(self):
        self._load_emp_dataset(end=self.num_items)

        # Create Partitioned indexes
        create_partitioned_index_query = "CREATE INDEX partitioned_idx1 ON default(name,dept,salary) partition by hash(meta().id) USING GSI with {{'num_partition':{0}}};".format(
            self.num_index_partitions)

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_partitioned_index_query,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        explain_query1 = "EXPLAIN select name,dept,salary from default where name is not missing order by name,dept,salary"
        results = self.n1ql_helper.run_cbq_query(query=explain_query1,
                                                 server=self.n1ql_node)

        self.log.info("Explain plan for query 1 : {0}".format(results))

        _, _, _, _, sorting_pushdown = self.validate_query_plan(
            plan=results["results"][0]["plan"],
            index_name="partitioned_idx1",
            index_order_list=[{'keypos': 0}, {'keypos': 1}, {'keypos': 2}])

        self.assertTrue(sorting_pushdown,
                        "Operators not pushed down to indexer")

        explain_query2 = "EXPLAIN select name,dept,salary from default where name is not missing order by name,dept"
        results = self.n1ql_helper.run_cbq_query(query=explain_query2,
                                                 server=self.n1ql_node)

        self.log.info("Explain plan for query 2 : {0}".format(results))

        _, _, _, _, sorting_pushdown = self.validate_query_plan(
            plan=results["results"][0]["plan"],
            index_name="partitioned_idx1",
            index_order_list=[{'keypos': 0}, {'keypos': 1}])

        self.assertTrue(sorting_pushdown,
                        "Operators not pushed down to indexer")

        explain_query3 = "EXPLAIN select meta().id from default where name is not missing order by name"
        results = self.n1ql_helper.run_cbq_query(query=explain_query3,
                                                 server=self.n1ql_node)

        self.log.info("Explain plan for query 3 : {0}".format(results))

        _, _, _, _, sorting_pushdown = self.validate_query_plan(
            plan=results["results"][0]["plan"],
            index_name="partitioned_idx1",
            index_order_list=[{'keypos': 0}])

        self.assertTrue(sorting_pushdown,
                        "Operators not pushed down to indexer")

    def test_indexer_pushdowns_sorting_desc(self):
        self._load_emp_dataset(end=self.num_items)

        # Create Partitioned indexes
        create_partitioned_index_query = "CREATE INDEX partitioned_idx1 ON default(name,dept,salary desc) partition by hash(meta().id) USING GSI with {{'num_partition':{0}}};".format(
            self.num_index_partitions)

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_partitioned_index_query,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        explain_query1 = "EXPLAIN select name,dept,salary from default where name is not missing order by name,dept,salary desc"
        results = self.n1ql_helper.run_cbq_query(query=explain_query1,
                                                 server=self.n1ql_node)

        self.log.info("Explain plan for query 1 : {0}".format(results))

        _, _, _, _, sorting_pushdown = self.validate_query_plan(
            plan=results["results"][0]["plan"],
            index_name="partitioned_idx1",
            index_order_list=[{'keypos': 0}, {'keypos': 1},
                              {"desc": True, 'keypos': 2}])

        self.assertTrue(sorting_pushdown,
                        "Operators not pushed down to indexer")

    def test_multiple_operator_indexer_pushdowns(self):
        self._load_emp_dataset(end=self.num_items)

        # Create Partitioned indexes
        create_partitioned_index_query = "CREATE INDEX partitioned_idx1 ON default(name,dept,salary) partition by hash(meta().id) USING GSI with {{'num_partition':{0}}};".format(
            self.num_index_partitions)

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_partitioned_index_query,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        explain_query1 = "EXPLAIN select name from default where name is not missing order by name OFFSET 10 LIMIT 10"
        results = self.n1ql_helper.run_cbq_query(query=explain_query1,
                                                 server=self.n1ql_node)

        self.log.info("Explain plan for query 1 : {0}".format(results))

        scan_pushdown, limit_pushdown, offset_pushdown, projection_pushdown, sorting_pushdown = self.validate_query_plan(
            plan=results["results"][0]["plan"], index_name="partitioned_idx1",
            num_spans=1, offset=10, limit=10, index_order_list=[{'keypos': 0}],
            projection_list=[0])

        self.assertTrue(
            scan_pushdown & limit_pushdown & offset_pushdown & projection_pushdown & sorting_pushdown,
            "Operators not pushed down to indexer")

    def test_aggregate_indexer_pushdowns_group_by_leading_keys(self):
        self._load_emp_dataset(end=self.num_items)

        # Create Partitioned indexes
        create_partitioned_index_query = "CREATE INDEX partitioned_idx1 ON default(dept,name,salary) partition by hash(meta().id) USING GSI with {{'num_partition':{0}}};".format(
            self.num_index_partitions)

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_partitioned_index_query,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        explain_query1 = "EXPLAIN select dept,count(*) from default where dept is not missing GROUP BY dept"
        results = self.n1ql_helper.run_cbq_query(query=explain_query1,
                                                 server=self.n1ql_node)

        self.log.info("Explain plan for query 1 : {0}".format(results))

        agg_pushdown = False
        if "index_group_aggs" in str(results):
            agg_pushdown = True

        self.assertTrue(agg_pushdown, "Operators not pushed down to indexer")

        explain_query2 = "EXPLAIN select dept,sum(salary), min(salary), max(salary), avg(salary) from default where dept is not missing GROUP BY dept"
        results = self.n1ql_helper.run_cbq_query(query=explain_query2,
                                                 server=self.n1ql_node)

        self.log.info("Explain plan for query 2 : {0}".format(results))

        agg_pushdown = False
        if "index_group_aggs" in str(results):
            agg_pushdown = True

        self.assertTrue(agg_pushdown, "Operators not pushed down to indexer")

    def test_aggregate_indexer_pushdowns_group_by_partition_keys(self):
        self._load_emp_dataset(end=self.num_items)

        # Create Partitioned indexes
        create_partitioned_index_query = "CREATE INDEX partitioned_idx1 ON default(dept,name,salary) partition by hash(LOWER(name),UPPER(dept)) USING GSI with {{'num_partition':{0}}};".format(
            self.num_index_partitions)

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_partitioned_index_query,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        explain_query1 = "EXPLAIN select name,dept,count(*) from default where dept is not missing GROUP BY name,dept"
        results = self.n1ql_helper.run_cbq_query(query=explain_query1,
                                                 server=self.n1ql_node)

        self.log.info("Explain plan for query 1 : {0}".format(results))

        agg_pushdown = False
        if "index_group_aggs" in str(results):
            agg_pushdown = True

        self.assertTrue(agg_pushdown, "Operators not pushed down to indexer")

        explain_query2 = "EXPLAIN select dept,sum(salary), min(salary), max(salary), avg(salary) from default where dept is not missing GROUP BY dept"
        results = self.n1ql_helper.run_cbq_query(query=explain_query2,
                                                 server=self.n1ql_node)

        self.log.info("Explain plan for query 2 : {0}".format(results))

        agg_pushdown = False
        if "index_group_aggs" in str(results):
            agg_pushdown = True

        self.assertTrue(agg_pushdown, "Operators not pushed down to indexer")

    def test_aggregate_indexer_pushdowns_partition_keys_index_keys(self):
        self._load_emp_dataset(end=self.num_items)

        # Create Partitioned indexes
        create_partitioned_index_query = "CREATE INDEX partitioned_idx1 ON default(dept,name,salary) partition by hash(LOWER(dept)) USING GSI with {{'num_partition':{0}}};".format(
            self.num_index_partitions)

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_partitioned_index_query,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        explain_query1 = "EXPLAIN select salary,count(*) from default where dept is not missing GROUP BY salary"
        results = self.n1ql_helper.run_cbq_query(query=explain_query1,
                                                 server=self.n1ql_node)

        self.log.info("Explain plan for query 1 : {0}".format(results))

        agg_pushdown = False
        if "index_group_aggs" in str(results):
            agg_pushdown = True

        self.assertTrue(agg_pushdown, "Operators not pushed down to indexer")

        explain_query2 = "EXPLAIN select dept,sum(salary), min(salary), max(salary), avg(salary) from default where dept is not missing GROUP BY dept"
        results = self.n1ql_helper.run_cbq_query(query=explain_query2,
                                                 server=self.n1ql_node)

        self.log.info("Explain plan for query 2 : {0}".format(results))

        agg_pushdown = False
        if "index_group_aggs" in str(results):
            agg_pushdown = True

        self.assertTrue(agg_pushdown, "Operators not pushed down to indexer")

    def test_aggregate_indexer_pushdowns_groupby_trailing_keys_partition_keys(
            self):
        self._load_emp_dataset(end=self.num_items)

        # Create Partitioned indexes
        create_partitioned_index_query = "CREATE INDEX partitioned_idx1 ON default(dept,name,salary) partition by hash(salary) USING GSI with {{'num_partition':{0}}};".format(
            self.num_index_partitions)

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_partitioned_index_query,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        explain_query1 = "EXPLAIN select salary,count(*) from default where dept is not missing GROUP BY salary"
        results = self.n1ql_helper.run_cbq_query(query=explain_query1,
                                                 server=self.n1ql_node)

        self.log.info("Explain plan for query 1 : {0}".format(results))

        agg_pushdown = False
        if "index_group_aggs" in str(results):
            agg_pushdown = True

        self.assertTrue(agg_pushdown, "Operators not pushed down to indexer")

    def test_aggregate_indexer_pushdowns_groupby_trailing_keys_not_partition_keys(
            self):
        self._load_emp_dataset(end=self.num_items)

        # Create Partitioned indexes
        create_partitioned_index_query = "CREATE INDEX partitioned_idx1 ON default(dept,name,salary) partition by hash(dept) USING GSI with {{'num_partition':{0}}};".format(
            self.num_index_partitions)

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_partitioned_index_query,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        explain_query1 = "EXPLAIN select salary,count(*) from default where dept is not missing GROUP BY salary"
        results = self.n1ql_helper.run_cbq_query(query=explain_query1,
                                                 server=self.n1ql_node)

        self.log.info("Explain plan for query 1 : {0}".format(results))

        agg_pushdown = False
        if "index_group_aggs" in str(results):
            agg_pushdown = True

        self.assertTrue(agg_pushdown, "Operators not pushed down to indexer")

    def test_rebalance_out_with_partitioned_indexes_with_concurrent_querying(
            self):
        self._load_emp_dataset(end=self.num_items)

        with_statement = "with {{'num_partition':{0}".format(self.num_index_partitions)
        if self.num_index_replica > 0:
            with_statement += ", 'num_replica':{0}".format(self.num_index_replica)
        if self.defer_build:
            with_statement += ", 'defer_build': true"
        with_statement += " }"

        create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) " + with_statement
        create_primary_index_statement = "CREATE PRIMARY INDEX pidx1 on default partition by hash(meta().id) " + with_statement

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_index_statement,
                server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(
                query=create_primary_index_statement,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))

        self.sleep(30)

        node_out = self.servers[self.node_out]
        #port = self.node_port
        #if self.use_https:
        #    port = CbServer.ssl_port_map.get(str(self.node_port),
        #                                          str(self.node_port))
        node_out_str = node_out.ip + ":" + str(self.node_port)

        # Get Index Names
        index_names = ["idx1", "pidx1"]
        if self.num_index_replica > 0:
            for i in range(1, self.num_index_replica + 1):
                index_names.append("idx1 (replica {0})".format(str(i)))
                index_names.append("pidx1 (replica {0})".format(str(i)))

        self.log.info(index_names)

        # Get Stats and index partition map before rebalance
        index_data_before = {}
        for index in index_names:
            _, total_item_count_before, _ = self.get_stats_for_partitioned_indexes(
                index_name=index)
            index_data_before[index] = {}
            index_data_before[index]["item_count"] = total_item_count_before
            index_data_before[index][
                "index_metadata"] = self.rest.get_indexer_metadata()

        # start querying
        query = "select name,dept,salary from default where name is not missing and dept='HR' and salary > 120000;"
        t1 = Thread(target=self._run_queries, args=(query, 30,))
        t1.start()
        # rebalance out a indexer node when querying is in progress
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [node_out])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        t1.join()

        self.sleep(30)

        # Get Stats and index partition map after rebalance
        node_list = copy.deepcopy(self.node_list)
        node_list.remove(node_out_str)
        self.log.info(node_list)

        index_data_after = {}

        for index in index_names:
            self.index_servers = self.get_nodes_from_services_map(
                service_type="index", get_all_nodes=True)
            _, total_item_count_after, _ = self.get_stats_for_partitioned_indexes(
                index_name=index, node_list=node_list)
            index_data_after[index] = {}
            index_data_after[index]["item_count"] = total_item_count_after
            index_data_after[index][
                "index_metadata"] = RestConnection(
                self.index_servers[0]).get_indexer_metadata()

        for index in index_names:
            # Validate index item count before and after
            self.assertEqual(index_data_before[index]["item_count"],
                             index_data_after[index]["item_count"],
                             "Item count in index do not match after cluster ops.")

            # Validate host list, partition count and partition distribution
            self.assertTrue(
                self.validate_partition_distribution_after_cluster_ops(
                    index, index_data_before[index]["index_metadata"],
                    index_data_after[index]["index_metadata"], [],
                    [node_out]),
                "Partition distribution post cluster ops has some issues")

    def test_rebalance_out_with_partitioned_indexes_with_concurrent_querying_stop_and_resume(
            self):
        resume = self.input.param("resume_stopped_rebalance", False)
        resume_delay = self.input.param("resume_delay", 0)

        self._load_emp_dataset(end=self.num_items)

        # Create partitioned index
        if self.num_index_replica > 0:
            create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'num_replica':{0}, 'num_partition':{1}}}".format(
                self.num_index_replica, self.num_index_partitions)
            create_primary_index_statement = "CREATE PRIMARY INDEX pidx1 on default partition by hash(meta().id) with {{'num_replica':{0}, 'num_partition':{1}}}".format(
                self.num_index_replica, self.num_index_partitions)
        else:
            create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'num_partition':{0}}}".format(
                self.num_index_partitions)
            create_primary_index_statement = "CREATE PRIMARY INDEX pidx1 on default partition by hash(meta().id) with {{'num_partition':{0}}}".format(
                self.num_index_partitions)

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_index_statement,
                server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(
                query=create_primary_index_statement,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))

        self.sleep(30)

        node_out = self.servers[self.node_out]
        node_out_str = node_out.ip + ":" + str(self.node_port)

        # Get Index Names
        index_names = ["idx1", "pidx1"]
        if self.num_index_replica > 0:
            for i in range(1, self.num_index_replica + 1):
                index_names.append("idx1 (replica {0})".format(str(i)))
                index_names.append("pidx1 (replica {0})".format(str(i)))

        self.log.info(index_names)

        # Get Stats and index partition map before rebalance
        index_data_before = {}
        for index in index_names:
            _, total_item_count_before, _ = self.get_stats_for_partitioned_indexes(
                index_name=index)
            index_data_before[index] = {}
            index_data_before[index]["item_count"] = total_item_count_before
            index_data_before[index][
                "index_metadata"] = self.rest.get_indexer_metadata()

        # start querying
        query = "select name,dept,salary from default where name is not missing and dept='HR' and salary > 120000;"
        t1 = Thread(target=self._run_queries, args=(query, 30,))
        t1.start()
        # rebalance out a indexer node when querying is in progress
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [node_out])
        stopped = RestConnection(self.master).stop_rebalance(
            wait_timeout=self.wait_timeout // 3)
        self.assertTrue(stopped, msg="unable to stop rebalance")
        rebalance.result()

        if resume:
            if resume_delay > 0:
                self.sleep(resume_delay,
                           "Sleep for some time before resume stopped rebalance")
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init],
                [], [node_out])

            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached,
                            "rebalance failed, stuck or did not complete")
            rebalance.result()

        t1.join()

        self.sleep(30)

        # Get Stats and index partition map after rebalance
        node_list = copy.deepcopy(self.node_list)
        node_list.remove(node_out_str)
        self.log.info(node_list)

        index_data_after = {}
        for index in index_names:
            self.index_servers = self.get_nodes_from_services_map(
                service_type="index", get_all_nodes=True)
            _, total_item_count_after, _ = self.get_stats_for_partitioned_indexes(
                index_name=index, node_list=node_list)
            index_data_after[index] = {}
            index_data_after[index]["item_count"] = total_item_count_after
            index_data_after[index][
                "index_metadata"] = RestConnection(self.index_servers[0]).get_indexer_metadata()

        for index in index_names:
            # Validate index item count before and after
            self.assertEqual(index_data_before[index]["item_count"],
                             index_data_after[index]["item_count"],
                             "Item count in index do not match after cluster ops.")

            # Validate host list, partition count and partition distribution
            self.assertTrue(
                self.validate_partition_distribution_after_cluster_ops(
                    index, index_data_before[index]["index_metadata"],
                    index_data_after[index]["index_metadata"], [],
                    [node_out]),
                "Partition distribution post cluster ops has some issues")

    def test_rebalance_in_with_partitioned_indexes_with_concurrent_querying(
            self):
        self._load_emp_dataset(end=self.num_items)

        with_statement = "with {{'num_partition':{0}".format(
            self.num_index_partitions)
        if self.num_index_replica > 0:
            with_statement += ", 'num_replica':{0}".format(
                self.num_index_replica)
        if self.defer_build:
            with_statement += ", 'defer_build': true"
        with_statement += " }"

        create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) " + with_statement
        create_primary_index_statement = "CREATE PRIMARY INDEX pidx1 on default partition by hash(meta().id) " + with_statement

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_index_statement,
                server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(
                query=create_primary_index_statement,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))

        self.sleep(30)

        node_in = self.servers[self.nodes_init]
        node_in_str = node_in.ip + ":" + str(self.node_port)
        services_in = ["index"]

        # Get Index Names
        index_names = ["idx1", "pidx1"]
        if self.num_index_replica > 0:
            for i in range(1, self.num_index_replica + 1):
                index_names.append("idx1 (replica {0})".format(str(i)))
                index_names.append("pidx1 (replica {0})".format(str(i)))

        self.log.info(index_names)

        # Get Stats and index partition map before rebalance
        index_data_before = {}
        for index in index_names:
            _, total_item_count_before, _ = self.get_stats_for_partitioned_indexes(
                index_name=index)
            index_data_before[index] = {}
            index_data_before[index]["item_count"] = total_item_count_before
            index_data_before[index][
                "index_metadata"] = self.rest.get_indexer_metadata()

        # start querying
        query = "select name,dept,salary from default where name is not missing and dept='HR' and salary > 120000;"
        t1 = Thread(target=self._run_queries, args=(query, 30,))
        t1.start()
        # rebalance out a indexer node when querying is in progress
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [node_in], [],
                                                 services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        t1.join()

        self.sleep(30)

        # Get Stats and index partition map after rebalance
        node_list = copy.deepcopy(self.node_list)
        node_list.append(node_in_str)
        self.log.info(node_list)

        index_data_after = {}
        for index in index_names:
            self.index_servers = self.get_nodes_from_services_map(
                service_type="index", get_all_nodes=True)
            _, total_item_count_after, _ = self.get_stats_for_partitioned_indexes(
                index_name=index, node_list=node_list)
            index_data_after[index] = {}
            index_data_after[index]["item_count"] = total_item_count_after
            index_data_after[index][
                "index_metadata"] = RestConnection(self.index_servers[0]).get_indexer_metadata()

        for index in index_names:
            # Validate index item count before and after
            self.assertEqual(index_data_before[index]["item_count"],
                             index_data_after[index]["item_count"],
                             "Item count in index do not match after cluster ops.")

            # Validate host list, partition count and partition distribution
            self.assertTrue(
                self.validate_partition_distribution_after_cluster_ops(
                    index, index_data_before[index]["index_metadata"],
                    index_data_after[index]["index_metadata"], [node_in],
                    []),
                "Partition distribution post cluster ops has some issues")

    def test_swap_rebalance_with_partitioned_indexes_with_concurrent_querying(
            self):
        self._load_emp_dataset(end=self.num_items)

        with_statement = "with {{'num_partition':{0}".format(
            self.num_index_partitions)
        if self.num_index_replica > 0:
            with_statement += ", 'num_replica':{0}".format(
                self.num_index_replica)
        if self.defer_build:
            with_statement += ", 'defer_build': true"
        with_statement += " }"

        create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) " + with_statement
        create_primary_index_statement = "CREATE PRIMARY INDEX pidx1 on default partition by hash(meta().id) " + with_statement

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_index_statement,
                server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(
                query=create_primary_index_statement,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))

        self.sleep(30)

        node_out = self.servers[self.node_out]
        node_out_str = node_out.ip + ":" + str(self.node_port)

        node_in = self.servers[self.nodes_init]
        node_in_str = node_in.ip + ":" + str(self.node_port)
        services_in = ["index"]

        # Get Index Names
        index_names = ["idx1", "pidx1"]
        if self.num_index_replica > 0:
            for i in range(1, self.num_index_replica + 1):
                index_names.append("idx1 (replica {0})".format(str(i)))
                index_names.append("pidx1 (replica {0})".format(str(i)))

        self.log.info(index_names)

        # Get Stats and index partition map before rebalance
        index_data_before = {}
        for index in index_names:
            _, total_item_count_before, _ = self.get_stats_for_partitioned_indexes(
                index_name=index)
            index_data_before[index] = {}
            index_data_before[index]["item_count"] = total_item_count_before
            index_data_before[index][
                "index_metadata"] = self.rest.get_indexer_metadata()

        try:
            # start querying
            query = "select name,dept,salary from default where name is not missing and dept='HR' and salary > 120000;"
            t1 = Thread(target=self._run_queries, args=(query, 30,))
            t1.start()
            # rebalance out a indexer node when querying is in progress
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                     [node_in], [node_out],
                                                     services=services_in)
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
            t1.join()
        except Exception as ex:
            self.log.info(str(ex))

        self.sleep(30)

        # Get Stats and index partition map after rebalance
        node_list = copy.deepcopy(self.node_list)
        node_list.append(node_in_str)
        node_list.remove(node_out_str)

        index_data_after = {}
        for index in index_names:
            self.index_servers = self.get_nodes_from_services_map(
                service_type="index", get_all_nodes=True)
            _, total_item_count_after, _ = self.get_stats_for_partitioned_indexes(
                index_name=index, node_list=node_list)
            index_data_after[index] = {}
            index_data_after[index]["item_count"] = total_item_count_after
            index_data_after[index][
                "index_metadata"] = RestConnection(self.index_servers[0]).get_indexer_metadata()

        for index in index_names:
            # Validate index item count before and after
            self.assertEqual(index_data_before[index]["item_count"],
                             index_data_after[index]["item_count"],
                             "Item count in index do not match after cluster ops.")

            # Validate host list, partition count and partition distribution
            self.assertTrue(
                self.validate_partition_distribution_after_cluster_ops(
                    index, index_data_before[index]["index_metadata"],
                    index_data_after[index]["index_metadata"], [node_in],
                    [node_out]),
                "Partition distribution post cluster ops has some issues")

    def test_failover_with_partitioned_indexes_with_concurrent_querying(
            self):
        self._load_emp_dataset(end=self.num_items)

        # Create partitioned index
        if self.num_index_replica > 0:
            create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'num_replica':{0}, 'num_partition':{1}}}".format(
                self.num_index_replica, self.num_index_partitions)
            create_primary_index_statement = "CREATE PRIMARY INDEX pidx1 on default partition by hash(meta().id) with {{'num_replica':{0}, 'num_partition':{1}}}".format(
                self.num_index_replica, self.num_index_partitions)
        else:
            create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'num_partition':{1}}}".format(
                self.num_index_replica, self.num_index_partitions)
            create_primary_index_statement = "CREATE PRIMARY INDEX pidx1 on default partition by hash(meta().id) with {{'num_partition':{1}}}".format(
                self.num_index_replica, self.num_index_partitions)

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_index_statement,
                server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(
                query=create_primary_index_statement,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))

        self.sleep(30)

        node_out = self.servers[self.node_out]
        node_out_str = node_out.ip + ":" + str(self.node_port)

        # Get Index Names
        index_names = ["idx1", "pidx1"]
        if self.num_index_replica > 0:
            for i in range(1, self.num_index_replica + 1):
                index_names.append("idx1 (replica {0})".format(str(i)))
                index_names.append("pidx1 (replica {0})".format(str(i)))

        self.log.info(index_names)

        # Get Stats and index partition map before rebalance
        index_data_before = {}
        for index in index_names:
            _, total_item_count_before, _ = self.get_stats_for_partitioned_indexes(
                index_name=index)
            index_data_before[index] = {}
            index_data_before[index]["item_count"] = total_item_count_before
            index_data_before[index][
                "index_metadata"] = self.rest.get_indexer_metadata()

        # start querying
        query = "select name,dept,salary from default where name is not missing and dept='HR' and salary > 120000;"
        t1 = Thread(target=self._run_queries, args=(query, 30,))
        t1.start()

        # failover and rebalance out a indexer node when querying is in progress
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            self.graceful, wait_for_pending=180)

        failover_task.result()

        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [node_out])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        t1.join()

        self.sleep(30)

        # Get Stats and index partition map after rebalance
        node_list = copy.deepcopy(self.node_list)
        node_list.remove(node_out_str)
        self.log.info(node_list)

        index_data_after = {}
        for index in index_names:
            self.index_servers = self.get_nodes_from_services_map(
                service_type="index", get_all_nodes=True)
            _, total_item_count_after, _ = self.get_stats_for_partitioned_indexes(
                index_name=index, node_list=node_list)
            index_data_after[index] = {}
            index_data_after[index]["item_count"] = total_item_count_after
            index_data_after[index][
                "index_metadata"] = RestConnection(self.index_servers[0]).get_indexer_metadata()

        for index in index_names:
            # Validate index item count before and after
            self.assertEqual(index_data_before[index]["item_count"],
                             index_data_after[index]["item_count"],
                             "Item count in index do not match after cluster ops.")

            # Validate host list, partition count and partition distribution
            self.assertTrue(
                self.validate_partition_distribution_after_cluster_ops(
                    index, index_data_before[index]["index_metadata"],
                    index_data_after[index]["index_metadata"], [],
                    [node_out]),
                "Partition distribution post cluster ops has some issues")

    def test_failover_addback_with_partitioned_indexes_with_concurrent_querying(
            self):
        self._load_emp_dataset(end=self.num_items)

        # Create partitioned index
        if self.num_index_replica > 0:
            create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'num_replica':{0}, 'num_partition':{1}}}".format(
                self.num_index_replica, self.num_index_partitions)
            create_primary_index_statement = "CREATE PRIMARY INDEX pidx1 on default partition by hash(meta().id) with {{'num_replica':{0}, 'num_partition':{1}}}".format(
                self.num_index_replica, self.num_index_partitions)
        else:
            create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'num_partition':{1}}}".format(
                self.num_index_replica, self.num_index_partitions)
            create_primary_index_statement = "CREATE PRIMARY INDEX pidx1 on default partition by hash(meta().id) with {{'num_partition':{1}}}".format(
                self.num_index_replica, self.num_index_partitions)

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_index_statement,
                server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(
                query=create_primary_index_statement,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))

        self.sleep(30)

        node_out = self.servers[self.node_out]
        node_out_str = node_out.ip + ":" + str(self.node_port)

        # Get Index Names
        index_names = ["idx1", "pidx1"]
        if self.num_index_replica > 0:
            for i in range(1, self.num_index_replica + 1):
                index_names.append("idx1 (replica {0})".format(str(i)))
                index_names.append("pidx1 (replica {0})".format(str(i)))

        self.log.info(index_names)

        # Get Stats and index partition map before rebalance
        index_data_before = {}
        for index in index_names:
            _, total_item_count_before, _ = self.get_stats_for_partitioned_indexes(
                index_name=index)
            index_data_before[index] = {}
            index_data_before[index]["item_count"] = total_item_count_before
            index_data_before[index][
                "index_metadata"] = self.rest.get_indexer_metadata()

        # start querying
        query = "select name,dept,salary from default where name is not missing and dept='HR' and salary > 120000;"
        t1 = Thread(target=self._run_queries, args=(query, 30,))
        t1.start()

        # failover and rebalance out a indexer node when querying is in progress
        nodes_all = self.rest.node_statuses()
        for node in nodes_all:
            if node.ip == node_out.ip:
                break

        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            self.graceful, wait_for_pending=180)

        failover_task.result()

        self.rest.set_recovery_type(node.id, self.recovery_type)
        self.rest.add_back_node(node.id)

        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [])

        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        t1.join()

        self.sleep(30)

        # Get Stats and index partition map after rebalance
        index_data_after = {}
        for index in index_names:
            self.index_servers = self.get_nodes_from_services_map(
                service_type="index", get_all_nodes=True)
            _, total_item_count_after, _ = self.get_stats_for_partitioned_indexes(
                index_name=index)
            index_data_after[index] = {}
            index_data_after[index]["item_count"] = total_item_count_after
            index_data_after[index][
                "index_metadata"] = RestConnection(self.index_servers[0]).get_indexer_metadata()

        for index in index_names:
            # Validate index item count before and after
            self.assertEqual(index_data_before[index]["item_count"],
                             index_data_after[index]["item_count"],
                             "Item count in index do not match after cluster ops.")

            # Validate host list, partition count and partition distribution
            self.assertTrue(
                self.validate_partition_distribution_after_cluster_ops(
                    index, index_data_before[index]["index_metadata"],
                    index_data_after[index]["index_metadata"], [],
                    []),
                "Partition distribution post cluster ops has some issues")

    def test_kv_rebalance_out_with_partitioned_indexes_with_concurrent_querying(
            self):
        self._load_emp_dataset(end=self.num_items)

        # Create partitioned index
        if self.num_index_replica > 0:
            create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'num_replica':{0}, 'num_partition':{1}}}".format(
                self.num_index_replica, self.num_index_partitions)
            create_primary_index_statement = "CREATE PRIMARY INDEX pidx1 on default partition by hash(meta().id) with {{'num_replica':{0}, 'num_partition':{1}}}".format(
                self.num_index_replica, self.num_index_partitions)
        else:
            create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'num_partition':{1}}}".format(
                self.num_index_replica, self.num_index_partitions)
            create_primary_index_statement = "CREATE PRIMARY INDEX pidx1 on default partition by hash(meta().id) with {{'num_partition':{1}}}".format(
                self.num_index_replica, self.num_index_partitions)

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_index_statement,
                server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(
                query=create_primary_index_statement,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))

        self.sleep(30)

        node_out = self.servers[self.node_out]
        node_out_str = node_out.ip + ":" + str(self.node_port)

        # Get Index Names
        index_names = ["idx1", "pidx1"]
        if self.num_index_replica > 0:
            for i in range(1, self.num_index_replica + 1):
                index_names.append("idx1 (replica {0})".format(str(i)))
                index_names.append("pidx1 (replica {0})".format(str(i)))

        self.log.info(index_names)

        # Get Stats and index partition map before rebalance
        index_data_before = {}
        for index in index_names:
            _, total_item_count_before, _ = self.get_stats_for_partitioned_indexes(
                index_name=index)
            index_data_before[index] = {}
            index_data_before[index]["item_count"] = total_item_count_before
            index_data_before[index][
                "index_metadata"] = self.rest.get_indexer_metadata()

        # start querying
        query = "select name,dept,salary from default where name is not missing and dept='HR' and salary > 120000;"
        t1 = Thread(target=self._run_queries, args=(query, 30,))
        t1.start()
        # rebalance out a indexer node when querying is in progress
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [node_out])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        t1.join()

        self.sleep(30)

        # Get Stats and index partition map after rebalance

        index_data_after = {}
        for index in index_names:
            self.index_servers = self.get_nodes_from_services_map(
                service_type="index", get_all_nodes=True)
            _, total_item_count_after, _ = self.get_stats_for_partitioned_indexes(
                index_name=index)
            index_data_after[index] = {}
            index_data_after[index]["item_count"] = total_item_count_after
            index_data_after[index][
                "index_metadata"] = RestConnection(self.index_servers[0]).get_indexer_metadata()

        for index in index_names:
            # Validate index item count before and after
            self.assertEqual(index_data_before[index]["item_count"],
                             index_data_after[index]["item_count"],
                             "Item count in index do not match after cluster ops.")

            # Validate host list, partition count and partition distribution
            self.assertTrue(
                self.validate_partition_distribution_after_cluster_ops(
                    index, index_data_before[index]["index_metadata"],
                    index_data_after[index]["index_metadata"], [],
                    []),
                "Partition distribution post cluster ops has some issues")

    def test_rebalance_out_with_replica_partitioned_indexes_partition_loss(
            self):
        self._load_emp_dataset(end=self.num_items)

        # Create partitioned index
        create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'num_replica':{0}, 'num_partition':{1}}}".format(
            self.num_index_replica, self.num_index_partitions)

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_index_statement,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))

        self.sleep(30)

        node_out = self.servers[self.node_out]
        node_out_str = node_out.ip + ":" + str(self.node_port)

        # Get Index Names
        index_names = ["idx1"]
        if self.num_index_replica > 0:
            for i in range(1, self.num_index_replica + 1):
                index_names.append("idx1 (replica {0})".format(str(i)))

        self.log.info(index_names)

        # rebalance out an indexer node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [node_out])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

        #Allow indexer metadata to catch up with the last rebalance
        self.sleep(60)

        # Get Stats and index partition map after rebalance
        node_list = copy.deepcopy(self.node_list)
        node_list.remove(node_out_str)
        self.log.info(node_list)

        index_data_after = {}
        total_index_item_count = 0
        bucket_item_count = 0
        total_partition_count = 0
        for index in index_names:
            bucket_item_count, total_item_count_after, _ = self.get_stats_for_partitioned_indexes(
                index_name=index, node_list=node_list)
            self.index_servers = self.get_nodes_from_services_map(
                service_type="index", get_all_nodes=True)
            index_data_after[index] = {}
            index_data_after[index]["item_count"] = total_item_count_after
            index_data_after[index][
                "index_metadata"] = RestConnection(self.index_servers[0]).get_indexer_metadata()
            total_index_item_count += total_item_count_after
            total_partition_count += self.get_num_partitions_for_index(
                RestConnection(self.index_servers[0]).get_indexer_metadata(), index)

        self.assertEqual(total_index_item_count, bucket_item_count,
                         "Item count in index do not match after cluster ops.")

        self.assertEqual(self.num_index_partitions, total_partition_count,
                         "Some partitions are not available after rebalance")

    def test_node_failure_during_rebalance_out_partitioned_indexes(
            self):
        fail_node = self.input.param("fail_node", None)
        if fail_node:
            node_to_fail = self.servers[fail_node]
        self._load_emp_dataset(end=self.num_items)

        # Create partitioned index
        create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name)"

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_index_statement,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))

        self.sleep(30)

        node_out = self.servers[self.node_out]
        node_out_str = node_out.ip + ":" + str(self.node_port)

        # Get Index Names
        index_names = ["idx1"]
        if self.num_index_replica > 0:
            for i in range(1, self.num_index_replica + 1):
                index_names.append("idx1 (replica {0})".format(str(i)))

        self.log.info(index_names)

        # Get Stats and index partition map before rebalance
        index_data_before = {}
        for index in index_names:
            _, total_item_count_before, _ = self.get_stats_for_partitioned_indexes(
                index_name=index)
            index_data_before[index] = {}
            index_data_before[index]["item_count"] = total_item_count_before
            index_data_before[index][
                "index_metadata"] = self.rest.get_indexer_metadata()

        try:
            # rebalance out an indexer node
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                     [], [node_out])

            reached = RestHelper(self.rest).rebalance_reached()

            remote_client = RemoteMachineShellConnection(node_to_fail)
            if self.node_operation == "kill_indexer":
                remote_client.terminate_process(process_name="indexer")
            elif self.node_operation == "kill_kv":
                remote_client.kill_memcached()
            else:
                self.reboot_node(node_to_fail)
            remote_client.disconnect()
            # wait for restart and warmup on all node
            self.sleep(self.wait_timeout*2)
            # wait till node is ready after warmup
            ClusterOperationHelper.wait_for_ns_servers_or_assert([node_to_fail],
                                                                 self,
                                                                 wait_if_warmup=True)
            rebalance.result()
        except Exception as ex:
            self.log.info(str(ex))

        # Rerun rebalance
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [node_out])
        self.sleep(30)
        reached_rerun = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached_rerun,
                        "retry of the failed rebalance failed, stuck or did not complete")
        rebalance.result()

        self.sleep(30)

        # Get Stats and index partition map after rebalance
        node_list = copy.deepcopy(self.node_list)
        node_list.remove(node_out_str)
        self.log.info(node_list)

        index_data_after = {}
        for index in index_names:
            self.index_servers = self.get_nodes_from_services_map(
                service_type="index", get_all_nodes=True)
            _, total_item_count_after, _ = self.get_stats_for_partitioned_indexes(
                index_name=index, node_list=node_list)
            index_data_after[index] = {}
            index_data_after[index]["item_count"] = total_item_count_after
            index_data_after[index][
                "index_metadata"] = RestConnection(self.index_servers[0]).get_indexer_metadata()
            self.log.info(index_data_after[index]["index_metadata"])

        for index in index_names:
            # Validate index item count before and after
            self.assertEqual(index_data_before[index]["item_count"],
                             index_data_after[index]["item_count"],
                             "Item count in index do not match after cluster ops.")

            # Validate host list, partition count and partition distribution
            self.assertTrue(
                self.validate_partition_distribution_after_cluster_ops(
                    index, index_data_before[index]["index_metadata"],
                    index_data_after[index]["index_metadata"], [],
                    [node_out]),
                "Partition distribution post cluster ops has some issues")

    def test_node_failure_during_rebalance_in_partitioned_indexes(
            self):
        fail_node = self.input.param("fail_node", None)

        if fail_node:
            node_to_fail = self.servers[fail_node]
        self._load_emp_dataset(end=self.num_items)

        # Create partitioned index
        create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name)"

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_index_statement,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))

        self.sleep(30)

        node_in = self.servers[self.nodes_init]
        node_in_str = node_in.ip + ":" + str(self.node_port)
        services_in = ["index"]

        # Get Index Names
        index_names = ["idx1"]
        if self.num_index_replica > 0:
            for i in range(1, self.num_index_replica + 1):
                index_names.append("idx1 (replica {0})".format(str(i)))

        self.log.info(index_names)

        # Get Stats and index partition map before rebalance
        index_data_before = {}
        for index in index_names:
            _, total_item_count_before, _ = self.get_stats_for_partitioned_indexes(
                index_name=index)
            index_data_before[index] = {}
            index_data_before[index]["item_count"] = total_item_count_before
            index_data_before[index][
                "index_metadata"] = self.rest.get_indexer_metadata()

        # rebalance in an indexer node
        try:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                     [node_in], [],
                                                     services=services_in)

            reached = RestHelper(self.rest).rebalance_reached()

            remote_client = RemoteMachineShellConnection(node_to_fail)
            if self.node_operation == "kill_indexer":
                remote_client.terminate_process(process_name="indexer")
            elif self.node_operation == "kill_kv":
                remote_client.kill_memcached()
            else:
                self.reboot_node(node_to_fail)
            remote_client.disconnect()
            # wait for restart and warmup on all node
            self.sleep(self.wait_timeout)
            # wait till node is ready after warmup
            ClusterOperationHelper.wait_for_ns_servers_or_assert([node_to_fail],
                                                                 self,
                                                                 wait_if_warmup=True)
            rebalance.result()
        except Exception as ex:
            self.log.info(str(ex))

        # Rerun Rebalance
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [])
        self.sleep(30)
        reached_rerun = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached_rerun, "rebalance failed, stuck or did not complete")
        rebalance.result()

        self.sleep(10)

        # Get Stats and index partition map after rebalance
        node_list = copy.deepcopy(self.node_list)
        node_list.append(node_in_str)
        self.log.info(node_list)

        index_data_after = {}
        for index in index_names:
            self.index_servers = self.get_nodes_from_services_map(
                service_type="index", get_all_nodes=True)
            _, total_item_count_after, _ = self.get_stats_for_partitioned_indexes(
                index_name=index, node_list=node_list)
            index_data_after[index] = {}
            index_data_after[index]["item_count"] = total_item_count_after
            index_data_after[index][
                "index_metadata"] = RestConnection(self.index_servers[0]).get_indexer_metadata()

        for index in index_names:
            # Validate index item count before and after
            self.assertEqual(index_data_before[index]["item_count"],
                             index_data_after[index]["item_count"],
                             "Item count in index do not match after cluster ops.")

            # Validate host list, partition count and partition distribution
            self.assertTrue(
                self.validate_partition_distribution_after_cluster_ops(
                    index, index_data_before[index]["index_metadata"],
                    index_data_after[index]["index_metadata"], [node_in],
                    []),
                "Partition distribution post cluster ops has some issues")

    def test_replica_partition_index_with_excluded_nodes_failover(self):
        self._load_emp_dataset(end=self.num_items)

        # Setting to exclude a node for planner
        self.rest.set_index_planner_settings("excludeNode=in")
        # Create partitioned index
        create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'num_replica':{0}}}".format(
            self.num_index_replica)

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_index_statement,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata :::")
        self.log.info(index_metadata)

        self.assertTrue(self.validate_partition_map(index_metadata, "idx1",
                                                    self.num_index_replica,
                                                    self.num_index_partitions),
                        "Partition map validation failed")

        # Validate index created and check the hosts on which partitions are hosted.
        expected_hosts = self.node_list[1:]
        expected_hosts.sort()
        index_names = []
        index_names.append("idx1")
        for i in range(1, self.num_index_replica + 1):
            index_names.append("idx1 (replica {0})".format(str(i)))

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata :::")
        self.log.info(index_metadata)

        index_validated = 0
        for index_name in index_names:
            for index in index_metadata["status"]:
                if index["name"] == index_name:
                    self.log.info("Expected Hosts : {0}".format(expected_hosts))
                    self.log.info("Actual Hosts   : {0}".format(index["hosts"]))
                    self.assertEqual(index["hosts"].sort(), expected_hosts.sort(),
                                     "Planner did not ignore excluded node during index creation for {0}".format(
                                         index_name))
                    index_validated += 1

        self.assertEqual(index_validated, (self.num_index_replica + 1),
                         "All index replicas not created")

        node_out = self.servers[self.node_out]
        node_out_str = node_out.ip + ":" + str(self.node_port)

        # failover and rebalance out a indexer node
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            self.graceful, wait_for_pending=180)

        failover_task.result()

        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [node_out])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

        self.sleep(30)

        node_list = copy.deepcopy(self.node_list[1:])
        if node_out_str in node_list:
            node_list.remove(node_out_str)
        self.log.info(node_list)

        index_data_after = {}
        total_index_item_count = 0
        bucket_item_count = 0
        total_partition_count = 0
        for index in index_names:
            self.index_servers = self.get_nodes_from_services_map(
                service_type="index", get_all_nodes=True)
            bucket_item_count, total_item_count_after, _ = self.get_stats_for_partitioned_indexes(
                index_name=index, node_list=node_list)
            index_data_after[index] = {}
            index_data_after[index]["item_count"] = total_item_count_after
            index_data_after[index][
                "index_metadata"] = RestConnection(self.index_servers[0]).get_indexer_metadata()
            total_index_item_count += total_item_count_after
            total_partition_count += self.get_num_partitions_for_index(
                self.rest.get_indexer_metadata(), index)

        self.assertEqual(total_index_item_count, bucket_item_count,
                         "Item count in index do not match after cluster ops.")

        self.assertEqual(self.num_index_partitions, total_partition_count,
                         "Some partitions are not available after rebalance")

    def test_replica_partition_index_with_excluded_nodes_failover_addback(self):
        self._load_emp_dataset(end=self.num_items)

        # Setting to exclude a node for planner
        self.rest.set_index_planner_settings("excludeNode=in")
        # Create partitioned index
        create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'num_replica':{0}}}".format(
            self.num_index_replica)

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_index_statement,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata :::")
        self.log.info(index_metadata)

        self.assertTrue(self.validate_partition_map(index_metadata, "idx1",
                                                    self.num_index_replica,
                                                    self.num_index_partitions),
                        "Partition map validation failed")

        # Validate index created and check the hosts on which partitions are hosted.
        expected_hosts = self.node_list[1:]
        expected_hosts.sort()
        index_names = []
        index_names.append("idx1")
        for i in range(1, self.num_index_replica + 1):
            index_names.append("idx1 (replica {0})".format(str(i)))

        # Need to see if the indexes get created in the first place

        # Validate index created and check the hosts on which partitions are hosted.
        expected_hosts = self.node_list[1:]
        expected_hosts.sort()
        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata :::")
        self.log.info(index_metadata)

        index_validated = 0
        for index_name in index_names:
            for index in index_metadata["status"]:
                if index["name"] == index_name:
                    self.log.info("Expected Hosts : {0}".format(expected_hosts))
                    self.log.info("Actual Hosts   : {0}".format(index["hosts"]))
                    #TODO revert after bug fix  https://issues.couchbase.com/browse/MB-51119
                    #ignoring port validation and just checking for the host IP
                    expected_hosts = ["{}".format(host.split(":")[0]) for host in expected_hosts]
                    actual_hosts = [f.split(":")[0] for f in index['hosts']]
                    self.assertEqual(actual_hosts, expected_hosts,
                                         "Planner did not ignore excluded node during index creation for {0}".format(
                                             index_name))
                    index_validated += 1

        self.assertEqual(index_validated, (self.num_index_replica + 1),
                         "All index replicas not created")

        node_out = self.servers[self.node_out]
        node_out_str = node_out.ip + ":" + str(self.node_port)

        # failover and rebalance out a indexer node
        nodes_all = self.rest.node_statuses()
        for node in nodes_all:
            if node.ip == node_out.ip:
                break

        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            self.graceful, wait_for_pending=180)

        failover_task.result()

        self.rest.set_recovery_type(node.id, self.recovery_type)
        self.rest.add_back_node(node.id)

        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [])

        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata :::")
        self.log.info(index_metadata)

        self.assertTrue(self.validate_partition_map(index_metadata, "idx1",
                                                    self.num_index_replica,
                                                    self.num_index_partitions),
                        "Partition map validation failed")

    def test_partition_placement_one_node_in_paused_state(self):
        index_server = self.index_servers[0]
        create_index_query1 = "CREATE PRIMARY INDEX ON default USING GSI"
        create_index_query2 = "CREATE INDEX idx_job_title ON default(job_title) USING GSI"
        create_index_query3 = "CREATE INDEX idx_join_yr ON default(join_yr) USING GSI"
        create_index_query4 = "CREATE INDEX idx_job_title_join_yr ON default(job_title,join_yr) USING GSI"
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query1,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index_query2,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index_query3,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index_query4,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        self.rest.set_service_memoryQuota(service='indexMemoryQuota',
                                          memoryQuota=256)

        # Ensure indexer reaches to paused state
        self._saturate_indexer_memory(index_server)

        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [self.servers[
                                                      self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()

        create_index_query = "CREATE INDEX pidx1 ON default(name,mutated) partition by hash(BASE64(meta().id)) USING GSI"
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        index_map = self.get_index_map()
        self.log.info(index_map)

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata Before Build:")
        self.log.info(index_metadata)

        index_details = {}
        index_details["index_name"] = "pidx1"
        index_details["num_partitions"] = self.num_index_partitions
        index_details["defer_build"] = False

        self.assertTrue(
            self.validate_partitioned_indexes(index_details, index_map,
                                              index_metadata),
            "Partitioned index created not as expected")

        index_node_list = self.node_list
        index_node_list.append(
            self.servers[self.nodes_init].ip + ":" + self.node_port)
        index_node_list.remove(index_server.ip + ":" + self.node_port)
        index_node_list.sort()
        validated = False

        for index in index_metadata["status"]:
            if index["name"] == "pidx1":
                self.log.info("Expected Hosts : {0}".format(index_node_list))
                self.log.info("Actual Hosts   : {0}".format(index["hosts"]))
                self.assertEqual(index["hosts"], index_node_list,
                                 "Planner did not exclude node in Paused state during index creation")
                validated = True

        if not validated:
            self.fail("Looks like index was not created.")

    def test_index_scans_one_node_memory_saturated(self):
        index_server = self.index_servers[0]
        index_server_str = index_server.ip + ":" + self.node_port
        create_index_query1 = "CREATE PRIMARY INDEX ON default USING GSI with {{'nodes':['{0}']}}".format(
            index_server_str)
        create_index_query2 = "CREATE INDEX idx_job_title ON default(job_title) USING GSI with {{'nodes':['{0}']}}".format(
            index_server_str)
        create_index_query3 = "CREATE INDEX idx_join_yr ON default(join_yr) USING GSI with {{'nodes':['{0}']}}".format(
            index_server_str)
        create_index_query4 = "CREATE INDEX idx_job_title_join_yr ON default(job_title,join_yr) USING GSI with {{'nodes':['{0}']}}".format(
            index_server_str)
        create_index_query5 = "CREATE INDEX pidx1 ON default(name,mutated) partition by hash(BASE64(meta().id)) USING GSI"
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query1,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index_query2,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index_query3,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index_query4,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index_query5,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        query = "select name,mutated from default where name is not null order by name limit 1000"
        results = self.n1ql_helper.run_cbq_query(query=query,
                                                 server=self.n1ql_node)
        self.assertIsNotNone(results["results"], "No results")
        num_results_before = results["metrics"]["resultCount"]
        self.log.info("num_results_before : {0}".format(num_results_before))

        self.rest.set_service_memoryQuota(service='indexMemoryQuota',
                                          memoryQuota=256)

        # Ensure indexer reaches to paused state
        self._saturate_indexer_memory(index_server)

        query = "select name,mutated from default where name is not null order by name limit 1000"
        results = self.n1ql_helper.run_cbq_query(query=query,
                                                 server=self.n1ql_node)
        self.assertIsNotNone(results["results"], "No results")
        num_results_after = results["metrics"]["resultCount"]
        self.log.info("num_results_after : {0}".format(str(num_results_after)))

    def test_rebalance_out_concurrent_querying_one_node_nw_partitioned(self):
        self._load_emp_dataset(end=self.num_items)

        # Create partitioned index
        create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'num_replica':{0}, 'num_partition':{1}}}".format(
            self.num_index_replica, self.num_index_partitions)
        create_primary_index_statement = "CREATE PRIMARY INDEX pidx1 on default partition by hash(meta().id) with {{'num_replica':{0}, 'num_partition':{1}}}".format(
            self.num_index_replica, self.num_index_partitions)

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_index_statement,
                server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(
                query=create_primary_index_statement,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))

        self.sleep(30)

        # Get Index Names
        index_names = ["idx1", "pidx1"]
        if self.num_index_replica > 0:
            for i in range(1, self.num_index_replica + 1):
                index_names.append("idx1 (replica {0})".format(str(i)))
                index_names.append("pidx1 (replica {0})".format(str(i)))

        self.log.info(index_names)

        # Get Stats and index partition map before rebalance
        index_data_before = {}
        for index in index_names:
            _, total_item_count_before, _ = self.get_stats_for_partitioned_indexes(
                index_name=index)
            index_data_before[index] = {}
            index_data_before[index]["item_count"] = total_item_count_before
            index_data_before[index][
                "index_metadata"] = self.rest.get_indexer_metadata()

        node_out = self.servers[self.node_out]
        node_out_str = node_out.ip + ":" + str(self.node_port)

        node_nw_partition_out = self.servers[self.node_out - 1]
        self.start_firewall_on_node(node_nw_partition_out)

        # start querying
        query = "select name,dept,salary from default where name is not missing and dept='HR' and salary > 120000;"
        t1 = Thread(target=self._run_queries, args=(query, 30,))
        t1.start()
        # rebalance out a indexer node when querying is in progress
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [node_out])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        t1.join()

        self.sleep(30)

        self.stop_firewall_on_node(node_nw_partition_out)

        # Get Stats and index partition map after rebalance
        node_list = copy.deepcopy(self.node_list)
        node_list.remove(node_out_str)
        self.log.info(node_list)

        index_data_after = {}
        for index in index_names:
            _, total_item_count_after, _ = self.get_stats_for_partitioned_indexes(
                index_name=index, node_list=node_list)
            index_data_after[index] = {}
            index_data_after[index]["item_count"] = total_item_count_after
            index_data_after[index][
                "index_metadata"] = self.rest.get_indexer_metadata()

        for index in index_names:
            # Validate index item count before and after
            self.assertEqual(index_data_before[index]["item_count"],
                             index_data_after[index]["item_count"],
                             "Item count in index do not match after cluster ops.")

            # Validate host list, partition count and partition distribution
            self.assertTrue(
                self.validate_partition_distribution_after_cluster_ops(
                    index, index_data_before[index]["index_metadata"],
                    index_data_after[index]["index_metadata"], [],
                    [node_out]),
                "Partition distribution post cluster ops has some issues")

    def test_rebalance_out_concurrent_querying_server_group_nw_partitioned(
            self):
        self._load_emp_dataset(end=self.num_items)
        self._create_server_groups()

        # Create partitioned index
        create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'num_replica':{0}, 'num_partition':{1}}}".format(
            self.num_index_replica, self.num_index_partitions)
        create_primary_index_statement = "CREATE PRIMARY INDEX pidx1 on default partition by hash(meta().id) with {{'num_replica':{0}, 'num_partition':{1}}}".format(
            self.num_index_replica, self.num_index_partitions)

        try:
            self.n1ql_helper.run_cbq_query(
                query=create_index_statement,
                server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(
                query=create_primary_index_statement,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))

        self.sleep(30)

        # Get Index Names
        index_names = ["idx1", "pidx1"]
        if self.num_index_replica > 0:
            for i in range(1, self.num_index_replica + 1):
                index_names.append("idx1 (replica {0})".format(str(i)))
                index_names.append("pidx1 (replica {0})".format(str(i)))

        self.log.info(index_names)

        # Get Stats and index partition map before rebalance
        index_data_before = {}
        for index in index_names:
            _, total_item_count_before, _ = self.get_stats_for_partitioned_indexes(
                index_name=index)
            index_data_before[index] = {}
            index_data_before[index]["item_count"] = total_item_count_before
            index_data_before[index][
                "index_metadata"] = self.rest.get_indexer_metadata()

        node_out = self.servers[self.node_out]
        node_out_str = node_out.ip + ":" + str(self.node_port)

        # Network partition out Server Group
        server_group_out = self.input.param("server_group_out", None)
        server_group_nodes = []
        if server_group_out:
            server_group_nodes = server_group_out.split(":")

            for node in server_group_nodes:
                self.start_firewall_on_node(node)

        # start querying
        query = "select name,dept,salary from default where name is not missing and dept='HR' and salary > 120000;"
        t1 = Thread(target=self._run_queries, args=(query, 30,))
        t1.start()
        # rebalance out a indexer node when querying is in progress
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [node_out])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        t1.join()

        self.sleep(30)

        if server_group_nodes:
            for node in server_group_nodes:
                self.stop_firewall_on_node(node)

        # Get Stats and index partition map after rebalance
        node_list = copy.deepcopy(self.node_list)
        node_list.remove(node_out_str)
        self.log.info(node_list)

        index_data_after = {}
        for index in index_names:
            _, total_item_count_after, _ = self.get_stats_for_partitioned_indexes(
                index_name=index, node_list=node_list)
            index_data_after[index] = {}
            index_data_after[index]["item_count"] = total_item_count_after
            index_data_after[index][
                "index_metadata"] = self.rest.get_indexer_metadata()

        for index in index_names:
            # Validate index item count before and after
            self.assertEqual(index_data_before[index]["item_count"],
                             index_data_after[index]["item_count"],
                             "Item count in index do not match after cluster ops.")

            # Validate host list, partition count and partition distribution
            self.assertTrue(
                self.validate_partition_distribution_after_cluster_ops(
                    index, index_data_before[index]["index_metadata"],
                    index_data_after[index]["index_metadata"], [],
                    [node_out]),
                "Partition distribution post cluster ops has some issues")

    def test_partitioned_index_recoverability(self):
        node_out = self.servers[self.node_out]
        create_index_query = "CREATE INDEX idx1 ON default(name,mutated) partition by hash(meta().id) USING GSI"
        if self.num_index_replica:
            create_index_query += " with {{'num_replica':{0}}};".format(
                self.num_index_replica)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        # Allow index to be built completely
        self.sleep(30)

        # Run query
        scan_query = "select name,mutated from default where name > 'a' and mutated >=0;"
        try:
            result_before = self.n1ql_helper.run_cbq_query(query=scan_query, min_output_size=10000000,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("Scan failed")

        # Kill indexer and allow it to recover and rebuild index
        remote = RemoteMachineShellConnection(node_out)
        remote.terminate_process(process_name="indexer")
        self.sleep(30, "Sleep after killing indexer")

        # Run same query again and check if results match from before recovery
        scan_query = "select name,mutated from default where name > 'a' and mutated >=0;"
        try:
            result_after = self.n1ql_helper.run_cbq_query(query=scan_query, min_output_size=10000000,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("Scan failed")

        # Validate if the same count of docs are returned after recovery
        self.assertEqual(result_before["metrics"]["resultCount"], result_after["metrics"]["resultCount"], "No. of rows returned before recovery and after recovery are different")

    def test_backup_restore_partitioned_index(self):
        self._load_emp_dataset(end=self.num_items)

        index_details = []
        index_detail = {}

        index_detail["index_name"] = "idx1"
        index_detail["num_partitions"] = self.num_index_partitions
        index_detail["defer_build"] = False
        index_detail[
            "definition"] = "CREATE INDEX idx1 on default(name,dept) partition by hash(salary) USING GSI"
        index_details.append(index_detail)
        index_detail = {}

        index_detail["index_name"] = "idx2"
        index_detail["num_partitions"] = 64
        index_detail["defer_build"] = False
        index_detail[
            "definition"] = "CREATE INDEX idx2 on default(name,dept) partition by hash(salary) USING GSI with {'num_partition':64}"
        index_details.append(index_detail)
        index_detail = {}

        index_detail["index_name"] = "idx3"
        index_detail["num_partitions"] = self.num_index_partitions
        index_detail["defer_build"] = False
        index_detail[
            "definition"] = "CREATE INDEX idx3 on default(name,dept) partition by hash(salary) USING GSI with {'num_replica':1}"
        index_details.append(index_detail)
        index_detail = {}

        index_detail["index_name"] = "idx4"
        index_detail["num_partitions"] = self.num_index_partitions
        index_detail["defer_build"] = True
        index_detail[
            "definition"] = "CREATE INDEX idx4 on default(name,dept) partition by hash(salary) USING GSI with {'defer_build':true}"
        index_details.append(index_detail)
        index_detail = {}

        try:
            for index in index_details:
                self.n1ql_helper.run_cbq_query(query=index["definition"],
                                               server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("index creation failed with error : {0}".format(str(ex)))

        self.sleep(10)
        index_map = self.get_index_map()
        self.log.info(index_map)

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata Before Build:")
        self.log.info(index_metadata)

        for index_detail in index_details:
            self.assertTrue(
                self.validate_partitioned_indexes(index_detail, index_map,
                                                  index_metadata),
                "Partitioned index created not as expected")

        kv_node = self.get_nodes_from_services_map(service_type="kv",
                                                   get_all_nodes=False)

        self._create_backup(kv_node)

        # Drop and recreate bucket
        self.cluster.bucket_delete(kv_node, bucket="default")
        default_params = self._create_bucket_params(server=self.master,
                                                    size=self.bucket_size,
                                                    replicas=self.num_replicas)

        self.cluster.create_default_bucket(default_params)

        if self.node_out > 0:
            node_out = self.servers[self.node_out]
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init],
                [], [node_out])

            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached,
                            "rebalance failed, stuck or did not complete")
            rebalance.result()

        # Restore backup
        self._create_restore(kv_node)

        self.sleep(60)

        # Validate all indexes restored correctly
        index_map = self.get_index_map()
        self.log.info(index_map)

        if self.node_out > 0:
            if self.node_out == self.index_servers[0]:
                rest = RestConnection(self.index_servers[1])
            else:
                rest = self.rest
        else:
            rest = self.rest


        index_metadata = rest.get_indexer_metadata()
        self.log.info("Indexer Metadata After Build:")
        self.log.info(index_metadata)

        # After restore, all indexes are going to be in unbuilt. So change the expected state of indexes.
        for index in index_details:
            index["defer_build"] = True

        for index_detail in index_details:
            self.assertTrue(
                self.validate_partitioned_indexes(index_detail, index_map,
                                                  index_metadata),
                "Partitioned index created not as expected")

    def test_backup_partitioned_index_with_failed_node(self):
        self._load_emp_dataset(end=self.num_items)
        node_out = self.servers[self.node_out]

        index_details = []
        index_detail = {}

        index_detail["index_name"] = "idx1"
        index_detail["num_partitions"] = self.num_index_partitions
        index_detail["defer_build"] = False
        index_detail[
            "definition"] = "CREATE INDEX idx1 on default(name,dept) partition by hash(salary) USING GSI"
        index_details.append(index_detail)
        index_detail = {}

        index_detail["index_name"] = "idx2"
        index_detail["num_partitions"] = 64
        index_detail["defer_build"] = False
        index_detail[
            "definition"] = "CREATE INDEX idx2 on default(name,dept) partition by hash(salary) USING GSI with {'num_partition':64}"
        index_details.append(index_detail)
        index_detail = {}

        index_detail["index_name"] = "idx3"
        index_detail["num_partitions"] = self.num_index_partitions
        index_detail["defer_build"] = False
        index_detail[
            "definition"] = "CREATE INDEX idx3 on default(name,dept) partition by hash(salary) USING GSI with {'num_replica':1}"
        index_details.append(index_detail)
        index_detail = {}

        index_detail["index_name"] = "idx4"
        index_detail["num_partitions"] = self.num_index_partitions
        index_detail["defer_build"] = True
        index_detail[
            "definition"] = "CREATE INDEX idx4 on default(name,dept) partition by hash(salary) USING GSI with {'defer_build':true}"
        index_details.append(index_detail)
        index_detail = {}

        try:
            for index in index_details:
                self.n1ql_helper.run_cbq_query(query=index["definition"],
                                               server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("index creation failed with error : {0}".format(str(ex)))

        self.sleep(10)
        index_map = self.get_index_map()
        self.log.info(index_map)

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata Before Build:")
        self.log.info(index_metadata)

        for index_detail in index_details:
            self.assertTrue(
                self.validate_partitioned_indexes(index_detail, index_map,
                                                  index_metadata),
                "Partitioned index created not as expected")

        kv_node = self.get_nodes_from_services_map(service_type="kv",
                                                   get_all_nodes=False)

        try:
            # Stop couchbase on indexer node before taking backup if test config specifies it
            remote = RemoteMachineShellConnection(node_out)
            remote.stop_couchbase()
            self.sleep(30, "Allow node to be marked as a failed node")
            self._create_backup(kv_node)
        except Exception as ex:
            self.log.info(str(ex))
        finally:
            remote = RemoteMachineShellConnection(node_out)
            remote.start_couchbase()

    def test_restore_partitioned_index_with_failed_node(self):
        self._load_emp_dataset(end=self.num_items)
        node_out = self.servers[self.node_out]

        index_details = []
        index_detail = {}

        index_detail["index_name"] = "idx1"
        index_detail["num_partitions"] = self.num_index_partitions
        index_detail["defer_build"] = False
        index_detail[
            "definition"] = "CREATE INDEX idx1 on default(name,dept) partition by hash(salary) USING GSI"
        index_details.append(index_detail)
        index_detail = {}

        index_detail["index_name"] = "idx2"
        index_detail["num_partitions"] = 64
        index_detail["defer_build"] = False
        index_detail[
            "definition"] = "CREATE INDEX idx2 on default(name,dept) partition by hash(salary) USING GSI with {'num_partition':64}"
        index_details.append(index_detail)
        index_detail = {}

        index_detail["index_name"] = "idx3"
        index_detail["num_partitions"] = self.num_index_partitions
        index_detail["defer_build"] = False
        index_detail[
            "definition"] = "CREATE INDEX idx3 on default(name,dept) partition by hash(salary) USING GSI with {'num_replica':1}"
        index_details.append(index_detail)
        index_detail = {}

        index_detail["index_name"] = "idx4"
        index_detail["num_partitions"] = self.num_index_partitions
        index_detail["defer_build"] = True
        index_detail[
            "definition"] = "CREATE INDEX idx4 on default(name,dept) partition by hash(salary) USING GSI with {'defer_build':true}"
        index_details.append(index_detail)
        index_detail = {}

        try:
            for index in index_details:
                self.n1ql_helper.run_cbq_query(query=index["definition"],
                                               server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("index creation failed with error : {0}".format(str(ex)))

        self.sleep(10)
        index_map = self.get_index_map()
        self.log.info(index_map)

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata Before Build:")
        self.log.info(index_metadata)

        for index_detail in index_details:
            self.assertTrue(
                self.validate_partitioned_indexes(index_detail, index_map,
                                                  index_metadata),
                "Partitioned index created not as expected")

        kv_node = self.get_nodes_from_services_map(service_type="kv",
                                                   get_all_nodes=False)

        self._create_backup(kv_node)

        # Drop and recreate bucket
        self.cluster.bucket_delete(kv_node, bucket="default")
        default_params = self._create_bucket_params(server=self.master,
                                                    size=self.bucket_size,
                                                    replicas=self.num_replicas)

        self.cluster.create_default_bucket(default_params)

        try:
            # Restore backup
            # Stop couchbase on indexer node before restoring backup if test config specifies it
            remote = RemoteMachineShellConnection(node_out)
            remote.stop_couchbase()
            self.sleep(30, "Allow node to be marked as a failed node")
            self._create_restore(kv_node)
        except Exception as ex:
            self.log.info(str(ex))
        finally:
            remote = RemoteMachineShellConnection(node_out)
            remote.start_couchbase()

    def test_backup_restore_partitioned_index_default_num_partitions(self):
        self._load_emp_dataset(end=self.num_items)

        index_details = []
        index_detail = {}

        index_detail["index_name"] = "idx1"
        index_detail["num_partitions"] = self.num_index_partitions
        index_detail["defer_build"] = False
        index_detail[
            "definition"] = "CREATE INDEX idx1 on default(name,dept) partition by hash(salary) USING GSI"
        index_detail["num_partitions_post_restore"] = 8
        index_details.append(index_detail)
        index_detail = {}

        index_detail["index_name"] = "idx2"
        index_detail["num_partitions"] = 64
        index_detail["defer_build"] = False
        index_detail[
            "definition"] = "CREATE INDEX idx2 on default(name,dept) partition by hash(salary) USING GSI with {'num_partition':64}"
        index_detail["num_partitions_post_restore"] = 64
        index_details.append(index_detail)
        index_detail = {}

        try:
            for index in index_details:
                self.n1ql_helper.run_cbq_query(query=index["definition"],
                                               server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("index creation failed with error : {0}".format(str(ex)))

        self.sleep(10)
        index_map = self.get_index_map()
        self.log.info(index_map)

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata Before Build:")
        self.log.info(index_metadata)

        for index_detail in index_details:
            self.assertTrue(
                self.validate_partitioned_indexes(index_detail, index_map,
                                                  index_metadata),
                "Partitioned index created not as expected")

        kv_node = self.get_nodes_from_services_map(service_type="kv",
                                                   get_all_nodes=False)

        self._create_backup(kv_node)

        # Drop and recreate bucket
        self.cluster.bucket_delete(kv_node, bucket="default")
        default_params = self._create_bucket_params(server=self.master,
                                                    size=self.bucket_size,
                                                    replicas=self.num_replicas)

        self.cluster.create_default_bucket(default_params)

        # Set default number of partitions
        self.rest.set_index_settings(
            {"indexer.numPartitions": 4})

        # Change expected num of partitions
        for index in index_details:
            index["num_partitions"] = index["num_partitions_post_restore"]

        # Restore backup
        self._create_restore(kv_node)

        self.sleep(60)

        # Validate all indexes restored correctly
        index_map = self.get_index_map()
        self.log.info(index_map)

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata After Build:")
        self.log.info(index_metadata)

        # After restore, all indexes are going to be in unbuilt. So change the expected state of indexes.
        for index in index_details:
            index["defer_build"] = True

        for index_detail in index_details:
            self.assertTrue(
                self.validate_partitioned_indexes(index_detail, index_map,
                                                  index_metadata),
                "Partitioned index created not as expected")

    def get_stats_for_partitioned_indexes(self, bucket_name="default",
                                          index_name=None, node_list=None):
        if node_list == None:
            node_list = self.node_list

        bucket_item_count = self.get_item_count(self.servers[0], bucket_name)

        index_stats = self.get_index_stats(perNode=True)
        total_item_count = 0
        total_items_processed = 0
        for node in node_list:
            if not index_name:
                index_names = []
                for key in index_stats[node][bucket_name]:
                    index_names.append(key)
                index_name = index_names[0]
            try:
                total_item_count += index_stats[node][bucket_name][index_name][
                    "items_count"]
                total_items_processed = \
                    index_stats[node][bucket_name][index_name][
                        "num_docs_processed"]
            except Exception as ex:
                self.log.info(str(ex))

        self.log.info(
            "Index {0} : Total Item Count={1} Total Items Processed={2}".format(
                index_name, str(total_item_count), str(total_items_processed)))

        return (bucket_item_count, total_item_count, total_items_processed)

    # Description : Validate index metadata : num_partitions, index status, index existence
    def validate_partitioned_indexes(self, index_details, index_map,
                                     index_metadata, skip_numpartitions_check=False):

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

        if skip_numpartitions_check:
            return isIndexPresent and isDeferBuildCorrect
        else:
            return isIndexPresent and isNumPartitionsCorrect and isDeferBuildCorrect

    # Description : Checks if same host contains same partitions from different replica, and also if for each replica, if the partitions are distributed across nodes
    def validate_partition_map(self, index_metadata, index_name, num_replica, num_partitions,dropped_replica=False, replicaId=0):
        index_names = []
        index_names.append(index_name)
        hosts = []

        # hosts = index_metadata["status"][0]["hosts"]
        for index in index_metadata['status']:
            for host in index['hosts']:
                if host not in hosts:
                    hosts.append(host)


        for i in range(1, num_replica + 1):
            if dropped_replica:
                if not i == replicaId:
                    index_names.append(index_name + " (replica {0})".format(str(i)))
                else:
                    dropped_replica_name = index_name + " (replica {0})".format(str(i))
            else:
                index_names.append(index_name + " (replica {0})".format(str(i)))

        partition_validation_per_host = True
        for host in hosts:
            pmap_host = []
            for idx_name in index_names:
                for index in index_metadata["status"]:
                    if (index["name"] == idx_name) and (host in index["hosts"]):
                        pmap_host += index["partitionMap"][host]

            self.log.info(
                "List of partitions on {0} : {1}".format(host, pmap_host))
            if len(set(pmap_host)) != len(pmap_host):
                partition_validation_per_host &= False
                self.log.info(
                    "Partitions on {0} for all replicas are not correct, host contains duplicate partitions".format(host))

        partitions_distributed_for_index = True
        for idx_name in index_names:
            for index in index_metadata["status"]:
                if index["name"] == idx_name:
                    totalPartitions = 0
                    for host in hosts:
                        if host in index["partitionMap"]:
                            totalPartitions += len(index["partitionMap"][host])

                    partitions_distributed_for_index &= (
                        totalPartitions == num_partitions)
                if dropped_replica:
                    if index['name'] == dropped_replica_name:
                        partitions_distributed_for_index = False
        return partition_validation_per_host & partitions_distributed_for_index

    def validate_partition_distribution_after_cluster_ops(self, index_name,
                                                          map_before_rebalance,
                                                          map_after_rebalance,
                                                          nodes_in, nodes_out):

        # Check for number of partitions before and after rebalance
        # Check the host list before rebalance and after rebalance, and see if the incoming or outgoing node is added/removed from the host list
        # Check for partition distribution across all indexer nodes

        for index in map_before_rebalance["status"]:
            if index["name"] == index_name:
                host_list_before = index["hosts"]
                num_partitions_before = index["numPartition"]
                partition_map_before = index["partitionMap"]

        for index in map_after_rebalance["status"]:
            if index["name"] == index_name:
                host_list_after = index["hosts"]
                num_partitions_after = index["numPartition"]
                partition_map_after = index["partitionMap"]

        is_num_partitions_equal = False
        if num_partitions_before == num_partitions_after:
            is_num_partitions_equal = True
        else:
            self.log.info(
                "Number of partitions before and after cluster operations is not equal. Some partitions missing/extra.")
            self.log.info(
                "Num Partitions Before : {0}, Num Partitions After : {1}".format(
                    num_partitions_before, num_partitions_after))

        expected_host_list_after = copy.deepcopy(host_list_before)
        for node in nodes_in:
            node_str = node.ip + ":" + str(self.node_port)
            expected_host_list_after.append(node_str)

        for node in nodes_out:
            node_str = node.ip + ":" + str(self.node_port)
            if node_str in expected_host_list_after:
                expected_host_list_after.remove(node_str)

        is_node_list_correct = False
        if (expected_host_list_after.sort() == host_list_after.sort()):
            is_node_list_correct = True
        else:
            self.log.info(
                "Host list for index is not expected after cluster operations.")
            self.log.info("Expected Nodes : {0}, Actual nodes : {1}",
                          format(str(expected_host_list_after),
                                 str(host_list_after)))

        is_partitions_distributed = False
        pmap_host_list = list(partition_map_after.keys())
        if pmap_host_list.sort() == host_list_after.sort():
            is_partitions_distributed = True
        else:
            self.log.info(
                "Partitions not distributed correctly post cluster ops")

        return is_num_partitions_equal & is_node_list_correct & is_partitions_distributed

    def get_num_partitions_for_index(self, index_map, index_name):
        num_partitions = 0
        for index_map_item in index_map["status"]:
            if index_map_item["name"] == index_name:
                num_partitions = index_map_item["numPartition"]

        if num_partitions == 0:
            self.log.info("Index not found, or some other issue")
        else:
            return num_partitions

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
                        randval = random.randint(1, len(index_fields)-1)
                        key = index_fields[randval]
                    else:
                        key = index_fields[0]

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
                    key = "meta().id"

                if partition_key_type == partition_key_type_list[4]:
                    key = "SUBSTR(meta().id, POSITION(meta().id, '__')+2)"

                if ((key is not None) or (key != "")) and (key not in partition_keys):
                    partition_keys.append(key)
                    self.log.info("Partition Keys : {0}, Partition Key Type : {1}".format(key, partition_key_type))

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
                    if self.gsi_type == "memory_optimized":
                        num_partitions = random.randint(4, 20)
                    else:
                        max_num_partitions = self.gsi_util_obj.get_max_num_partitions()
                        num_partitions = random.randint(4, max_num_partitions)
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
            if num_partitions == 0:
                num_partitions = 8
            index_detail["num_partitions"] = num_partitions
            index_detail["num_replica"] = num_replica
            index_detail["defer_build"] = defer_build
            index_detail["index_definition"] = create_index_statement
            index_detail["nodes"] = nodes

            if key is not None or key != "":
                index_details.append(index_detail)
            else:
                self.log.info(
                    "Generated a malformed index definition. Discarding it.")

        return index_details

    def validate_query_plan(self, plan, index_name, num_spans=0, limit=0,
                            offset=0, projection_list=[], index_order_list=[]):
        span_pushdown = False
        limit_pushdown = False
        offset_pushdown = False
        projection_pushdown = False
        sorting_pushdown = False

        index_section_found = False
        plan_index_section = {}
        for plan_child in plan["~children"]:
            if "index" in plan_child:
                index_section_found = True
                plan_index_section = plan_child
                break;

        for plan_child in plan["~children"]:
            if not index_section_found:
                for plan_child_child in plan_child["~children"]:
                    if "index" in plan_child_child:
                        index_section_found = True
                        plan_index_section = plan_child_child
                        break;
            else:
                break

        if index_section_found:
            if plan_index_section["index"] == index_name:
                if num_spans > 0:
                    if "spans" in plan_index_section:
                        if len(plan_index_section["spans"][0][
                                   "range"]) != num_spans:
                            self.log.info(
                                "Looks like all spans not pushed down to indexer. Spans pushed down to indexer = %s",
                                len(plan_index_section["spans"]["range"]))
                        else:
                            self.log.info(
                                "All spans pushed down to indexer")
                            span_pushdown = True
                    else:
                        self.log.info("Spans not pushed down to indexer")

                if limit > 0:
                    if "limit" in plan_index_section:
                        if int(plan_index_section["limit"]) != limit:
                            self.log.info(
                                "Limit not correctly pushed down to indexer")
                        else:
                            self.log.info(
                                "Limit pushed down to indexer")
                            limit_pushdown = True
                    else:
                        self.log.info("Limit not pushed down to indexer")

                if offset > 0:
                    if "offset" in plan_index_section:
                        if int(plan_index_section["offset"]) != offset:
                            self.log.info(
                                "Offset not correctly pushed down to indexer")
                        else:
                            self.log.info(
                                "Offset pushed down to indexer")
                            offset_pushdown = True
                    else:
                        self.log.info("Offset not pushed down to indexer")

                if projection_list:
                    if "index_projection" in plan_index_section:
                        if plan_index_section["index_projection"][
                            "entry_keys"] != projection_list:
                            self.log.info(
                                "Projection not correctly pushed down to indexer")
                        else:
                            self.log.info(
                                "Projection pushed down to indexer")
                            projection_pushdown = True

                if index_order_list:
                    if "index_order" in plan_index_section:
                        if plan_index_section[
                            "index_order"] != index_order_list:

                            self.log.info(
                                "Sorting not correctly pushed down to indexer")
                        else:
                            self.log.info(
                                "Sorting pushed down to indexer")
                            sorting_pushdown = True

        return span_pushdown, limit_pushdown, offset_pushdown, projection_pushdown, sorting_pushdown

    def _load_emp_dataset(self, op_type="create", expiration=0, start=0,
                          end=1000):
        # Load Emp Dataset
        self.cluster.bucket_flush(self.master)

        if end > 0:
            self._kv_gen = JsonDocGenerator("emp_",
                                            encoding="utf-8",
                                            start=start,
                                            end=end)
            gen = copy.deepcopy(self._kv_gen)

            self._load_bucket(self.buckets[0], self.servers[0], gen, op_type,
                              expiration)

    def _run_queries(self, query, count=10):
        for i in range(0, count):
            try:
                self.n1ql_helper.run_cbq_query(query=query,
                                               server=self.n1ql_node)
            except Exception as ex:
                self.log.info(str(ex))
                raise Exception("query failed")
            self.sleep(1)

    def _saturate_indexer_memory(self, index_server=None):
        cnt = 0
        step = 100000
        docs = 100000
        while cnt < 50:
            if self.gsi_type == "memory_optimized":
                if self._validate_indexer_status_oom(index_server):
                    self.log.info("OOM on index server is achieved")
                    return True
            elif self.gsi_type == "plasma":
                if self._validate_indexer_in_dgm(index_server):
                    self.log.info("DGM on index server is achieved")
                    return True

            for task in self.kv_mutations(docs, start=docs - step):
                task.result()
            self.sleep(5)
            cnt += 1
            docs += step
        return False

    def _validate_indexer_status_oom(self, index_server=None):
        if not index_server:
            index_server = self.get_nodes_from_services_map(
                service_type="index", get_all_nodes=False)
        rest = RestConnection(index_server)
        index_stats = rest.get_indexer_stats()
        self.log.info(index_stats["indexer_state"])
        if index_stats["indexer_state"].lower() == "paused":
            return True
        else:
            return False

    def _validate_indexer_in_dgm(self, index_server=None):
        indexer_rest = RestConnection(index_server)
        content = indexer_rest.get_index_storage_stats()
        for index in list(content.values()):
            for stats in list(index.values()):
                if stats["MainStore"]["resident_ratio"] >= 1.00:
                    return False
            return True

    def kv_mutations(self, docs=1, start=0):
        self.log.info("Inside kv_mutations")
        if not docs:
            docs = self.docs_per_day
        gens_load = self.generate_docs(docs, start=start)
        self.full_docs_list = self.generate_full_docs_list(gens_load)
        self.gen_results = TuqGenerators(self.log, self.full_docs_list)
        tasks = self.async_load(generators_load=gens_load, op_type="create",
                                batch_size=1000)
        return tasks
