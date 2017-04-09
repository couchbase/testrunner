from datetime import datetime

from base_2i import BaseSecondaryIndexingTests, log
from membase.api.rest_client import RestConnection, RestHelper
import random
import threading
from lib import testconstants
from lib.couchbase_helper.query_definitions import SQLDefinitionGenerator
from lib.couchbase_helper.tuq_generators import TuqGenerators
from lib.remote.remote_util import RemoteMachineShellConnection
from pytests.ent_backup_restore.enterprise_backup_restore_base import \
    EnterpriseBackupRestoreBase, Backupset
from pytests.fts.fts_base import NodeHelper
from pytests.query_tests_helper import QueryHelperTests


class GSIReplicaIndexesTests(BaseSecondaryIndexingTests):
    def setUp(self):
        super(GSIReplicaIndexesTests, self).setUp()
        self.rest = RestConnection(self.servers[0])
        self.n1ql_server = self.get_nodes_from_services_map(service_type="n1ql",
                                                            get_all_nodes=False)
        self.create_primary_index = False
        shell = RemoteMachineShellConnection(self.servers[0])
        info = shell.extract_remote_info().type.lower()
        if info == 'linux':
            self.cli_command_location = testconstants.LINUX_COUCHBASE_BIN_PATH
        elif info == 'windows':
            self.cmd_ext = ".exe"
            self.cli_command_location = testconstants.WIN_COUCHBASE_BIN_PATH_RAW
        elif info == 'mac':
            self.cli_command_location = testconstants.MAC_COUCHBASE_BIN_PATH
        else:
            raise Exception("OS not supported.")

    def tearDown(self):
        super(GSIReplicaIndexesTests, self).tearDown()

    def test_create_replica_index_with_num_replica(self):
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(
            self.num_replicas)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes(index_map,
                                                    self.num_replicas)

    def test_create_replica_index_with_node_list(self):
        nodes = self._get_node_list()
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}}};".format(nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes(index_map,
                                                    len(nodes)-1, nodes)

    def test_create_replica_index_with_num_replicas_and_node_list(self):
        nodes = self._get_node_list()
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0},'nodes': {1}}};".format(
            self.num_replicas,nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes(index_map,
                                                    self.num_replicas, nodes)

    def test_default_num_indexes(self):
        self.rest.set_indexer_num_replica(self.num_replicas)
        nodes = self._get_node_list()
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age)"

        if self.override_default_num_replica_with_nodes and self.override_default_num_replica_with_num:
            create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0},'nodes': {1}}};".format(
                self.override_default_num_replica_with_num, nodes)

        elif self.override_default_num_replica_with_nodes:
            create_index_query += "USING GSI  WITH {{'nodes': {0}}};".format(nodes)

        elif self.override_default_num_replica_with_num:
            create_index_query += "USING GSI  WITH {{'num_replica': {0}}};".format(self.override_default_num_replica_with_num)

        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        expected_num_replicas = self.num_replicas
        if self.override_default_num_replica_with_num > 0:
            expected_num_replicas = self.override_default_num_replica_with_num
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes(index_map, expected_num_replicas, nodes)

        # Reset the default value for num_replica
        self.rest.set_indexer_num_replica(0)

    def test_build_index_with_replica(self):
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}, 'defer_build': true}};".format(
            self.num_replicas)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes(index_map,
                                                    self.num_replicas)

        self.n1ql_helper.verify_replica_indexes_build_status(index_map, self.num_replicas, defer_build=True)

        build_index_query = "BUILD INDEX on `default`(" + index_name_prefix +")"

        try:
            self.n1ql_helper.run_cbq_query(query=build_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index building did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index building failed as expected")

        self.sleep(30)
        index_map = self.get_index_map()
        self.n1ql_helper.verify_replica_indexes_build_status(index_map,
                                                             self.num_replicas,
                                                             defer_build=False)

    def test_drop_index_with_replica(self):
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}, 'defer_build': {1}}};".format(
            self.num_replicas, self.defer_build)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes(index_map,
                                                    self.num_replicas)

        drop_index_query = "DROP INDEX `default`." + index_name_prefix

        try:
            self.n1ql_helper.run_cbq_query(query=drop_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "Drop index did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Drop index failed as expected")

        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info("Index map after drop index: %s",index_map)
        if not index_map == {}:
            self.fail("Indexes not dropped correctly")

    def _get_node_list(self):
        #1. Parse node string
        nodes = []
        invalid_ip = "10.111.151.256"
        if self.nodes:
            nodes = self.nodes.split(":")
            for i in range(0,len(nodes)):
                if nodes[i] not in ("empty", "invalid"):
                    nodes[i] = self.servers[int(nodes[i])].ip + ":" + self.servers[int(nodes[i])].port
                elif nodes[i] == "invalid":
                    nodes[i] = invalid_ip + ":" + "8091"
                elif nodes[i] == "empty":
                    nodes[i] = ""
        else:
            self.log.info("No nodes in list")

        return nodes
