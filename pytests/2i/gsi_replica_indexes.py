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


class GSIReplicaIndexesTests(BaseSecondaryIndexingTests, QueryHelperTests,
                             NodeHelper):
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
        if self.num_replicas > 0:
            self.n1ql_helper.verify_replica_indexes(index_map,
                                                    self.num_replicas)
