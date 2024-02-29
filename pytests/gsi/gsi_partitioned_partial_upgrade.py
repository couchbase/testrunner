from remote.remote_util import RemoteMachineShellConnection
from .upgrade_gsi import UpgradeSecondaryIndex
from membase.api.rest_client import RestConnection, RestHelper
import time

UPGRADE_VERS = ["5.0.0", "5.0.1", "5.1.0"]

class UpgradePartialParititionedIndex(UpgradeSecondaryIndex):
    def setUp(self):
        super(UpgradePartialParititionedIndex, self).setUp()
        self.partial_partitioned = self.input.param("partial_partitioned", False)
        self.partial_index = self.input.param("partial_index", False)
        self.secondary_index = self.input.param("secondary_index", False)
        self.mixed_mode_index = self.input.param("mixed_mode_index", False)
        self.mixed_mode_kv = self.input.param("mixed_mode_kv", False)

    def tearDown(self):
        super(UpgradePartialParititionedIndex, self).tearDown()

    def test_full_upgrade(self):
        # First reproduce the bug seen
        if self.partial_partitioned:
            create_index_query = "CREATE INDEX idx_pending ON {0}(createdDateTime) PARTITION BY HASH(meta().id) WHERE status = 'PENDING'".format(
                self.bucket_name)
            self.n1ql_helper.run_cbq_query(query=create_index_query,server=self.n1ql_node)
            create_index_query = "CREATE INDEX idx_success ON {0}(createdDateTime) PARTITION BY HASH(meta().id) WHERE status = 'SUCCESS'".format(
                self.bucket_name)
            self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
            create_index_query = "CREATE INDEX idx_error ON {0}(createdDateTime) PARTITION BY HASH(meta().id) WHERE status = 'ERROR'".format(
                self.bucket_name)
            self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
        elif self.partial_index:
            create_index_query = "CREATE INDEX idx_pending ON {0}(createdDateTime) WHERE status = 'PENDING'".format(
                self.bucket_name)
            self.n1ql_helper.run_cbq_query(query=create_index_query,server=self.n1ql_node)
            create_index_query = "CREATE INDEX idx_success ON {0}(createdDateTime) WHERE status = 'SUCCESS'".format(
                self.bucket_name)
            self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
            create_index_query = "CREATE INDEX idx_error ON {0}(createdDateTime) WHERE status = 'ERROR'".format(
                self.bucket_name)
            self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
        elif self.secondary_index:
            create_index_query = "CREATE INDEX idx_pending ON {0}(createdDateTime)".format(
                self.bucket_name)
            self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
        else:
            create_index_query = "CREATE INDEX idx_pending ON {0}(createdDateTime) PARTITION BY HASH(meta().id)".format(
                self.bucket_name)
            self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
        self.wait_until_indexes_online()

        insert1 = {"createdDateTime": 4, "status": "PENDING"}
        insert2 = {"createdDateTime": 5, "status": "PENDING"}
        insert3 = {"createdDateTime": 6, "status": "PENDING"}
        insert4 = {"createdDateTime": 7, "status": "PENDING"}

        self.n1ql_helper.run_cbq_query(
            query="INSERT INTO `{0}` ( KEY, VALUE ) VALUES ('K1', {1})".format(self.bucket_name, insert1),
            server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(
            query="INSERT INTO `{0}` ( KEY, VALUE ) VALUES ('K2', {1})".format(self.bucket_name, insert2),
            server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(
            query="INSERT INTO `{0}` ( KEY, VALUE ) VALUES ('K3', {1})".format(self.bucket_name, insert3),
            server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(
            query="INSERT INTO `{0}` ( KEY, VALUE ) VALUES ('K4', {1})".format(self.bucket_name, insert4),
            server=self.n1ql_node)

        time.sleep(40)

        pending_results = self.n1ql_helper.run_cbq_query('SELECT meta().id FROM `{0}` WHERE createdDateTime BETWEEN 1 AND 10 AND status = "PENDING" order by meta().id'.format(self.bucket_name))
        success_results = self.n1ql_helper.run_cbq_query('SELECT meta().id FROM `{0}` WHERE createdDateTime BETWEEN 1 AND 10 AND status = "SUCCESS" order by meta().id'.format(self.bucket_name))
        error_results = self.n1ql_helper.run_cbq_query('SELECT meta().id FROM `{0}` WHERE createdDateTime BETWEEN 1 AND 10 AND status = "ERROR" order by meta().id'.format(self.bucket_name))

        initial_expected = [{u'id': u'K1'}, {u'id': u'K2'}, {u'id': u'K3'}, {u'id': u'K4'}]
        self.assertEqual(pending_results['results'], initial_expected)
        self.assertEqual(success_results['metrics']['resultCount'], 0)
        self.assertEqual(error_results['metrics']['resultCount'], 0)

        update_success = ["K1","K2","K3"]
        update_error = "K4"

        self.n1ql_helper.run_cbq_query(
            query="UPDATE `{0}` USE KEYS {1} SET status = 'SUCCESS'".format(self.bucket_name, update_success),
            server=self.n1ql_node)

        self.n1ql_helper.run_cbq_query(
            query="UPDATE `{0}` USE KEYS '{1}' SET status = 'ERROR'".format(self.bucket_name, update_error),
            server=self.n1ql_node)

        time.sleep(5)

        pending_results = self.n1ql_helper.run_cbq_query('SELECT meta().id FROM `{0}` WHERE createdDateTime BETWEEN 1 AND 10 AND status = "PENDING" order by meta().id'.format(self.bucket_name))
        success_results = self.n1ql_helper.run_cbq_query('SELECT meta().id FROM `{0}` WHERE createdDateTime BETWEEN 1 AND 10 AND status = "SUCCESS" order by meta().id'.format(self.bucket_name))
        error_results = self.n1ql_helper.run_cbq_query('SELECT meta().id FROM `{0}` WHERE createdDateTime BETWEEN 1 AND 10 AND status = "ERROR" order by meta().id'.format(self.bucket_name))

        success_expected = [{u'id': u'K1'}, {u'id': u'K2'}, {u'id': u'K3'}]
        error_expected = [{u'id': u'K4'}]

        # mixed_mode_index means upgrade the indexer nodes leave the kv node old
        if self.mixed_mode_index:
            upgrade_nodes = self.servers[1:self.nodes_init]
        # mixed_mode_kv means upgrade the kv node and leave indexer nodes old
        elif self.mixed_mode_kv:
            upgrade_nodes = self.servers[:1]
        # full upgrade
        else:
            upgrade_nodes = self.servers[:self.nodes_init]

        # Now upgrade to a version that has the fix
        for server in upgrade_nodes:
            remote = RemoteMachineShellConnection(server)
            remote.stop_server()
            remote.disconnect()
            upgrade_threads = self._async_update(self.upgrade_to, [server])
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            self.upgrade_servers.append(server)
        self.sleep(100)
        msg = "Cluster is not healthy after upgrade"
        self.assertTrue(self.wait_until_cluster_is_healthy(), msg)
        self.log.info("Cluster is healthy")
        self.add_built_in_server_user()
        self.sleep(20)
        try:
            # Depending on the order of the async upgrade indexes could be lost (since it is offline upgrade) For the specific fix this is fine as indexes need to be recreated anyway
            if self.partial_partitioned:
                create_index_query = "CREATE INDEX idx_pending ON {0}(createdDateTime) PARTITION BY HASH(meta().id) WHERE status = 'PENDING'".format(
                    self.bucket_name)
                self.n1ql_helper.run_cbq_query(query=create_index_query,server=self.n1ql_node)
                create_index_query = "CREATE INDEX idx_success ON {0}(createdDateTime) PARTITION BY HASH(meta().id) WHERE status = 'SUCCESS'".format(
                    self.bucket_name)
                self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
                create_index_query = "CREATE INDEX idx_error ON {0}(createdDateTime) PARTITION BY HASH(meta().id) WHERE status = 'ERROR'".format(
                    self.bucket_name)
                self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
            elif self.partial_index:
                create_index_query = "CREATE INDEX idx_pending ON {0}(createdDateTime) WHERE status = 'PENDING'".format(
                    self.bucket_name)
                self.n1ql_helper.run_cbq_query(query=create_index_query,server=self.n1ql_node)
                create_index_query = "CREATE INDEX idx_success ON {0}(createdDateTime) WHERE status = 'SUCCESS'".format(
                    self.bucket_name)
                self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
                create_index_query = "CREATE INDEX idx_error ON {0}(createdDateTime) WHERE status = 'ERROR'".format(
                    self.bucket_name)
                self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
            elif self.secondary_index:
                create_index_query = "CREATE INDEX idx_pending ON {0}(createdDateTime)".format(
                    self.bucket_name)
                self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
            else:
                create_index_query = "CREATE INDEX idx_pending ON {0}(createdDateTime) PARTITION BY HASH(meta().id)".format(
                    self.bucket_name)
                self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
        except Exception as e:
            self.log.error(str(e))
        self.wait_until_indexes_online()
        self.log.info("All indexes are online")

        # Check behavior post-upgrade
        pending_results = self.n1ql_helper.run_cbq_query(
            'SELECT meta().id FROM `{0}` WHERE createdDateTime BETWEEN 1 AND 10 AND status = "PENDING" order by meta().id'.format(self.bucket_name))
        success_results = self.n1ql_helper.run_cbq_query(
            'SELECT meta().id FROM `{0}` WHERE createdDateTime BETWEEN 1 AND 10 AND status = "SUCCESS" order by meta().id'.format(self.bucket_name))
        error_results = self.n1ql_helper.run_cbq_query(
            'SELECT meta().id FROM `{0}` WHERE createdDateTime BETWEEN 1 AND 10 AND status = "ERROR" order by meta().id'.format(self.bucket_name))

        self.assertEqual(success_results['results'], success_expected)
        self.assertEqual(error_results['results'], error_expected)

        # Update check
        update_pending = ["K1","K2"]
        keylist = ["K1","K2","K3","K4"]

        # Verify after each mutatation after fix
        for key in keylist:
            if key in update_pending:
                self.n1ql_helper.run_cbq_query(
                    query="UPDATE `{0}` USE KEYS '{1}' SET status = 'PENDING'".format(self.bucket_name, key),
                    server=self.n1ql_node)
                time.sleep(.5)
                pending_results = self.n1ql_helper.run_cbq_query(
                    'SELECT meta().id FROM `{0}` WHERE createdDateTime BETWEEN 1 AND 10 AND status = "PENDING" order by meta().id'.format(
                        self.bucket_name))
                success_results = self.n1ql_helper.run_cbq_query(
                    'SELECT meta().id FROM `{0}` WHERE createdDateTime BETWEEN 1 AND 10 AND status = "SUCCESS" order by meta().id'.format(
                        self.bucket_name))
                if key == "K1":
                    pending_expected = [{u'id': u'K1'}]
                    success_expected = [{u'id': u'K2'}, {u'id': u'K3'}]
                    self.assertEqual(pending_results['results'], pending_expected)
                    self.assertEqual(success_results['results'], success_expected)
                if key == "K2":
                    pending_expected = [{u'id': u'K1'},{u'id': u'K2'}]
                    success_expected = [{u'id': u'K3'}]
                    self.assertEqual(pending_results['results'], pending_expected)
                    self.assertEqual(success_results['results'], success_expected)

        self.n1ql_helper.run_cbq_query(
            query="UPDATE `{0}` USE KEYS {1} SET status = 'PENDING'".format(self.bucket_name, update_pending),
            server=self.n1ql_node)

        time.sleep(40)
        pending_results = self.n1ql_helper.run_cbq_query(
            'SELECT meta().id FROM `{0}` WHERE createdDateTime BETWEEN 1 AND 10 AND status = "PENDING" order by meta().id'.format(self.bucket_name))
        success_results = self.n1ql_helper.run_cbq_query(
            'SELECT meta().id FROM `{0}` WHERE createdDateTime BETWEEN 1 AND 10 AND status = "SUCCESS" order by meta().id'.format(self.bucket_name))
        error_results = self.n1ql_helper.run_cbq_query(
            'SELECT meta().id FROM `{0}` WHERE createdDateTime BETWEEN 1 AND 10 AND status = "ERROR" order by meta().id'.format(self.bucket_name))

        pending_expected = [{u'id': u'K1'}, {u'id': u'K2'}]
        success_expected = [{u'id': u'K3'}]
        error_expected = [{u'id': u'K4'}]

        self.assertEqual(pending_results['results'], pending_expected)
        self.assertEqual(success_results['results'], success_expected)
        self.assertEqual(error_results['results'], error_expected)
        try:
            self.n1ql_helper.run_cbq_query(query="CREATE PRIMARY INDEX on `standard_bucket0`", server=self.n1ql_node)
            self.wait_until_indexes_online()
        except Exception as e:
            self.log.error(str(e))

        # Delete check
        self.n1ql_helper.run_cbq_query(
            query="DELETE FROM `{0}` WHERE meta().id = '{1}'".format(self.bucket_name, update_error), server=self.n1ql_node)
        time.sleep(40)

        error_results = self.n1ql_helper.run_cbq_query(
            'SELECT meta().id FROM `{0}` WHERE createdDateTime BETWEEN 1 AND 10 AND status = "ERROR" order by meta().id'.format(self.bucket_name))

        self.assertEqual(error_results['metrics']['resultCount'], 0)
