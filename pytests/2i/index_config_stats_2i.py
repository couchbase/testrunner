from .base_2i import BaseSecondaryIndexingTests
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection, RestHelper

from pytests.query_tests_helper import QueryHelperTests
import time


class SecondaryIndexingStatsConfigTests(BaseSecondaryIndexingTests, QueryHelperTests):

    def setUp(self):
        super(SecondaryIndexingStatsConfigTests, self).setUp()
        self.flush_bucket = self.input.param('flush_bucket', False)
        self.move_index = self.input.param('move_index', False)

    def tearDown(self):
        super(SecondaryIndexingStatsConfigTests, self).tearDown()

    def test_key_size_distribution(self):
        index_node = self.get_nodes_from_services_map(service_type="index",
                                                         get_all_nodes=False)
        rest = RestConnection(index_node)
        doc = {"indexer.statsPersistenceInterval": 60}
        rest.set_index_settings_internal(doc)

        string_70 = "x" * 70
        string_260 = "x" * 260
        string_1030 = "x" * 1030
        string_5000 = "x" * 5000
        string_103000 = "x" * 103000

        insert_query1 = 'INSERT INTO default (KEY, VALUE) VALUES ("id1", { "name" : "%s" })' % string_70
        insert_query2 = 'INSERT INTO default (KEY, VALUE) VALUES ("id2", { "name" : "%s" })' % string_260
        insert_query3 = 'INSERT INTO default (KEY, VALUE) VALUES ("id3", { "name" : "%s" })' % string_1030
        insert_query4 = 'INSERT INTO default (KEY, VALUE) VALUES ("id4", { "name" : "%s" })' % string_5000
        insert_query5 = 'INSERT INTO default (KEY, VALUE) VALUES ("id5", { "name" : "%s" })' % string_103000

        self.n1ql_helper.run_cbq_query(query=insert_query1,
                                       server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(query=insert_query2,
                                       server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(query=insert_query3,
                                       server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(query=insert_query4,
                                       server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(query=insert_query5,
                                       server=self.n1ql_node)

        insert_query1 = 'INSERT INTO standard_bucket0 (KEY, VALUE) VALUES ("id4", { "name" : "%s" })' % string_5000
        self.n1ql_helper.run_cbq_query(query=insert_query1,
                                       server=self.n1ql_node)

        create_index_query1 = "CREATE INDEX idx ON default(name) USING GSI"
        create_index_query2 = "CREATE INDEX idx2 ON default(join_mo) USING GSI"
        create_index_query3 = "CREATE INDEX idx ON standard_bucket0(name) USING GSI"
        create_index_query4 = "CREATE INDEX idx2 ON standard_bucket0(join_mo) USING GSI"

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
                "index creation did not fail with expected error : {0}".format(
                    str(ex)))

        expected_distr = []
        expected_distr2 = []
        common_distr = "{u'(0-64)': 2016, u'(257-1024)': 0, u'(65-256)': 0, u'(4097-102400)': 0, u'(1025-4096)': 0, u'(102401-max)': 0}"

        expected_distr.append("{u'(0-64)': 2016, u'(257-1024)': 1, u'(65-256)': 1, u'(4097-102400)': 1, u'(1025-4096)': 1, u'(102401-max)': 1}")
        expected_distr.append(common_distr)
        expected_distr2.append("{u'(0-64)': 2016, u'(257-1024)': 0, u'(65-256)': 0, u'(4097-102400)': 1, u'(1025-4096)': 0, u'(102401-max)': 0}")
        expected_distr2.append(common_distr)

        index_map = self.get_index_stats()
        self.log.info(index_map)
        self.verify_key_size(index_map, 'default', expected_distr)
        self.verify_key_size(index_map, 'standard_bucket0', expected_distr2)

        self.sleep(60)

        shell = RemoteMachineShellConnection(index_node)
        output1, error1 = shell.execute_command("killall -9 indexer")

        self.sleep(30)

        index_map = self.get_index_stats()
        self.log.info(index_map)
        self.verify_key_size(index_map, 'default', expected_distr)
        self.verify_key_size(index_map, 'standard_bucket0', expected_distr2)

    def test_key_size_distribution_nulls(self):
        string_70 = "x" * 70
        string_103000 = "x" * 103000

        insert_query1 = 'INSERT INTO default (KEY, VALUE) VALUES ("id1", { "name" : "%s" })' % string_70
        insert_query2 = 'INSERT INTO default (KEY, VALUE) VALUES ("id2", { "name" : NULL })'
        insert_query3 = 'INSERT INTO default (KEY, VALUE) VALUES ("id3", { "name" : ""})'
        insert_query5 = 'INSERT INTO default (KEY, VALUE) VALUES ("id5", { "name" : "%s" })' % string_103000

        self.n1ql_helper.run_cbq_query(query=insert_query1,
                                       server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(query=insert_query2,
                                       server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(query=insert_query3,
                                       server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(query=insert_query5,
                                       server=self.n1ql_node)

        create_index_query1 = "CREATE INDEX idx ON default(name) USING GSI"
        create_index_query2 = "CREATE INDEX idx2 ON default(join_mo) USING GSI"

        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query1,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index_query2,
                                           server=self.n1ql_node)

        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation did not fail with expected error : {0}".format(
                    str(ex)))

        expected_distr = []
        common_distr = "{u'(0-64)': 2016, u'(257-1024)': 0, u'(65-256)': 0, u'(4097-102400)': 0, u'(1025-4096)': 0, u'(102401-max)': 0}"

        expected_distr.append("{u'(0-64)': 2018, u'(257-1024)': 0, u'(65-256)': 1, u'(4097-102400)': 0, u'(1025-4096)': 0, u'(102401-max)': 1}")
        expected_distr.append(common_distr)


        index_map = self.get_index_stats()
        self.log.info(index_map)
        self.verify_key_size(index_map, 'default', expected_distr)

    def test_key_size_distribution_objects(self):
        index_node = self.get_nodes_from_services_map(service_type="index",
                                                         get_all_nodes=False)
        rest = RestConnection(index_node)

        string_70 = "x" * 70
        string_3000 = "x" * 3000
        string_103000 = "x" * 103000

        insert_query1 = 'INSERT INTO default (KEY, VALUE) VALUES ("id1", { "name" : "%s" })' % string_70
        insert_query2 = 'INSERT INTO default (KEY, VALUE) VALUES ("id2", { "name" : {"name": "%s", "fake": "%s"} })' % (string_70, string_3000)
        insert_query5 = 'INSERT INTO default (KEY, VALUE) VALUES ("id5", { "name" : "%s" })' % string_103000

        self.n1ql_helper.run_cbq_query(query=insert_query1,
                                       server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(query=insert_query2,
                                       server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(query=insert_query5,
                                       server=self.n1ql_node)

        create_index_query1 = "CREATE INDEX idx ON default(name) USING GSI"
        create_index_query2 = "CREATE INDEX idx2 ON default(join_mo) USING GSI"

        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query1,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index_query2,
                                           server=self.n1ql_node)

        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation did not fail with expected error : {0}".format(
                    str(ex)))

        expected_distr = []
        common_distr = "{u'(0-64)': 2016, u'(257-1024)': 0, u'(65-256)': 0, u'(4097-102400)': 0, u'(1025-4096)': 0, u'(102401-max)': 0}"
        expected_distr.append("{u'(0-64)': 2016, u'(257-1024)': 0, u'(65-256)': 1, u'(4097-102400)': 0, u'(1025-4096)': 1, u'(102401-max)': 1}")
        expected_distr.append(common_distr)


        index_map = self.get_index_stats()
        self.log.info(index_map)
        self.verify_key_size(index_map, 'default', expected_distr)

        if self.flush_bucket:
            rest.flush_bucket("default")
            self.sleep(30)
            insert_query1 = 'INSERT INTO default (KEY, VALUE) VALUES ("id1", { "name" : "%s" })' % string_70
            insert_query2 = 'INSERT INTO default (KEY, VALUE) VALUES ("id2", { "name" : {"name": "%s", "fake": "%s"} })' % (
            string_70, string_3000)
            insert_query5 = 'INSERT INTO default (KEY, VALUE) VALUES ("id5", { "name" : "%s" })' % string_103000

            self.n1ql_helper.run_cbq_query(query=insert_query1,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=insert_query2,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=insert_query5,
                                           server=self.n1ql_node)

            expected_distr = []
            common_distr = "{u'(0-64)': 0, u'(257-1024)': 0, u'(65-256)': 0, u'(4097-102400)': 0, u'(1025-4096)': 0, u'(102401-max)': 0}"
            expected_distr.append(
                "{u'(0-64)': 0, u'(257-1024)': 0, u'(65-256)': 1, u'(4097-102400)': 0, u'(1025-4096)': 1, u'(102401-max)': 1}")
            expected_distr.append(common_distr)

            index_map = self.get_index_stats()
            self.log.info(index_map)
            self.verify_key_size(index_map, 'default', expected_distr)

    def test_key_size_distribution_dml(self):
        create_index_query1 = "CREATE INDEX idx ON default(name) USING GSI"
        create_index_query2 = "CREATE INDEX idx2 ON default(join_mo) USING GSI"

        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query1,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index_query2,
                                           server=self.n1ql_node)

        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation did not fail with expected error : {0}".format(
                    str(ex)))

        string_103000 = "x" * 103000

        update_query = "UPDATE default SET name = '%s' WHERE name = 'employee-9'" % string_103000

        self.n1ql_helper.run_cbq_query(query=update_query,
                                       server=self.n1ql_node)

        expected_distr = []
        common_distr = "{u'(0-64)': 2016, u'(257-1024)': 0, u'(65-256)': 0, u'(4097-102400)': 0, u'(1025-4096)': 0, u'(102401-max)': 0}"
        expected_distr.append("{u'(0-64)': 2000, u'(257-1024)': 0, u'(65-256)': 0, u'(4097-102400)': 0, u'(1025-4096)': 0, u'(102401-max)': 16}")
        expected_distr.append(common_distr)


        index_map = self.get_index_stats()
        self.log.info(index_map)
        self.verify_key_size(index_map, 'default', expected_distr)

        delete_query = "delete from default where name = 'employee-6'"

        self.n1ql_helper.run_cbq_query(query=delete_query,
                                       server=self.n1ql_node)
        expected_distr2 = []
        common_distr = "{u'(0-64)': 1944, u'(257-1024)': 0, u'(65-256)': 0, u'(4097-102400)': 0, u'(1025-4096)': 0, u'(102401-max)': 0}"
        expected_distr2.append("{u'(0-64)': 1872, u'(257-1024)': 0, u'(65-256)': 0, u'(4097-102400)': 0, u'(1025-4096)': 0, u'(102401-max)': 72}")
        expected_distr2.append(common_distr)

        index_map = self.get_index_stats()
        self.log.info(index_map)
        self.verify_key_size(index_map, 'default', expected_distr2)

    def test_arrkey_size_distribution(self):
        index_node = self.get_nodes_from_services_map(service_type="index",
                                                      get_all_nodes=False)
        rest = RestConnection(index_node)
        doc = {"indexer.statsPersistenceInterval": 60}
        rest.set_index_settings_internal(doc)

        string_70 = "x" * 70
        string_260 = "x" * 260
        string_1030 = "x" * 1030
        string_5000 = "x" * 5000
        string_103000 = "x" * 103000

        insert_query1 = 'INSERT INTO default (KEY, VALUE) VALUES ("id1", { "name" : ["%s","",null] })' % string_70
        insert_query2 = 'INSERT INTO default (KEY, VALUE) VALUES ("id2", { "name" : ["%s"] })' % string_260
        insert_query3 = 'INSERT INTO default (KEY, VALUE) VALUES ("id3", { "name" : ["%s"] })' % string_1030
        insert_query4 = 'INSERT INTO default (KEY, VALUE) VALUES ("id4", { "name" : ["%s","string1"] })' % string_5000
        insert_query5 = 'INSERT INTO default (KEY, VALUE) VALUES ("id5", { "name" : ["%s", "string2"] })' % string_103000

        self.n1ql_helper.run_cbq_query(query=insert_query1,
                                       server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(query=insert_query2,
                                       server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(query=insert_query3,
                                       server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(query=insert_query4,
                                       server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(query=insert_query5,
                                       server=self.n1ql_node)

        insert_query1 = 'INSERT INTO standard_bucket0 (KEY, VALUE) VALUES ("id4", { "name" : ["%s"] })' % string_5000
        self.n1ql_helper.run_cbq_query(query=insert_query1,
                                       server=self.n1ql_node)

        create_index_query1 = "CREATE INDEX idx ON default(distinct name) USING GSI"
        create_index_query2 = "CREATE INDEX idx2 ON default(join_mo) USING GSI"
        create_index_query3 = "CREATE INDEX idx ON standard_bucket0(distinct name) USING GSI"
        create_index_query4 = "CREATE INDEX idx2 ON standard_bucket0(join_mo) USING GSI"

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
                "index creation did not fail with expected error : {0}".format(
                    str(ex)))

        expected_distr = "{u'(0-64)': 2016, u'(257-1024)': 1, u'(65-256)': 1, u'(4097-102400)': 1, u'(1025-4096)': 1, u'(102401-max)': 1}"
        expected_distr2 = "{u'(0-64)': 2016, u'(257-1024)': 0, u'(65-256)': 0, u'(4097-102400)': 1, u'(1025-4096)': 0, u'(102401-max)': 0}"

        index_map = self.get_index_stats()
        self.log.info(index_map)
        self.verify_arrkey_size(index_map, 'default', expected_distr)
        self.verify_arrkey_size(index_map, 'standard_bucket0', expected_distr2)

        self.sleep(60)

        shell = RemoteMachineShellConnection(index_node)
        output1, error1 = shell.execute_command("killall -9 indexer")

        self.sleep(30)

        index_map = self.get_index_stats()
        self.log.info(index_map)
        self.verify_arrkey_size(index_map, 'default', expected_distr)
        self.verify_arrkey_size(index_map, 'standard_bucket0', expected_distr2)

    def test_keysize_rebalance_out(self):
        rest = RestConnection(self.master)

        create_index_query1 = "CREATE INDEX idx ON default(name) USING GSI  WITH {'nodes': ['%s:%s']}" % (self.servers[1].ip, self.servers[1].port)
        create_index_query2 = "CREATE INDEX idx2 ON default(join_mo) USING GSI WITH {'nodes': ['%s:%s']}" % (self.servers[1].ip, self.servers[1].port)
        create_index_query3 = "CREATE INDEX idx ON standard_bucket0(name) USING GSI"
        create_index_query4 = "CREATE INDEX idx2 ON standard_bucket0(join_mo) USING GSI"

        self.n1ql_helper.run_cbq_query(query=create_index_query1,
                                       server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(query=create_index_query2,
                                       server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(query=create_index_query3,
                                       server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(query=create_index_query4,
                                       server=self.n1ql_node)

        string_70 = "x" * 70
        string_260 = "x" * 260
        string_1030 = "x" * 1030
        string_5000 = "x" * 5000
        string_103000 = "x" * 103000

        insert_query1 = 'INSERT INTO default (KEY, VALUE) VALUES ("id1", { "name" : "%s" })' % string_70
        insert_query2 = 'INSERT INTO default (KEY, VALUE) VALUES ("id2", { "name" : "%s" })' % string_260
        insert_query3 = 'INSERT INTO default (KEY, VALUE) VALUES ("id3", { "name" : "%s" })' % string_1030
        insert_query4 = 'INSERT INTO default (KEY, VALUE) VALUES ("id4", { "name" : "%s" })' % string_5000
        insert_query5 = 'INSERT INTO default (KEY, VALUE) VALUES ("id5", { "name" : "%s" })' % string_103000

        self.n1ql_helper.run_cbq_query(query=insert_query1,
                                       server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(query=insert_query2,
                                       server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(query=insert_query3,
                                       server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(query=insert_query4,
                                       server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(query=insert_query5,
                                       server=self.n1ql_node)

        insert_query1 = 'INSERT INTO standard_bucket0 (KEY, VALUE) VALUES ("id4", { "name" : "%s" })' % string_5000
        self.n1ql_helper.run_cbq_query(query=insert_query1,
                                       server=self.n1ql_node)

        expected_distr = []
        expected_distr2 = []
        common_distr = "{u'(0-64)': 2016, u'(257-1024)': 0, u'(65-256)': 0, u'(4097-102400)': 0, u'(1025-4096)': 0, u'(102401-max)': 0}"

        expected_distr.append("{u'(0-64)': 2016, u'(257-1024)': 1, u'(65-256)': 1, u'(4097-102400)': 1, u'(1025-4096)': 1, u'(102401-max)': 1}")
        expected_distr.append(common_distr)
        expected_distr2.append("{u'(0-64)': 2016, u'(257-1024)': 0, u'(65-256)': 0, u'(4097-102400)': 1, u'(1025-4096)': 0, u'(102401-max)': 0}")
        expected_distr2.append(common_distr)

        index_map = self.get_index_stats()
        self.log.info(index_map)
        self.verify_key_size(index_map, 'default', expected_distr)
        self.verify_key_size(index_map, 'standard_bucket0', expected_distr2)

        # remove the n1ql node which is being rebalanced out
        # rebalance out a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [self.servers[1]])
        reached = RestHelper(rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)

        index_map = self.get_index_stats()
        self.log.info(index_map)
        self.verify_key_size(index_map, 'default', expected_distr)
        self.verify_key_size(index_map, 'standard_bucket0', expected_distr2)

    def test_keysize_rebalance_in(self):
        rest = RestConnection(self.master)

        if self.move_index:
            create_index_query1 = "CREATE INDEX idx ON default(name) USING GSI"
            self.n1ql_helper.run_cbq_query(query=create_index_query1,
                                           server=self.n1ql_node)

        create_index_query2 = "CREATE INDEX idx2 ON default(join_mo) USING GSI "
        create_index_query3 = "CREATE INDEX idx ON standard_bucket0(name) USING GSI"
        create_index_query4 = "CREATE INDEX idx2 ON standard_bucket0(join_mo) USING GSI"

        self.n1ql_helper.run_cbq_query(query=create_index_query2,
                                       server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(query=create_index_query3,
                                       server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(query=create_index_query4,
                                       server=self.n1ql_node)

        string_70 = "x" * 70
        string_260 = "x" * 260
        string_1030 = "x" * 1030
        string_5000 = "x" * 5000
        string_103000 = "x" * 103000

        insert_query1 = 'INSERT INTO default (KEY, VALUE) VALUES ("id1", { "name" : "%s" })' % string_70
        insert_query2 = 'INSERT INTO default (KEY, VALUE) VALUES ("id2", { "name" : "%s" })' % string_260
        insert_query3 = 'INSERT INTO default (KEY, VALUE) VALUES ("id3", { "name" : "%s" })' % string_1030
        insert_query4 = 'INSERT INTO default (KEY, VALUE) VALUES ("id4", { "name" : "%s" })' % string_5000
        insert_query5 = 'INSERT INTO default (KEY, VALUE) VALUES ("id5", { "name" : "%s" })' % string_103000

        self.n1ql_helper.run_cbq_query(query=insert_query1,
                                       server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(query=insert_query2,
                                       server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(query=insert_query3,
                                       server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(query=insert_query4,
                                       server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(query=insert_query5,
                                       server=self.n1ql_node)

        insert_query1 = 'INSERT INTO standard_bucket0 (KEY, VALUE) VALUES ("id4", { "name" : "%s" })' % string_5000
        self.n1ql_helper.run_cbq_query(query=insert_query1,
                                       server=self.n1ql_node)

        expected_distr = []
        expected_distr2 = []
        common_distr = "{u'(0-64)': 2016, u'(257-1024)': 0, u'(65-256)': 0, u'(4097-102400)': 0, u'(1025-4096)': 0, u'(102401-max)': 0}"

        expected_distr.append("{u'(0-64)': 2016, u'(257-1024)': 1, u'(65-256)': 1, u'(4097-102400)': 1, u'(1025-4096)': 1, u'(102401-max)': 1}")
        expected_distr.append(common_distr)
        expected_distr2.append("{u'(0-64)': 2016, u'(257-1024)': 0, u'(65-256)': 0, u'(4097-102400)': 1, u'(1025-4096)': 0, u'(102401-max)': 0}")
        expected_distr2.append(common_distr)

        index_map = self.get_index_stats()
        self.log.info(index_map)
        self.verify_key_size(index_map, 'default', expected_distr)
        self.verify_key_size(index_map, 'standard_bucket0', expected_distr2)

        services_in = ["index"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        reached = RestHelper(rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)

        if not self.move_index:
            create_index_query1 = "CREATE INDEX idx ON default(name) USING GSI"
            self.n1ql_helper.run_cbq_query(query=create_index_query1, server=self.n1ql_node)
        else:
            alter_index_query = 'ALTER INDEX default.idx WITH {{"action":"move","nodes": ["{0}:{1}"]}}'.format(self.servers[self.nodes_init].ip, self.servers[self.nodes_init].port)
            self.n1ql_helper.run_cbq_query(query=alter_index_query, server=self.n1ql_node)

        self.sleep(20)

        index_map = self.get_index_stats()
        self.log.info(index_map)
        self.verify_key_size(index_map, 'default', expected_distr)
        self.verify_key_size(index_map, 'standard_bucket0', expected_distr2)


    def verify_key_size(self, index_map, bucket, expected_distr):
        for index in index_map[bucket]:
            if index == 'idx':
                self.log.info(index_map[bucket][index]['key_size_distribution'])
                self.assertTrue(str(index_map[bucket][index]['key_size_distribution']) == expected_distr[0])
            else:
                self.log.info(index_map[bucket][index]['key_size_distribution'])
                self.assertTrue(str(index_map[bucket][index]['key_size_distribution']) == expected_distr[1])

    def verify_arrkey_size(self, index_map, bucket, expected_distr):
        for index in index_map[bucket]:
            if index == 'idx':
                self.log.info(index_map[bucket][index]['arrkey_size_distribution'])
                self.assertTrue(str(index_map[bucket][index]['arrkey_size_distribution']) == expected_distr)
            else:
                self.assertTrue("arrkey_size_distribution" not in str(index_map[bucket][index]))

    def test_num_scan_timeouts(self):
        index_node = self.get_nodes_from_services_map(service_type="index",
                                                      get_all_nodes=False)
        rest = RestConnection(index_node)
        shell = RemoteMachineShellConnection(self.master)

        create_index_query = "CREATE INDEX idx ON default(age) USING GSI"

        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation did not fail with expected error : {0}".format(
                    str(ex)))

        shell.execute_cbworkloadgen(rest.username, rest.password, 1000000, 70, 'default', 1024, '-j')
        doc = {"indexer.settings.scan_timeout": 10}
        rest.set_index_settings(doc)
        query_params = {'scan_consistency': 'request_plus'}

        select_query = "SELECT age from default"


        self.n1ql_helper.run_cbq_query(query=select_query, server=self.n1ql_node, query_params=query_params)

        index_map = self.get_index_stats()
        official_stats = rest.get_index_official_stats()
        self.log.info(index_map)
        self.log.info(official_stats)

    def test_avg_scan_latency(self):
        index_node = self.get_nodes_from_services_map(service_type="index",
                                                      get_all_nodes=False)
        rest = RestConnection(index_node)
        create_index_query = "CREATE INDEX idx ON default(name) USING GSI"
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
        select_query = "SELECT count(name) from default"
        # Run select query 10 times
        for i in range(0, 10):
            self.n1ql_helper.run_cbq_query(query=select_query,
                                           server=self.n1ql_node)

        index_map = self.get_index_stats()
        official_stats = rest.get_index_official_stats()
        self.log.info(index_map)
        self.log.info(official_stats)

        self.assertTrue(index_map['default']['idx']['avg_scan_latency'] == official_stats['default:idx']['avg_scan_latency'])

    def test_initial_build_progress(self):
        index_node = self.get_nodes_from_services_map(service_type="index",
                                                      get_all_nodes=False)
        rest = RestConnection(index_node)
        create_index_query = "CREATE INDEX idx ON default(name) USING GSI"

        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation did not fail with expected error : {0}".format(
                    str(ex)))

        init_time = time.time()
        check = False
        next_time = init_time
        while not check:
            index_status = rest.get_index_official_stats()
            self.log.info(index_status)
            if index_status['default:idx']['initial_build_progress'] == 100:
                check = True
            else:
                check = False
                time.sleep(1)
                next_time = time.time()
            check = check or (next_time - init_time > 60)

        official_stats = rest.get_index_official_stats()
        self.log.info(official_stats)
        self.assertTrue(official_stats['default:idx']['initial_build_progress'] == 100)

    def test_num_items_flushed(self):
        index_node = self.get_nodes_from_services_map(service_type="index",
                                                      get_all_nodes=False)
        rest = RestConnection(index_node)

        create_index_query = "CREATE INDEX idx ON default(age) USING GSI"

        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation did not fail with expected error : {0}".format(
                    str(ex)))

        official_stats = rest.get_index_official_stats()
        self.log.info(official_stats)
        self.assertTrue(official_stats['default:idx']['num_items_flushed'] == self.docs_per_day*2016)

    def test_avg_drain_rate(self):
        index_node = self.get_nodes_from_services_map(service_type="index",
                                                      get_all_nodes=False)
        rest = RestConnection(index_node)
        shell = RemoteMachineShellConnection(self.master)

        create_index_query = "CREATE INDEX idx ON default(age) USING GSI"

        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation did not fail with expected error : {0}".format(
                    str(ex)))

        shell.execute_cbworkloadgen(rest.username, rest.password, 500000, 70, 'default', 1024, '-j')

        official_stats = rest.get_index_official_stats()
        index_map = self.get_index_stats()
        self.log.info(index_map)
        self.log.info(official_stats)
        self.assertTrue(index_map['default']['idx']['avg_drain_rate'] == official_stats['default:idx']['avg_drain_rate'])


    def test_index_stats(self):
        """
        Tests index stats when indexes are created and dropped
        """
        #Create Index
        self.run_multi_operations(buckets = self.buckets,
            query_definitions = self.query_definitions,
            create_index = True, drop_index = False)
        #Check Index Stats
        self.sleep(30)
        index_map = self.get_index_stats()
        self.log.info(index_map)
        for query_definition in self.query_definitions:
            index_name = query_definition.index_name
            for bucket in self.buckets:
                bucket_name = bucket.name
                check_keys = ['items_count', 'total_scan_duration', 'num_docs_queued',
                 'num_requests', 'num_rows_returned', 'num_docs_queued',
                 'num_docs_pending', 'delete_bytes' ]
                map = self._create_stats_map(items_count=2016)
                self._verify_index_stats(index_map, index_name, bucket_name, map, check_keys)

    def test_index_storage_stats(self):
        indexer_nodes = self.get_nodes_from_services_map(service_type="index",
                                                         get_all_nodes=True)
        self.run_multi_operations(buckets = self.buckets,
            query_definitions = self.query_definitions,
            create_index = True, drop_index = False)
        for node in indexer_nodes:
            indexer_rest = RestConnection(node)
            content = indexer_rest.get_index_storage_stats()
            for index in list(content.values()):
                for stats in list(index.values()):
                    self.log.info("MainStore Stats - {0}: {1}".format(
                        index, stats["MainStore"]))
                    self.log.info("BackStore Stats - {0}: {1}".format(
                        index, stats["BackStore"]))
                    self.assertEqual(stats["MainStore"]["resident_ratio"], 1.00,
                                     "Resident ratio not 1")

    def test_indexer_logs_for_leaked_password(self):
        expected_msg = "http://%40index-cbauth@127.0.0.1:8091"
        indexers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        self.assertGreater(len(indexers), 0, "No indexer found in cluster")
        for server in indexers:
            shell = RemoteMachineShellConnection(server)
            _, dir = RestConnection(server).diag_eval('filename:absname('
                                                      'element(2, '
                                                      'application:get_env('
                                                      'ns_server, error_logger_mf_dir))).')
            indexer_log = str(dir) + '/indexer.log*'
            count, err = shell.execute_command("zgrep \"{0}\" {1} | wc -l".
                                        format(expected_msg, indexer_log))
            if isinstance(count, list):
                count = int(count[0])
            else:
                count = int(count)
            shell.disconnect()
            self.assertGreater(count, 0, "Password leak found in Indexer {"
                                         "0}".format(server.ip))

    def test_get_index_settings(self):
        #Check Index Settings
        map = self.get_index_settings()
        for node in list(map.keys()):
            val = map[node]
            gen = self._create_settings_map()
            for key in list(gen.keys()):
                self.assertTrue(key in list(val.keys()), "{0} not in {1} ".format(key, val))

    def test_set_index_settings(self):
        #Check Index Settings
        map1 = self._set_settings_map()
        self.log.info(map1)
        self.set_index_settings(map1)
        map = self.get_index_settings()
        for node in list(map.keys()):
            val = map[node]
            for key in list(map1.keys()):
                self.assertTrue(key in list(val.keys()), "{0} not in {1} ".format(key, val))

    def _verify_index_stats(self, index_map, index_name, bucket_name, index_stat_values, check_keys=None):
        self.assertIn(bucket_name, list(index_map.keys()), "bucket name {0} not present in stats".format(bucket_name))
        self.assertIn(index_name, list(index_map[bucket_name].keys()),
                        "index name {0} not present in set of indexes {1}".format(index_name,
                                                                                  list(index_map[bucket_name].keys())))
        for key in list(index_stat_values.keys()):
            self.assertIn(key, list(index_map[bucket_name][index_name].keys()),
                            "stats {0} not present in Index stats {1}".format(key,
                                                                                  index_map[bucket_name][index_name]))
            if check_keys:
                if key in check_keys:
                    self.assertEqual(str(index_map[bucket_name][index_name][key]), str(index_stat_values[key]),
                                    " for key {0} : {1} != {2}".format(key,
                                                                       index_map[bucket_name][index_name][key],
                                                                       index_stat_values[key]))
            else:
                self.assertEqual(str(index_stat_values[key]), str(index_map[bucket_name][index_name][key]),
                                " for key {0} : {1} != {2}".format(key,
                                                                   index_map[bucket_name][index_name][key],
                                                                   index_stat_values[key]))

    def set_index_settings(self, settings):
        servers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        for server in servers:
            RestConnection(server).set_index_settings(settings)

    def get_index_settings(self):
        servers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        index_settings_map = {}
        for server in servers:
            key = "{0}:{1}".format(server.ip, server.port)
            index_settings_map[key] = RestConnection(server).get_index_settings()
        return index_settings_map

    def _create_stats_map(self, items_count = 0, total_scan_duration = 0,
        delete_bytes = 0, scan_wait_duration = 0, insert_bytes = 0,
        num_rows_returned = 0, num_docs_indexed = 0, num_docs_pending = 0,
        scan_bytes_read = 0, get_bytes = 0, num_docs_queued = 0,num_requests = 0,
        disk_size = 0):
        map = {}
        map['items_count'] = items_count
        map['disk_size'] = disk_size
        map['items_count'] = items_count
        map['total_scan_duration'] = total_scan_duration
        map['delete_bytes'] = delete_bytes
        map['scan_wait_duration'] = scan_wait_duration
        map['insert_bytes'] = insert_bytes
        map['num_rows_returned'] = num_rows_returned
        map['num_docs_indexed'] = num_docs_indexed
        map['num_docs_pending'] = num_docs_pending
        map['scan_bytes_read'] = scan_bytes_read
        map['get_bytes'] = get_bytes
        map['num_docs_queued'] = num_docs_queued
        map['num_requests'] = num_requests
        return map

    def _create_settings_map(self):
        map = { "indexer.settings.recovery.max_rollbacks" : 5,
        "indexer.settings.bufferPoolBlockSize" : 16384,
        "indexer.settings.max_cpu_percent" : 400,
        "queryport.client.settings.poolOverflow" : 30,
        "indexer.settings.memProfile" : False,
        "indexer.settings.statsLogDumpInterval" : 60,
        "indexer.settings.persisted_snapshot.interval" : 5000,
        "indexer.settings.inmemory_snapshot.interval" : 200,
        "indexer.settings.compaction.check_period" : 30,
        "indexer.settings.largeSnapshotThreshold" : 200,
        "indexer.settings.log_level" : "debug",
        "indexer.settings.scan_timeout" : 120000,
        "indexer.settings.maxVbQueueLength" : 0,
        "indexer.settings.send_buffer_size" : 1024,
        "indexer.settings.compaction.min_size" : 1048576,
        "indexer.settings.cpuProfFname" : "",
        "indexer.settings.memory_quota" : 268435456,
        "indexer.settings.memProfFname" : "",
        "projector.settings.log_level" : "debug",
        "queryport.client.settings.poolSize" : 1000,
        "indexer.settings.max_writer_lock_prob" : 20,
        "indexer.settings.compaction.interval" : "00:00,00:00",
        "indexer.settings.cpuProfile" : False,
        "indexer.settings.compaction.min_frag" : 30,
        "indexer.settings.sliceBufSize" : 50000,
        "indexer.settings.wal_size" : 4096,
        "indexer.settings.fast_flush_mode" : True,
        "indexer.settings.smallSnapshotThreshold" : 30,
        "indexer.settings.persisted_snapshot_init_build.interval": 5000
}
        return map

    def _set_settings_map(self):
        map = { "indexer.settings.recovery.max_rollbacks" : 4,
        "indexer.settings.bufferPoolBlockSize" : 16384,
        "indexer.settings.max_cpu_percent" : 400, 
        "indexer.settings.memProfile" : False,
        "indexer.settings.statsLogDumpInterval" : 60,
        "indexer.settings.persisted_snapshot.interval" : 5000,
        "indexer.settings.inmemory_snapshot.interval" : 200,
        "indexer.settings.compaction.check_period" : 31,
        "indexer.settings.largeSnapshotThreshold" : 200,
        "indexer.settings.log_level" : "debug",
        "indexer.settings.scan_timeout" : 120000,
        "indexer.settings.maxVbQueueLength" : 0,
        "indexer.settings.send_buffer_size" : 1024,
        "indexer.settings.compaction.min_size" : 1048576,
        "indexer.settings.cpuProfFname" : "",
        "indexer.settings.memory_quota" : 268435456,
        "indexer.settings.memProfFname" : "",
        "indexer.settings.persisted_snapshot_init_build.interval": 5000,
        "indexer.settings.max_writer_lock_prob" : 20,
        "indexer.settings.compaction.interval" : "00:00,00:00",
        "indexer.settings.cpuProfile" : False,
        "indexer.settings.compaction.min_frag" : 31,
        "indexer.settings.sliceBufSize" : 50000,
        "indexer.settings.wal_size" : 4096,
        "indexer.settings.fast_flush_mode" : True,
        "indexer.settings.smallSnapshotThreshold" : 30,
        "projector.settings.log_level" : "debug",
        "queryport.client.settings.poolSize" : 1000,
        "queryport.client.settings.poolOverflow" : 30
}
        return map
