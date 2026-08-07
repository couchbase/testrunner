from .base_gsi import BaseSecondaryIndexingTests
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection, RestHelper

from pytests.query_tests_helper import QueryHelperTests
import re
import shlex
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
        index_map = self.get_index_stats(perNode=True)
        self.log.info(index_map)
        parsed_index_stats = []
        required_index_map = {}
        for element in index_map:
            parsed_index_stats.append(index_map[element])
        check_keys = ['items_count', 'total_scan_duration', 'num_docs_queued',
                      'num_requests', 'num_rows_returned', 'num_docs_indexed',
                      'num_docs_pending', 'delete_bytes','disk_size','scan_wait_duration','insert_bytes','scan_bytes_read','get_bytes']
        index_list = []
        for query_definition in self.query_definitions:
            index_list.append(query_definition.index_name)
        for index_name in index_list:
            for bucket in self.buckets:
                bucket_name = bucket.name
                for key in check_keys:
                    for header in parsed_index_stats:
                        if index_name in header[bucket_name]:
                            stats = bucket_name + ':' + index_name + ':' + key
                            if key in header[bucket_name][index_name]:
                                required_index_map[stats] = header[bucket_name][index_name][key]

        for index_name in index_list:
            for bucket in self.buckets:
                bucket_name = bucket.name
                if 'join_yr' in index_name:
                    items_count = 1008
                else:
                    items_count = 2016
                expected_map = self._create_stats_map(items_count=items_count)
                for key in expected_map:
                    stats = bucket_name + ':' + index_name + ':' + key
                    if key == 'items_count':
                        self.assertEqual(str(expected_map[key]), str(required_index_map[stats]),
                                         " for key {0} : {1} != {2}".format(key, required_index_map[stats], expected_map[key]))

                    else:
                        self.assertIn(stats, list(required_index_map.keys()),
                                      "key name {0} not present in stats".format(key))


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
        expected_msg = "http://%40index@127.0.0.1:8091"
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
            self.assertGreater(count, 0, "Password leak found in Indexer {0}".format(server.ip))

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
        "indexer.settings.cpuProfDir" : "",
        "indexer.settings.memory_quota" : 268435456,
        "indexer.settings.memProfDir" : "",
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
        "indexer.settings.cpuProfDir" : "",
        "indexer.settings.memory_quota" : 268435456,
        "indexer.settings.memProfDir" : "",
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

    def _wait_for_indexer_ready(self, node, timeout=120, poll_interval=5, label=""):
        """Poll the indexer stats endpoint on ``node`` until it responds.

        Returns True once the indexer serves a non-empty stats payload,
        False if it never becomes ready within ``timeout`` seconds.
        """
        end_time = time.time() + timeout
        while time.time() < end_time:
            try:
                if RestConnection(node).get_indexer_stats(timeout=10):
                    self.log.info(f"{label}Indexer is ready on {node.ip}")
                    return True
            except Exception as e:
                self.log.info(f"{label}Indexer not ready yet on {node.ip}: {e}")
            self.sleep(poll_interval,
                       f"{label}Waiting for indexer readiness on {node.ip}")
        return False

    def _restart_indexer_on_all_nodes(self, label="", readiness_timeout=120):
        """Restart indexer service on every index node.

        After starting the indexer, waits for it to actually serve requests
        again (rather than a blind sleep) so callers can safely run scans or
        read indexer state immediately after this returns.
        """
        index_nodes = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True,
        )
        errors = []
        for node in index_nodes:
            remote = RemoteMachineShellConnection(node, verbose=False)
            try:
                self.log.info(f"{label}Restarting indexer on {node.ip}")
                remote.stop_indexer()
                self.sleep(5, f"{label}Waiting for indexer to stop on {node.ip}")
                remote.start_indexer()
                if not self._wait_for_indexer_ready(
                        node, timeout=readiness_timeout, label=label):
                    msg = (f"{label}{node.ip}: indexer did not become ready "
                           f"within {readiness_timeout}s after restart")
                    self.log.error(msg)
                    errors.append(msg)
            except Exception as e:
                msg = f"{label}{node.ip}: indexer restart failed: {e}"
                self.log.error(msg)
                errors.append(msg)
            finally:
                remote.disconnect()
        return errors

    def _get_indexer_pid(self, shell, node_ip, label=""):
        """Return the PID of the real indexer process on ``node_ip``.

        ``pgrep -f '/indexer'`` matches against the full command line, which
        also matches the babysitter/goport supervisor (its argv embeds the
        indexer binary path even though it is a different process). Cross
        check /proc/<pid>/comm so we validate the process that actually
        holds the memory, not a parent/supervisor.
        """
        out, _ = shell.execute_command("pgrep -f '/indexer'")
        candidate_pids = [l.strip() for l in out if l.strip()]
        for pid in candidate_pids:
            comm_out, _ = shell.execute_command(f"cat /proc/{pid}/comm 2>/dev/null")
            comm = comm_out[0].strip() if comm_out else ""
            if comm == "indexer":
                return pid
        self.log.warning(
            f"{label}No process with comm='indexer' found on {node_ip} among "
            f"candidate PIDs {candidate_pids} (matched via pgrep -f '/indexer')"
        )
        return None

    @staticmethod
    def _extract_proc_value(lines, field_name):
        """Pull the integer value for ``field_name:`` out of /proc-style lines."""
        for line in lines:
            line = line.strip()
            if line.startswith(field_name + ":"):
                parts = line.split()
                if len(parts) >= 2 and parts[1].isdigit():
                    return int(parts[1])
        return None

    def _log_thp_process_tree(self, shell, node_ip, label=""):
        """Diagnostic only: log THP_enabled across indexer/projector/babysitter.

        PR_SET_THP_DISABLE is inherited across fork() but its behavior across
        execve() is kernel-version dependent, so it's worth seeing whether the
        flag actually landed on indexer specifically rather than assuming the
        babysitter's prctl call carried through.
        """
        out, _ = shell.execute_command("pgrep -f 'indexer|babysitter|projector'")
        for pid in [l.strip() for l in out if l.strip()]:
            comm_out, _ = shell.execute_command(f"cat /proc/{pid}/comm 2>/dev/null")
            status_out, _ = shell.execute_command(
                f"grep '^THP_enabled:' /proc/{pid}/status 2>/dev/null"
            )
            comm = comm_out[0].strip() if comm_out else "?"
            thp = self._extract_proc_value(status_out, "THP_enabled")
            self.log.info(f"{label}{node_ip}: pid={pid} comm={comm} THP_enabled={thp}")

    _THP_SYSFS_PATHS = {
        "enabled": "/sys/kernel/mm/transparent_hugepage/enabled",
        "defrag": "/sys/kernel/mm/transparent_hugepage/defrag",
        "shmem_enabled": "/sys/kernel/mm/transparent_hugepage/shmem_enabled",
    }

    def _read_sysfs_thp_choice(self, shell, path):
        """Return the active choice from a THP sysfs knob, e.g. 'never' from
        the line 'always madvise [never]'. None if the file is unreadable
        (kernel built without CONFIG_TRANSPARENT_HUGEPAGE)."""
        out, _ = shell.execute_command(f"cat {path} 2>/dev/null")
        line = out[0].strip() if out else ""
        match = re.search(r"\[(\w+)\]", line)
        return match.group(1) if match else None

    def _capture_sysfs_thp_settings(self, shell, node_ip):
        settings = {}
        for name, path in self._THP_SYSFS_PATHS.items():
            settings[name] = self._read_sysfs_thp_choice(shell, path)
        self.log.info(f"[THP-SYSFS] {node_ip}: current settings = {settings}")
        return settings

    def _write_sysfs_thp_settings(self, shell, node_ip, settings, label=""):
        """Write ``settings`` (name -> chosen value) to the THP sysfs knobs.

        Skips a knob when its value is None (path was unreadable, e.g. this
        kernel doesn't expose it, or we have nothing to restore it to).
        """
        # Minimal/root-only images (common for AL2023, some Debian/Ubuntu
        # cloud images) often don't ship a 'sudo' binary at all, since the
        # SSH user is already root -- prepending 'sudo' unconditionally
        # would fail there with 'sudo: command not found'.
        sudo_prefix = "" if getattr(shell, "username", None) == "root" else "sudo "
        for name, value in settings.items():
            if value is None:
                continue
            path = self._THP_SYSFS_PATHS[name]
            shell.execute_command(
                f'{sudo_prefix}bash -c "echo {shlex.quote(value)} > {shlex.quote(path)}" 2>/dev/null'
            )
            readback = self._read_sysfs_thp_choice(shell, path)
            if readback != value:
                self.log.warning(
                    f"{label}{node_ip}: tried to set {name}={value} but "
                    f"readback is {readback} (write may have failed/needs root)"
                )
            else:
                self.log.info(f"{label}{node_ip}: set {name}={value}")

    def _check_thp_state(self, index_nodes, expect_disabled, label=""):
        """Validate THP state for the indexer process on every index node.

        THP_enabled in /proc/<pid>/status only reflects whether the process
        called prctl(PR_SET_THP_DISABLE): 0 means it opted out, 1 means it
        did not. ``expect_disabled=True`` (platform.disable_thp=true) asserts
        THP_enabled == 0 and AnonHugePages == 0 in smaps_rollup -- both must
        hold regardless of the box's global THP config, since prctl forces
        VM_NOHUGEPAGE for that process's future anonymous mappings.

        ``expect_disabled=False`` asserts THP_enabled == 1, but only asserts
        AnonHugePages > 0 when /proc/meminfo shows the box is actively
        serving THP system-wide. Without that control, a 0 could just mean
        the box's global THP is off (madvise/never) or the huge-page pool is
        exhausted -- not that the setting failed.
        """
        errors = []
        for index_node in index_nodes:
            shell = RemoteMachineShellConnection(index_node, verbose=False)
            try:
                pid = self._get_indexer_pid(shell, index_node.ip, label=label)
                if pid is None:
                    errors.append(f"{label} {index_node.ip}: no indexer PID found")
                    continue
                self.log.info(f"{label} Indexer PID on {index_node.ip}: {pid}")
                self._log_thp_process_tree(shell, index_node.ip, label=label)

                status_out, _ = shell.execute_command(
                    f"grep '^THP_enabled:' /proc/{pid}/status 2>/dev/null"
                )
                thp_enabled = self._extract_proc_value(status_out, "THP_enabled")
                self.log.info(
                    f"{label} /proc/{pid}/status THP_enabled on {index_node.ip}: "
                    f"{thp_enabled}"
                )

                smaps_out, _ = shell.execute_command(
                    f"grep -E '^(Anonymous|AnonHugePages):' "
                    f"/proc/{pid}/smaps_rollup 2>/dev/null"
                )
                anon_kb = self._extract_proc_value(smaps_out, "Anonymous")
                anon_huge_kb = self._extract_proc_value(smaps_out, "AnonHugePages")
                self.log.info(
                    f"{label} /proc/{pid}/smaps_rollup on {index_node.ip}: "
                    f"Anonymous={anon_kb} kB, AnonHugePages={anon_huge_kb} kB"
                )

                meminfo_out, _ = shell.execute_command(
                    "grep '^AnonHugePages:' /proc/meminfo 2>/dev/null"
                )
                system_anon_huge_kb = self._extract_proc_value(meminfo_out, "AnonHugePages")
                self.log.info(
                    f"{label} System-wide /proc/meminfo AnonHugePages on "
                    f"{index_node.ip}: {system_anon_huge_kb} kB"
                )

                if thp_enabled is None or anon_huge_kb is None:
                    errors.append(
                        f"{label} {index_node.ip}: could not read THP_enabled/"
                        f"AnonHugePages for PID {pid} (process may have exited "
                        f"or /proc files unreadable)"
                    )
                    continue

                if expect_disabled:
                    if thp_enabled != 0:
                        errors.append(
                            f"{label} {index_node.ip}: expected THP_enabled=0 "
                            f"(opted out) with platform.disable_thp=true, got "
                            f"{thp_enabled}"
                        )
                    if anon_huge_kb != 0:
                        errors.append(
                            f"{label} {index_node.ip}: expected AnonHugePages=0 kB "
                            f"with platform.disable_thp=true, got {anon_huge_kb} kB"
                        )
                else:
                    if thp_enabled != 1:
                        errors.append(
                            f"{label} {index_node.ip}: expected THP_enabled=1 "
                            f"(not opted out) with platform.disable_thp=false, "
                            f"got {thp_enabled}"
                        )
                    if system_anon_huge_kb:
                        if not anon_huge_kb:
                            errors.append(
                                f"{label} {index_node.ip}: platform.disable_thp=false "
                                f"but indexer AnonHugePages=0 kB even though the box "
                                f"is actively serving THP system-wide "
                                f"({system_anon_huge_kb} kB) -- the huge-page pool "
                                f"isn't exhausted, the indexer just isn't getting THP"
                            )
                    else:
                        self.log.warning(
                            f"{label} {index_node.ip}: system-wide AnonHugePages is "
                            f"0 kB, so this box isn't serving THP at all right now "
                            f"(global sysfs config or no memory pressure) -- "
                            f"skipping the indexer AnonHugePages>0 check, only "
                            f"THP_enabled is verified here"
                        )
            finally:
                shell.disconnect()
        return errors

    def test_thp_disable_enable_setting_check(self):
        """
        MB-65503: THP disable/enable setting check for the indexer process.

        Some lab machines ship with THP auto-disabled at the OS level
        (/sys/kernel/mm/transparent_hugepage/enabled = madvise or never), in
        which case the indexer would never get huge pages regardless of the
        platform.disable_thp setting -- so this first forces enabled, defrag,
        and shmem_enabled to 'always' on every index node (restored to their
        original values at the end, however the test exits).

        With THP actually available, validate that it is disabled for the
        indexer process when platform.disable_thp is true (default): cat
        /proc/<indexer PID>/status and verify THP_enabled is 0, and cat
        /proc/<PID>/smaps_rollup and verify AnonHugePages is 0 kB. Then disable
        the indexer setting (platform.disable_thp to false), restart the
        indexer on all index nodes, and verify THP_enabled flips to 1 and
        AnonHugePages becomes non-zero. Restores platform.disable_thp=true
        afterwards regardless of outcome.

        The indexer PID is cross-checked via /proc/<pid>/comm because
        babysitter/goport's cmdline also matches a plain ``pgrep -f indexer``.

        Works across RHEL 9, Ubuntu 24, Debian 13, Alma 9, SUSE 15, Rocky 9, AL2023.

        https://jira.issues.couchbase.com/browse/MB-65503
        """
        # ---- Phase 0: Load hotel dataset (1M items) and create indexes ----
        self.bucket_params = self._create_bucket_params(
            server=self.master, size=self.bucket_size,
            replicas=self.num_replicas, bucket_type=self.bucket_type,
            enable_replica_index=self.enable_replica_index,
            eviction_policy=self.eviction_policy, lww=self.lww,
        )
        self.cluster.create_standard_bucket(
            name=self.test_bucket, port=11222,
            bucket_params=self.bucket_params,
        )
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(
            num_scopes=1, num_collections=1,
            num_of_docs_per_collection=1000000,
            json_template="Hotel",
        )
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition()
        select_queries = []
        for namespace in self.namespaces:
            queries = self.gsi_util_obj.get_create_index_list(
                definition_list=query_definitions, namespace=namespace,
                num_replica=self.num_index_replica,
            )
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=queries, database=namespace,
                query_node=query_node,
            )
            select_queries.extend(self.gsi_util_obj.get_select_queries(
                definition_list=query_definitions, namespace=namespace,
            ))
        self.wait_until_indexes_online()

        index_nodes = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True,
        )
        self.assertGreaterEqual(len(index_nodes), 1, "Need at least one index node")

        # ---- Phase 0b: Capture each node's current OS-level THP posture ----
        # Some distros/lab images ship with THP auto-disabled (madvise/never).
        # If we don't force it on, the "AFTER" check below would see
        # AnonHugePages=0 even when platform.disable_thp correctly re-enables
        # THP for the indexer -- there'd simply be no huge pages to take, and
        # the test would wrongly look like the flag doesn't work.
        original_sysfs_settings = {}
        for index_node in index_nodes:
            shell = RemoteMachineShellConnection(index_node, verbose=False)
            try:
                original_sysfs_settings[index_node.ip] = (
                    self._capture_sysfs_thp_settings(shell, index_node.ip)
                )
            finally:
                shell.disconnect()

        try:
            # ---- Phase 0c: Force THP on at the OS level on every index node ----
            for index_node in index_nodes:
                shell = RemoteMachineShellConnection(index_node, verbose=False)
                try:
                    self._write_sysfs_thp_settings(
                        shell, index_node.ip,
                        {"enabled": "always", "defrag": "always",
                         "shmem_enabled": "always"},
                        label="[ENABLE-THP] ",
                    )
                finally:
                    shell.disconnect()

            # ---- Phase 1: THP should be disabled (platform.disable_thp=true by default) ----
            before_errors = self._check_thp_state(
                index_nodes, expect_disabled=True, label="[BEFORE]",
            )
            if before_errors:
                self.fail(
                    "THP state incorrect before disabling platform.disable_thp:\n"
                    + "\n".join(before_errors)
                )

            # ---- Phase 2: Disable platform.disable_thp (set to false) ----
            self.log.info(
                f"[DISABLE] Setting platform.disable_thp=false on {index_nodes[0].ip}"
            )
            rest = RestConnection(index_nodes[0])
            rest.set_index_settings({"platform.disable_thp": False})
            self.sleep(15, "Wait for THP setting to take effect")

            try:
                # ---- Phase 3: Restart indexer on all nodes so the new setting takes effect ----
                restart_errors = self._restart_indexer_on_all_nodes(
                    label="[DISABLE-RESTART] ",
                )
                if restart_errors:
                    self.fail(
                        "Indexer restart failed on some nodes:\n"
                        + "\n".join(restart_errors)
                    )

                # ---- Phase 3b: Run scans so the indexer actually touches/faults in data pages ----
                # prctl (and its absence) only affects *future* page faults, so THP
                # coverage has to be judged after fresh faults, not a config reload.
                self.log.info(
                    f"[SCAN] Running {len(select_queries)} select queries "
                    f"against created indexes"
                )
                scan_consistency = getattr(self, 'scan_consistency', None) or 'not_bounded'
                scan_tasks = self.gsi_util_obj.aysnc_run_select_queries(
                    select_queries=select_queries, query_node=query_node,
                    scan_consistency=scan_consistency,
                )
                for task in scan_tasks:
                    try:
                        task.result()
                    except Exception as err:
                        self.log.error(f"[SCAN] Select query failed: {err}")
                self.sleep(10, "Wait for indexer to settle after scans")

                # ---- Phase 4: THP should be enabled now that platform.disable_thp=false ----
                after_errors = self._check_thp_state(
                    index_nodes, expect_disabled=False, label="[AFTER]",
                )
                if after_errors:
                    self.fail(
                        "THP state incorrect after disabling platform.disable_thp:\n"
                        + "\n".join(after_errors)
                    )
            finally:
                # ---- Cleanup: restore platform.disable_thp=true so later tests aren't affected ----
                self.log.info(
                    f"[CLEANUP] Restoring platform.disable_thp=true on {index_nodes[0].ip}"
                )
                try:
                    rest.set_index_settings({"platform.disable_thp": True})
                    self.sleep(15, "Wait for THP setting to take effect")
                    cleanup_restart_errors = self._restart_indexer_on_all_nodes(
                        label="[CLEANUP-RESTART] ",
                    )
                    if cleanup_restart_errors:
                        self.log.error(
                            "[CLEANUP] Indexer restart failed on some nodes:\n"
                            + "\n".join(cleanup_restart_errors)
                        )
                except Exception as e:
                    self.log.error(
                        f"[CLEANUP] Failed to restore platform.disable_thp: {e}"
                    )
        finally:
            # ---- Cleanup: put each node's OS-level THP posture back the way it was ----
            for index_node in index_nodes:
                shell = RemoteMachineShellConnection(index_node, verbose=False)
                try:
                    self._write_sysfs_thp_settings(
                        shell, index_node.ip,
                        original_sysfs_settings.get(index_node.ip, {}),
                        label="[RESTORE-THP] ",
                    )
                finally:
                    shell.disconnect()

