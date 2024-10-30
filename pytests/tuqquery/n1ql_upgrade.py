import threading
from .tuq import QueryTests
from upgrade.newupgradebasetest import NewUpgradeBaseTest
from .flex_index_phase1 import FlexIndexTests
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator
import couchbase.subdocument as SD
from membase.api.rest_client import RestHelper
from security.audittest import auditTest
from security.auditmain import audit
import socket
import urllib.request, urllib.parse, urllib.error
from SystemEventLogLib.SystemEventOperations import SystemEventRestHelper
from security.rbac_base import RbacBase
from deepdiff import DeepDiff
import threading
import time
import json

class QueriesUpgradeTests(QueryTests, NewUpgradeBaseTest):

    def setUp(self):
        super(QueriesUpgradeTests, self).setUp()
        if self._testMethodName == 'suite_setUp':
            return
        self.log.info("==============  QueriesUpgradeTests setup has started ==============")

        # general setup
        self.feature = self.input.param('feature', None)
        self.upgrade_type = self.input.param('upgrade_type', None)
        self._all_buckets_flush()
        self.load(self.gens_load, flag=self.item_flag)
        self.bucket_doc_map = {"default": 2016, "standard_bucket0": 2016}
        self.bucket_status_map = {"default": "healthy", "standard_bucket0": "healthy"}
        self.custom_map = self.input.param("custom_map", False)
        self.fts_index_type = self.input.param("fts_index_type", None)

        # feature specific setup
        if self.feature == "ansi-joins" or self.feature == "n1ql":
            self.rest.load_sample("travel-sample")
            self.bucket_doc_map["travel-sample"] = 31591
            self.bucket_status_map["travel-sample"] = "healthy"
        if self.feature == "backfill":
            self.directory_path = self.input.param("directory_path", "/opt/couchbase/var/lib/couchbase/tmp")
            self.create_directory = self.input.param("create_directory", True)
            self.tmp_size = self.input.param("tmp_size", 5120)
            self.nonint_size = self.input.param("nonint_size", False)
            self.out_of_range_size = self.input.param("out_of_range_size", False)
            self.set_backfill_directory = self.input.param("set_backfill_directory", True)
            self.change_directory = self.input.param("change_directory", False)
            self.reset_settings = self.input.param("reset_settings", False)
            self.curl_url = "http://%s:%s/settings/querySettings" % (self.master.ip, self.master.port)
        if self.feature == "xattrs":
            self.system_xattr_data = []
            self.user_xattr_data = []
            self.meta_ids = []
        if self.feature == "curl-whitelist":
            self.google_error_msg = "Errorevaluatingprojection-cause:Theendpointhttps://maps.googleapis.com/maps/api/geocode/jsonisnotpermitted"
            self.jira_error_msg = "Theendpointhttps://jira.atlassian.com/rest/api/latest/issue/JRA-9isnotpermitted.Listallowedendpointsintheconfiguration."
            self.cbqpath = '%scbq' % self.path + " -e %s:%s -q -u %s -p %s" \
                                                 % (self.master.ip, self.n1ql_port, self.rest.username, self.rest.password)
        if self.feature == "auditing":
            self.audit_codes = [28672, 28673, 28674, 28675, 28676, 28677, 28678, 28679, 28680, 28681,
                                28682, 28683, 28684, 28685, 28686, 28687, 28688]
            self.unauditedID = self.input.param("unauditedID", "")
            self.audit_url = "http://%s:%s/settings/audit" % (self.master.ip, self.master.port)
            self.filter = self.input.param("filter", False)
        self.log.info("==============  QueriesUpgradeTests setup has completed ==============")

    def suite_setUp(self):

        super(QueriesUpgradeTests, self).suite_setUp()
        self.log.info("==============  QueriesUpgradeTests suite_setup has started ==============")
        self.log.info("==============  QueriesUpgradeTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueriesUpgradeTests tearDown has started ==============")
        if not hasattr(self, 'rest'):
            self.rest = RestConnection(self.master)
        self.upgrade_servers = self.servers
        if hasattr(self, 'upgrade_versions') and self.initial_version is '6.5.1-6299':
            self.log.info("checking upgrade version")
            upgrade_major = self.upgrade_versions[0][0]
            self.log.info("upgrade major version: " + str(upgrade_major))
            if int(upgrade_major) == 5:
                self.log.info("setting intial_version to: 4.6.5-4742")
                self.initial_version = "4.6.5-4742"
            elif int(upgrade_major) == 6:
                self.log.info("setting intial_version to: 5.5.6-4733")
                self.initial_version = "5.5.6-4733"
            elif int(upgrade_major) == 7:
                self.log.info("setting intial_version to: 6.0.3-2895")
                self.initial_version = "6.0.3-2895"
            else:
                self.log.info("upgrade version invalid: " + str(self.upgrade_versions[0]))
                self.fail()
        self.log.info("==============  QueriesUpgradeTests tearDown has completed ==============")
        super(QueriesUpgradeTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QueriesUpgradeTests suite_tearDown has started ==============")
        self.log.info("==============  QueriesUpgradeTests suite_tearDown has completed ==============")
        super(QueriesUpgradeTests, self).suite_tearDown()

    def test_upgrade(self):
        """
        Upgrade Test.
        1) Run pre-upgrade feature test
        2) Upgrade a single node
        3) Run mixed-mode feature test on upgraded node
        4) Upgrade the remaining nodes
        5) Run post-upgrade feature test
        """

        # Perform pre_upgrade operations on cluster
        # install version defined in self.initial_version (newupgradebasetest)
        self.log.info("Begin n1ql upgrade test: {0}".format(self.upgrade_type))
        self.log_config_info()
        self.wait_for_buckets_status(self.bucket_status_map, 5, 120)
        self.wait_for_bucket_docs(self.bucket_doc_map, 5, 120)
        self.log_config_info()
        self.wait_for_all_indexes_online()
        self.ensure_primary_indexes_exist()
        self.wait_for_all_indexes_online(build_deferred=True)

        self.log.info("UPGRADE_VERSIONS = " + str(self.upgrade_versions))
        # run pre upgrade test
        self.log.info("running pre upgrade test")
        self.run_upgrade_test_for_feature(self.feature, "pre-upgrade", self.upgrade_type)
        self.log.info("completed pre upgrade test")

        # take 1 server to upgrade to test mixed mode scenarios
        mixed_servers = [server for server in self.servers[:1]]  # first server to upgrade for mixed mode tests
        remaining_servers = [server for server in self.servers[1:]]  # the rest of the servers

        # set this to have initial version reinstalled in tearDown
        self.upgrade_servers = [server for server in self.servers]

        # upgrade mixed mode servers
        self.log.info("upgrading servers for mixed mode")

        if self.upgrade_type == "offline":
            # stop server, upgrade, rebalance
            self.offline_upgrade(self.servers)


        if self.upgrade_type == "online":
            # rebalance out, upgrade, rebalance in
            self.online_upgrade(mixed_servers)

        if self.upgrade_type == "online_with_swap_rebalance":
            # 4 servers, only 3 servers in cluster, 1 will be used to do the swap rebalance
            # rebalance out initial node, upgrade swap rebalance in
            rebalance = self.cluster.async_rebalance(remaining_servers, [], mixed_servers)
            rebalance.result()

            self.online_upgrade_with_swap_rebalance(mixed_servers[0], [remaining_servers[0]])

        if self.upgrade_type == "online_with_failover":
            # graceful failover, upgrade, full recovery
            self.master = remaining_servers[0]
            self.online_upgrade_with_failover(mixed_servers)

        # set master to the upgraded server for mixed mode tests, run_cbq_query executes against master
        self.master = mixed_servers[0]
        self.log.info("upgraded {0} servers: {1}".format(str(len(mixed_servers)), str(mixed_servers)))
        self.log.info("cluster is now in mixed mode")

        self.log_config_info()
        self.wait_for_buckets_status(self.bucket_status_map, 5, 120)
        self.wait_for_bucket_docs(self.bucket_doc_map, 5, 120)
        self.wait_for_all_indexes_online()
        self.log_config_info()

        # run mixed mode test
        self.ensure_primary_indexes_exist()
        self.log.info("running mixed mode test")
        self.run_upgrade_test_for_feature(self.feature, "mixed-mode", self.upgrade_type)
        self.log.info("completed mixed mode test")

        # upgrade remaining servers
        self.log.info("upgrading remaining servers")

        #if self.upgrade_type == "offline":
        #    # stop server, upgrade, rebalance in
        #    self.offline_upgrade(remaining_servers)

        if self.upgrade_type == "online":
            # rebalance out, upgrade, rebalance in
            self.online_upgrade(remaining_servers)

        if self.upgrade_type == "online_with_swap_rebalance":
            # rebalance out initial node, upgrade swap rebalance in
            # mixed server is upgraded and remaining_server[0] is out of cluster
            self.online_upgrade_with_swap_rebalance(remaining_servers[0], remaining_servers[1:])

        if self.upgrade_type == "online_with_failover":
            # graceful failover, upgrade, full recovery
            self.online_upgrade_with_failover(remaining_servers)

        self.log.info("successfully upgraded {0} remaining servers: {1}".format(str(len(remaining_servers)), str(remaining_servers)))

        self.log_config_info()
        self.wait_for_buckets_status(self.bucket_status_map, 5, 120)
        self.wait_for_bucket_docs(self.bucket_doc_map, 5, 120)
        self.wait_for_all_indexes_online()
        self.log_config_info()

        # run post upgrade test
        self.ensure_primary_indexes_exist()
        self.log.info("running post upgrade test")
        self.run_upgrade_test_for_feature(self.feature, "post-upgrade", self.upgrade_type)
        self.log.info("completed post upgrade test")

    def stop_cb_servers(self, server_list):
        for server in server_list:
            remote = RemoteMachineShellConnection(server)
            remote.stop_server()
            remote.disconnect()

    def offline_upgrade(self, servers=[]):
        # stop server, upgrade, rebalance in
        self.stop_cb_servers(servers)
        upgrade_threads = self._async_update(self.upgrade_versions[0], servers)
        for upgrade_thread in upgrade_threads:
            upgrade_thread.join()

    def online_upgrade(self, upgrade_servers=[]):
        self.log.info("online upgrade servers: {0}".format(str(upgrade_servers)))
        for server in upgrade_servers:
            self.log.info("upgrading: {0}".format(str(server)))
            participating_servers = [s for s in self.servers]
            participating_servers.remove(server)
            self.log.info("participating servers: {0}".format(str(participating_servers)))
            rebalance = self.cluster.async_rebalance(participating_servers, [], [server])
            rebalance.result()
            upgrade_th = self._async_update(self.upgrade_versions[0], [server])
            for th in upgrade_th:
                th.join()
            rebalance = self.cluster.async_rebalance(participating_servers,
                                                     [server], [],
                                                     services=['kv,n1ql,index,fts'])
            rebalance.result()

    def online_upgrade_with_failover(self, upgrade_servers):
        self.log.info("online upgrade servers: {0}".format(str(upgrade_servers)))
        for server in upgrade_servers:
            self.log.info("upgrading: {0}".format(str(server)))
            participating_servers = [s for s in self.servers]
            failover_task = self.cluster.async_failover([self.master], failover_nodes=[server], graceful=False)
            failover_task.result()
            upgrade_th = self._async_update(self.upgrade_versions[0], [server])
            for th in upgrade_th:
                th.join()
            rest = RestConnection(self.master)
            nodes_all = rest.node_statuses()
            for cluster_node in nodes_all:
                if cluster_node.ip == server.ip:
                    rest.add_back_node(cluster_node.id)
                    rest.set_recovery_type(otpNode=cluster_node.id, recoveryType="full")


            participating_servers.remove(server)
            self.log.info("participating servers: {0}".format(str(participating_servers)))
            rebalance = self.cluster.async_rebalance(participating_servers, [], [])
            rebalance.result()

    def online_upgrade_with_swap_rebalance(self, out_server, upgrade_servers):
        self.log.info("online upgrade servers: {0}".format(str(upgrade_servers)))
        for server in upgrade_servers:
            self.log.info("upgrading: {0}".format(str(out_server)))
            participating_servers = [s for s in self.servers]
            participating_servers.remove(out_server)
            participating_servers.remove(server)
            self.log.info("participating servers: {0}".format(str(participating_servers)))
            upgrade_th = self._async_update(self.upgrade_versions[0], [out_server])
            for th in upgrade_th:
                th.join()
            rebalance = self.cluster.async_rebalance(participating_servers,
                                                     [out_server], [server],
                                                     services=["kv,index,n1ql"])
            rebalance.result()
            out_server = server

    def run_upgrade_test_for_feature(self, feature, phase, upgrade_type):
        if feature == "ansi-joins":
            self.run_ansi_join_upgrade_test(phase)
        elif feature == "n1ql":
            self.run_n1ql_upgrade_test(phase, upgrade_type)
        elif feature == "xattrs":
            self.run_xattrs_upgrade_test(phase)
        elif feature == "auditing":
            self.run_auditing_upgrade_test(phase)
        elif feature == "backfill":
            self.run_backfill_upgrade_test(phase)
        elif feature == "curl-whitelist":
            self.run_curl_whitelist_upgrade_test(phase)
        elif feature == "flex-index":
            self.run_flex_index_upgrade_test(phase)
        else:
            self.fail("FAIL: feature {0} not found".format(feature))

    def run_ansi_join_upgrade_test(self, phase):
        if phase == "pre-upgrade":
            self.log.info("running pre-upgrade test for ansi joins")
        elif phase == "mixed-mode":
            self.log.info("running mixed-mode test for ansi joins")
        elif phase == "post-upgrade":
            self.run_test_basic_join()
        else:
            self.fail("FAIL: (ansi-join) invalid phase: {0}".format(phase))

    def run_n1ql_upgrade_test(self, phase, upgrade_type):
        if phase == "pre-upgrade":
            self.log.info("running pre-upgrade test for N1QL")
            if upgrade_type != "OFFLINE":
                with self.subTest("PRE PREPARE TEST"):
                    self.run_test_prepare(phase=phase)
            if int(self.initial_version[0]) == 7:
                with self.subTest("PRE UDF INLINE TEST"):
                    self.run_test_udf_inline(phase=phase)
                with self.subTest("PRE CBO MIGRATION TEST"):
                    self.run_test_cbo_migration(phase=phase)
                with self.subTest("PRE UDF JAVASCRIPT TEST"):
                    self.run_test_udf_javascript_migration(phase=phase)
        elif phase == "mixed-mode":
            self.log.info("running mixed-mode test for N1QL")
            with self.subTest("MIXED FILTER TEST"):
                self.run_test_filter()
            with self.subTest("MIXED WINDOW CLAUSE TEST"):
                self.run_test_window()
            with self.subTest("MIXED UDF INLINE TEST"):
                self.run_test_udf_inline(phase=phase)
            with self.subTest("MIXED UDF JAVASCRIPT MIGRATION TEST"):
                self.run_test_udf_javascript_migration(phase=phase)
            with self.subTest("MIXED UDF N1QL JAVASCRIPT TEST"):
                self.run_test_udf_n1ql_javascript()
            # if int(self.initial_version[0]) == 7:
            #     with self.subTest("MIXED QUERY LIMIT TEST"):
            #         self.test_query_limit()
            #with self.subTest("MIXED SYSTEM EVENT TEST"):
            #    self.test_system_event()
            with self.subTest("MIXED ADVISOR STATEMENT TEST"):
                self.run_test_advisor_statement()
            with self.subTest("MIXED ADVISOR SESSION TEST"):
                self.run_test_advisor_session()
            #with self.subTest("MIXED UPDATE STATISTICS TEST"):
            #    self.run_test_update_stats()
            #with self.subTest("MIXED CBO TEST"):
            #    self.run_test_cbo()
            #with self.subTest("MIXED COLLECTION TEST"):
            #    self.run_test_collection(phase=phase)
            if upgrade_type != "offline":
                with self.subTest("MIXED PREPARE TEST"):
                    self.run_test_prepare(phase=phase)
            with self.subTest("MIXED SYSTEM TABLE TEST"):
                self.run_test_system_tables()
        elif phase == "post-upgrade":
            with self.subTest("POST FILTER TEST"):
                self.run_test_filter()
            with self.subTest("POST WINDOW CLAUSE TEST"):
                self.run_test_window()
            with self.subTest("POST UDF INLINE TEST"):
                self.run_test_udf_inline(phase=phase)
            with self.subTest("POST UDF JAVASCRIPT MIGRATION TEST"):
                self.run_test_udf_javascript_migration(phase=phase)
            with self.subTest("POST UDF JAVASCRIPT TEST"):
                self.run_test_udf_javascript()
            with self.subTest("POST UDF N1QL JAVASCRIPT TEST"):
                self.run_test_udf_n1ql_javascript()
            with self.subTest("POST FLATTEN ARRAY INDEX TEST"):
                self.test_flatten_array_index()
            with self.subTest("POST SYSTEM EVENT TEST"):
                self.test_system_event()
            # with self.subTest("POST QUERY LIMIT TEST"):
            #     self.test_query_limit()
            with self.subTest("POST ADVISOR STATEMENT TEST"):
                self.run_test_advisor_statement()
            with self.subTest("POST ADVISOR SESSION TEST"):
                self.run_test_advisor_session()
            with self.subTest("POST CBO MIGRATION TEST"):
                self.run_test_cbo_migration()
            with self.subTest("POST UPDATE STATISTICS TEST"):
                self.run_test_update_stats(phase=phase)
            with self.subTest("POST CBO TEST"):
                self.run_test_cbo()
            with self.subTest("POST COLLECTION TEST"):
                self.run_test_collection(phase=phase)
            with self.subTest("POST TRANSACTION TEST"):
                self.run_test_transaction()
            with self.subTest("POST SEQUENCE TEST"):
                self.run_test_sequences()
            if upgrade_type != "offline":
                with self.subTest("POST PREPARE TEST"):
                    self.run_test_prepare(phase=phase)
            with self.subTest("POST SYSTEM TABLE TEST"):
                self.run_test_system_tables()
        else:
            self.fail("FAIL: (N1QL) invalid phase: {0}".format(phase))

    def run_flex_index_upgrade_test(self, phase):
        ft_object = FlexIndexTests()
        ft_object.init_flex_object(self)
        flex_query_list = ["select meta().id from default {0} where name = 'employee-6'"]
        if phase == "pre-upgrade":
            self.log.info("running pre-upgrade test for flex index")
            self.create_fts_index(
                name="default_index", source_name="default", doc_count=2016, index_storage_type=self.fts_index_type)
        elif phase == "mixed-mode":
            self.log.info("running mixed-mode test for flex index")
        elif phase == "post-upgrade":
            self.log.info("running post-upgrade test for flex index")
            failed_to_run_query, not_found_index_in_response, result_mismatch = ft_object.run_query_and_validate(flex_query_list)
            if failed_to_run_query or not_found_index_in_response or result_mismatch:
                self.fail("Found queries not runnable: {0} or required index not found in the query resonse: {1} "
                          "or flex query and gsi query results not matching: {2}"
                          .format(failed_to_run_query, not_found_index_in_response, result_mismatch))
            else:
                self.log.info("All queries passed")
        else:
            self.fail("FAIL: (flex index) invalid phase: {0}".format(phase))

    def run_xattrs_upgrade_test(self, phase):
        if phase == "pre-upgrade":
            self.log.info("running pre-upgrade test for xattrs")
        elif phase == "mixed-mode":
            self.log.info("running mixed-mode test for xattrs")
        elif phase == "post-upgrade":
            self.log.info("running post-upgrade test for xattrs")
            self.run_test_system_xattr_composite_secondary_index()
        else:
            self.fail("FAIL: (xattrs) invalid phase: {0}".format(phase))

    def run_auditing_upgrade_test(self, phase):
        if phase == "pre-upgrade":
            self.log.info("running pre-upgrade test for auditing")
        elif phase == "mixed-mode":
            self.log.info("running mixed-mode test for auditing")
        elif phase == "post-upgrade":
            self.log.info("running post-upgrade test for auditing")
            self.set_audit()
            self.eventID = 28676
            self.op_type = "insert"
            self.run_test_queryEvents()
            self.op_type = "select"
            self.unauditedID = 28678
            self.eventID = 28672
            self.filter = True
            self.run_test_queryEvents()
        else:
            self.fail("FAIL: (auditing) invalid phase: {0}".format(phase))

    def run_backfill_upgrade_test(self, phase):
        if phase == "pre-upgrade":
            self.log.info("running pre-upgrade test for backfill")
        elif phase == "mixed-mode":
            self.log.info("running mixed-mode test for backfill")
        elif phase == "post-upgrade":
            self.log.info("running post-upgrade test for backfill")
            self.reset_settings = True
            self.run_test_backfill()
            self.reset_settings = False
            self.directory_path = "/opt/couchbase/var/lib/couchbase/testing"
            self.change_directory = True
            self.run_test_backfill()
        else:
            self.fail("FAIL: (backfill) invalid phase: {0}".format(phase))

    def run_curl_whitelist_upgrade_test(self, phase):
        if phase == "pre-upgrade":
            self.log.info("running pre-upgrade test for curl whitelist")
        elif phase == "mixed-mode":
            self.log.info("running mixed-mode test for curl whitelist")
        elif phase == "post-upgrade":
            self.log.info("running post-upgrade test for curl whitelist")
            self.run_test_all_access_true()
            self.run_test_allowed_url()
            self.run_test_disallowed_url()
        else:
            self.fail("FAIL: (curl whitelist) invalid phase: {0}".format(phase))

    ###############################
    #
    # ANSI Joins Tests
    #
    ###############################

    # test_basic_join in tuq_ansi_joins.py
    def run_test_basic_join(self):
        idx_list = []
        queries_to_run = []
        index = "CREATE INDEX idx1 on `travel-sample`(id)"
        idx_list.append((index, ("`travel-sample`", "idx1")))
        query = "select * from default d1 INNER JOIN `travel-sample` t on (d1.join_day == t.id)"
        queries_to_run.append((query, 288)) # 288 for doc-per-day=1
        self.run_common_body(index_list=idx_list, queries_to_run=queries_to_run)

    def run_test_filter(self):
        filter_query = "SELECT COUNT(*) FILTER (WHERE city = 'Paris') as count_paris FROM `travel-sample` WHERE type = 'airport'"
        case_query = "SELECT COUNT(CASE WHEN city = 'Paris' THEN 1 ELSE NULL END) as count_paris FROM `travel-sample` WHERE type = 'airport'"
        filter_results = self.run_cbq_query(filter_query)
        case_results = self.run_cbq_query(case_query)
        self.assertEqual(filter_results['results'], case_results['results'])

    def run_test_window(self):
        window_clause_query = "SELECT d.id, d.destinationairport, \
            AVG(d.distance) OVER ( window1 ) AS `rank` \
            FROM `travel-sample` AS d WHERE d.type='route' \
            WINDOW window1 AS (PARTITION BY d.destinationairport) \
            ORDER BY 1 \
            LIMIT 5"
        window_query = "SELECT d.id, d.destinationairport, \
            AVG(d.distance) OVER ( PARTITION BY d.destinationairport ) AS `rank` \
            FROM `travel-sample` AS d WHERE d.type='route' \
            ORDER BY 1 \
            LIMIT 5"
        window_clause_query_results = self.run_cbq_query(window_clause_query)
        window_query_results = self.run_cbq_query(window_query)
        self.assertEqual(window_clause_query_results['results'], window_query_results['results'])

    def run_test_update_stats(self,phase=None):
        histogram_expected = [
            {"bucket": "travel-sample", "collection": "_default", "histogramKey": "city", "scope": "_default"},
            {"bucket": "travel-sample", "collection": "_default", "histogramKey": "country", "scope": "_default"}]
        if phase == "post-upgrade":
            histogram_expected = [
                {'scope': '_default', 'collection': '_default', 'histogramKey': '(meta().id)'},
                { "scope": "_default", "collection": "_default", "histogramKey": "city"},
                { "scope": "_default", "collection": "_default", "histogramKey": "country"},
                {'scope': '_default', 'collection': '_default', 'histogramKey': '(distinct (array ((_usv_1.ratings).Cleanliness) for _usv_1 in reviews end))'},
                {'scope': '_default', 'collection': '_default', 'histogramKey': 'free_parking'},
                {'scope': '_default', 'collection': '_default',
                 'histogramKey': '(distinct (array (_usv_1.author) for _usv_1 in reviews end))'},
                {'scope': '_default', 'collection': '_default', 'histogramKey': 'type'},
                {'scope': '_default', 'collection': '_default', 'histogramKey': 'email'}]
        update_stats = "UPDATE STATISTICS FOR `travel-sample`(city, country) WITH {'update_statistics_timeout':180}"
        try:
            self.run_cbq_query(query=update_stats)
            if phase == "post-upgrade":
                histogram = self.run_cbq_query(query="select `bucket`, `scope`, `collection`, `histogramKey` from `travel-sample`.`_system`.`_query` data WHERE type = 'histogram' and `scope` = '_default' and `collection` = '_default'")
            else:
                histogram = self.run_cbq_query(query="select `bucket`, `scope`, `collection`, `histogramKey` from `N1QL_SYSTEM_BUCKET`.`N1QL_SYSTEM_SCOPE`.`N1QL_CBO_STATS` data WHERE type = 'histogram'")
            diffs = DeepDiff(histogram['results'],histogram_expected, ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        except Exception as e:
            self.log.error(f"Update statistics failed: {e}")
            self.fail()
        finally:
            self.run_cbq_query(query="UPDATE STATISTICS FOR `travel-sample` DELETE ALL")

    def run_test_neg_update_stats(self):
        pass

    def run_test_udf_n1ql_javascript(self):
        functions = 'function select1() {\
            var query = SELECT airportname FROM `travel-sample` WHERE type = "airport" AND city = "Lyon" ORDER BY airportname;\
            var acc = [];\
            for (const row of query) {\
                acc.push(row);\
            }\
            return acc;\
        }'
        function_names = ["select1"]
        self.log.info("Create n1ql library")
        self.create_library("n1ql", functions, function_names)
        try:
            self.run_cbq_query(query='CREATE FUNCTION select1() LANGUAGE JAVASCRIPT AS "select1" AT "n1ql"')
            results = self.run_cbq_query(query="EXECUTE FUNCTION select1()")
            self.assertEqual(results['results'], [[{'airportname': 'Bron'}, {'airportname': 'Lyon Part-Dieu Railway'}, {'airportname': 'Saint Exupery'}]])
        finally:
            try:
                self.log.info("Delete n1ql library")
                self.delete_library("n1ql")
                self.run_cbq_query("DROP FUNCTION select1")
            except Exception as e:
                self.log.error(str(e))

    def run_test_udf_javascript(self):
        functions = 'function adder(a, b, c) { for (i=0; i< b; i++){a = a + c;} return a; }'
        function_names = ["adder"]
        self.log.info("Create math library")
        self.create_library("math", functions, function_names)
        try:
            self.run_cbq_query(query='CREATE FUNCTION func2(a,b,c) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            results = self.run_cbq_query(query="EXECUTE FUNCTION func2(1,3,5)")
            self.assertEqual(results['results'], [16])
        finally:
            try:
                self.log.info("Delete math library")
                self.delete_library("math")
                self.run_cbq_query("DROP FUNCTION func2")
            except Exception as e:
                self.log.error(str(e))

    def run_test_udf_javascript_migration(self,phase=None):
        functions = 'function adder(a, b, c) { for (i=0; i< b; i++){a = a + c;} return a; }'
        function_names = ["adder"]
        if phase == "pre-upgrade" or int(self.initial_version[0]) < 7:
            self.log.info("Create math library")
            self.create_library("math", functions, function_names)
        try:
            if phase == "pre-upgrade" or int(self.initial_version[0]) < 7:
                self.run_cbq_query(query='CREATE FUNCTION func2(a,b,c) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            results = self.run_cbq_query(query="EXECUTE FUNCTION func2(1,3,5)")
            self.assertEqual(results['results'], [16])
        finally:
            try:
                if phase == "post-upgrade":
                    self.log.info("Delete math library")
                    self.delete_library("math")
                    self.run_cbq_query("DROP FUNCTION func2")
            except Exception as e:
                self.log.error(str(e))

    def run_test_udf_inline(self, phase):
        try:
            if phase == "pre-upgrade" or int(self.initial_version[0]) < 7:
                self.run_cbq_query("CREATE OR REPLACE FUNCTION func1(a,b,c) { (SELECT RAW SUM((a+b+c-40))) }")
            if phase == "mixed-mode" or phase == "post-upgrade":
                results = self.run_cbq_query("EXECUTE FUNCTION func1(10,20,30)")
                self.assertEqual(results['results'], [[20]])
                results = self.run_cbq_query("SELECT func1(10,20,30)")
                self.assertEqual(results['results'], [{'$1': [20]}])
        except Exception as e:
            self.log.error(str(e))
            self.fail()
        finally:
            try:
                if int(self.initial_version[0]) < 7:
                    self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

    def test_flatten_array_index(self):
        self.run_cbq_query("DROP INDEX idx1 IF EXISTS ON `travel-sample`")
        create_query = "create index idx1 on `travel-sample`(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking, type)"
        query = "SELECT MIN(r.ratings.Cleanliness), MAX(r.ratings.Cleanliness) FROM `travel-sample` AS d unnest reviews as r WHERE d.type = 'hotel' and r.author LIKE 'N%' and r.author is not missing and r.ratings.Cleanliness > 1 AND d.free_parking = False AND d.email is not missing GROUP BY r.ratings.Cleanliness"
        self.run_cbq_query(query=create_query)
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue("idx1" in str(explain_results),"The query should be using idx1, check explain results {0}".format(explain_results))
        self.assertTrue("covers" in str(explain_results),
                        "The index is not covering, it should be. Check plan {0}".format(explain_results))
        self.assertTrue("index_group_aggs" in str(explain_results), "Index should be pushing down but it isn't. Please check the plan {0}".format(explain_results))

    def test_system_event(self):
        self.event_rest = SystemEventRestHelper([self.master])
        event_seen = False
        log_fields = ["uuid", "component", "event_id", "description", "severity", "timestamp", "extra_attributes", "node"]
        query = "'statement=select (select * from `default`)&memory_quota=1'"
        curl_output = self.shell.execute_command(
            f"{self.curl_path} -X POST -u {self.rest.username}:{self.rest.password} http://{self.master.ip}:{self.n1ql_port}/query/service -d {query}")
        self.log.info(curl_output)
        output = self.convert_list_to_json(curl_output[0])
        requestID = output['requestID']
        events = self.event_rest.get_events(server=self.master, events_count=-1)["events"]
        for event in events:
            if event['event_id'] == 1026:
                event_seen = True
                for field in log_fields:
                    self.assertTrue(field in event.keys(), f"Field {field} is not in the event and it should be, please check the event {event}")
                    self.assertEqual(event['component'], "query")
                    self.assertEqual(event['description'], "Request memory quota exceeded")
                    self.assertEqual(event['severity'], "info")
                    self.assertEqual(event['node'], self.master.ip)
                    self.assertEqual(event['extra_attributes']['request-id'], requestID)
        self.assertTrue(event_seen, f"We did not see the event id we were looking for: {events}")

    def test_query_limit(self):
        self.enforce_limits = True
        self.expected_error = False
        self.log.info("==============  Create SLEEP function ==============")
        functions = 'function sleep(delay) { var start = new Date().getTime(); while (new Date().getTime() < start + delay); return delay; }'
        function_names = ["sleep"]
        self.create_library("sleep", functions, function_names)
        self.run_cbq_query(query="CREATE OR REPLACE FUNCTION sleep(t) LANGUAGE JAVASCRIPT AS \"sleep\" AT \"sleep\"")

        self.log.info("==============  Creating USER limited1 ==============")
        testuser = [
            {'id': 'limited1', 'name': 'limited1', 'password': 'password'}
        ]
        RbacBase().create_user_source(testuser, 'builtin', self.master)
        full_permissions = 'bucket_full_access[*]:query_select[*]:query_update[*]:' \
                           'query_insert[*]:query_delete[*]:query_manage_index[*]:' \
                           'query_system_catalog:query_external_access:query_execute_global_external_functions'
        # Assign user to role
        role_list = [
            {'id': 'limited1', 'name': 'limited1', 'roles': f'{full_permissions}', 'password': 'password'}
        ]
        RbacBase().add_user_role(role_list, self.rest, 'builtin')
        self.cbqpath_user1 = f'{self.path}cbq -e {self.master.ip}:{self.n1ql_port} -q -u "limited1" -p {self.rest.password}'

        limits_user_1 = '{"query":{"num_concurrent_requests": 2}}'
        self.set_query_limits(username="limited1", limits=limits_user_1)
        query = "EXECUTE FUNCTION sleep(10000)"

        t51 = threading.Thread(name='run_first', target=self.run_dummy_query_thread, args=("limited1", 20000))
        t51.start()
        t52 = threading.Thread(name='run_second', target=self.run_dummy_query_thread, args=("limited1", 20000))
        t52.start()
        time.sleep(1)
        # User 1 should not be allowed to run another query
        cbq_user1 = self.execute_commands_inside(self.cbqpath_user1, query, '', '', '', '', '')
        self.log.info(cbq_user1)
        self.assertTrue("429 Too Many Requests" in cbq_user1, f"The statement should have error'd {cbq_user1}")
        # After a query finishes, we should be able to run another query
        t51.join()
        cbq_user1 = self.execute_commands_inside(self.cbqpath_user1, query, '', '', '', '', '')
        results = self.convert_list_to_json(cbq_user1)
        self.assertEqual(results['status'], "success")
        self.assertEqual(results['results'], [10000])

    def run_test_collection(self, phase):
        scope = phase
        lyon_airport = ['Bron', 'Lyon Part-Dieu Railway', 'Saint Exupery']
        lyon_airport = ['Lyon Part-Dieu Railway', 'Saint Exupery']
        select_collection = 'select count(*) from `travel-sample`.`_default`.`_default` where type = "airport"'
        create_scope = f'CREATE SCOPE `travel-sample`.`{scope}`'
        create_collection = f'CREATE COLLECTION `travel-sample`.`{scope}`.airport'
        insert_query = f'INSERT INTO `travel-sample`.`{scope}`.airport(KEY _k, VALUE _v) SELECT META().id _k, _v FROM `travel-sample` _v WHERE type = "airport"'
        update_query = f'UPDATE `travel-sample`.`{scope}`.airport SET city = "lyon" WHERE city = "Lyon"'
        delete_query = f'DELETE FROM `travel-sample`.`{scope}`.airport WHERE airportname = "Bron"'
        select_collection_new = f'SELECT array_agg(airportname) from `travel-sample`.`{scope}`.airport WHERE city = "lyon"'
        select_context = 'SELECT array_agg(airportname) FROM airport WHERE city = "lyon"'
        # TODO advise_query = 'SELECT ADVISOR([""])'
        drop_scope = f'DROP SCOPE `travel-sample`.`{scope}`'
        drop_collection = f'DROP COLLECTION `travel-sample`.`{scope}`.airport'
        prepare_query = f'PREPARE `lyon_airport_{scope}` AS SELECT array_agg(airportname) FROM `travel-sample`.`{scope}`.airport WHERE city = "lyon"'
        execute_query = f'EXECUTE `lyon_airport_{scope}`'
        create_index = f'CREATE PRIMARY INDEX ON `travel-sample`.`{scope}`.`airport`'
        try:
            results = self.run_cbq_query(query=select_collection)
            self.assertEqual(results['results'], [{"$1": 1968}])

            results = self.run_cbq_query(query=create_scope)
            self.sleep(3)
            results = self.run_cbq_query(query=create_collection)
            self.sleep(3)
            results = self.run_cbq_query(query=f'SELECT `path` FROM system:keyspaces WHERE `scope` = "{scope}"')
            self.assertEqual(results['results'][0]['path'], f"default:travel-sample.{scope}.airport")

            results = self.run_cbq_query(query=insert_query)
            self.assertEqual(results['metrics']['mutationCount'], 1968)

            results = self.run_cbq_query(query=create_index)

            results = self.run_cbq_query(query=update_query)
            self.assertEqual(results['metrics']['mutationCount'], 3)

            results = self.run_cbq_query(query=delete_query)
            self.assertEqual(results['metrics']['mutationCount'], 1)

            results = self.run_cbq_query(query=select_collection_new)
            self.assertEqual(results['results'][0]['$1'], lyon_airport)

            results = self.run_cbq_query(query=select_context, query_context=f'travel-sample.{scope}')
            self.assertEqual(results['results'][0]['$1'], lyon_airport)

            results = self.run_cbq_query(query=prepare_query)
            results = self.run_cbq_query(query=execute_query)
            self.assertEqual(results['results'][0]['$1'], lyon_airport)
        except Exception as e:
            self.log.error("Query collection failed: {0}".format(e))
            self.fail()
        finally:
            results = self.run_cbq_query(query=drop_collection)
            results = self.run_cbq_query(query=drop_scope)

    def run_test_transaction(self):
        select_query = "select count(*) from `travel-sample`"
        insert_query = "INSERT INTO `travel-sample`(KEY,VALUE) VALUES ('airport_9999', {'id':9999,'type':'airport','airportname':'Bron','city':'Lyon','country':'France'})"
        update_query = "UPDATE `travel-sample` SET city = 'San Francisco' WHERE lower(city) = 'sanfrancisco'"
        delete_query = "DELETE FROM `travel-sample` WHERE type = 'airport' AND airportname = 'Bron'"
        merge_query = "MERGE INTO `travel-sample` AS target USING\
                [{'iata':'DSA', 'name': 'Doncaster Sheffield Airport'}, {'iata':'VLY', 'name': 'Anglesey Airport / Maes Awyr Môn'}] AS source\
                ON target.faa = source.iata \
                WHEN MATCHED THEN UPDATE SET target.old_name = target.airportname, target.airportname = source.name, target.updated = true\
                WHEN NOT MATCHED THEN INSERT (KEY UUID(), VALUE {'faa': source.iata, 'airportname': source.name, 'type': 'airport', 'inserted': true})"
        try:
            self.run_cbq_query(query="CREATE INDEX faa_idx on `travel-sample`(faa)")
            results = self.run_cbq_query(query="BEGIN WORK", server=self.master, txtimeout="180s")
            txid = results['results'][0]['txid']
            results = self.run_cbq_query(query=select_query, txnid=txid, server=self.master)
            results = self.run_cbq_query(query="SAVEPOINT S1", txnid=txid, server=self.master)
            results = self.run_cbq_query(query=insert_query, txnid=txid, server=self.master)
            results = self.run_cbq_query(query=update_query, txnid=txid, server=self.master)
            results = self.run_cbq_query(query=merge_query, txnid=txid, server=self.master)
            results = self.run_cbq_query(query=delete_query, txnid=txid, server=self.master)
            results = self.run_cbq_query(query="ROLLBACK TO SAVEPOINT S1", txnid=txid, server=self.master)
            results = self.run_cbq_query(query=select_query, txnid=txid, server=self.master)
            results = self.run_cbq_query(query="COMMIT", txnid=txid, server=self.master)
        except Exception as e:
            self.log.error("Transaction statement failed: {0}".format(e))
            self.fail()
        finally:
            self.run_cbq_query(query="DROP INDEX faa_idx on `travel-sample`")

    def run_test_advisor_statement(self):
        try:
            results = self.run_cbq_query(query="SELECT ADVISOR([ \
                \"UPDATE `travel-sample` SET city = 'San Francisco' WHERE lower(city) = 'sanfrancisco'\", \
                \"UPDATE `travel-sample` SET city = 'San Francisco' WHERE lower(city) = 'sanfrancisco'\" \
            ])", server=self.master)
            statements = results['results'][0]['$1']['recommended_indexes'][0]['statements']
            self.assertEqual(statements[0]['run_count'], 2)
        except Exception as e:
            self.log.error("Advisor statement failed: {0}".format(e))
            self.fail()

    def run_test_advisor_session(self):
        select_query = "SELECT airportname FROM `travel-sample` WHERE lower(city) = 'lyon' AND country = 'France'"
        try:
            start_session = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '1h', 'query_count': 2 })")
            session = start_session['results'][0]['$1']['session']
            results = self.run_cbq_query(query=select_query)
            results = self.run_cbq_query(query=select_query)
            self.sleep(3)
            stop_session = self.run_cbq_query(query=f"SELECT ADVISOR({{'action': 'stop', 'session': '{session}'}})")
            get_session = self.run_cbq_query(query=f"SELECT ADVISOR({{'action': 'get', 'session': '{session}'}})")
            self.assertTrue('recommended_indexes' in get_session['results'][0]['$1'][0][0], f"There are no recommended index: {get_session['results'][0]['$1'][0][0]}")
        except Exception as e:
            self.log.error("Advisor session failed: {0}".format(e))
            self.fail()
        finally:
            purge_session = self.run_cbq_query(query=f"SELECT ADVISOR({{'action': 'purge', 'session': '{session}'}})")

    def run_test_prepare(self, phase):
        prepare_query = "prepare lyon_airport as select array_agg(airportname) from `travel-sample` where type = 'airport' and city = 'Lyon'"
        execute_query = "execute lyon_airport"
        lyon_airport = ['Bron', 'Lyon Part-Dieu Railway', 'Saint Exupery']
        if phase == "pre-upgrade":
            self.run_cbq_query(query=prepare_query)
        if phase == "mixed-mode" or phase == "post-upgrade":
            results = self.run_cbq_query(query=execute_query)
            self.assertEqual(results['results'][0]['$1'], lyon_airport)

    def run_test_cbo(self):
        update_stats = "UPDATE STATISTICS FOR `travel-sample`(city) WITH {'update_statistics_timeout':600}"
        explain_query = "EXPLAIN select airportname from `travel-sample` where city = 'Lyon'"
        try:
            self.run_cbq_query(query=update_stats)
            explain_after = self.run_cbq_query(query=explain_query)
            self.assertTrue(explain_after['results'][0]['cost'] > 0 and explain_after['results'][0]['cardinality'] > 0)
        except Exception as e:
            self.log.error(f"cbo failed: {e}")
            self.fail()
        finally:
            self.run_cbq_query(query="UPDATE STATISTICS FOR `travel-sample` DELETE ALL")

    '''This test differs from the one above because it assumes stats were made pre upgrade then uses it post upgrade to see if they still work'''
    def run_test_cbo_migration(self,phase=None):
        if phase == "pre-upgrade":
            update_stats = "UPDATE STATISTICS FOR `travel-sample`(`type`) WITH {'update_statistics_timeout':600}"
        explain_query = "EXPLAIN select type from `travel-sample` where type = 'hotel'"
        try:
            if phase =="pre-upgrade":
                self.run_cbq_query(query=update_stats)
            explain_after = self.run_cbq_query(query=explain_query)
            self.assertTrue(explain_after['results'][0]['cost'] > 0 and explain_after['results'][0]['cardinality'] > 0)
        except Exception as e:
            self.log.error(f"cbo failed: {e}")
            self.fail()
        finally:
            if phase =="post-upgrade":
                self.run_cbq_query(query="UPDATE STATISTICS FOR `travel-sample` DELETE ALL")

    def run_test_system_tables(self):
        query_keyspaces_info = "select * from system:keyspaces_info where id = 'travel-sample'"
        results = self.run_cbq_query(query=query_keyspaces_info)
        keyspaces_info = results['results'][0]['keyspaces_info']
        self.assertEqual(keyspaces_info['count'], 31591)

        query_all_keyspaces = "select array_agg(`path`) from system:all_keyspaces where datastore_id = 'system'"
        system_keyspaces = [
            "system:active_requests","system:all_indexes","system:all_keyspaces","system:all_keyspaces_info",
            "system:all_scopes","system:all_sequences","system:applicable_roles","system:buckets",
            "system:completed_requests","system:completed_requests_history","system:datastores","system:dictionary","system:dictionary_cache",
            "system:dual","system:functions","system:functions_cache","system:indexes","system:keyspaces","system:keyspaces_info",
            "system:my_user_info","system:namespaces","system:nodes","system:prepareds","system:scopes",
            "system:sequences","system:tasks_cache","system:transactions","system:user_info","system:vitals"]
        results = self.run_cbq_query(query=query_all_keyspaces)
        all_keyspaces = results['results'][0]['$1']
        self.assertEqual(all_keyspaces, system_keyspaces)

        query_all_indexes = "select array_agg(distinct keyspace_id) from system:all_indexes where all_indexes.`namespace_id` = '#system'"
        system_all_indexes = [
            "active_requests", "all_indexes", "all_keyspaces", "all_keyspaces_info", "all_scopes", "all_sequences", "applicable_roles", "buckets",
            "completed_requests", "completed_requests_history", "datastores", "dictionary", "dictionary_cache", "dual", "functions", "functions_cache", "indexes",
            "keyspaces", "keyspaces_info", "my_user_info", "namespaces", "nodes", "prepareds", "scopes", "sequences", "tasks_cache", "transactions",
            "user_info", "vitals"]
        results = self.run_cbq_query(query=query_all_indexes)
        all_indexes = results['results'][0]['$1']
        self.assertEqual(all_indexes, system_all_indexes)

        query_all_keyspaces_info = "select array_agg(id) from system:all_keyspaces_info where all_keyspaces_info.datastore_id = 'system'"
        system_keyspaces_info = [
            "active_requests", "all_indexes", "all_keyspaces", "all_keyspaces_info", "all_scopes", "all_sequences", "applicable_roles", "buckets",
            "completed_requests", "completed_requests_history", "datastores", "dictionary", "dictionary_cache", "dual", "functions", "functions_cache", "indexes",
            "keyspaces", "keyspaces_info", "my_user_info", "namespaces", "nodes", "prepareds", "scopes", "sequences", "tasks_cache", "transactions",
            "user_info", "vitals"
        ]
        results = self.run_cbq_query(query=query_all_keyspaces_info)
        all_keyspaces_info = results['results'][0]['$1']
        self.assertEqual(all_keyspaces_info, system_keyspaces_info)

    def run_test_sequences(self):
        sequence_name = "seq_default_option"
        expected_default = [{'cache': 50, 'cycle': False, 'increment': 1, 'max': 9223372036854775807, 'min': -9223372036854775808, 'path': f'`default`:`default`.`_default`.`{sequence_name}`'}]
        self.run_cbq_query(f"DROP SEQUENCE `default`.`_default`.{sequence_name} IF EXISTS")
        self.run_cbq_query(f"CREATE SEQUENCE `default`.`_default`.{sequence_name}")

        result = self.run_cbq_query(f"SELECT `cache`, `cycle`, `increment`, `max`, `min`, `path` FROM system:sequences WHERE name = '{sequence_name}'")
        self.assertEqual(result['results'], expected_default)

        nextval = self.run_cbq_query(f"SELECT NEXTVAL FOR `default`.`_default`.{sequence_name} as val")
        self.assertEqual(nextval['results'][0]['val'], 0)

        nextval = self.run_cbq_query(f"SELECT NEXTVAL FOR `default`.`_default`.{sequence_name} as val")
        self.assertEqual(nextval['results'][0]['val'], 1)

        prevval = self.run_cbq_query(f"SELECT PREVVAL FOR `default`.`_default`.{sequence_name} as val")
        self.assertEqual(prevval['results'][0]['val'], 1)

        prevval = self.run_cbq_query(f"SELECT PREVVAL FOR `default`.`_default`.{sequence_name} as val")
        self.assertEqual(prevval['results'][0]['val'], 1)

        nextval = self.run_cbq_query(f"SELECT NEXTVAL FOR `default`.`_default`.{sequence_name} as val")
        self.assertEqual(nextval['results'][0]['val'], 2)
    
    ###############################
    #
    # Curl Whitelist Tests
    #
    ###############################

    # test_all_access_true from tuq_curl_whitelist.py
    def run_test_all_access_true(self):
        self.rest.create_whitelist(self.master, {"all_access": True,
                                                 "allowed_urls": ["blahahahahaha"], "disallowed_urls": ["fake"]})
        curl_output = self.shell.execute_command("%s https://jira.atlassian.com/rest/api/latest/issue/JRA-9"
                                                 % self.curl_path)
        expected_curl = self.convert_list_to_json(curl_output[0])
        url = "'https://jira.atlassian.com/rest/api/latest/issue/JRA-9'"
        query="select curl(" + url + ")"
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        # self.assertEqual(actual_curl['results'][0]['$1'], expected_curl)
        self.assertEqual(actual_curl['results'][0]['$1']['expand'], expected_curl['expand'])
        self.assertEqual(actual_curl['results'][0]['$1']['id'], expected_curl['id'])
        self.assertEqual(actual_curl['results'][0]['$1']['self'], expected_curl['self'])
        self.assertEqual(actual_curl['results'][0]['$1']['key'], expected_curl['key'])

        self.rest.create_whitelist(self.master, {"all_access": True,
                                                 "allowed_urls": None,
                                                 "disallowed_urls": None})
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        # self.assertEqual(actual_curl['results'][0]['$1'], expected_curl)
        self.assertEqual(actual_curl['results'][0]['$1']['expand'], expected_curl['expand'])
        self.assertEqual(actual_curl['results'][0]['$1']['id'], expected_curl['id'])
        self.assertEqual(actual_curl['results'][0]['$1']['self'], expected_curl['self'])
        self.assertEqual(actual_curl['results'][0]['$1']['key'], expected_curl['key'])

    # test_allowed_url from tuq_curl_whitelist.py
    def run_test_allowed_url(self):
        self.rest.create_whitelist(self.master, {"all_access": False, "allowed_urls": ["https://maps.googleapis.com"]})

        url = "'https://jira.atlassian.com/rest/api/latest/issue/JRA-9'"
        query="select curl("+ url +")"
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.log.info(f"Error: {actual_curl['errors'][0]}")
        self.assertTrue(self.jira_error_msg in actual_curl['errors'][0]['reason']['cause']['error'],
                        "Error message is %s this is incorrect it should be %s"
                        % (actual_curl['errors'][0]['msg'], self.jira_error_msg))

        curl_output = self.shell.execute_command("%s --get https://maps.googleapis.com/maps/api/geocode/json "
                                                 "-d 'address=santa+cruz&components=country:ES&key=AIzaSyCT6niGCMsgegJkQSYSqpoLZ4_rSO59XQQ'"
                                                 % self.curl_path)
        expected_curl = self.convert_list_to_json(curl_output[0])
        url = "'https://maps.googleapis.com/maps/api/geocode/json'"
        options= "{'get':True,'data': 'address=santa+cruz&components=country:ES&key=AIzaSyCT6niGCMsgegJkQSYSqpoLZ4_rSO59XQQ'}"
        query="select curl("+ url +", %s" % options + ")"
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertEqual(actual_curl['results'][0]['$1'], expected_curl)

    # test_disallowed_url from tuq_curl_whitelist.py
    def run_test_disallowed_url(self):
        self.rest.create_whitelist(self.master, {"all_access": False, "disallowed_urls": ["https://maps.googleapis.com"]})

        url = "'https://jira.atlassian.com/rest/api/latest/issue/JRA-9'"
        query="select curl("+ url +")"
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(self.jira_error_msg in actual_curl['errors'][0]['msg'],
                        "Error message is %s this is incorrect it should be %s"
                        % (actual_curl['errors'][0]['msg'], self.jira_error_msg))

        url = "'https://maps.googleapis.com/maps/api/geocode/json'"
        options= "{'get':True,'data': 'address=santa+cruz&components=country:ES&key=AIzaSyCT6niGCMsgegJkQSYSqpoLZ4_rSO59XQQ'}"
        query="select curl("+ url +", %s" % options + ")"
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(self.google_error_msg in actual_curl['errors'][0]['msg'],
                        "Error message is %s this is incorrect it should be %s"
                        % (actual_curl['errors'][0]['msg'], self.google_error_msg))

    ###############################
    #
    # Backfill Tests
    #
    ###############################

    # test_backfill from tuq_n1ql_backfill.py
    def run_test_backfill(self):
        if self.reset_settings:
            self.set_directory()
            self.set_tmpspace()
        try:
            if self.change_directory:
                self.set_directory()

            self.run_cbq_query(query="CREATE INDEX join_day on standard_bucket0(join_day)")
            for bucket in self.buckets:
                if bucket.name == 'standard_bucket0':
                    self._wait_for_index_online(bucket, 'join_day')

            thread1 = threading.Thread(name='monitor_backfill', target=self.monitor_backfill)
            thread1.setDaemon(True)
            thread2 = threading.Thread(name='execute_query', target=self.execute_query)
            thread1.start()
            thread2.start()
            thread2.join()

            if thread1.isAlive():
                self.assertTrue(False, "The backfill thread never registered any files")
            else:
                self.log.info("The backfill directory was being used during the query")
                self.assertTrue(True)
        finally:
            self.run_cbq_query(query="DROP INDEX standard_bucket0.join_day")

    def set_directory(self):
        # Try to create directory if it doesn't exist because backfill directories have to be created manually
        if self.create_directory:
            self.shell.create_directory(self.directory_path)
            self.shell.execute_command("chmod 777 %s" % self.directory_path)

        curl_output = self.shell.execute_command("%s -u Administrator:password -X POST -d 'queryTmpSpaceDir=%s' %s"
                                                 % (self.curl_path, self.directory_path, self.curl_url))
        expected_curl = self.convert_list_to_json(curl_output[0])
        self.log.info(expected_curl)
        return expected_curl

    def set_tmpspace(self):
        curl_output = self.shell.execute_command("%s -u Administrator:password -X POST -d 'queryTmpSpaceSize=%s' %s"
                                                 % (self.curl_path,  self.tmp_size, self.curl_url))
        expected_curl = self.convert_list_to_json(curl_output[0])
        self.log.info(expected_curl)
        return expected_curl

    def monitor_backfill(self):
        sftp = self.shell._ssh_client.open_sftp()
        no_backfill = True
        while no_backfill:
            if sftp.listdir(self.directory_path):
                no_backfill = False
        self.log.info("backfill is being used")
        return

    def execute_query(self):
        actual_results = self.run_cbq_query(query="select * from default d JOIN standard_bucket0 s on "
                                                  "(d.join_day == s.join_day)")
        return actual_results

    ###############################
    #
    # Auditing Tests
    #
    ###############################

    def run_test_queryEvents(self):
        # required for local testing: uncomment below
        self.ipAddress = self.master.ip
        #self.ipAddress = self.getLocalIPAddress()
        # self.eventID = self.input.param('id', None)
        auditTemp = audit(host=self.master)
        currentState = auditTemp.getAuditStatus()
        self.log.info("Current status of audit on ip - {0} is {1}".format(self.master.ip, currentState))
        if not currentState:
            self.log.info("Enabling Audit ")
            auditTemp.setAuditEnable('true')
            self.sleep(30)
        rest = RestConnection(self.master)
        self.setupLDAPSettings(rest)
        query_type = self.op_type
        user = self.master.rest_username
        source = 'ns_server'
        if query_type == 'select':
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(server=self.master, query="SELECT * FROM default LIMIT 100")
            expectedResults = {'node':'%s:%s' % (self.master.ip, self.master.port), 'status': 'success', 'errors': None, 'isAdHoc': True,
                               'name': 'SELECT statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'SELECT * FROM default LIMIT 100',
                               'userAgent': 'Python-httplib2/0.13.1 (gzip)', 'id': self.eventID,
                               'description': 'A N1QL SELECT statement was executed'}
        elif query_type == 'insert':
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(server=self.master, query='INSERT INTO default ( KEY, VALUE ) VALUES ("1",{ "order_id": "1", "type": '
                                     '"order", "customer_id":"24601", "total_price": 30.3, "lineitems": '
                                     '[ "11", "12", "13" ] })')
            expectedResults = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success', 'errors': None, 'isAdHoc': True,
                               'name': 'INSERT statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'INSERT INTO default ( KEY, VALUE ) VALUES ("1",{ "order_id": "1", "type": '
                                            '"order", "customer_id":"24601", "total_price": 30.3, "lineitems": '
                                            '[ "11", "12", "13" ] })',
                               'userAgent': 'Python-httplib2/0.13.1 (gzip)', 'id': self.eventID,
                               'description': 'A N1QL INSERT statement was executed'}

        if query_type == 'delete':
            self.checkConfig(self.eventID, self.servers[1], expectedResults, n1ql_audit=True)
            if self.filter:
                self.checkFilter(self.unauditedID, self.servers[1])
        else:
            self.checkConfig(self.eventID, self.master, expectedResults, n1ql_audit=True)
            if self.filter:
                self.checkFilter(self.unauditedID, self.master)

    def getLocalIPAddress(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('couchbase.com', 0))
        return s.getsockname()[0]

    def setupLDAPSettings (self, rest):
        api = rest.baseUrl + 'settings/saslauthdAuth'
        params = urllib.parse.urlencode({"enabled":'true',"admins":[],"roAdmins":[]})
        status, content, header = rest._http_request(api, 'POST', params)
        return status, content, header

    def set_audit(self, set_disabled=False, disable_user=False):
        if set_disabled:
            curl_output = self.shell.execute_command("%s -u Administrator:password -X POST -d 'auditdEnabled=%s;disabled=%s' %s"
                                                     % (self.curl_path, 'true', ','.join(map(str, self.audit_codes)), self.audit_url))
        elif disable_user:
            curl_output = self.shell.execute_command("%s -u Administrator:password -X POST -d 'auditdEnabled=%s;disabledUsers=%s' %s"
                                                     % (self.curl_path, 'true', 'no_select/local', self.audit_url))
        else:
            curl_output = self.shell.execute_command("%s -u Administrator:password -X POST -d 'auditdEnabled=%s;disabled=' %s"
                                                     % (self.curl_path, 'true', self.audit_url))
        if "errors" in str(curl_output):
            self.log.error("Auditing settings were not set correctly")
        self.sleep(10)

    def execute_filtered_query(self):
        self.audit_codes.remove(self.eventID)
        self.set_audit(set_disabled=True)
        self.run_cbq_query(query="delete from default limit 1")

    def checkConfig(self, eventID, host, expectedResults, n1ql_audit=False):
        Audit = audit(eventID=self.eventID, host=host)
        fieldVerification, valueVerification = Audit.validateEvents(expectedResults, n1ql_audit)
        self.assertTrue(fieldVerification, "One of the fields is not matching")
        self.assertTrue(valueVerification, "Values for one of the fields is not matching")

    def checkFilter(self, eventID, host):
        Audit = audit(eventID=eventID, host=host)
        exists, entry = Audit.validateEmpty()
        self.assertTrue(exists, "There was an audit entry found. Audits for the code %s should not be logged. Here is the entry: %s" % (eventID, entry))

    ###############################
    #
    # XAttrs Tests
    #
    ###############################

    def run_test_system_xattr_composite_secondary_index(self):
        #self.reload_data()
        self.fail_if_no_buckets()
        self.system_xattr_data = self.create_xattr_data(type='system')
        # full path query non-leading
        index_statement = "CREATE INDEX idx1 ON default(meta().id, meta().xattrs._system1, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system1 FROM default where meta().id is not missing"
        self.run_xattrs_query(query, index_statement, '_system1', 'idx1', 'default', xattr_data=self.system_xattr_data)

        # full path query non-leading
        index_statement = "CREATE INDEX idx2 ON default(meta().id, meta().xattrs._system2, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system2 FROM default where meta().id is not missing"
        self.run_xattrs_query(query, index_statement, '_system2', 'idx2', 'default', xattr_data=self.system_xattr_data)

        # full path query non-leading
        index_statement = "CREATE INDEX idx3 ON default(meta().id, meta().xattrs._system3, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system3 FROM default where meta().id is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx3', 'default', xattr_data=self.system_xattr_data)

        # partial path query non-leading
        index_statement = "CREATE INDEX idx4 ON default(meta().id, meta().xattrs._system3.field1, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system3.field1 FROM default where meta().id is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx4', 'default', xattr_data=self.system_xattr_data)

        # nested partial path query non-leading
        index_statement = "CREATE INDEX idx5 ON default(meta().id, meta().xattrs._system3.field1.sub_field1a, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system3.field1.sub_field1a FROM default where meta().id is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx5', 'default', xattr_data=self.system_xattr_data)

        # multiple paths single xattr query non-leading
        index_statement = "CREATE INDEX idx6 ON default(meta().id, meta().xattrs._system3, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system3, meta().xattrs._system3.field1, meta().xattrs._system3.field1.sub_field1a FROM default where meta().id is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx6', 'default', xattr_data=self.system_xattr_data)

        # deleted doc xattr query non-leading
        index_statement = "CREATE INDEX idx7 ON default(meta().id, meta().xattrs._system3, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system3 FROM default where meta().id is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx7', 'default', xattr_data=self.system_xattr_data, deleted_compare=True)

        # partial index non-leading
        index_statement = "CREATE INDEX idx8 ON default(meta().id, meta().xattrs._system3, join_day) where meta().xattrs._system3.field1.sub_field1a > 0 USING " + self.index_type
        query = "SELECT meta().xattrs._system3 FROM default where meta().id is not missing and meta().xattrs._system3.field1.sub_field1a > 0"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx8', 'default', xattr_data=self.system_xattr_data)

        # functional index non-leading
        index_statement = "CREATE INDEX idx9 ON default(meta().id, ABS(meta().xattrs._system3.field1.sub_field1a) + 2, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system3 FROM default where meta().id is not missing and ABS(meta().xattrs._system3.field1.sub_field1a) + 2 > 0"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx9', 'default', xattr_data=self.system_xattr_data)

        # full path query leading
        index_statement = "CREATE INDEX idx10 ON default(meta().xattrs._system1, meta().id, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system1 FROM default where meta().xattrs._system1 is not missing"
        self.run_xattrs_query(query, index_statement, '_system1', 'idx10', 'default', xattr_data=self.system_xattr_data)

        # full path query leading
        index_statement = "CREATE INDEX idx11 ON default(meta().xattrs._system2, meta().id, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system2 FROM default where meta().xattrs._system2 is not missing"
        self.run_xattrs_query(query, index_statement, '_system2', 'idx11', 'default', xattr_data=self.system_xattr_data)

        # full path query leading
        index_statement = "CREATE INDEX idx12 ON default(meta().xattrs._system3, meta().id, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system3 FROM default where meta().xattrs._system3 is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx12', 'default', xattr_data=self.system_xattr_data)

        # partial path query leading
        index_statement = "CREATE INDEX idx13 ON default(meta().xattrs._system3.field1, meta().id, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system3.field1 FROM default where meta().xattrs._system3.field1 is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx13', 'default', xattr_data=self.system_xattr_data)

        # nested partial path query leading
        index_statement = "CREATE INDEX idx14 ON default(meta().xattrs._system3.field1.sub_field1a, meta().id, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system3.field1.sub_field1a FROM default where meta().xattrs._system3.field1.sub_field1a is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx14', 'default', xattr_data=self.system_xattr_data)

        # multiple paths single xattr query leading
        index_statement = "CREATE INDEX idx15 ON default(meta().xattrs._system3.field1.sub_field1a, meta().id, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system3, meta().xattrs._system3.field1, meta().xattrs._system3.field1.sub_field1a FROM default where meta().xattrs._system3.field1.sub_field1a is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx15', 'default', xattr_data=self.system_xattr_data)

        # deleted doc xattr query leading
        index_statement = "CREATE INDEX idx16 ON default(meta().xattrs._system3, meta().id, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system3 FROM default where meta().xattrs._system3 is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx16', 'default', xattr_data=self.system_xattr_data, deleted_compare=True)

        # partial index leading
        index_statement = "CREATE INDEX idx17 ON default(meta().xattrs._system3, meta().id, join_day) where meta().xattrs._system3.field1.sub_field1a > 0 USING " + self.index_type
        query = "SELECT meta().xattrs._system3 FROM default where meta().xattrs._system3 is not missing and meta().xattrs._system3.field1.sub_field1a > 0"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx17', 'default', xattr_data=self.system_xattr_data)

        # functional index leading
        index_statement = "CREATE INDEX idx18 ON default(ABS(meta().xattrs._system3.field1.sub_field1a) + 2, meta().id, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system3 FROM default where ABS(meta().xattrs._system3.field1.sub_field1a) + 2 > 0"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx18', 'default', xattr_data=self.system_xattr_data)

    def reload_data(self):
        self._all_buckets_delete(self.master)
        self._bucket_creation()
        self.gens_load = self.gen_docs(self.docs_per_day)
        self.load(self.gens_load, batch_size=1000, flag=self.item_flag)
        self.create_primary_index_for_3_0_and_greater()

    def create_xattr_data(self, type="system"):
        cluster = Cluster('couchbase://'+str(self.master.ip))
        authenticator = PasswordAuthenticator(self.username, self.password)
        cluster.authenticate(authenticator)
        cb = cluster.open_bucket('default')
        docs = self.get_meta_ids()
        self.log.info("Docs: " + str(docs[0:5]))
        xattr_data = []
        self.log.info("Adding xattrs to data")
        val = 0
        for doc in docs:
            if type == "system":
                rv = cb.mutate_in(doc["id"], SD.upsert('_system1', val, xattr=True, create_parents=True))
                xattr_data.append({'_system1': val})
                rv = cb.mutate_in(doc["id"], SD.upsert('_system2', {'field1': val, 'field2': val*val}, xattr=True, create_parents=True))
                xattr_data.append({'_system2': {'field1': val, 'field2': val*val}})
                rv = cb.mutate_in(doc["id"], SD.upsert('_system3', {'field1': {'sub_field1a': val, 'sub_field1b': val*val}, 'field2': {'sub_field2a': 2*val, 'sub_field2b': 2*val*val}}, xattr=True, create_parents=True))
                xattr_data.append({'_system3': {'field1': {'sub_field1a': val, 'sub_field1b': val*val}, 'field2': {'sub_field2a': 2*val, 'sub_field2b': 2*val*val}}})
            if type == "user":
                rv = cb.mutate_in(doc["id"], SD.upsert('user1', val, xattr=True, create_parents=True))
                xattr_data.append({'user1': val})
                rv = cb.mutate_in(doc["id"], SD.upsert('user2', {'field1': val, 'field2': val*val}, xattr=True, create_parents=True))
                xattr_data.append({'user2': {'field1': val, 'field2': val*val}})
                rv = cb.mutate_in(doc["id"], SD.upsert('user3', {'field1': {'sub_field1a': val, 'sub_field1b': val*val}, 'field2': {'sub_field2a': 2*val, 'sub_field2b': 2*val*val}}, xattr=True, create_parents=True))
                xattr_data.append({'user3': {'field1': {'sub_field1a': val, 'sub_field1b': val*val}, 'field2': {'sub_field2a': 2*val, 'sub_field2b': 2*val*val}}})
            val = val + 1

        self.log.info("Completed adding " + type + "xattrs to data to " + str(val) + " docs")
        return xattr_data

    def get_meta_ids(self):
        return self.get_values_for_compare('meta().id')

    def get_values_for_compare(self, field):
        query_id = f'SELECT {field} FROM default'
        query_response = self.run_cbq_query(query=query_id)
        docs = sorted(query_response['results'], key=lambda item: item.get("id"))
        return docs

    def run_xattrs_query(self, query, index_statement, xattr_name, index_name, bucket_name, xattr_data=[], compare_fields=[], primary_compare=True, deleted_compare=False, with_retain=False, xattr_type="system", with_aggs=False, delete_leading=False):
        if index_statement != "":
            self.run_cbq_query(index_statement)
            self._wait_for_index_online(bucket_name, index_name)
            query_response = self.run_cbq_query("EXPLAIN " + query)
            self.assertTrue(index_name in str(query_response['results'][0]))
            if with_aggs:
                self.assertTrue("index_group_aggs" in str(query_response['results'][0]))

        query_response = self.run_cbq_query(query)
        docs_1 = query_response['results']
        self.log.info("XAttrs: " + str(docs_1[0:5]))
        compare = []

        if primary_compare:
            temp_query = query.split("FROM " + bucket_name)
            compare_query = temp_query[0] + "FROM " + bucket_name + ' use index(`#primary`)' + temp_query[1]
            compare_response = self.run_cbq_query(compare_query)
            compare = compare_response['results']
        else:
            if len(compare_fields) == 0:
                compare = [xattr for xattr in xattr_data if xattr_name in xattr]
            elif len(compare_fields) == 1:
                compare = [{compare_fields[0]: xattr[xattr_name][compare_fields[0]]} for xattr in xattr_data if xattr_name in xattr]
            elif len(compare_fields) == 2:
                compare = [{compare_fields[1]: xattr[xattr_name][compare_fields[0]][compare_fields[1]]} for xattr in xattr_data if xattr_name in xattr]

        # Virtual xattrs cant be compared in the way the rest of the stuff is compared because it is autogenerated by CB
        # ,therefore we do different checks and then return the results of the query passed in instead
        if xattr_type == "virtual":
            for docs in docs_1:
                if not compare_fields:
                    self.assertTrue('$document' in str(docs) and 'CAS' in str(docs) and 'datatype' in str(docs) and
                                    'deleted' in str(docs) and 'exptime' in str(docs) and 'flags' in str(docs) and
                                    'last_modified' in str(docs) and 'seqno' in str(docs) and
                                    'value_bytes' in str(docs) and 'vbucket_uuid' in str(docs))
                else:
                    self.assertTrue(docs == {"deleted": False})
        else:
            self.log.info("Compare: " + str(compare[0:5]))
            key1 = list(compare[0].keys())[0]
            if type(compare[0][key1]) == int:
                self.assertTrue(sorted(docs_1, key=lambda item: item.get(key1)) == sorted(compare, key=lambda item: item.get(key1)))
            else:
                key2 = list(compare[0][key1].keys())[0]
                if type(compare[0][key1][key2]) == int:
                    self.assertTrue(sorted(docs_1, key=lambda item: item.get(key1).get(key2)) == sorted(compare, key=lambda item: item.get(key1).get(key2)))
                else:
                    key3 = list(compare[0][key1][key2].keys())[0]
                    if type(compare[0][key1][key2][key3] == int):
                        self.assertTrue(sorted(docs_1, key=lambda item: item.get(key1).get(key2).get(key3)) == sorted(compare, key=lambda item: item.get(key1).get(key2).get(key3)))

        if deleted_compare:
            meta_ids = self.get_meta_ids()
            delete_ids = meta_ids[0:10]
            for id in delete_ids:
                query_response = self.run_cbq_query('DELETE FROM default USE KEYS[\"' + id['id'] + '\"] returning meta().id')
                self.log.info(query_response['results'])
                self.assertTrue(query_response['results'][0]["id"] == id["id"])

            new_meta_ids = self.get_meta_ids()
            for new_id in new_meta_ids:
                self.assertTrue(new_id not in delete_ids)

            query_response = self.run_cbq_query("EXPLAIN " + query)
            self.assertTrue(index_name in str(query_response['results'][0]))

            query_response_2 = self.run_cbq_query(query)
            docs_2 = query_response_2['results']
            self.log.info("XAttrs: " + str(docs_2[0:5]))

            temp_query = query.split("FROM " + bucket_name)
            compare_query = temp_query[0] + "FROM " + bucket_name + ' use index(`#primary`)' + temp_query[1]

            compare_response = self.run_cbq_query(compare_query)
            compare_docs = compare_response['results']

            self.log.info("Compare: " + str(compare_docs[0:5]))

            if with_retain and not xattr_type == 'user':
                self.assertTrue(len(docs_1)-10 == len(compare_docs))
                if delete_leading:
                    self.assertTrue(len(docs_2) == len(compare_docs))
                    self.assertTrue(sorted(docs_2, key=lambda item: item.get(xattr_name)) == sorted(compare_docs, key=lambda item: item.get(xattr_name)))
                else:
                    self.assertTrue(len(docs_2)-10 == len(compare_docs))
                    self.assertTrue(sorted(docs_1, key=lambda item: item.get(xattr_name)) == sorted(docs_2, key=lambda item: item.get(xattr_name)))
            else:
                key1 = list(compare_docs[0].keys())[0]
                if type(compare_docs[0][key1]) == int:
                    self.assertTrue(sorted(docs_2, key=lambda item: item.get(key1)) == sorted(compare_docs, key=lambda item: item.get(key1)))
                else:
                    key2 = list(compare_docs[0][key1].keys())[0]
                    if type(compare_docs[0][key1][key2]) == int:
                        self.assertTrue(sorted(docs_2, key=lambda item: item.get(key1).get(key2)) == sorted(compare_docs, key=lambda item: item.get(key1).get(key2)))
                    else:
                        key3 = list(compare_docs[0][key1][key2].keys())[0]
                        if type(compare_docs[0][key1][key2][key3] == int):
                            self.assertTrue(sorted(docs_2, key=lambda item: item.get(key1).get(key2).get(key3)) == sorted(compare_docs, key=lambda item: item.get(key1).get(key2).get(key3)))

                if not with_aggs:
                    self.assertTrue(len(docs_1)-10 == len(docs_2))

        if index_statement != "":
            self.run_cbq_query("DROP INDEX default." + index_name)

        if deleted_compare and with_retain:
            self.run_cbq_query(index_statement)
            self._wait_for_index_online(bucket_name, index_name)

            query_response = self.run_cbq_query("EXPLAIN " + query)
            self.assertTrue(index_name in str(query_response['results'][0]))

            query_response_1 = self.run_cbq_query(query)
            docs_3 = query_response_1['results']

            if delete_leading or xattr_type == 'user':
                self.assertTrue(len(docs_3) == len(compare_docs))
                self.assertTrue(sorted(docs_3, key=lambda item: item.get(xattr_name)) == sorted(compare_docs, key=lambda item: item.get(xattr_name)))
            else:
                self.assertTrue(len(docs_3)-10 == len(compare_docs))
                self.assertTrue(sorted(docs_2, key=lambda item: item.get(xattr_name)) == sorted(docs_3, key=lambda item: item.get(xattr_name)))

            self.run_cbq_query("DROP INDEX default." + index_name)

            self.reload_data()
            self.system_xattr_data = self.create_xattr_data(type=xattr_type)
        # Virtual xattrs cant be compared in the way the rest of the stuff is compared because it is autogenerated by CB
        # ,therefore we do different checks and then return the results of the query passed in instead
        if xattr_type == 'virtual':
            return docs_1
        else:
            return compare


    def set_query_limits(self, username="", limits="", server=None, skip_verify=False):
        if server is None:
            server = self.master
        if self.enforce_limits:
            curl_output = self.shell.execute_command(
                f"{self.curl_path} -X POST -u {self.rest.username}:{self.rest.password} http://{server.ip}:{server.port}/internalSettings "
                "-d 'enforceLimits=true' ")
            self.log.info(curl_output)
            curl_output = self.shell.execute_command(
                f"{self.curl_path} -X GET -u {self.rest.username}:{self.rest.password} http://{server.ip}:{server.port}/internalSettings")
            self.log.info(curl_output)
            enforce_limits_setting = self.convert_list_to_json(curl_output[0])
            self.assertEqual(enforce_limits_setting['enforceLimits'], True)
        full_permissions = 'bucket_full_access[*],query_select[*],query_update[*],' \
                           'query_insert[*],query_delete[*],query_manage_index[*],' \
                           'query_system_catalog,query_external_access,query_execute_global_external_functions'
        curl_output = self.shell.execute_command(f"{self.curl_path} -X PUT -u {self.rest.username}:{self.rest.password} http://{server.ip}:{server.port}/settings/rbac/users/local/{username} "
                                                 f"-d 'limits={limits}' -d 'roles={full_permissions}' ")
        self.log.info(curl_output)
        actual_output = self.convert_list_to_json(curl_output[0])
        if self.expected_error:
            self.assertTrue("Thevaluemustbeinrangefrom1toinfinity" in str(actual_output), f"The error is not right {actual_output}")
        else:
            curl_output = self.shell.execute_command(f"{self.curl_path} -X GET -u {self.rest.username}:{self.rest.password} http://{server.ip}:{server.port}/settings/rbac/users/local/{username}")
            self.log.info(curl_output)
            expected_settings = json.loads(limits)
            actual_settings = self.convert_list_to_json(curl_output[0])
            if not skip_verify:
                diffs = DeepDiff(actual_settings['limits'], expected_settings, ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)

    def run_dummy_query_thread(self, username='', delay=5000):
        try:
            cbqpath = f'{self.path}cbq -e {self.master.ip}:{self.n1ql_port} -q -u {username} -p {self.rest.password}'
            query = f"EXECUTE FUNCTION sleep({delay})"
            curl = self.execute_commands_inside(cbqpath, query, '', '', '', '', '')
            self.log.info(curl)
        finally:
            return
