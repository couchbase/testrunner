import threading
from .tuq import QueryTests
from upgrade.newupgradebasetest import NewUpgradeBaseTest
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

        # feature specific setup
        if self.feature == "ansi-joins":
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
            self.google_error_msg = "Errorevaluatingprojection.-cause:URLendpointisn'twhitelisted" \
                                    "https://maps.googleapis.com/maps/api/geocode/json."
            self.jira_error_msg ="Errorevaluatingprojection.-cause:URLendpointisn'twhitelistedhttps://jira.atlassian." \
                                 "com/rest/api/latest/issue/JRA-9.PleasemakesuretowhitelisttheURLontheUI."
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
        self.upgrade_servers = self.servers
        if hasattr(self, 'upgrade_versions'):
            self.log.info("checking upgrade version")
            upgrade_major = self.upgrade_versions[0][0]
            self.log.info("upgrade major version: " + str(upgrade_major))
            if int(upgrade_major) == 5:
                self.log.info("setting intial_version to: 4.6.5-4742")
                self.initial_version = "4.6.5-4742"
            elif int(upgrade_major) == 6:
                self.log.info("setting intial_version to: 5.5.2-3733")
                self.initial_version = "5.5.2-3733"
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
        self.wait_for_all_indexes_online()

        self.log.info("UPGRADE_VERSIONS = " + str(self.upgrade_versions))
        # run pre upgrade test
        self.log.info("running pre upgrade test")
        self.run_upgrade_test_for_feature(self.feature, "pre-upgrade")
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
            self.offline_upgrade(mixed_servers)

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
        self.log.info("running mixed mode test")
        self.run_upgrade_test_for_feature(self.feature, "mixed-mode")
        self.log.info("completed mixed mode test")

        # upgrade remaining servers
        self.log.info("upgrading remaining servers")

        if self.upgrade_type == "offline":
            # stop server, upgrade, rebalance in
            self.offline_upgrade(remaining_servers)

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
        self.run_upgrade_test_for_feature(self.feature, "post-upgrade")
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
                                                     services=['kv,n1ql,index'])
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

    def run_upgrade_test_for_feature(self, feature, phase):
        if feature == "ansi-joins":
            self.run_ansi_join_upgrade_test(phase)
        elif feature == "xattrs":
            self.run_xattrs_upgrade_test(phase)
        elif feature == "auditing":
            self.run_auditing_upgrade_test(phase)
        elif feature == "backfill":
            self.run_backfill_upgrade_test(phase)
        elif feature == "curl-whitelist":
            self.run_curl_whitelist_upgrade_test(phase)
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
        self.assertEqual(actual_curl['results'][0]['$1'], expected_curl)

        self.rest.create_whitelist(self.master, {"all_access": True,
                                                 "allowed_urls": None,
                                                 "disallowed_urls": None})
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertEqual(actual_curl['results'][0]['$1'], expected_curl)

    # test_allowed_url from tuq_curl_whitelist.py
    def run_test_allowed_url(self):
        self.rest.create_whitelist(self.master, {"all_access": False, "allowed_urls": ["https://maps.googleapis.com"]})

        url = "'https://jira.atlassian.com/rest/api/latest/issue/JRA-9'"
        query="select curl("+ url +")"
        curl = self.shell.execute_commands_inside(self.cbqpath, query, '', '', '', '', '')
        actual_curl = self.convert_to_json(curl)
        self.assertTrue(self.jira_error_msg in actual_curl['errors'][0]['msg'],
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
            expectedResults = {'node':'%s:%s' % (self.master.ip, self.master.port), 'status': 'success', 'isAdHoc': True,
                               'name': 'SELECT statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'SELECT * FROM default LIMIT 100',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
                               'description': 'A N1QL SELECT statement was executed'}
        elif query_type == 'insert':
            if self.filter:
                self.execute_filtered_query()
            self.run_cbq_query(server=self.master, query='INSERT INTO default ( KEY, VALUE ) VALUES ("1",{ "order_id": "1", "type": '
                                     '"order", "customer_id":"24601", "total_price": 30.3, "lineitems": '
                                     '[ "11", "12", "13" ] })')
            expectedResults = {'node': '%s:%s' % (self.master.ip, self.master.port), 'status': 'success', 'isAdHoc': True,
                               'name': 'INSERT statement', 'real_userid': {'source': source, 'user': user},
                               'statement': 'INSERT INTO default ( KEY, VALUE ) VALUES ("1",{ "order_id": "1", "type": '
                                            '"order", "customer_id":"24601", "total_price": 30.3, "lineitems": '
                                            '[ "11", "12", "13" ] })',
                               'userAgent': 'Python-httplib2/$Rev: 259 $', 'id': self.eventID,
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
        query_response = self.run_cbq_query("SELECT " + field + " FROM default")
        docs = sorted(query_response['results'])
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
            self.assertTrue(sorted(docs_1) == sorted(compare))

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
                    self.assertTrue(sorted(docs_2) == sorted(compare_docs))
                else:
                    self.assertTrue(len(docs_2)-10 == len(compare_docs))
                    self.assertTrue(sorted(docs_1) == sorted(docs_2))
            else:
                self.assertTrue(sorted(docs_2) == sorted(compare_docs))
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
                self.assertTrue(sorted(docs_3) == sorted(compare_docs))
            else:
                self.assertTrue(len(docs_3)-10 == len(compare_docs))
                self.assertTrue(sorted(docs_2) == sorted(docs_3))

            self.run_cbq_query("DROP INDEX default." + index_name)

            self.reload_data()
            self.system_xattr_data = self.create_xattr_data(type=xattr_type)
        # Virtual xattrs cant be compared in the way the rest of the stuff is compared because it is autogenerated by CB
        # ,therefore we do different checks and then return the results of the query passed in instead
        if xattr_type == 'virtual':
            return docs_1
        else:
            return compare