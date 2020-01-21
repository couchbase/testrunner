import re
import logger
import time
import unittest
from selenium.common.exceptions import StaleElementReferenceException, ElementNotVisibleException
from lib.testconstants import STANDARD_BUCKET_PORT

from .uibasetest import * 
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from TestInput import TestInputSingleton
from couchbase_helper.cluster import Cluster
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from memcached.helper.data_helper import MemcachedClientHelper


class NavigationTest(BaseUITestCase):
    def setUp(self):
        super(NavigationTest, self).setUp()
        self.helper = BaseHelper(self)

    def tearDown(self):
        super(NavigationTest, self).tearDown()

    def test_navigation(self):
        tab = self.input.param('tab', None)
        if not tab:
            self.fail("Please add tab parameter to test config")
        self.helper.login()
        NavigationHelper(self).navigate(tab)


class BucketTests(BaseUITestCase):
    def setUp(self):
        super(BucketTests, self).setUp()
        self.helper = BaseHelper(self)
        self.helper.login()
        self.master =self.servers[0]

    def tearDown(self):
        super(BucketTests, self).tearDown()

    def test_add_bucket(self):
        bucket = Bucket(parse_bucket=self.input)
        NavigationHelper(self).navigate('Buckets')
        BucketHelper(self).create(bucket)

    def test_add_bucket_with_ops(self):
        error = self.input.param('error', None)
        # bucket_helper = BucketHelper(self)
        bucket = Bucket(parse_bucket=self.input)
        bucket.num_replica = '0'
        NavigationHelper(self).navigate('Buckets')
        try:
            BucketHelper(self).create(bucket)
        except Exception as ex:
            if error and str(ex).find('create new bucket pop up is not closed') != -1:
                actual_err = BucketHelper(self).get_error()
                self.assertTrue(actual_err.find(error) != -1, 'Expected error %s. Actual %s' % (error, actual_err))
                self.log.info('Error verified')
            else:
                raise ex
        else:
            if error:
                self.fail('Error was expected')

    def test_bucket_stats_mb_8538(self):
        self.bucket = Bucket()
        NavigationHelper(self).navigate('Buckets')
        BucketHelper(self).create(self.bucket)

        NavigationHelper(self).navigate('Indexes')
        DdocViewHelper(self).click_view_tab(text='Views')
        view_name = 'test_view_ui'
        DdocViewHelper(self).create_view(view_name, view_name)

        NavigationHelper(self).navigate('Buckets')
        BaseHelper(self).wait_ajax_loaded()
        BucketHelper(self).open_stats(self.bucket)
        total_views_st = BucketHelper(self).get_stat("views total disk size").replace(' views total', '')
        view_st = BucketHelper(self).get_stat("disk size", block="view")
        self.assertEqual(total_views_st, view_st,
                          "Stats should be equal, but views total disk size is %s"
                          " and disk size from view section is %s" % (
                            total_views_st, view_st))
        self.log.info("Stat 'views total disk size' and 'disk size' are %s"
                      " as expected" % total_views_st)

    def test_bucket_stats_connections(self):
        self.bucket = Bucket()
        NavigationHelper(self).navigate('Buckets')
        BucketHelper(self).create(self.bucket)

        BucketHelper(self).open_stats(self.bucket)
        conn_st = BucketHelper(self).get_stat("connections").replace(' connections', '')
        conn_per_sec_st = BucketHelper(self).get_stat("port 8091 reqs/sec").replace(' port 8091 reqs/sec', '')
        idle_st = BucketHelper(self).get_stat("idle streaming requests").replace(' idle streaming requests', '')
        wakeups_st = BucketHelper(self).get_stat("streaming wakeups/sec").replace(' streaming wakeups/sec', '')
        testuser = [{'id': self.bucket.name, 'name': self.bucket.name, 'password': 'password'}]
        rolelist = [{'id': self.bucket.name, 'name': self.bucket.name, 'roles': 'admin'}]
        self.add_built_in_server_user(testuser=testuser, rolelist=rolelist)
        client = MemcachedClientHelper.direct_client(self.servers[0], self.bucket.name)
        conn_stat = int(client.stats()["curr_connections"])
        self.assertEqual(int(conn_st), conn_stat - 1,
                          "Stats should be equal, but connections on UI is %s"
                          " and curr_connections is %s" % (
                            conn_st, conn_stat))
        self.assertFalse(int(conn_per_sec_st) == 0,
                        "At least one connection per second is present, and port 8091 reqs/sec is  0")
        self.assertFalse(int(idle_st) == 0,
                        "At least one idle streaming requests is present, UI shows 0")
        self.assertTrue(wakeups_st, "There is no wakup stat on UI")
        self.log.info("Stats are verified")

class InitializeTest(BaseUITestCase):
    def setUp(self):
        super(InitializeTest, self).setUp()
        self.cluster = Cluster()
        self._deinitialize_api()

    def tearDown(self):
        try:
            super(InitializeTest, self).tearDown()
        finally:
            if hasattr(self, 'cluster'):
                self._initialize_api()
                self.cluster.shutdown()

    def test_initialize(self):
        time.sleep(3)
        NodeInitializeHelper(self).initialize(self.input)

    def _initialize_api(self):
        init_tasks = []
        for server in self.servers:
            init_port = server.port or '8091'
            init_tasks.append(self.cluster.async_init_node(server, port=init_port))
        for task in init_tasks:
            task.result()

    def _deinitialize_api(self):
        for server in self.servers:
            try:
                rest = RestConnection(server)
                rest.force_eject_node()
                time.sleep(10)
                self.driver.refresh()
            except BaseException as e:
                self.fail(e)

class DocumentsTest(BaseUITestCase):
    def setUp(self):
        super(DocumentsTest, self).setUp()
        BaseHelper(self).login()

    def tearDown(self):
        super(DocumentsTest, self).tearDown()

    def test_create_doc(self):
        self.bucket = Bucket()
        RestConnection(self.servers[0]).create_bucket(bucket=self.bucket.name, ramQuotaMB=self.bucket.ram_quota or 100,
                                                              proxyPort=STANDARD_BUCKET_PORT + 1)
        NavigationHelper(self).navigate('Buckets')
        time.sleep(3)
        BucketHelper(self).open_documents(self.bucket)

        doc_name = self.input.param('doc_name', 'test')
        doc_content = self.input.param('content', '{"test" : "test"}')
        doc = Document(doc_name, doc_content)

        DocsHelper(self).create_doc(doc)
        self.assertTrue(DocsHelper(self).get_error() is None, "error appears: %s" \
                         % DocsHelper(self).get_error())

    def test_search_doc(self):
        self.bucket = Bucket()
        RestConnection(self.servers[0]).create_bucket(bucket=self.bucket.name, ramQuotaMB=self.bucket.ram_quota or 100,
                                                              proxyPort=STANDARD_BUCKET_PORT + 2)
        NavigationHelper(self).navigate('Buckets')
        BucketHelper(self).open_documents(self.bucket)
        doc_name = self.input.param('doc_name', 'test')
        doc_content = self.input.param('content', None)
        doc = Document(doc_name, doc_content)
        DocsHelper(self).create_doc(doc)

        DocsHelper(self).search(doc)
        self.assertTrue(DocsHelper(self).get_error() is None, "error appears: %s" \
                         % DocsHelper(self).get_error())

    def test_edit_doc(self):
        self.bucket = Bucket()
        RestConnection(self.servers[0]).create_bucket(bucket=self.bucket.name, ramQuotaMB=self.bucket.ram_quota or 100,
                                                              proxyPort=STANDARD_BUCKET_PORT + 3)
        NavigationHelper(self).navigate('Buckets')
        BucketHelper(self).open_documents(self.bucket)
        doc_name = self.input.param('doc_name', 'test')
        doc_content = self.input.param('content', '{"test" : "test"}')
        old_doc = Document(doc_name, doc_content)
        DocsHelper(self).create_doc(old_doc)

        error = self.input.param('error', None)
        doc_name = self.input.param('new_doc_name', 'test_edited')
        doc_content = self.input.param('doc_content', '{"test":"edited"}')
        new_doc = Document(doc_name, doc_content)
        action = self.input.param('action', 'save')
        try:
            result_doc = DocsHelper(self).edit_doc(old_doc, new_doc, action)
        except Exception as ex:
            if error:
                self.assertTrue(ex.message.find(error) > -1,
                                'Expected error is %s, but actual is %s' % (error, ex))
            else:
                raise ex
        else:
            NavigationHelper(self).navigate_breadcrumbs('Documents')
            DocsHelper(self).verify_doc_in_documents_screen(result_doc)

    def test_edit_doc_from_views_screen(self):
        self.bucket = Bucket()
        RestConnection(self.servers[0]).create_bucket(bucket=self.bucket.name, ramQuotaMB=self.bucket.ram_quota or 100,
                                                              proxyPort=STANDARD_BUCKET_PORT + 4)
        NavigationHelper(self).navigate('Buckets')
        BucketHelper(self).open_documents(self.bucket)
        doc_name = self.input.param('doc_name', 'test')
        doc_content = self.input.param('content', '{"test" : "test"}')
        old_doc = Document(doc_name, doc_content)
        DocsHelper(self).create_doc(old_doc)

        doc_content = self.input.param('doc_content', '{"test":"edited"}')
        new_doc = Document(old_doc.name, doc_content)

        NavigationHelper(self).navigate('Indexes')
        DdocViewHelper(self).click_view_tab(text='Views')
        view_name = 'test_view_ui'
        DdocViewHelper(self).create_view(view_name, view_name)
        DdocViewHelper(self).open_view(view_name)
        DdocViewHelper(self).click_edit_doc()
        DocsHelper(self).fill_edit_doc_screen(new_doc)
        NavigationHelper(self).navigate_breadcrumbs('Documents')
        DocsHelper(self).verify_doc_in_documents_screen(new_doc)

    def test_pagination_docs(self):
        self.bucket = Bucket()
        RestConnection(self.servers[0]).create_bucket(bucket=self.bucket.name, ramQuotaMB=self.bucket.ram_quota or 100,
                                                              proxyPort=STANDARD_BUCKET_PORT + 5)
        NavigationHelper(self).navigate('Buckets')
        BucketHelper(self).open_documents(self.bucket)

        items_per_page = self.input.param('items-per-page', 5)
        num_docs = self.input.param('num-docs', 10)
        doc_name = self.input.param('doc_name', 'test')
        doc_content = self.input.param('content', '{"test" : "test"}')
        num_pages = int(num_docs / items_per_page)

        DocsHelper(self).select_docs_per_page('100')
        for i in range(num_docs):
            doc = Document(doc_name + str(i), doc_content)
            DocsHelper(self).create_doc(doc)
            self.assertTrue(DocsHelper(self).get_error() is None, "error appears: %s" \
                            % DocsHelper(self).get_error())

        DocsHelper(self).select_docs_per_page(str(items_per_page))
        self.assertEqual(num_pages, DocsHelper(self).get_total_pages_num(),
                          "Total number of pages should be %s, actual is %s" % \
                          (num_pages, DocsHelper(self).get_total_pages_num()))

        self.log.info("total number of pages is %s as expected" % num_pages)

        for page in range(1, num_pages + 1):
            self.assertTrue(items_per_page >= DocsHelper(self).get_rows_count(),
                            "Items number per page is incorrect %s, expected %s" % \
                            (DocsHelper(self).get_rows_count(), items_per_page))
            self.log.info("Page has correct number of items: %s" % \
                          DocsHelper(self).get_rows_count())
            if page != num_pages:
                DocsHelper(self).go_to_next_page()

class SettingsTests(BaseUITestCase):
    def setUp(self):
        super(SettingsTests, self).setUp()
        self.helper = BaseHelper(self)

    def tearDown(self):
        super(SettingsTests, self).tearDown()

    def test_alerts(self):
        self.helper.login()
        NavigationHelper(self).navigate('Settings')
        SettingsHelper(self).navigate('Alerts')
        time.sleep(10)
        SettingsHelper(self).fill_alerts_info(self.input)
        time.sleep(10)
        NavigationHelper(self).navigate('Server Nodes')
        ServerHelper(self).add(self.input)
        ServerHelper(self).start_rebalancing()
        RestConnection(self.servers[0]).monitorRebalance()
        NavigationHelper(self).navigate('Settings')
        SettingsHelper(self).navigate('Auto-Failover')
        time.sleep(10)
        SettingsHelper(self).fill_auto_failover_info(self.input)

    def test_add_sample(self):
        sample = self.input.param('sample', 'beer-sample')
        num_expected = self.input.param('num_items', 7303)
        self.helper.login()
        NavigationHelper(self).navigate('Settings')
        SettingsHelper(self).navigate('Sample Buckets')
        sample_bucket = SettingsHelper(self).select_sample_bucket(sample)
        NavigationHelper(self).navigate('Buckets')
        self.assertTrue(BucketHelper(self).is_bucket_present(sample_bucket),
                        "Bucket %s is not present" % sample_bucket)
        end_time = time.time() + 120
        while time.time() < end_time:
            self.sleep(10)
            num_actual = BucketHelper(self).get_items_number(sample_bucket)
            if num_actual == str(num_expected):
                break
        self.assertTrue(num_actual == str(num_expected),
                        "Items number expected %s, actual %s" % (
                                                    num_expected, num_actual))
        self.log.info("Bucket items number is %s as expected" % num_actual)


class ROuserTests(BaseUITestCase):
    def setUp(self):
        super(ROuserTests, self).setUp()
        self.helper = BaseHelper(self)
        self.helper.login()
        if not self.input.param('skip_preparation', False):
            self.log.info("create bucket, view for check")
            self.bucket = Bucket()
            NavigationHelper(self).navigate('Buckets')
            RestConnection(self.servers[0]).create_bucket(bucket=self.bucket.name, ramQuotaMB=self.bucket.ram_quota or 100,
                                                           proxyPort=STANDARD_BUCKET_PORT + 6)
            NavigationHelper(self).navigate('Indexes')
            DdocViewHelper(self).click_view_tab(text='Views')
            self.view_name = 'test_view_ui'
            DdocViewHelper(self).create_view(self.view_name, self.view_name)

    def tearDown(self):
        RestConnection(self.servers[0]).delete_ro_user()
        super(ROuserTests, self).tearDown()

    def test_read_only_user(self):
        username = self.input.param('username', 'myrouser')
        password = self.input.param('password', 'myropass')

        NavigationHelper(self).navigate('Security')
        # SettingsHelper(self).click_ro_tab()
        SettingsHelper(self).create_user(username, password)
        self.log.info("Login with just created user")
        BaseHelper(self).logout()
        BaseHelper(self).login(user=username, password=password)
        self.verify_read_only(self.bucket, self.view_name)

    def test_delete_read_only_user(self):
        username = self.input.param('username', 'myrouser')
        password = self.input.param('password', 'myropass')
        time.sleep(2)
        NavigationHelper(self).navigate('Security')
        # SettingsHelper(self).click_ro_tab()
        SettingsHelper(self).create_user(username, password)
        SettingsHelper(self).delete_user(username)

        BaseHelper(self).logout()
        BaseHelper(self).login(user=username, password=password)
        time.sleep(3)
        self.assertTrue(BaseHelper(self).controls.error.is_displayed(), "Able to login")
        self.log.info("Unable to login as expected. %s" % BaseHelper(self).controls.error.get_text())

    def test_negative_read_only_user(self):
        username = self.input.param('username', 'myrouser')
        password = self.input.param('password', 'myropass')
        verify_password = self.input.param('verify_password', None)
        error = self.input.param('error', '')
        time.sleep(2)
        NavigationHelper(self).navigate('Security')
        # SettingsHelper(self).click_ro_tab()
        try:
            SettingsHelper(self).create_user(username, password, verify_password)
        except Exception as ex:
            self.assertTrue(str(ex).find(error) != -1, "Error message is incorrect. Expected %s, actual %s" % (error, str(ex)))
        else:
            self.fail("Error %s expected but not appeared" % error)

    def verify_read_only(self, bucket, view):
        navigator = NavigationHelper(self)
        self.log.info("Servers check")
        navigator.navigate('Servers')
        for btn in ServerHelper(self).controls.server_row_controls().failover_btns:
            self.assertFalse(btn.is_displayed(), "There is failover btn")
        for btn in ServerHelper(self).controls.server_row_controls().remove_btns:
            self.assertFalse(btn.is_displayed(), "There is remove btn")
        self.log.info("Bucket check")
        navigator.navigate('Buckets')
        BaseHelper(self).wait_ajax_loaded()
        BucketHelper(self).controls.bucket_info(bucket.name).arrow.click()
        self.assertFalse(BucketHelper(self).controls.edit_btn().is_displayed(),
                         "Bucket can be edited")
        self.log.info("Views check")
        NavigationHelper(self).navigate('Indexes')
        DdocViewHelper(self).click_view_tab(text='Views')
        DdocViewHelper(self).open_view(view)
        self.assertTrue(DdocViewHelper(self).controls.view_map_reduce_fn().save_btn.get_attribute("disabled") == 'true',
                        "Save button not disabled")
        self.assertTrue(DdocViewHelper(self).controls.view_map_reduce_fn().saveas_btn.get_attribute("disabled") == 'true',
                        "Save as button not disabled")


class ExternalUserTests(BaseUITestCase):
    def setUp(self):
        super(ExternalUserTests, self).setUp()
        self.helper = BaseHelper(self)
        self.helper.login()
        self.log.info("create bucket")
        self.bucket = Bucket()
        NavigationHelper(self).navigate('Buckets')
        RestConnection(self.servers[0]).create_bucket(bucket=self.bucket.name, ramQuotaMB=self.bucket.ram_quota or 100,
                                                           proxyPort=STANDARD_BUCKET_PORT + 6)

    def tearDown(self):
        super(ExternalUserTests, self).tearDown()

    def test_external_user(self):
        username = self.input.param('username', '')
        full_name = self.input.param('full_name', '')
        roles = self.input.param('roles', '')
        expected_error = self.input.param('expected_error', '')
        # mode = self.input.param('mode', 'enable')

        if roles == "":
            roles = []
        else:
            roles = roles.split(";")
            roles = [role.replace("@", "*") for role in roles]

        NavigationHelper(self).navigate('Security')
        # SettingsHelper(self).click_external_user_tab()
        # if mode == 'enable':
        #     SettingsHelper(self).enable_authentication()
        # else:
        #     SettingsHelper(self).disable_authentication()
        SettingsHelper(self).create_external_user(username, full_name, roles, expected_error)
        if not expected_error:
            SettingsHelper(self).delete_external_user(username)


class RebalanceProgressTests(BaseUITestCase):
    def setUp(self):
        super(RebalanceProgressTests, self).setUp()
        self.helper = ServerHelper(self)
        self.baseHelper = BaseHelper(self)
        self.baseHelper.login()
        num_buckets = self.input.param("num_buckets", 1)
        self.buckets = []
        NavigationHelper(self).navigate('Buckets')
        for i in range(num_buckets):
            bucket = Bucket(name='bucket%s' % i, ram_quota=200, sasl_pwd='password')
            RestConnection(self.servers[0]).create_bucket(bucket=bucket.name, ramQuotaMB=bucket.ram_quota or 100,
                                                          saslPassword=bucket.sasl_password,
                                                           proxyPort=STANDARD_BUCKET_PORT + i + 1)
            self.buckets.append(bucket)
        self.baseHelper.loadSampleBucket(self.servers[0], 'beer')

    def tearDown(self):
        super(RebalanceProgressTests, self).tearDown()

    def test_rebalance_in(self):
        self.baseHelper = BaseHelper(self)
        NavigationHelper(self).navigate('Server Nodes')
        ServerHelper(self).add(self.input)
        ServerHelper(self).start_rebalancing()
        transfer_out_stat = ServerHelper(self).get_server_rebalance_progress(self.servers[0], 'out')
        transfer_in_stat = ServerHelper(self).get_server_rebalance_progress(self.servers[1], 'in')
        self._verify_stats(transfer_out_stat)
        self._verify_stats(transfer_in_stat)

    def _verify_stats(self, stats):
        self.assertTrue(int(stats["total_transfer"]) >= int(stats["estimated_transfer"]),
                        "total_transfer should be greater than estimated  in stats %s" % stats)
        self.assertTrue(re.match(r'.*Active#-.*Replica#-.*', str(stats["vbuckets"])),
                        "VBuckets in stats %s has incorrect format" % stats)


class GracefullFailoverTests(BaseUITestCase):
    def setUp(self):
        super(GracefullFailoverTests, self).setUp()
        self.master = self.servers[0]
        try:
            self.nodes_init = self.input.param("nodes_init", 2)
            self.rebalance = self.input.param("rebalance", False)
            self.cluster = Cluster()
            if self.nodes_init > 1:
                self.cluster.rebalance(self.servers[:1], self.servers[1:self.nodes_init], [])
            BaseHelper(self).login()
            num_buckets = self.input.param("num_buckets", 1)
            self.num_replica = self.input.param("replica", 1)
            self.buckets = []
            NavigationHelper(self).navigate('Buckets')
            for i in range(num_buckets):
                bucket = Bucket(name='bucket%s' % i, ram_quota=200, sasl_pwd='password',
                                replica=self.num_replica)
                RestConnection(self.servers[0]).create_bucket(bucket=bucket.name, ramQuotaMB=bucket.ram_quota or 100,
                                                              saslPassword=bucket.sasl_password, replicaNumber=bucket.num_replica,
                                                              proxyPort=STANDARD_BUCKET_PORT + i + 1)
                self.buckets.append(bucket)
        except:
            self.tearDown()
            self.cluster.shutdown()

    def tearDown(self):
        try:
            super(GracefullFailoverTests, self).tearDown()
            if hasattr(self, 'driver') and self.driver:
                self._deinitialize_api()
                self._initialize_nodes()
                self.sleep(3)
        finally:
            if hasattr(self, 'cluster'):
                self.cluster.shutdown()

    def _deinitialize_api(self):
        ClusterOperationHelper.cleanup_cluster(self.servers, master=self.master)

    def _initialize_nodes(self):
        for server in self.servers:
            RestConnection(server).init_cluster(self.input.membase_settings.rest_username,
                                                self.input.membase_settings.rest_password, '8091')

    def test_failover(self):
        confirm = self.input.param("confirm_failover", True)
        NavigationHelper(self).navigate('Server Nodes')
        if len(self.servers) < 2:
            self.fail("There is no enough VMs. Need at least 2")
        ServerHelper(self).failover(self.servers[1], confirm=confirm, graceful=True)

    def test_failover_multiply_nodes(self):
        is_graceful = self.input.param("graceful", "true;true")
        is_graceful = is_graceful.split(';')
        is_graceful = [(False, True)[item.lower() == "true"] for item in is_graceful]
        if len(self.servers) < (len(is_graceful) + 1):
            self.fail("There is no enough VMs. Need at least %s" % len(is_graceful))
        NavigationHelper(self).navigate('Server Nodes')
        confirm_failover_check = True
        for iter in range(len(is_graceful)):
            if self.num_replica < self.nodes_init - 1 and self.rebalance:
                confirm_failover_check = False
            ServerHelper(self).failover(self.servers[iter + 1], confirm=True, graceful=is_graceful[iter], confirm_failover=confirm_failover_check)

    def test_delta_recovery_failover(self):
        confirm = self.input.param("confirm_recovery", True)
        option = self.input.param("option", 'delta')
        NavigationHelper(self).navigate('Server Nodes')
        if len(self.servers) < 2:
            self.fail("There is no enough VMs. Need at least 2")
        ServerHelper(self).failover(self.servers[1], confirm=True, graceful=False)
        ServerHelper(self).set_recovery(self.servers[1], option=option, confirm=confirm)
        if confirm:
            ServerHelper(self).start_rebalancing()
            RestConnection(self.servers[0]).monitorRebalance()
        self.log.info("Recovery checked")

    def test_delta_recovery_failover_251(self):
        confirm = self.input.param("confirm_recovery", True)
        # delta or full
        option = self.input.param("option", 'delta')
        NavigationHelper(self).navigate('Server Nodes')
        if len(self.servers) < 2:
            self.fail("There is no enough VMs. Need at least 2")
        helper = ServerHelper(self)
        server = self.servers[1]
        helper.failover(server, confirm=True, graceful=False)
        helper.add_node_back()
        helper.set_recovery(server, option=option, confirm=confirm)
        option = ('full', 'delta')[confirm]
        helper.check_recovery(server, option=option)
        if confirm:
            helper.start_rebalancing()
            RestConnection(self.servers[0]).monitorRebalance()
        self.log.info("Recovery checked")


class ViewsTests(BaseUITestCase):
    def setUp(self):
        super(ViewsTests, self).setUp()
        self._initialize_nodes()
        num_buckets = self.input.param("num_buckets", 1)
        self.ddoc_name = self.input.param("ddoc_name", "ddoc1")
        self.view_name = self.input.param("view_name", "view1")
        self.ddoc_num = self.input.param("ddoc_num", 1)
        self.view_num = self.input.param("view_num", 1)
        self.buckets = []
        helper = BaseHelper(self)
        helper.login()
        NavigationHelper(self).navigate('Buckets')
        for i in range(num_buckets):
            bucket = Bucket(name='bucket%s' % i, ram_quota=200, sasl_pwd='password')
            RestConnection(self.servers[0]).create_bucket(bucket=bucket.name, ramQuotaMB=bucket.ram_quota or 100,
                                                          saslPassword=bucket.sasl_password,
                                                          proxyPort=STANDARD_BUCKET_PORT + i + 1)
            self.buckets.append(bucket)
        self.driver.refresh()

    def tearDown(self):
        super(ViewsTests, self).tearDown()

    def _initialize_nodes(self):
        for server in self.servers:
            RestConnection(server).init_cluster(self.input.membase_settings.rest_username, self.input.membase_settings.rest_password, '8091')

    def test_add_dev_view(self):
        try:
            NavigationHelper(self).navigate('Indexes')
            DdocViewHelper(self).click_view_tab(text='Views')
            DdocViewHelper(self).create_view(self.ddoc_name, self.view_name)
        except Exception as ex:
            self.log.error(str(ex))
            raise ex

    def test_add_prod_view(self):
        try:
            NavigationHelper(self).navigate('Indexes')
            DdocViewHelper(self).click_view_tab(text='Views')
            DdocViewHelper(self).create_view(self.ddoc_name, self.view_name, dev_view=False)
        except Exception as ex:
            self.log.error(str(ex))
            raise ex

    def test_delete_view(self):
        try:
            NavigationHelper(self).navigate('Indexes')
            DdocViewHelper(self).click_view_tab(text='Views')
            DdocViewHelper(self).create_view(self.ddoc_name, self.view_name)
            DdocViewHelper(self).delete_view(self.view_name)
        except Exception as ex:
            self.log.error(str(ex))
            raise ex

    def test_edit_view(self):
        try:
            action = self.input.param('action', 'save')
            NavigationHelper(self).navigate('Indexes')
            DdocViewHelper(self).click_view_tab(text='Views')
            DdocViewHelper(self).create_view(self.ddoc_name, self.view_name)
            DdocViewHelper(self).edit_view(self.view_name)
            DdocViewHelper(self).fill_edit_view_screen(self.view_name, action)
        except Exception as ex:
            self.log.error(str(ex))
            raise ex

    def test_show_view_results(self):
        for bucket in self.buckets:
            BucketHelper(self).open_documents(bucket)
        num_docs = self.input.param('num-docs', 5)
        view_set = self.input.param('view_set', 'dev')
        doc_name = self.input.param('doc_name', 'test')
        doc_content = self.input.param('content', '{"test" : "test"}')
        for i in range(num_docs):
            doc = Document(doc_name + str(i), doc_content)
            DocsHelper(self).create_doc(doc)
            self.assertTrue(DocsHelper(self).get_error() is None, "error appears: %s" \
                            % DocsHelper(self).get_error())
        NavigationHelper(self).navigate('Indexes')
        DdocViewHelper(self).click_view_tab(text='Views')
        DdocViewHelper(self).create_view(self.ddoc_name, self.view_name)
        DdocViewHelper(self).open_view(self.view_name)
        DdocViewHelper(self).verify_view_results(view_set, None)

    def test_show_view_results_with_reduce(self):
        for bucket in self.buckets:
            BucketHelper(self).open_documents(bucket)
        num_docs = self.input.param('num-docs', 5)
        action = self.input.param('action', 'save')
        reduce_fn = self.input.param('reduce_fn', '_count')
        view_set = self.input.param('view_set', 'dev')
        doc_name = self.input.param('doc_name', 'test')
        doc_content = self.input.param('content', '{{ "age": {0}, "first_name": "{1}" }}')
        age_sum = 0
        for i in range(num_docs):
            doc = Document(doc_name + str(i), doc_content.format(i, doc_name + str(i)))
            DocsHelper(self).create_doc(doc)
            self.assertTrue(DocsHelper(self).get_error() is None, "error appears: %s" \
                            % DocsHelper(self).get_error())
            age_sum = age_sum + i
        NavigationHelper(self).navigate('Indexes')
        DdocViewHelper(self).click_view_tab(text='Views')
        DdocViewHelper(self).create_view(self.ddoc_name, self.view_name)
        DdocViewHelper(self).edit_view(self.view_name)
        DdocViewHelper(self).fill_edit_view_screen(self.view_name, action, reduce_fn)
        if reduce_fn == '_count':
            value = num_docs
        elif reduce_fn == '_sum':
            value = age_sum
        else:
            value = age_sum
        DdocViewHelper(self).verify_view_results(view_set, reduce_fn, value)


'''
Controls classes for tests
'''
class NavigationTestControls():
     def __init__(self, driver):
        self.helper = ControlsHelper(driver)

     def _navigation_tab(self, text):
        return self.helper.find_control('navigation', 'navigation_tab',
                                        parent_locator='navigation_bar',
                                        text=text)

     def _navigation_tab_link(self, text):
        return self.helper.find_control('navigation', 'navigation_tab_link',
                                        parent_locator='navigation_bar',
                                        text=text)


class BreadcrumbsTestControls():
    def __init__(self, driver):
        self.helper = ControlsHelper(driver)

    def navigation_trace(self, text):
        return self.helper.find_control('breadcrumbs', 'breadcrumbs_tab',
                                        parent_locator='breadcrumbs_bar',
                                        text=text)

class ServerTestControls():
    def __init__(self, driver):
        self.helper = ControlsHelper(driver)
        self.add_server_btn = self.helper.find_control('server_nodes', 'add_server_btn')
        self.rebalance_btn = self.helper.find_control('server_nodes', 'rebalance_btn')

        self.num_pend_rebalance = self.helper.find_control('server_nodes', 'num_pend_rebalance',
                                                           parent_locator='pend_rebalance_btn')


    def set_quota_btn(self):
        return self.helper.find_control('server_nodes', 'set_quota')

    def pending_rebalance_tab(self):
        return self.helper.find_control('server_nodes', 'pending_rebalance_tab')

    def stop_rebalance_btn(self):
        return self.helper.find_control('server_nodes', 'stop_rebalance_btn')

    def common_rebalance_progress_bar(self):
        return self.helper.find_control('server_nodes', 'common_rebalance_progress_bar')

    def add_server_dialog(self, parent='add_server_pop_up'):
        self.parent = parent
        self.add_server_pop_up = self.helper.find_control('server_nodes', 'add_server_pop_up')
        self.ip_address = self.helper.find_control('server_nodes', 'ip_address', parent_locator=self.parent)
        self.username = self.helper.find_control('server_nodes', 'username', parent_locator=self.parent)
        self.password = self.helper.find_control('server_nodes', 'password', parent_locator=self.parent)
        self.add_server_dialog_btn = self.helper.find_control('server_nodes', 'add_server_dialog_btn',
                                                              parent_locator=self.parent)
        self.confirm_server_addition = self.helper.find_control('server_nodes', 'confirm_server_addition')
        self.add_server_confirm_btn = self.helper.find_control('server_nodes', 'add_server_dialog_btn',
                                                               parent_locator='confirm_server_addition')
        self.index = self.helper.find_control('server_nodes', 'index')
        self.n1ql = self.helper.find_control('server_nodes', 'n1ql')
        return self

    def remove_server_dialog(self, parent='remove_server_pop_up'):
        self.remove_server_pop_up = self.helper.find_control('server_nodes', 'remove_server_pop_up')
        self.remove_diag_btn = self.helper.find_control('server_nodes', 'remove_btn_diag', parent_locator=parent)
        return self

    def server_row_controls(self):
        self.failover_btns = self.helper.find_controls('server_nodes', 'failover_btn')
        self.remove_btns = self.helper.find_controls('server_nodes', 'remove_btn')
        return self

    def server_row_btns(self, server_ip):
        self.failover_btn = self.helper.find_control('server_info', 'failover_btn',
                                                     parent_locator='server_row',
                                                     text=server_ip)
        self.repair_status = self.helper.find_control('server_info', 'repair_status',
                                                     parent_locator='server_row',
                                                     text=server_ip)
        return self

    def server_info(self, server_ip):
        self.server_arrow = self.helper.find_control('server_info', 'server_arrow',
                                                     parent_locator='server_row',
                                                     text=server_ip)
        self.server_arrow_opened = self.helper.find_control('server_info', 'server_arrow_opened',
                                                     parent_locator='server_row',
                                                     text=server_ip)
        return self

    def server_info_rebalance_progress(self, server_ip):
        return self.helper.find_control('server_info', 'rebalance_progress')

    def rebalance_progress_bar(self, server_ip):
        return self.helper.find_control('server_info', 'rebalance_bar',
                                                     parent_locator='server_row',
                                                     text=server_ip)

    def failed_over_msg(self, server_ip):
        return self.helper.find_control('server_info', 'failover_msg',
                                                     parent_locator='server_row',
                                                     text=server_ip)

    def failover_confirmation(self):
        self.failover_conf_dialog = self.helper.find_control('failover_dialog', 'dialog')
        self.failover_conf_cb = self.helper.find_control('failover_dialog', 'confirm_cb',
                                                         parent_locator='dialog')
        self.failover_conf_submit_btn = self.helper.find_control('failover_dialog', 'submit_btn',
                                                                 parent_locator='dialog')
        self.failover_conf_cancel_btn = self.helper.find_control('failover_dialog', 'cancel_btn',
                                                                 parent_locator='dialog')
        self.failover_conf_gracefull_option = self.helper.find_control('failover_dialog', 'graceful_option',
                                                                       parent_locator='dialog')
        self.failover_conf_hard_failover = self.helper.find_control('failover_dialog', 'hard_failover',
                                                                       parent_locator='dialog')
        self.confirm_failover_option = self.helper.find_control('failover_dialog', 'confirm_failover',
                                                                       parent_locator='dialog')
        return self

    def failover_warning(self):
        return self.helper.find_control('failover_dialog', 'warn', parent_locator='dialog')

    def add_back_failover(self):
        return self.helper.find_control('add_back_failover', 'add_back_btn')

    def recovery_btn(self, server_ip):
        return self.helper.find_control('server_info', 'recovery_btn', parent_locator='server_row',
                                        text=server_ip)

    def select_recovery(self, server_ip):
        self.conf_dialog = self.helper.find_control('pending_server_list', 'server_row', parent_locator='pending_server_container', text=server_ip)
        self.delta_option = self.helper.find_control('pending_server_list', 'delta_recv_option')
        self.full_option = self.helper.find_control('pending_server_list', 'full_recv_option')
        return self

    def recovery_dialog(self):
        self.conf_dialog = self.helper.find_control('recovery_dialog', 'dialog')
        self.delta_option = self.helper.find_control('recovery_dialog', 'delta_option', parent_locator='dialog')
        self.full_option = self.helper.find_control('recovery_dialog', 'full_option', parent_locator='dialog')
        self.save_recovery_btn = self.helper.find_control('recovery_dialog', 'save_btn', parent_locator='dialog')
        self.cancel_recovery_btn = self.helper.find_control('recovery_dialog', 'cancel_btn', parent_locator='dialog')
        return self

    def server_rows(self):
        return self.helper.find_controls('server_info', 'server_rows')


class BucketTestsControls():
    def __init__(self, driver):
        self.helper = ControlsHelper(driver)
        self.create_bucket_btn = self.helper.find_control('bucket', 'new_create_btn')

    def bucket_pop_up(self, parent='create_bucket_pop_up'):
        self.parent = parent
        self.create_bucket_pop_up = self.helper.find_control('bucket', 'create_bucket_pop_up')
        self.show_advanced_settings = self.helper.find_control('bucket', 'show_advanced_settings')
        self.hide_advanced_settings = self.helper.find_control('bucket', 'hide_advanced_settings')
        self.name = self.helper.find_control('bucket', 'name', 'create_bucket_pop_up')
        self.ram_quota = self.helper.find_control('bucket', 'ram_quota', parent_locator=self.parent)
        self.standart_port_radio = self.helper.find_control('bucket', 'standart_port_radio',
                                                            parent_locator=self.parent)
        self.dedicated_port_radio = self.helper.find_control('bucket', 'dedicated_port_radio',
                                                             parent_locator=self.parent)
        self.sasl_password = self.helper.find_control('bucket', 'sasl_password',
                                                      parent_locator=self.parent)
        self.port = self.helper.find_control('bucket', 'port', parent_locator=self.parent)
        self.enable_replica_cb = self.helper.find_control('bucket', 'enable_replica_cb',
                                                          parent_locator=self.parent)
        self.replica_num = self.helper.find_control('bucket', 'replica_num', parent_locator=self.parent)
        self.index_replica_cb = self.helper.find_control('bucket', 'index_replica_cb',
                                                         parent_locator=self.parent)
        self.override_comp_cb = self.helper.find_control('bucket', 'auto_comp_settings_override',
                                                         parent_locator=self.parent)
        self.create_btn = self.helper.find_control('bucket', 'create_btn',
                                                   parent_locator='create_bucket_pop_up')
        return self

    def bucket_meta_data(self, text='valueOnly', parent='create_bucket_pop_up'):
        return self.helper.find_control('bucket', 'cache_meta_data', parent_locator=parent, text=text)

    def bucket_io_priority(self, text='hight', parent='create_bucket_pop_up'):
        return self.helper.find_control('bucket', 'io_priority', parent_locator=parent, text=text)

    def bucket_compaction(self, parent='create_bucket_pop_up'):
        self.frag_percent_cb = self.helper.find_control('bucket', 'frag_percent_cb',
                                                      parent_locator=parent)
        self.frag_percent = self.helper.find_control('bucket', 'frag_percent', parent_locator=parent)
        self.frag_mb_cb = self.helper.find_control('bucket', 'frag_mb_cb',
                                                          parent_locator=parent)
        self.frag_mb = self.helper.find_control('bucket', 'frag_mb', parent_locator=parent)
        self.view_frag_percent_cb = self.helper.find_control('bucket', 'view_frag_percent_cb',
                                                         parent_locator=parent)
        self.view_frag_percent = self.helper.find_control('bucket', 'view_frag_percent',
                                                   parent_locator=parent)
        self.view_frag_mb = self.helper.find_control('bucket', 'view_frag_mb',
                                                      parent_locator=parent)
        self.view_frag_mb_cb = self.helper.find_control('bucket', 'view_frag_mb_cb', parent_locator=parent)
        self.comp_allowed_period_cb = self.helper.find_control('bucket', 'comp_allowed_period_cb',
                                                          parent_locator=parent)
        self.comp_allowed_period_start_h = self.helper.find_control('bucket', 'comp_allowed_period_start_h', parent_locator=parent)
        self.comp_allowed_period_start_min = self.helper.find_control('bucket', 'comp_allowed_period_start_min',
                                                         parent_locator=parent)
        self.comp_allowed_period_end_h = self.helper.find_control('bucket', 'comp_allowed_period_end_h',
                                                   parent_locator=parent)
        self.comp_allowed_period_end_min = self.helper.find_control('bucket', 'comp_allowed_period_end_min',
                                                         parent_locator=parent)
        self.abort_comp_cb = self.helper.find_control('bucket', 'abort_comp_cb',
                                                   parent_locator=parent)
        self.comp_in_parallel_cb = self.helper.find_control('bucket', 'comp_in_parallel_cb',
                                                         parent_locator=parent)
        self.purge_intervals = self.helper.find_controls('bucket', 'purge_interval',
                                                   parent_locator=parent)
        return self

    def bucket_create_error(self):
        return self.helper.find_controls('bucket', 'error', parent_locator='create_bucket_pop_up')

    def bucket_info(self, bucket_name):
        self.arrow = self.helper.find_control('bucket_row', 'arrow', parent_locator='bucket_row',
                                               text=bucket_name)
        self.name = self.helper.find_control('bucket_row', 'name', parent_locator='bucket_row',
                                              text=bucket_name)
        # self.nodes = self.helper.find_control('bucket_row', 'nodes', parent_locator='bucket_row',
        #                                        text=bucket_name)
        self.items_count = self.helper.find_control('bucket_row', 'items_count',
                                                    parent_locator='bucket_row', text=bucket_name)
        self.documents = self.helper.find_control('bucket_row', 'documents',
                                                  parent_locator='bucket_row', text=bucket_name)
        # self.views = self.helper.find_control('bucket_row', 'views', parent_locator='bucket_row',
        #                                        text=bucket_name)
        # self.health = self.helper.find_first_visible('bucket_row', 'health',
        #                                              parent_locator='bucket_row',
        #                                              text=bucket_name)
        self.statistics = self.helper.find_first_visible('bucket_row', 'statistics',
                                                     parent_locator='bucket_row',
                                                     text=bucket_name)
        return self

    def type(self, type):
        return self.helper.find_control('bucket', 'type', text=type)

    def warning_pop_up(self, text):
        return self.helper.find_control('errors', 'warning_pop_up', text=text)

    def edit_btn(self):
        return self.helper.find_control('bucket', 'edit_btn')

    def bucket_stats(self, stat=None, tab=None):
        self.arrow = self.helper.find_control('bucket_stats', 'value_stat_arrow', text=tab)
        self.stats = self.helper.find_control('bucket_stats', 'value_stat', text=stat)
        return self

    def bucket_stat_view_block(self):
        return self.helper.find_control('bucket_stats', 'view_stats_block')

    def bucket_stat_from_view_block(self, stat):
        return self.helper.find_control('bucket_stats', 'value_stat', parent_locator='view_stats_block', text=stat)


class NodeInitializeControls():
    def __init__(self, driver):
        self.helper = ControlsHelper(driver)
        self.setup_btn = self.helper.find_control('initialize', 'setup_btn')
        self.configure_dms = self.helper.find_control('initialize', 'configure_dms')
        self.finish_with_defaults = self.helper.find_control('initialize', 'finish_with_defaults')

    def errors(self):
        self.error_inline = self.helper.find_controls('initialize', 'errors')
        self.error_warn = self.helper.find_control('errors', 'warning_pop_up', text='Error')
        return self

    def main_page(self):
        return self.helper.find_control('initialize', 'main_page')

    def step_screen(self):
        self.current_step = self.helper.find_first_visible('initialize', 'current_step')
        self.next_btn = self.helper.find_first_visible('initialize', 'next_btn')
        self.finish_btn = self.helper.find_first_visible('initialize', 'finish_btn')
        return self

    def step_1(self):
        self.db_path = self.helper.find_control('step_1', 'db_path')
        self.indeces_path = self.helper.find_control('step_1', 'indeces_path')
        self.new_cluster_cb = self.helper.find_control('step_1', 'new_cluster_cb')
        self.ram_quota = self.helper.find_control('step_1', 'ram_quota')
        self.join_cluster = self.helper.find_control('step_1', 'join_cluster')
        self.ip_cluster = self.helper.find_control('step_1', 'ip_cluster')
        self.user_cluster = self.helper.find_control('step_1', 'user_cluster')
        self.pass_cluster = self.helper.find_control('step_1', 'pass_cluster')
        return self

    def step_2_sample(self, sample):
        return self.helper.find_control('step_2', 'sample', text=sample)

    def step_4(self):
        self.enable_updates = self.helper.find_control('step_4', 'enable_updates')
        self.email = self.helper.find_control('step_4', 'email')
        self.first_name = self.helper.find_control('step_4', 'first_name')
        self.last_name = self.helper.find_control('step_4', 'last_name')
        self.company = self.helper.find_control('step_4', 'company')
        self.agree_terms = self.helper.find_control('step_4', 'agree_terms')
        self.expand_registry = self.helper.find_control('step_4', 'expand_registry')
        return self

    def step_new_cluater(self):
        self.cluster_name = self.helper.find_control('step_new_cluater', 'cluster_name')
        self.password_confirm = self.helper.find_control('step_new_cluater', 'password_confirm')
        self.user = self.helper.find_control('step_new_cluater', 'user')
        self.password = self.helper.find_control('step_new_cluater', 'pass')
        return self


class DdocViewControls():
    def __init__(self, driver):
        self.helper = ControlsHelper(driver)

    def view_btn(self):
        self.create_view_btn = self.helper.find_control('views_screen', 'create_view_btn')
        return self

    def views_screen(self, text=''):
        self.views_tab = self.helper.find_control('views_screen', 'views_tab', text=text)
        return self

    def error(self):
        return self.helper.find_first_visible('edit_view_screen', 'error')

    def create_pop_up(self):
        self.pop_up = self.helper.find_control('create_pop_up', 'pop_up')
        self.ddoc_name = self.helper.find_control('create_pop_up', 'ddoc_name',
                                                  parent_locator='pop_up')
        self.view_name = self.helper.find_control('create_pop_up', 'view_name',
                                                  parent_locator='pop_up')
        self.save_btn = self.helper.find_control('create_pop_up', 'save_btn',
                                                  parent_locator='pop_up')
        return self

    def prod_view(self, view_count=1):
        self.prod_view_tab = self.helper.find_control('prod_view', 'prod_view_tab')
        self.prod_view_count = self.helper.find_control('prod_view', 'prod_view_count', text=view_count)
        return self

    def view_row(self, view=''):
        self.row = self.helper.find_control('view_row', 'row', text=view)
        self.name = self.helper.find_control('view_row', 'name', text=view)
        # self.row_name = self.helper.find_control('view_row', 'row_name', text=view)
        self.edit_btn = self.helper.find_control('view_row', 'edit_btn', text=view, parent_locator='row')
        self.delete_btn = self.helper.find_control('view_row', 'delete_btn', text=view, parent_locator='row')
        return self

    def del_view_dialog(self):
        self.dialog = self.helper.find_control('delete_view', 'dialog')
        self.ok_btn = self.helper.find_control('delete_view', 'ok_btn')
        self.cancel_btn = self.helper.find_control('delete_view', 'cancel_btn')
        return self

    def ddoc_row(self, ddoc=''):
        self.row = self.helper.find_control('ddoc_row', 'row', text=ddoc)
        self.name = self.helper.find_control('ddoc_row', 'name', parent_locator='row')
        self.publish_btn = self.helper.find_control('ddoc_row', 'publish_btn')
        return self

    def view_screen(self, view=''):
        self.screen = self.helper.find_control('edit_view_screen', 'screen')
        self.random_document = self.helper.find_control('edit_view_screen', 'random_doc')
        self.random_doc_name = self.helper.find_control('edit_view_screen', 'random_doc_name',
                                                        parent_locator='random_doc')
        self.random_doc_btn = self.helper.find_control('edit_view_screen', 'random_doc_btn',
                                                        parent_locator='random_doc')
        self.random_doc_content = self.helper.find_control('edit_view_screen', 'random_doc_content',
                                                        parent_locator='random_doc')
        self.random_doc_meta = self.helper.find_control('edit_view_screen', 'random_doc_meta',
                                                        parent_locator='random_doc')
        self.random_doc_edit_btn = self.helper.find_control('edit_view_screen', 'random_doc_edit_btn')
        self.view_name_set = self.helper.find_control('edit_view_screen', 'view_name_set', text=view)
        return self

    def view_map_reduce_fn(self):
        self.map_fn = self.helper.find_control('edit_view_screen', 'map_fn')
        self.reduce_fn = self.helper.find_control('edit_view_screen', 'reduce_fn')
        self.save_btn = self.helper.find_control('edit_view_screen', 'save_btn')
        self.saveas_btn = self.helper.find_control('edit_view_screen', 'saveas_btn')
        return self

    def view_results_container(self, value=0):
        self.results_block = self.helper.find_control('view_results', 'results_block')
        self.show_results_btn = self.helper.find_control('view_results', 'show_results_btn')
        self.dev_subset = self.helper.find_control('view_results', 'dev_subset', parent_locator='results_block')
        self.full_subset = self.helper.find_control('view_results', 'full_subset', parent_locator='results_block')
        self.results_container = self.helper.find_control('view_results', 'results_container')
        self.table_id = self.helper.find_control('view_results', 'table_id')
        self.doc_count = self.helper.find_control('view_results', 'doc_count', text=str(value))
        self.doc_arrow = self.helper.find_control('view_results', 'doc_arrow')
        self.view_arrow = self.helper.find_control('view_results', 'view_arrow')
        return self


class DocumentsControls():
    def __init__(self, driver):
        self.helper = ControlsHelper(driver)
        self.lookup_input = self.helper.find_control('docs_screen', 'lookup_input')
        self.lookup_btn = self.helper.find_control('docs_screen', 'lookup_btn')

    def create_doc_screen(self):
        self.documents_screen = self.helper.find_control('docs_screen', 'screen')
        self.create_doc = self.helper.find_control('docs_screen', 'create_doc_btn')
        self.lookup_input = self.helper.find_control('docs_screen', 'lookup_input')
        self.lookup_btn = self.helper.find_control('docs_screen', 'lookup_btn')
        return self

    def error(self):
        return self.helper.find_first_visible('docs_screen', 'error')

    def docs_rows(self):
        return self.helper.find_controls('docs_screen', 'rows')

    def pagination(self):
        self.current_page_num = self.helper.find_control('pagination', 'current_page_num')
        self.total_page_num = self.helper.find_control('pagination', 'total_page_num')
        self.next_page_btn = self.helper.find_first_visible('pagination', 'next_page_btn')
        self.page_num_selector = self.helper.find_controls('pagination', 'page_num_selector')
        self.page_num_selector_arrow = self.helper.find_control('pagination', 'page_num_selector_arrow')
        return self

    def create_doc_pop_up(self):
        self.doc_name = self.helper.find_control('create_doc_pop_up', 'doc_name',
                                                 parent_locator='pop_up')
        self.save_btn = self.helper.find_control('create_doc_pop_up', 'save_btn',
                                                 parent_locator='pop_up')
        return self

    def document_row(self, doc=''):
        self.name = self.helper.find_control('doc_row', 'name',
                                              parent_locator='row', text=doc)
        self.content = self.helper.find_control('doc_row', 'content',
                                                parent_locator='row', text=doc)
        self.edit_btn = self.helper.find_control('doc_row', 'edit_btn',
                                                 parent_locator='row', text=doc)
        self.delete_btn = self.helper.find_control('doc_row', 'delete_btn',
                                                   parent_locator='row', text=doc)
        return self

    def edit_document_screen(self, parent='screen', doc=None):
        self.name = self.helper.find_control('edit_doc_screen', 'name', text=doc)
        self.content = self.helper.find_first_visible('edit_doc_screen', 'content',
                                                      parent_locator=parent)
        self.delete_btn = self.helper.find_control('edit_doc_screen', 'delete_btn',
                                                    parent_locator=parent)
        self.save_btn = self.helper.find_control('edit_doc_screen', 'save_btn',
                                                  parent_locator=parent)
        self.save_as_btn = self.helper.find_control('edit_doc_screen', 'save_as_btn',
                                                     parent_locator=parent)
        self.documents_link = self.helper.find_control('edit_doc_screen', 'documents_link')
        return self


class SettingsTestControls():
    def __init__(self, driver):
        self.helper = ControlsHelper(driver)

    def _settings_tab_link(self, text):
        return self.helper.find_control('settings', 'settings_tab_link',
                                        parent_locator='settings_bar',
                                        text=text)

    def security_tabs(self):
        self.ro_tab = self.helper.find_control('user', 'ro_tab')
        self.external_user_tab = self.helper.find_control('external_user', 'external_user_tab')
        return self

    def alerts_info(self):
        self.enable_email_alerts = self.helper.find_control('alerts', 'enable_email_alerts')
        self.email_host = self.helper.find_control('alerts', 'email_host')
        self.email_user = self.helper.find_control('alerts', 'email_user')
        self.email_port = self.helper.find_control('alerts', 'email_port')
        self.email_pass = self.helper.find_control('alerts', 'email_pass')
        self.email_encrypt = self.helper.find_control('alerts', 'email_encrypt')
        self.email_sender = self.helper.find_control('alerts', 'email_sender')
        self.email_recipients = self.helper.find_control('alerts', 'email_recipients')
        self.test_email_btn = self.helper.find_control('alerts', 'test_email_btn')
        self.sent_email_btn = self.helper.find_control('alerts', 'sent_email_btn')
        self.save_btn = self.helper.find_control('settings', 'save_btn', parent_locator='alert_screen')
        self.done_btn = self.helper.find_control('settings', 'done_btn', parent_locator='alert_screen')
        return self

    def auto_failover_info(self):
        self.enable_auto_failover = self.helper.find_control('auto_failover', 'enable_auto_failover')
        self.failover_timeout = self.helper.find_control('auto_failover', 'failover_timeout')
        self.what_is_this = self.helper.find_control('auto_failover', 'what_is_this')
        self.save_btn = self.helper.find_control('settings', 'save_btn', parent_locator='auto_failover_screen')
        self.done_btn = self.helper.find_control('settings', 'done_btn', parent_locator='auto_failover_screen')
        return self

    def user_create_info(self, roles =[]):
        self.username = self.helper.find_control('user', 'username')
        self.password = self.helper.find_control('user', 'password')
        self.verify_password = self.helper.find_control('user', 'verify_password')
        self.create_btn = self.helper.find_control('user', 'create_btn')
        self.roles_items = []
        for role in roles:
            self.roles_items.append(self.helper.find_control('user', 'roles_item', text=role))
        return self

    def user_error_msgs(self):
        return self.helper.find_controls('user', 'error_msg')

    def external_user_error_msgs(self):
        return self.helper.find_controls('external_user', 'ext_error_message')

    def confirmation_user_delete(self):
        self.confirmation_dlg = self.helper.find_control('confirm_delete_ro', 'dlg')
        self.delete_btn = self.helper.find_control('confirm_delete_ro', 'confirm_btn', parent_locator='dlg')
        return self

    def confirmation_external_user_delete(self):
        self.confirmation_dlg = self.helper.find_control('confirm_delete_external_user', 'dlg')
        self.delete_btn = self.helper.find_control('confirm_delete_external_user', 'confirm_btn', parent_locator='dlg')
        return self

    def samples_buckets(self, bucket=''):
        self.sample_cb = self.helper.find_control('sample_buckets', 'sample_cb', text=bucket)
        self.installed_sample = self.helper.find_control('sample_buckets', 'installed_sample', text=bucket)
        self.save_btn = self.helper.find_control('sample_buckets', 'save_btn')
        self.error_msg = self.helper.find_controls('sample_buckets', 'error')
        return self

    def external_user_create_info(self, roles=[]):
        self.img_enabled = self.helper.find_control('external_user', 'img_enabled')
        self.img_disabled = self.helper.find_control('external_user', 'img_disabled')
        self.enable_link = self.helper.find_control('external_user', 'enable_link')
        self.disable_link = self.helper.find_control('external_user', 'disable_link')
        self.label_disabled = self.helper.find_control('external_user', 'label_disabled')
        self.add_user = self.helper.find_control('external_user', 'add_user')
        self.select_external = self.helper.find_control('external_user', 'select_external')
        self.name_inp = self.helper.find_control('external_user', 'name_inp')
        self.name_full_inp = self.helper.find_control('external_user', 'name_full_inp')
        self.roles_selector = self.helper.find_control('external_user', 'roles_selector')
        self.save_button = self.helper.find_control('external_user', 'save_button')
        self.cancel_button = self.helper.find_control('external_user', 'cancel_button')
        self.ext_error_message = self.helper.find_control('external_user', 'ext_error_message')
        self.delete_ext_user_btn = self.helper.find_control('external_user', 'delete_ext_user_btn')
        self.edit_ext_user_btn = self.helper.find_control('external_user', 'edit_ext_user_btn')
        self.roles_items = []
        for role in roles:
            self.roles_items.append(self.helper.find_control('external_user', 'roles_item', text=role))
        return self


'''
Helpers
'''


class NavigationHelper:
    def __init__(self, tc):
        self.tc = tc
        self.controls = NavigationTestControls(tc.driver)
        self.breadcrumbs = BreadcrumbsTestControls(tc.driver)
        self.wait = WebDriverWait(tc.driver, timeout=25)

    def _is_tab_selected(self, text):
        return self.controls._navigation_tab(text).is_displayed() and\
               self.controls._navigation_tab(text).get_attribute('class').find('currentnav') > -1

    def navigate(self, tab):
        self.wait.until(lambda fn: self.controls._navigation_tab_link(tab).is_displayed(),
                        "tab '%s' is not displayed in %d sec" % (tab, self.wait._timeout))
        self.tc.log.info("try to navigate to '%s' tab" % tab)
        if self._is_tab_selected(tab):
            self.tc.log.info("tab '%s' is already selected" % tab)
        self.controls._navigation_tab_link(tab).click()
        self.wait.until(lambda fn: self._is_tab_selected(tab),
                        "tab '%s' is not selected in %d sec" % (tab, self.wait._timeout))
        self.tc.log.info("tab '%s' is selected" % tab)

    def navigate_breadcrumbs(self, tab):
        self.breadcrumbs.navigation_trace(tab).click()
        self.tc.log.info("tab '%s' is selected via breadcrumb" % tab)


class ServerHelper():
    def __init__(self, tc):
        self.tc = tc
        self.wait = WebDriverWait(tc.driver, timeout=25)
        self.controls = ServerTestControls(tc.driver)

    def _is_btn_enabled(self, btn):
        return btn.get_attribute('class').find('disabled') == -1

    def add(self, input, index=1):
        self.tc.log.info("trying add server %s" % (input.param("add_server_ip", self.tc.servers[index].ip)))
        self.wait.until(lambda fn: self.controls.add_server_btn.is_displayed(),
                        "Add Server btn is not displayed in %d sec" % (self.wait._timeout))
        self.controls.add_server_btn.click()
        self.wait.until(lambda fn: self.controls.add_server_dialog().add_server_pop_up.is_displayed(),
                        "no reaction for click create new bucket btn in %d sec" % (self.wait._timeout))
        self.fill_server_info(input)
        self.controls.add_server_dialog().index.click()
        self.controls.add_server_dialog().n1ql.click()
        self.controls.add_server_dialog().confirm_server_addition.click()
        # On Windows, it might take some more time than the default timeout for the Popup to disappear causing the test
        # to fail. Adding some sleep to give it some more time.
        time.sleep(10)
        self.wait.until_not(lambda fn:
                            self.controls.add_server_dialog().confirm_server_addition.is_displayed(),
                            "Add server pop up is not closed in %d sec" % self.wait._timeout)
        self.wait.until_not(lambda fn:
                            self.controls.add_server_dialog().add_server_pop_up.is_displayed(),
                            "Add server pop up is not closed in %d sec" % self.wait._timeout)
        self.tc.log.info("added server %s" % (self.tc.servers[1].ip))

    def remove_node(self, ip=None):
        if not ip:
            ip = self.tc.servers[1].ip
        rows = self.controls.server_rows()
        ind = row_num = 0
        for row in rows:
            row_num += 1
            for i in range(5):
                try:
                    rows = self.controls.server_rows()
                    if row.get_text().find(str(ip)) != -1:
                        ind = row_num
                        break
                    else:
                        break
                except:
                    pass
            if ind > 0:
                break
        self.tc.assertTrue(ind != 0, 'row is not found')
        self.controls.server_row_controls().remove_btns[ind - 1].click()
        self.wait.until(lambda fn:
                            self.controls.remove_server_dialog().remove_server_pop_up.is_displayed(),
                            "Remove server pop up is closed in %d sec" % self.wait._timeout)
        self.wait.until(lambda fn:
                            self.controls.remove_server_dialog().remove_diag_btn.is_displayed(),
                            "Remove server pop up is closed in %d sec" % self.wait._timeout)
        self.controls.remove_server_dialog().remove_diag_btn.click()
        self.wait.until_not(lambda fn:
                            self.controls.remove_server_dialog().remove_server_pop_up.is_displayed(),
                            "Remove server pop up is not closed in %d sec" % self.wait._timeout)

    def fill_server_info(self, input):
        self.controls.add_server_dialog().ip_address.type(input.param("add_server_ip", self.tc.servers[1].ip))
        self.controls.add_server_dialog().username.type(input.membase_settings.rest_username)
        self.controls.add_server_dialog().password.type(input.membase_settings.rest_password, is_pwd=True)

    def rebalance(self):
        self.start_rebalancing()
        self.wait.until_not(lambda fn: self._is_btn_enabled(self.controls.rebalance_btn),
                            "Rebalance btn is enabled in %d sec" % (self.wait._timeout))
        time.sleep(5)
        self.tc.log.info("Cluster rebalanced")

    def start_rebalancing(self, without_pending=False):
        if not without_pending:
            self.wait.until(lambda fn: self.controls.num_pend_rebalance.is_displayed(),
                            "Number of pending rebalance servers is not displayed in %d sec" % (self.wait._timeout))
        self.wait.until(lambda fn: self._is_btn_enabled(self.controls.rebalance_btn),
                        "Rebalance btn is not enabled in %d sec" % (self.wait._timeout))
        if self.controls.set_quota_btn().is_displayed():
            self.controls.set_quota_btn().click()
            self.wait.until(lambda fn: self._is_btn_enabled(self.controls.rebalance_btn),
                        "Rebalance btn is not enabled in %d sec" % (self.wait._timeout))
        self.controls.rebalance_btn.click()
        self.tc.log.info("Start rebalancing")

    def wait_for_rebalance_stops(self, timeout=None):
        self.wait._timeout = timeout or self.wait._timeout * 500
        self.wait.until(lambda fn: not self.controls.common_rebalance_progress_bar().is_displayed(),
                        "Rebalance progress bar is displayed in %d sec" % (timeout or self.wait._timeout * 500))
        self.wait._timeout = timeout or self.wait._timeout * 10
        self.wait.until(lambda fn: not self.controls.stop_rebalance_btn().is_displayed(),
                        "Stop rebalance is displayed in %d sec" % (timeout or self.wait._timeout * 10))
        self.tc.log.info("Rebalance is stopped")

    def stop_rebalance(self):
        self.controls.stop_rebalance_btn().click()

    def get_number_server_rows(self):
        return len(self.controls.server_rows())

    def open_server_stats(self, server):
        self.tc.log.info("Open stats for server % s" % server.ip)
        for i in [1, 2, 3]:
            try:
                self.controls.server_info(server.ip).server_arrow.click()
                break
            except:
                pass
        self.wait.until(lambda fn:
                        self.controls.server_info(server.ip).server_arrow_opened.is_displayed(),
                        "Server info %s is not enabled in %d sec" % (server.ip, self.wait._timeout * 3))
        time.sleep(3)
        self.tc.log.info("Stats for %s are opened" % server.ip)

    def close_server_stats(self, server):
        self.tc.log.info("Close stats for server % s" % server.ip)
        for i in range(3):
            try:
                self.controls.server_info(server.ip).server_arrow_opened.click()
                break
            except:
                pass
        time.sleep(3)
        self.tc.log.info("Stats for %s are closed" % server.ip)

    def is_server_stats_opened(self, server):
        return (self.controls.server_info(server.ip).server_info(server.ip).server_arrow_opened.is_present() and\
               self.controls.server_info(server.ip).server_info(server.ip).server_arrow_opened.is_displayed())

    def get_server_rebalance_progress(self, server, direction):
        if not self.is_server_stats_opened(server):
            self.open_server_stats(server)
        src = self.controls.server_info_rebalance_progress(server.ip).get_text()
        src = src.split("Data being transferred %s" % direction)[1]
        src = src.split('\n')
        self.tc.log.info("Stats for %s: %s" % (server, src))
        stats = {}
        stats["total_transfer"] = src[1].replace("Total number of keys to be transferred:", "")
        stats["estimated_transfer"] = src[2].replace("Estimated number of keys transferred:", "")
        stats["vbuckets"] = src[3].replace("Number of Active# vBuckets and Replica# vBuckets to transfer:", "")
        self.close_server_stats(server)
        return stats

    def failover(self, server, confirm=True, error=None, graceful=True, confirm_failover=True):
        self.open_failover_confirmation_dialog(server)
        self.confirm_failover(confirm=confirm, is_graceful=graceful, confirm_failover_check=confirm_failover)
        if confirm:
            if error:
                actual_error = self.get_error_failover()
                self.tc.assertTrue(actual_error.contains(error),
                               "Error '%s' is expected. But actual is %s" % (error, actual_error))
            else:
                if not confirm_failover:
                    self.start_rebalancing()
                    RestConnection(self.tc.servers[0]).monitorRebalance()
                else:
                    self.tc.assertTrue(self.is_node_failed_over(server), "Node %s wasn't failed over" % server.ip)
        else:
            self.tc.assertFalse(self.is_node_failed_over(server), "Node %s was failed over" % server.ip)

    def open_failover_confirmation_dialog(self, server):
        self.tc.log.info("Try to open Confirmation failover dialog for server %s" % server.ip)
        i = 0
        while (i < 5):
            try:
                self.controls.server_row_btns(server.ip).failover_btn.click()
                break
            except Exception as ex:
                i += 1
                if i == 4:
                    raise ex
        self.wait.until(lambda fn: self.is_confirmation_failover_opened(),
                        "Confirmation dialog is not displayed in %d sec" % (self.wait._timeout))
        self.tc.log.info("Confirmation failover dialog for server %s is opened" % server.ip)

    def is_confirmation_failover_opened(self):
        opened = True
        try:
            opened &= self.controls.failover_confirmation().failover_conf_dialog.is_displayed()
            opened &= self.controls.failover_confirmation().failover_conf_hard_failover.is_displayed()
            opened &= self.controls.failover_confirmation().failover_conf_submit_btn.is_displayed()
        except StaleElementReferenceException:
            opened = False
        return opened

    def confirm_failover(self, confirm=True, is_graceful=None, confirm_failover_check=False):
        time.sleep(1)
        if is_graceful:
            self.controls.failover_confirmation().failover_conf_gracefull_option.check()
            self.tc.log.info("Graceful Failover Enabled")
        else:
            self.controls.failover_confirmation().failover_conf_hard_failover.check()
            self.tc.log.info("Hard Failover Enabled")
        if confirm_failover_check and self.controls.failover_confirmation().failover_conf_gracefull_option.get_attribute('disabled') == 'true':
                self.controls.failover_confirmation().confirm_failover_option.check()
                self.tc.log.info("Hard Failover Enabled with warnings")
        if confirm:
            if self.controls.failover_confirmation().failover_conf_cb.is_present() and\
                    self.controls.failover_confirmation().failover_conf_cb.is_displayed():
                self.controls.failover_confirmation().failover_conf_cb.check()
            self.controls.failover_confirmation().failover_conf_submit_btn.click_native()
            if self.controls.failover_confirmation().failover_conf_dialog.is_present() or \
                self.controls.failover_warning().is_present():
                self.wait.until(lambda fn: not self.is_confirmation_failover_opened() or\
                                       self.is_error_present_failover(),
                        "No reaction for failover btn click in %d sec" % (self.wait._timeout))
            self.tc.log.info("Failover confirmed")
            RestConnection(self.tc.servers[0]).monitorRebalance()
        else:
            self.controls.failover_confirmation().failover_conf_cancel_btn.click()
            self.tc.log.info("Failover cancelled")

    def is_error_present_failover(self):
        try:
            return self.controls.failover_warning().is_displayed()
        except StaleElementReferenceException as ex:
            self.tc.log.info("Fail Over Warning: At least two servers with the data service are required to provide "\
                             "replication! - is not displayed")
            return False

    def get_error_failover(self):
        return self.controls.failover_warning().get_text()

    def is_node_failed_over(self, server):
        for i in range(3):
            try:
                return self.controls.failed_over_msg(server.ip).is_displayed()
            except:
                pass

    def add_node_back(self):
        self.wait.until(lambda fn: self.controls.add_back_failover().is_displayed(),
                        "Add back node is not displayed in %d sec" % (self.wait._timeout))
        self.controls.add_back_failover().click()
        time.sleep(3)

    def open_recovery(self, server):
        self.tc.log.info("Try to open recovery dialog %s" % (server.ip))
        self.controls.pending_rebalance_tab().click()
        time.sleep(3)
        i = 0
        while (i < 4):
            try:
                self.controls.recovery_btn(server.ip).click()
                break
            except Exception as ex:
                i += 1
                if i == 4:
                    raise ex
        self.wait.until(lambda fn: self.recovery_dialog_opened(),
                        "Recovery btn is not displayed in %d sec" % (self.wait._timeout))
        self.tc.log.info("Dialog is opened")

    def recovery_dialog_opened(self):
        try:
            return self.controls.recovery_dialog().conf_dialog.is_displayed()
        except:
            return False

    def set_recovery(self, server, option='full', confirm=True):
        self.controls.pending_rebalance_tab().click()
        self.tc.log.info("Try to set %s option in recovery %s" % (option, server.ip))
        if option == 'delta':
            self.controls.select_recovery(server.ip).delta_option.click(highlight=False)
        if option == 'full':
            self.controls.select_recovery(server.ip).full_option.click(highlight=False)
        self.tc.log.info("%s option in recovery %s is set" % (option, server.ip))

    def set_recovery_251(self, server, option='full', confirm=True):
        self.tc.log.info("Try to set %s option in recovery %s" % (option, server.ip))
        self.open_recovery(server)
        if option == 'delta':
            self.controls.recovery_dialog().delta_option.click()
        if option == 'full':
            self.controls.recovery_dialog().full_option.click()
        if confirm:
            self.controls.recovery_dialog().save_recovery_btn.click()
        else:
            self.controls.recovery_dialog().cancel_recovery_btn.click()
        self.wait.until(lambda fn: not self.controls.recovery_dialog().conf_dialog.is_displayed(),
                        "Recovery btn is not displayed in %d sec" % (self.wait._timeout))
        self.tc.log.info("%s option in recovery %s is set" % (option, server.ip))

    def check_recovery(self, server, option='full'):
        self.open_recovery(server)
        if option == 'delta':
            if self.controls.recovery_dialog().delta_option.get_attribute('checked') != 'true':
                raise Exception('Delta option is not selected')
        if option == 'full':
            if self.controls.recovery_dialog().full_option.get_attribute('checked') != 'true':
                raise Exception('Full option is not selected')
        self.controls.recovery_dialog().cancel_recovery_btn.click()
        self.wait.until(lambda fn: not self.controls.recovery_dialog().conf_dialog.is_displayed(),
                        "Recovery btn is not displayed in %d sec" % (self.wait._timeout))
        self.tc.log.info("Recovery checked")


class BucketHelper():
    def __init__(self, tc):
        self.tc = tc
        self.wait = WebDriverWait(tc.driver, timeout=25)
        self.controls = BucketTestsControls(tc.driver)

    def create(self, bucket):
        self.tc.log.info("trying create bucket '%s' with options %s" % (bucket.name, bucket))
        self.controls.create_bucket_btn.click()
        self.wait.until(lambda fn:
                        self.controls.bucket_pop_up().create_bucket_pop_up.is_displayed() or
                        self.controls.warning_pop_up('Memory Fully Allocated').is_displayed(),
                        "no reaction for click create new bucket btn in %d sec" % self.wait._timeout)
        self.tc.assertFalse(self.controls.warning_pop_up('Memory Fully Allocated').is_displayed(),
                            "Warning 'Cluster Memory Fully Allocated' appeared")
        self.fill_bucket_info(bucket)
        self.controls.bucket_pop_up().create_btn.click()
        self.tc.log.info("created bucket '%s'" % bucket.name)
        if self.controls.bucket_pop_up().create_bucket_pop_up.is_present():
            self.wait.until_not(lambda fn:
                                self.controls.bucket_pop_up().create_bucket_pop_up.is_displayed(),
                                "create new bucket pop up is not closed in %d sec" % self.wait._timeout)
        self.wait.until(lambda fn: self.is_bucket_present(bucket),
                        "Bucket '%s' is not displayed" % bucket)
        self.tc.log.info("bucket '%s' is displayed" % bucket)
        # self.wait.until(lambda fn: self.is_bucket_healthy(bucket),
        #                "Bucket '%s' is not  in healthy state" % bucket)

    def fill_bucket_info(self, bucket, parent='create_bucket_pop_up'):
        if not parent == 'initialize_step':
            self.controls.bucket_pop_up(parent).name.type(bucket.name)
        if bucket.type:
            self.controls.bucket_pop_up(parent).show_advanced_settings.click()
            self.controls.bucket_pop_up(parent).type(bucket.type).click()
        self.controls.bucket_pop_up(parent).ram_quota.type(bucket.ram_quota)
        if bucket.sasl_password:
            self.controls.bucket_pop_up().standart_port_radio.click()
            self.controls.bucket_pop_up().sasl_password.type(bucket.sasl_password, is_pwd=True)
        if bucket.protocol_port:
            self.controls.bucket_pop_up().dedicated_port_radio.click()
            self.controls.bucket_pop_up().port.type(bucket.protocol_port)
        if bucket.num_replica:
            if bucket.num_replica == '0':
                self.controls.bucket_pop_up(parent).enable_replica_cb.click()
            else:
                self.controls.bucket_pop_up(parent).replica_num.select(str(bucket.num_replica))
        if bucket.index_replica is not None:
            self.controls.bucket_pop_up(parent).index_replica_cb.click()
        if bucket.meta_data is not None:
            self.controls.bucket_meta_data(parent=parent, text=bucket.meta_data).click()
        if bucket.io_priority is not None:
            self.controls.bucket_io_priority(parent=parent, text=bucket.io_priority).click()
        if bucket.frag_percent_cb or bucket.frag_percent or bucket.frag_mb_cb or\
          bucket.frag_mb or bucket.view_frag_percent_cb or bucket.view_frag_percent or\
          bucket.view_frag_mb or bucket.view_frag_mb_cb or bucket.comp_allowed_period_cb or\
          bucket.comp_allowed_period_start_h or bucket.comp_allowed_period_start_min or\
          bucket.comp_allowed_period_end_h or bucket.comp_allowed_period_end_min or\
          bucket.abort_comp_cb or bucket.comp_in_parallel_cb or bucket.purge_interval:
            self.controls.bucket_pop_up(parent).override_comp_cb.check(setTrue=True)
            if bucket.frag_percent_cb is not None or bucket.frag_percent is not None:
                # self.controls.bucket_compaction(parent).frag_percent_cb.check(setTrue=True)
                self.controls.bucket_compaction(parent).frag_percent.web_element.clear()
            if bucket.frag_percent is not None:
                self.controls.bucket_compaction(parent).frag_percent.type(bucket.frag_percent)
            if bucket.frag_mb_cb is not None or bucket.frag_mb is not None:
                self.controls.bucket_compaction(parent).frag_mb_cb.check(setTrue=True)
            if bucket.frag_mb is not None:
                self.controls.bucket_compaction(parent).frag_mb.type(bucket.frag_mb)
            if bucket.view_frag_percent_cb is not None or bucket.view_frag_percent is not None:
                self.controls.bucket_compaction(parent).view_frag_percent_cb.check(setTrue=True)
                self.controls.bucket_compaction(parent).view_frag_percent.web_element.clear()
            if bucket.view_frag_percent is not None:
                self.controls.bucket_compaction(parent).view_frag_percent.type(bucket.view_frag_percent)
            if bucket.view_frag_mb_cb is not None or bucket.view_frag_mb is not None:
                self.controls.bucket_compaction(parent).view_frag_mb_cb.check(setTrue=True)
            if bucket.view_frag_mb is not None:
                self.controls.bucket_compaction(parent).view_frag_mb.type(bucket.frag_percent)
            if bucket.comp_allowed_period_cb is not None or bucket.comp_allowed_period_start_h is not None or\
             bucket.comp_allowed_period_start_min is not None or bucket.comp_allowed_period_end_h is not None or\
             bucket.comp_allowed_period_end_min is not None:
                self.controls.bucket_compaction(parent).comp_allowed_period_cb.click()
            if bucket.comp_allowed_period_start_h is not None:
                self.controls.bucket_compaction(parent).comp_allowed_period_start_h.type(bucket.comp_allowed_period_start_h)
            if bucket.comp_allowed_period_start_min is not None:
                self.controls.bucket_compaction(parent).comp_allowed_period_start_min.type(bucket.comp_allowed_period_start_min)
            if bucket.comp_allowed_period_end_h is not None:
                self.controls.bucket_compaction(parent).comp_allowed_period_end_h.type(bucket.comp_allowed_period_end_h)
            if bucket.comp_allowed_period_end_min is not None:
                self.controls.bucket_compaction(parent).comp_allowed_period_end_min.type(bucket.comp_allowed_period_end_min)
            if bucket.abort_comp_cb is not None:
                self.controls.bucket_compaction(parent).abort_comp_cb.check(setTrue=bucket.abort_comp_cb)
            if bucket.comp_in_parallel_cb is not None:
                self.controls.bucket_compaction(parent).comp_in_parallel_cb.check(setTrue=bucket.comp_in_parallel_cb)
            if bucket.purge_interval is not None:
                purge_intervals = self.controls.bucket_compaction(parent).purge_intervals
                for purge_interval in purge_intervals:
                    if purge_interval.is_displayed():
                        purge_interval.type(bucket.purge_interval)

    def is_bucket_present(self, bucket):
        try:
            bucket_present = self.controls.bucket_info(bucket.name).name.is_displayed()
            # bucket_present &= self.controls.bucket_info(bucket.name).nodes.is_displayed()
            bucket_present &= self.controls.bucket_info(bucket.name).items_count.is_displayed()
            bucket_present &= self.controls.bucket_info(bucket.name).documents.is_displayed()
            # bucket_present &= self.controls.bucket_info(bucket.name).views.is_displayed()
            bucket_present &= self.controls.bucket_info(bucket.name).statistics.is_displayed()
            return bucket_present
        except:
            time.sleep(1)
            return False

    def get_error(self):
        for err in self.controls.bucket_create_error():
            if err.is_displayed() and err.get_text() != '':
                 return err.get_text()

    def is_bucket_healthy(self, bucket):
        try:
            self.controls.bucket_info(bucket.name).health.mouse_over()
            self.wait.until(lambda fn:
                                self.controls.bucket_info(bucket.name).health.get_attribute('title') != '')
            status = self.controls.bucket_info(bucket.name).health.get_attribute('title')
            status_dict = {}
            for item in status.split(','):
                item = item.strip()
                status_dict[item.split(' ')[1]] = int(item.split(' ')[0])
            if status_dict['unhealthy'] > 0 or status_dict['down'] > 0:
                self.controls.bucket_info(bucket.name).name.mouse_over()
                self.tc.refresh()
                return False
            return True
        except:
            return False

    def open_documents(self, bucket):
        for i in range(3):
            try:
                self.controls.bucket_info(bucket.name).documents.click()
                break
            except (ElementNotVisibleException, AttributeError):
                time.sleep(1)
                pass

    def open_stats(self, bucket):
        self.controls.bucket_info(bucket.name).statistics.click()
        self.tc.log.info("Stats page is opened")
        time.sleep(10)
        # self.controls.bucket_stats(tab="Server Resources").arrow.click()

    def open_view_block_stats(self):
        self.wait.until(lambda fn:
                            self.controls.bucket_stat_view_block().is_displayed(),
                            "stat view block is not displayed in %d sec" % (
                                                  self.wait._timeout))
        self.controls.bucket_stat_view_block().click()
        self.tc.log.info("Stats page is opened")

    def get_stat(self, stat, block=None):
        if block is None:
            self.wait.until(lambda fn:
                            self.controls.bucket_stats(stat).stats.is_displayed(),
                            "stat %s is not displayed in %d sec" % (
                                                  stat, self.wait._timeout))
            return self.controls.bucket_stats(stat).stats.get_text()
        elif block == 'view':
            self.open_view_block_stats()
            self.wait.until(lambda fn:
                            self.controls.bucket_stat_from_view_block(stat).is_displayed(),
                            "stat %s is not displayed in %d sec" % (
                                                  stat, self.wait._timeout))
            return self.controls.bucket_stat_from_view_block(stat).get_text()
        else:
            raise Exception("Block is not implemented yet!!!")

    def get_items_number(self, bucket):
        return self.controls.bucket_info(bucket.name).items_count.get_text()


class NodeInitializeHelper():
    def __init__(self, tc):
        self.tc = tc
        self.wait = WebDriverWait(tc.driver, timeout=25)
        self.controls = NodeInitializeControls(tc.driver)

    def _get_error(self):
        error_text = ''
        for error in self.controls.errors().error_inline:
            if error.is_displayed():
                if not error.get_text():
                    time.sleep(1)
                error_text += error.get_text()
        # TODO
        # if self.controls.errors().error_warn.is_displayed():
        #     error_text += self.controls.errors().error_warn.get_text()
        return error_text

    def _go_next_step(self, last_step=False):
        # step = self._get_current_step_num()
        # self.tc.log.info("try to open next step. Now you are on %s" % step)
        if last_step:
            self.controls.step_screen().finish_btn.click()
            self.wait.until(lambda fn: NavigationHelper(self.tc)._is_tab_selected('Dashboard'),
                            "Main page is not opened")
        else:
            self.controls.step_screen().next_btn.click()
        # else:

            # self.wait.until(lambda fn: self._get_current_step_num() == step + 1 or
            #                            self._get_error() != '',
            #                 "no reaction for clicking next btn")
        if self._get_error():
            raise Exception("error '%s' appears" % self._get_error())
        self.tc.log.info("Next step screen is opened")

    # def _get_current_step_num(self):
    #     if self.controls.step_screen().current_step is None:
    #         return 0
    #     if self.controls.step_screen().current_step.get_text():
    #         return int(re.match(r'Step (.*) of (.*)', self.controls.step_screen().current_step.get_text()).group(1))
    #     else:
    #         return 0

    '''
    Following params from test input will be processed:
    db_path
    indeces_path
    ram_quota  - ram quota for starting new cluster
    user_cluster - user for join cluster
    password_cluster - password for user joining cluster
    ip_cluster - ip for joining cluster
    '''
    def _fill_dms(self, input):
        if input.param("db_path", None):
            self.controls.step_1().db_path.type(input.param("db_path", None))
        if input.param("indeces_path", None):
            self.controls.step_1().indeces_path.type(input.param("indeces_path", None))
        if input.param("ram_quota_node", None):
            # if self.controls.step_1().new_cluster_cb.is_displayed():
            # self.controls.step_1().new_cluster_cb.click()
            self.controls.step_1().ram_quota.type(input.param("ram_quota_node", None))
            self.controls.step_1().ram_quota.mouse_over()
        # else:
        #     self.controls.step_1().new_cluster_cb.click()
        #     self.controls.step_1().ram_quota.type(input.param("ram_quota_node", None))
        #     self.controls.step_1().ram_quota.mouse_over()

        if input.param("user_cluster", None) or input.param("password_cluster", None) \
                                            or input.param("ip_cluster", None):
            if self.controls.step_1().join_cluster.is_displayed():
                self.controls.step_1().join_cluster.click()
            self.controls.step_1().user_cluster.type(input.param("user_cluster", None))
            self.controls.step_1().password_cluster.type(input.param("password_cluster", None), is_pwd=True)
            self.controls.step_1().ip_cluster.type(input.param("ip_cluster", None))

    def _fill_2_step(self, input):
        if input.param("sample", None):
            self.controls.step_2_sample(input.param("sample", None)).check()
            # TODO successful loading?

    def _fill_3_step(self, input):
        BucketHelper(self.tc).fill_bucket_info(Bucket(parse_bucket=input),
                                               parent='initialize_step')

    def _fill_accept_terms_step(self, input):
        if input.param("enable_updates", None) is not None:
            self.controls.step_4().enable_updates.check(setTrue=input.param("enable_updates", None))
        if input.param("email", None) or input.param("last_name", None) or \
                input.param("last_name", None) or input.param("company", None):
            self.controls.step_4().expand_registry.click()
        self.controls.step_4().email.type(input.param("email", None))
        self.controls.step_4().first_name.type(input.param("first_name", None))
        self.controls.step_4().last_name.type(input.param("last_name", None))
        self.controls.step_4().company.type(input.param("company", None))
        if input.param("agree_terms", None) is not None:
            if self.controls.step_4().agree_terms.is_displayed():
                action = webdriver.common.action_chains.ActionChains(self.tc.driver)
                action.move_to_element_with_offset(self.tc.driver.find_element_by_link_text('terms & conditions'), -100,
                                                   0).click().perform()
            else:
                self.tc.log.info("This version of application doesn't contain agree checkbox(step 4)")

    def _fill_new_cluster_step(self, input):
        time.sleep(2)
        self.controls.step_new_cluater().cluster_name.type("My Cluster", is_pwd=True)
        self.controls.step_new_cluater().password_confirm.type(input.membase_settings.rest_password, is_pwd=True)
        self.controls.step_new_cluater().user.type(input.membase_settings.rest_username)
        self.controls.step_new_cluater().password.type(input.membase_settings.rest_password, is_pwd=True)

    def initialize(self, input):
        self.tc.log.info('Starting initializing node')
        self.controls.setup_btn.click()
        self._fill_new_cluster_step(input)
        self._go_next_step(last_step=False)
        self._fill_accept_terms_step(input)
        ControlsHelper(self.tc.driver).find_control('initialize', 'configure_dms').click_with_mouse_over()
        self._fill_dms(input)
        self._go_next_step(last_step=True)

        # self.wait.until(lambda fn: self._get_current_step_num() == 1, "first step screen is not opened")
        # for i in xrange(1, 6):
        #     self.tc.log.info('Filling step %d ...' % i)
        #     getattr(self, '_fill_{0}_step'.format(i))(input)
        #     self.tc.log.info('Step %d filled in' % i)
        #     if i == 5:
        #         self._go_next_step(last_step=True)
        #     else:
        #         self._go_next_step()


class DdocViewHelper():
    def __init__(self, tc):
        self.tc = tc
        self.wait = WebDriverWait(tc.driver, timeout=25)
        self.controls = DdocViewControls(tc.driver)

    def click_view_tab(self, text=''):
        self.controls.views_screen(text=text).views_tab.click()
        self.tc.log.info("tab '%s' is selected" % text)
        time.sleep(10)

    def create_view(self, ddoc_name, view_name, dev_view=True):
        self.tc.log.info('trying create a view %s' % view_name)
        try:
            self.wait.until(lambda fn:
                            self.controls.view_btn().create_view_btn.is_displayed(),
                            "Create View button is not displayed")
            self.controls.view_btn().create_view_btn.click()
            self.wait.until(lambda fn:
                            self.controls.create_pop_up().pop_up.is_displayed(),
                            "Create pop up is not opened")
        except Exception as e:
            BaseHelper(self.tc).create_screenshot()
            raise e
        self.controls.create_pop_up().ddoc_name.type(ddoc_name)
        self.controls.create_pop_up().view_name.type(view_name)
        self.controls.create_pop_up().save_btn.click()
        self.wait.until(lambda fn:
                        self.is_view_present(view_name),
                        "view %s is not appeared" % view_name)
        self.tc.log.info('View %s created' % view_name)
        if not dev_view:
            self.controls.ddoc_row(ddoc_name).publish_btn.click()
            self.wait.until(lambda fn:
                        self.controls.prod_view().prod_view_tab.is_displayed(),
                        "Production View tab is not displayed")
            self.wait.until(lambda fn:
                        self.controls.prod_view().prod_view_count.is_displayed(),
                        "View is not published successfully")
            self.tc.log.info('View %s published' % view_name)

    def is_view_present(self, view_name):
        try:
            return self.controls.view_row(view_name).row.is_displayed()
        except Exception as ex:
            print(ex)
            return False

    def is_new_view_present(self, view_name):
        try:
            return self.controls.view_screen(view_name).view_name_set.is_displayed()
        except Exception as ex:
            print(ex)
            return self.controls.view_screen(view_name).view_name_set.is_displayed()

    def open_view(self, view_name):
        self.tc.log.info('trying open view %s' % view_name)
        for i in range(3):
            try:
                self.controls.view_row(view_name).name.click()
                break
            except StaleElementReferenceException:
                pass
        self.wait.until(lambda fn:
                        self.controls.view_map_reduce_fn().map_fn.is_displayed(),
                        "view screen is not opened")
        self.tc.log.info('view screen is opened successfully')

    def get_random_doc_name(self):
        return self.controls.view_screen().random_doc_name.get_text()

    def get_meta_data_doc(self):
        return re.sub(r'\s', '', self.controls.view_screen().random_doc_meta.get_text())

    def get_content_data_doc(self):
        return re.sub(r'\s', '', self.controls.view_screen().random_doc_content.get_text())

    def click_edit_doc(self):
        self.controls.view_screen().random_doc_edit_btn.click()
        time.sleep(3)
        self.wait.until(lambda fn:
                        self.controls.view_screen().screen.is_displayed(),
                        "edit screen wasn't opened")

    def edit_view(self, view_name):
        self.tc.log.info('trying edit view %s' % view_name)
        for i in range(3):
            try:
                self.controls.view_row(view_name).edit_btn.click()
            except:
                pass
        self.wait.until(lambda fn:
                        self.controls.view_map_reduce_fn().map_fn.is_displayed(),
                        "Edit view screen is not opened")

    def delete_view(self, view_name):
        self.wait.until(lambda fn:
                       self.controls.view_row(view_name).row.is_displayed(),
                      "View row %s is not displayed" % view_name)
        self.controls.view_row(view_name).delete_btn.click()
        self.wait.until(lambda fn:
                        self.controls.del_view_dialog().ok_btn.is_displayed(),
                        "Delete view dialog is not opened")
        self.controls.del_view_dialog().ok_btn.click()
        self.tc.log.info('View %s deleted' % view_name)

    def fill_edit_view_screen(self, view_name, action='save', reduce_fn='_count'):
        self.tc.log.info('Fill edit view %s screen' % view_name)
        new_view_name = "test1"
        updated_map_fn = 'function (doc) {if(doc.age !== undefined) { emit(doc.id, doc.age);}}'
        if updated_map_fn:
            self.controls.view_map_reduce_fn().map_fn.type_native(updated_map_fn)
            if self.get_error():
                raise Exception("Error '%s' appeared" % self.get_error())
        if reduce_fn:
            self.controls.view_map_reduce_fn().reduce_fn.type_native(reduce_fn)
            if self.get_error():
                raise Exception("Error '%s' appeared" % self.get_error())
        if action == 'save':
            self.wait.until(lambda fn:
                            self.controls.view_map_reduce_fn().save_btn.is_displayed(),
                            "Save Button is not displayed")
            self.controls.view_map_reduce_fn().save_btn.click()
        if action == 'save_as':
            self.wait.until(lambda fn:
                            self.controls.view_map_reduce_fn().saveas_btn.is_displayed(),
                            "Save As Button is not displayed")
            self.controls.view_map_reduce_fn().saveas_btn.click()
            self.wait.until(lambda fn:
                            self.controls.create_pop_up().view_name.is_displayed(),
                            "Popup is not displayed")
            self.controls.create_pop_up().view_name.type(new_view_name)
            self.controls.create_pop_up().save_btn.click()
            time.sleep(2)
            self.click_view_tab(text='Views')
            self.wait.until(lambda fn:
                            self.is_view_present(new_view_name),
                            "view %s is not appeared" % new_view_name)
            time.sleep(1)
        self.tc.log.info('View is successfully edited')

    def verify_view_results(self, view_set, reduce_fn, value=0):
        self.tc.log.info('Verify View Results')
        if view_set == 'dev':
            self.wait.until(lambda fn:
                            self.controls.view_results_container().dev_subset.is_displayed,
                            "View Results Container Dev Subset is not displayed")
            self.controls.view_results_container().dev_subset.click()
        else:
            self.wait.until(lambda fn:
                            self.controls.view_results_container().full_subset.is_displayed,
                            "View Results Container Full Subset is not displayed")
            self.controls.view_results_container().full_subset.click()
        self.wait.until(lambda fn:
                        self.controls.view_results_container().doc_arrow.is_displayed,
                        "Doc Arrow is not displayed")
        self.controls.view_results_container().doc_arrow.click()
        self.wait.until(lambda fn:
                        self.controls.view_results_container().view_arrow.is_displayed,
                        "View Arrow is not displayed")
        self.controls.view_results_container().view_arrow.click()
        self.wait.until(lambda fn:
                        self.controls.view_results_container().show_results_btn.is_displayed,
                        "Show Results Button is not displayed")
        self.controls.view_results_container().show_results_btn.click()
        self.wait.until(lambda fn:
                        self.reaction_with_stale_element(self.controls.view_results_container().table_id.is_displayed),
                        "View Results Table is not displayed")
        if reduce_fn == '_count':
            self.wait.until(lambda fn:
                        self.reaction_with_stale_element(self.controls.view_results_container(value).doc_count.is_displayed),
                        "Correct Document count is not displayed")
        elif reduce_fn == '_sum':
            self.wait.until(lambda fn:
                        self.reaction_with_stale_element(self.controls.view_results_container(value).doc_count.is_displayed),
                        "Correct Age Sume is not displayed")
        elif reduce_fn == '_stats':
            self.wait.until(lambda fn:
                        self.reaction_with_stale_element(self.controls.view_results_container(value).doc_count.is_displayed),
                        "Correct Stats are displayed")
        else:
            self.tc.log.info("No Reduce fn specified")
        self.tc.log.info('View Results are successfully verified')

    def reaction_with_stale_element(self, fn):
        try:
            return fn()
        except StaleElementReferenceException:
            return False

    def get_error(self):
        if self.controls.error() and self.controls.error().get_text() != '':
            return self.controls.error().get_text()
        else:
            return None


class DocsHelper():
    def __init__(self, tc):
        self.tc = tc
        self.wait = WebDriverWait(tc.driver, timeout=25)
        self.controls = DocumentsControls(tc.driver)

    def create_doc(self, doc):
        self.tc.log.info('trying create a doc %s' % doc.name)
        self.wait.until(lambda fn:
                        self.controls.create_doc_screen().documents_screen.is_displayed())
        time.sleep(5)
        self.controls.create_doc_screen().create_doc.click()
        self.fill_create_doc_pop_up(doc.name)
        self.wait.until(lambda fn:
                        self.controls.edit_document_screen().content is not None and \
                        self.controls.edit_document_screen().content.is_displayed(),
                        "edit doc screen didn't appeared")
        self.tc.log.info("edit doc screen appeared")
        self.fill_edit_doc_screen(doc)
        NavigationHelper(self.tc).navigate_breadcrumbs('Documents')
        self.verify_doc_in_documents_screen(doc)
        self.tc.log.info('created doc')

    def fill_create_doc_pop_up(self, doc_name):
        self.wait.until(lambda fn:
                        self.controls.create_doc_pop_up().doc_name.is_displayed(),
                        "'create document' pop up didn't appear")
        self.tc.log.info("create document pop up appeared")
        self.controls.create_doc_pop_up().doc_name.type(doc_name)
        self.controls.create_doc_pop_up().save_btn.click()
    '''
    action can be save and save_as
    '''
    def edit_doc(self, old_doc, new_doc=None, action='save'):
        self.tc.log.info('trying to edit doc %s' % old_doc)
        self.controls.document_row(old_doc.name).edit_btn.click()
        self.tc.log.info('verify old content')
        self.wait.until(lambda fn:
                        self.controls.edit_document_screen(doc=old_doc.name).name.is_displayed(),
                        "edit screen wasn't opened")
        self.tc.assertTrue(self.is_doc_opened(old_doc.name, old_doc.content),
                           "Doc %s is not opened" % old_doc)
        name = (old_doc.name, new_doc.name or old_doc.name)[action == 'save_as']
        content = new_doc.content or old_doc.content
        self.fill_edit_doc_screen(Document(name, content), action)
        return Document(name, content)

    def fill_edit_doc_screen(self, doc, action='save'):
        self.tc.log.info('trying to edit doc %s' % doc)
        if doc.content:
            self.controls.edit_document_screen().content.type_native(doc.content)
            if self.get_error():
                raise Exception("Error '%s' appeared" % self.get_error())
        if action == 'save':
            self.controls.edit_document_screen().save_btn.click(highlight=False)
            # self.wait.until(lambda fn:
            #                 self.controls.edit_document_screen()
            #                 .save_btn.get_attribute('class').find('disabled') > -1,
            #                 "Doc %s is not saved" % doc)
        if action == 'save_as':
            self.controls.edit_document_screen().save_as_btn.click()
            self.fill_create_doc_pop_up(doc.name)
            self.wait.until(lambda fn:
                            self.is_doc_opened(doc.name, doc.content),
                            "Doc %s is not saved" % doc)
        time.sleep(1)
        self.tc.log.info('doc is successfully edited')

    def verify_doc_in_documents_screen(self, doc):
        self.tc.log.info("verify doc '%s' on documents page" % doc)
        # self.controls.edit_document_screen().documents_link.click()
        time.sleep(1)
        self.tc.driver.refresh()
        self.wait.until(lambda fn:
                        self.is_doc_present(doc.name, doc.content),
                        "Doc %s is not appeared" % doc.name)
        if self.get_error():
            raise Exception("Error '%s' appeared" % self.get_error())
        self.tc.log.info("doc '%s' is displayed on documents page" % doc)

    def is_doc_opened(self, name, content=None):
        opened = self.controls.edit_document_screen(doc=name).name.is_displayed()
        if content:
            original_content = self.controls.edit_document_screen().content.get_text()
            original_content = original_content.split('\n')
            original_content = "".join(original_content[1::2])
            opened &= (re.sub(r'\s', '', original_content) == 
                       re.sub(r'\s', '', content))
        return opened

    def select_docs_per_page(self, num_docs):
        self.tc.log.info('select documents per page number %s' % num_docs)
        self.controls.pagination().page_num_selector_arrow.click()
        self.wait.until(lambda fn:
                        len(self.controls.pagination().page_num_selector) > 0,
                        "dropdown is not appeared")
        for option in self.controls.pagination().page_num_selector:
            if option.get_text() == num_docs:
                option.click()
                break

    def go_to_next_page(self):
        page = self.get_current_page_num()
        self.controls.pagination().next_page_btn.click()
        self.wait.until(lambda fn:
                        self.get_current_page_num() == (page + 1),
                        "Next page is not opened")

    def get_current_page_num(self):
        return int(self.controls.pagination().current_page_num.get_text())

    def get_total_pages_num(self):
        return int(self.controls.pagination().total_page_num.get_text())

    def get_rows_count(self):
        return len(self.controls.docs_rows())

    def is_doc_present(self, doc_name, doc_content):
        is_present = self.controls.document_row(doc_name).name.is_displayed()
        if doc_content and is_present:
            is_present &= \
                (re.sub(r'\s', '', self.controls.document_row(doc_name).content.get_text()) == 
                re.sub(r'\s', '', doc_content))
        return is_present

    def get_error(self):
        if self.controls.error() and self.controls.error().get_text() != '':
            return self.controls.error().get_text()
        else:
            return None

    def search(self, doc):
        self.controls.lookup_input.type(doc.name)
        self.tc.assertTrue(self.get_error() is None, "error appears: %s"
                           % self.get_error())
        self.controls.lookup_btn.click()
        self.wait.until(lambda fn:
                        self.controls.edit_document_screen(doc=doc.name).name.is_displayed(),
                        "Doc %s is not found" % doc.name)


class SettingsHelper:
    def __init__(self, tc):
        self.tc = tc
        self.controls = SettingsTestControls(tc.driver)
        self.wait = WebDriverWait(tc.driver, timeout=25)

    def click_ro_tab(self):
        self.controls.security_tabs().ro_tab.click()
        self.tc.log.info("tab Internal User/roles is selected")

    def click_external_user_tab(self):
        self.controls.security_tabs().external_user_tab.click()
        self.tc.log.info("tab External User/Roles is selected")

    def navigate(self, tab):
        self.wait.until(lambda fn: self.controls._settings_tab_link(tab).is_displayed(),
                        "tab '%s' is not displayed in %d sec" % (tab, self.wait._timeout))
        self.tc.log.info("try to navigate to '%s' tab" % tab)
        self.controls._settings_tab_link(tab).click()
        self.tc.log.info("tab '%s' is selected" % tab)

    def fill_alerts_info(self, input):
        self.controls.alerts_info().enable_email_alerts.check(setTrue=input.param("enable_email_alerts", True))
        self.controls.alerts_info().email_host.type(input.param("alerts_email_host", 'itweb01.hq.northscale.net'))
        self.controls.alerts_info().email_user.type(input.param("alerts_email_username", None))
        self.controls.alerts_info().email_port.type(input.param("alerts_email_port", None))
        self.controls.alerts_info().email_pass.type(input.param("alerts_email_password", None), is_pwd=True)
        self.controls.alerts_info().email_encrypt.check(setTrue=input.param("alerts_email_encrypt", True))
        self.controls.alerts_info().email_sender.type(input.param("alerts_email_sender", 'qa@couchbase.com'))
        self.controls.alerts_info().email_recipients.type(input.param("alerts_email_recipients", 'iryna@couchbase.com'))
        self.wait.until(lambda fn: self.controls.alerts_info().test_email_btn.is_displayed(),
                        "Test Mail btn is not displayed in %d sec" % (self.wait._timeout))
        # self.controls.alerts_info().test_email_btn.click()
        #        self.wait.until(lambda fn: self.controls.alerts_info().sent_email_btn.is_displayed(),
        #           "Test Mail btn is not selected in %d sec" % (self.wait._timeout))
        self.tc.log.info("Test Mail btn is selected")

        self.wait.until(lambda fn: self.controls.alerts_info().save_btn.is_displayed(),
                        "Save btn is not displayed in %d sec" % (self.wait._timeout))
        self.controls.alerts_info().save_btn.click()
        # self.wait.until(lambda fn: self.controls.alerts_info().done_btn.is_displayed() or
        #                (self.controls.alerts_info().save_btn.is_displayed() and\
        #                 self.controls.alerts_info().save_btn.get_attribute('disabled') == 'true'),
        #                "Save btn is not selected in %d sec" % (self.wait._timeout))
        self.tc.log.info("Save btn is selected")

    def fill_auto_failover_info(self, input):
        self.controls.auto_failover_info().enable_auto_failover.check(setTrue=input.param("enable_auto_failover", True))
        self.controls.auto_failover_info().failover_timeout.type(input.param("auto_failover_timeout", 40))
        self.wait.until(lambda fn: self.controls.auto_failover_info().what_is_this.is_displayed(),
                        "What is this? link is not displayed in %s sec" % (self.wait._timeout))
        self.controls.auto_failover_info().what_is_this.click()
        self.wait.until(lambda fn: self.controls.auto_failover_info().save_btn.is_displayed(),
                        "Save tab is not displayed in %s sec" % (self.wait._timeout))
        self.controls.auto_failover_info().save_btn.click()
        # self.wait.until(lambda fn: self.controls.auto_failover_info().done_btn.is_displayed() or
        #                (self.controls.auto_failover_info().save_btn.is_displayed() and\
        #                 self.controls.auto_failover_info().save_btn.get_attribute('disabled') == 'true'),
        #                "Save btn is not selected in %d sec" % (self.wait._timeout))
        self.tc.log.info("Save btn is selected")

    def select_sample_bucket(self, sample):
        self.tc.log.info("Selecting sample %s ..." % sample)
        self.controls.samples_buckets(sample).sample_cb.click()
        self.wait.until(lambda fn:
                        self.is_sample_bucket_installed(sample).is_displayed() or
                        self.get_error_samples(),
                        "No reaction for sample bucket in %d sec" % (self.wait._timeout))
        if self.get_error_samples():
            raise Exception("Error during adding sample %s" % self.get_error_samples())
        self.tc.log.info("Selected sample %s" % sample)
        return Bucket(name=sample)

    def get_error_samples(self):
        msgs = []
        for control in self.controls.samples_buckets().error_msg:
            if control.is_displayed() and control.get_text() != '':
                msgs.append(control.get_text())
        return msgs

    def is_sample_bucket_installed(self, sample):
        return self.controls.samples_buckets(sample).installed_sample.is_displayed()

    def is_user_created(self, username):
        return ControlsHelper(self.tc.driver). \
            find_control('external_user', 'username_in_table', text=username).is_displayed()

    def is_external_user_created(self, username):
        return ControlsHelper(self.tc.driver).\
            find_control('external_user', 'username_in_table', text=username).is_displayed()

    # def is_error_present(self):
    #     if self.get_error_msg():
    #         return True
    #     return False

    def is_user_error_present(self):
        if self.get_user_error_msg():
            return True
        return False

    # def get_error_msg(self):
    #     msgs = []
    #     for control in self.controls.user_error_msgs():
    #         if control.is_displayed() and control.get_text() != '':
    #             msgs.append(control.get_text())
    #     return msgs

    def get_user_error_msg(self):
        msgs = []
        for control in self.controls.external_user_error_msgs():
            if control.is_displayed() and control.get_text() != '':
                msgs.append(control.get_text())
        return msgs

    def delete_user(self, user=''):
        self.tc.log.info("Delete RO user")
        ControlsHelper(self.tc.driver).find_control('external_user', 'username_in_table', text=user).click()
        ControlsHelper(self.tc.driver).find_control('external_user', 'delete_by_user', text=user).click()
        # self.controls.user_create_info().delete_btn.click()
        self.wait.until(lambda fn: self.controls.confirmation_user_delete().delete_btn.is_displayed(),
                        "Confirmation pop up didn't appear in %d sec" % self.wait._timeout)
        self.controls.confirmation_user_delete().delete_btn.click()
        if self.controls.user_create_info().username.is_present():
            self.wait.until(lambda fn: self.controls.user_create_info().username.is_displayed(),
                            "Username is not displayed in %d sec" % self.wait._timeout)
        self.tc.log.info("RO user is deleted")

    def create_user(self, user, pwd, verify_pwd=None):
        if verify_pwd is None:
            verify_pwd = pwd
        self.tc.log.info("Try to create user %s" % user)
        self.wait.until(lambda fn: self.controls.external_user_create_info().add_user.is_displayed(),
                        "Add User button is not displayed in %d sec" % self.wait._timeout)
        self.controls.external_user_create_info().add_user.click()
        self.wait.until(lambda fn: self.controls.user_create_info().username.is_displayed(),
                        "Username is not displayed in %d sec" % self.wait._timeout)
        self.controls.user_create_info().username.type_native(user)
        self.controls.user_create_info().password.type_native(pwd)
        self.controls.user_create_info().verify_password.type_native(verify_pwd)
        self.controls.external_user_create_info(['Read Only Admin']).roles_items[0].click()
        self.controls.user_create_info().create_btn.click()
        self.wait.until(lambda fn: self.is_user_created(user) or self.is_user_error_present(),
                        "No reaction for create btn in %d sec" % self.wait._timeout)
        if self.is_user_error_present():
            error = self.get_user_error_msg()
            self.tc.log.error("User %s not created. error %s" % (user, error))
            raise Exception("ERROR while creating user: %s" % error)
        self.tc.log.info("User %s created" % user)

    def create_external_user(self, user, fullname='', roles=[], expected_error=''):
        self.tc.log.info("Try to add external user %s" % user)
        self.wait.until(lambda fn: self.controls.external_user_create_info().add_user.is_displayed(),
                        "Add User button is not displayed in %d sec" % self.wait._timeout)
        self.controls.external_user_create_info().add_user.click()
        self.wait.until(lambda fn: self.controls.external_user_create_info().name_inp.is_displayed(),
                        "Username is not displayed in %d sec" % self.wait._timeout)
        self.controls.external_user_create_info().select_external.click()
        self.controls.external_user_create_info().name_inp.type_native(user)
        self.controls.external_user_create_info().name_full_inp.type_native(fullname)
        if roles:
            for i in range(len(roles)):
                # self.controls.external_user_create_info().roles_selector.click()
                self.controls.external_user_create_info(roles[i:]).roles_items[0].click()
        self.controls.external_user_create_info().save_button.click()
        self.wait.until(lambda fn: self.is_external_user_created(user) or self.is_user_error_present(),
                        "No reaction for create btn in %d sec" % self.wait._timeout)
        if self.is_user_error_present():
            error = self.get_user_error_msg()
            self.tc.log.error("User %s not created. error %s" % (user, error))
            if expected_error:
                if expected_error != error[0]:
                    raise Exception("Wrong ERROR while creating user: %s, expected: %s" % (error, expected_error))
                else:
                    self.tc.log.info("Expected error found: %s" % error)
            else:
                raise Exception("ERROR while creating user: %s" % error)
        else:
            if expected_error:
                raise Exception("Expected ERROR '%s' not found. USer %s created!" % (expected_error, user))
            else:
                self.tc.log.info("User %s created" % user)

    def delete_external_user(self, user):
        self.tc.log.info("Delete external user %s" % user)
        ControlsHelper(self.tc.driver).find_control('external_user', 'username_in_table', text=user).click()
        ControlsHelper(self.tc.driver).find_control('external_user', 'delete_by_user', text=user).click()
        self.wait.until(lambda fn: self.controls.confirmation_external_user_delete().delete_btn.is_displayed(),
                        "Confirmation pop up didn't appear in %d sec" % self.wait._timeout)
        self.controls.confirmation_external_user_delete().delete_btn.click()
        if self.controls.helper.find_control('external_user', 'delete_by_user', text=user).is_present():
            self.wait.until(lambda fn: not self.controls.helper.find_control('external_user', 'delete_by_user', text=user).is_displayed(),
                            "Username is not displayed in %d sec" % self.wait._timeout)
        self.tc.log.info("External user %s deleted" % user)

    def disable_authentication(self):
        if not self.controls.external_user_create_info().label_disabled.is_displayed():
            self.controls.external_user_create_info().disable_link.click()

    def enable_authentication(self):
        if self.controls.external_user_create_info().label_disabled.is_displayed():
            self.controls.external_user_create_info().enable_link.click()


# Objects
class Bucket:
    def __init__(self, name='default', type='Couchbase', ram_quota=None, sasl_pwd=None,
                 port=None, replica=None, index_replica=None, parse_bucket=None,
                 meta_data=None, io_priority=None, frag_percent_cb=None,
                 frag_percent=None, frag_mb_cb=None, frag_mb=None, view_frag_percent_cb=None, view_frag_percent=None,
                 view_frag_mb=None, view_frag_mb_cb=None, comp_allowed_period_cb=None, comp_allowed_period_start_h=None,
                 comp_allowed_period_start_min=None, comp_allowed_period_end_h=None,
                 comp_allowed_period_end_min=None, abort_comp_cb=None, comp_in_parallel_cb=None,
                 purge_interval=None):
        self.name = name or 'default'
        self.type = type
        self.ram_quota = ram_quota
        self.sasl_password = sasl_pwd
        self.protocol_port = port
        self.num_replica = replica
        self.index_replica = index_replica
        self.meta_data = meta_data
        self.io_priority = io_priority
        self.frag_percent_cb = frag_percent_cb
        self.frag_percent = frag_percent
        self.frag_mb_cb = frag_mb_cb
        self.frag_mb = frag_mb
        self.view_frag_percent_cb = view_frag_percent_cb
        self.view_frag_percent = view_frag_percent
        self.view_frag_mb = view_frag_mb
        self.view_frag_mb_cb = view_frag_mb_cb
        self.comp_allowed_period_cb = comp_allowed_period_cb
        self.comp_allowed_period_start_h = comp_allowed_period_start_h
        self.comp_allowed_period_start_min = comp_allowed_period_start_min
        self.comp_allowed_period_end_h = comp_allowed_period_end_h
        self.comp_allowed_period_end_min = comp_allowed_period_end_min
        self.abort_comp_cb = abort_comp_cb
        self.comp_in_parallel_cb = comp_in_parallel_cb
        self.purge_interval = purge_interval
        if parse_bucket:
            for param in parse_bucket.test_params:
                if hasattr(self, str(param)):
                    setattr(self, str(param), parse_bucket.test_params[param])

    def __str__(self):
        return '<Bucket: name={0}, type={1}, ram_quota={2}, sasl_pwd={3} >'.format(self.name,
                                                                    self.type, self.ram_quota, self.sasl_password)


class Document():
    def __init__(self, name, content=None, bucket='default'):
        self.name = name
        self.content = content
        self.bucket = bucket

    def __str__(self):
        return '<Document: name={0}, content={1}'.format(self.name, self.content)
