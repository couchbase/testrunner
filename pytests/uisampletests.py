import re
import logger
import time
import unittest

from uibasetest import *
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from TestInput import TestInputSingleton
from couchbase.cluster import Cluster
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper


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

    def tearDown(self):
        super(BucketTests, self).tearDown()

    def test_add_bucket(self):
        bucket = Bucket(parse_bucket=self.input)
        NavigationHelper(self).navigate('Data Buckets')
        BucketHelper(self).create(bucket)

    def test_bucket_stats_mb_8538(self):
        self.bucket = Bucket()
        NavigationHelper(self).navigate('Data Buckets')
        BucketHelper(self).create(self.bucket)

        NavigationHelper(self).navigate('Views')
        view_name = 'test_view_ui'
        DdocViewHelper(self).create_view(view_name, view_name)

        NavigationHelper(self).navigate('Data Buckets')
        BucketHelper(self).open_stats(self.bucket)
        total_views_st = BucketHelper(self).get_stat("views total disk size")
        view_st = BucketHelper(self).get_stat("disk size", block="view")
        self.assertEquals(total_views_st, view_st,
                          "Stats should be equal, but views total disk size is %s"
                          " and disk size from view section is %s" % (
                            total_views_st, view_st))
        self.log.info("Stat 'views total disk size' and 'disk size' are %s"
                      " as expected" % total_views_st)

class InitializeTest(BaseUITestCase):
    def setUp(self):
        super(InitializeTest, self).setUp()
        self._deinitialize_api()

    def tearDown(self):
        super(InitializeTest, self).tearDown()

    def test_initialize(self):
        try:
            NodeInitializeHelper(self).initialize(self.input)
        except:
            self._initialize_api()

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
                time.sleep(5)
                self.driver.refresh()
            except BaseException, e:
                self.fail(e)

class DocumentsTest(BaseUITestCase):
    def setUp(self):
        super(DocumentsTest, self).setUp()
        BaseHelper(self).login()

    def tearDown(self):
        super(DocumentsTest, self).tearDown()

    def test_create_doc(self):
        self.bucket = Bucket()
        NavigationHelper(self).navigate('Data Buckets')
        BucketHelper(self).create(self.bucket)
        BucketHelper(self).open_documents(self.bucket)

        doc_name = self.input.param('doc_name', 'test')
        doc_content = self.input.param('content', '{"test" : "test"}')
        doc = Document(doc_name, doc_content)

        DocsHelper(self).create_doc(doc)
        self.assertTrue(DocsHelper(self).get_error() is None, "error appears: %s" \
                         % DocsHelper(self).get_error())

    def test_search_doc(self):
        self.bucket = Bucket()
        NavigationHelper(self).navigate('Data Buckets')
        BucketHelper(self).create(self.bucket)
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
        NavigationHelper(self).navigate('Data Buckets')
        BucketHelper(self).create(self.bucket)
        BucketHelper(self).open_documents(self.bucket)
        old_doc = Document('test', '{"test":"test"}')
        DocsHelper(self).create_doc(old_doc)

        error = self.input.param('error', None)
        doc_name = self.input.param('doc_name', 'test_edited')
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
            DocsHelper(self).verify_doc_in_documents_screen(result_doc)

    def test_edit_doc_from_views_screen(self):
        self.bucket = Bucket()
        NavigationHelper(self).navigate('Data Buckets')
        BucketHelper(self).create(self.bucket)
        BucketHelper(self).open_documents(self.bucket)
        old_doc = Document('test', '{"test":"test"}')
        DocsHelper(self).create_doc(old_doc)

        doc_content = self.input.param('doc_content', '{"test":"edited"}')
        new_doc = Document(old_doc.name, doc_content)

        NavigationHelper(self).navigate('Views')
        view_name = 'test_view_ui'
        DdocViewHelper(self).create_view(view_name, view_name)
        DdocViewHelper(self).open_view(view_name)
        self.assertEquals(DdocViewHelper(self).get_random_doc_name(), old_doc.name,
                          "There is only one document %s, but views screen shows %s" %\
                          (old_doc, DdocViewHelper(self).get_random_doc_name()))
        DdocViewHelper(self).click_edit_doc()

        DocsHelper(self).fill_edit_doc_screen(new_doc)
        DocsHelper(self).verify_doc_in_documents_screen(new_doc)

    def test_pagination_docs(self):
        self.bucket = Bucket()
        NavigationHelper(self).navigate('Data Buckets')
        BucketHelper(self).create(self.bucket)
        BucketHelper(self).open_documents(self.bucket)

        items_per_page = self.input.param('items-per-page', 5)
        num_docs = self.input.param('num-docs', 10)
        doc_name = self.input.param('doc_name', 'test')
        doc_content = self.input.param('content', '{"test" : "test"}')
        num_pages = int(num_docs/items_per_page)

        DocsHelper(self).select_docs_per_page('100')
        for i in xrange(num_docs):
            doc = Document(doc_name + str(i), doc_content)
            DocsHelper(self).create_doc(doc)
            self.assertTrue(DocsHelper(self).get_error() is None, "error appears: %s" \
                            % DocsHelper(self).get_error())

        DocsHelper(self).select_docs_per_page(str(items_per_page))
        self.assertEquals(num_pages,  DocsHelper(self).get_total_pages_num(),
                          "Total number of pages should be %s, actual is %s" %\
                          (num_pages, DocsHelper(self).get_total_pages_num()))

        self.log.info("total number of pages is %s as expected" % num_pages)

        for page in xrange(1, num_pages + 1):
            self.assertTrue(items_per_page >= DocsHelper(self).get_rows_count(),
                            "Items number per page is incorrect %s, expected %s" %\
                            (DocsHelper(self).get_rows_count(), items_per_page))
            self.log.info("Page has correct number of itemes: %s" %\
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
        SettingsHelper(self).fill_alerts_info(self.input)
        NavigationHelper(self).navigate('Server Nodes')
        ServerHelper(self).add(self.input)
        ServerHelper(self).rebalance()
        NavigationHelper(self).navigate('Settings')
        SettingsHelper(self).navigate('Auto-Failover')
        SettingsHelper(self).fill_auto_failover_info(self.input)
        time.sleep(self.input.param("auto_failover_timeout", 40))
        time.sleep(10)

    def test_add_sample(self):
        sample = self.input.param('sample', 'beer-sample')
        num_expected = self.input.param('num_items', 7303)
        self.helper.login()
        NavigationHelper(self).navigate('Settings')
        SettingsHelper(self).navigate('Sample Buckets')
        sample_bucket = SettingsHelper(self).select_sample_bucket(sample)
        NavigationHelper(self).navigate('Data Buckets')
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
            NavigationHelper(self).navigate('Data Buckets')
            BucketHelper(self).create(self.bucket)
            NavigationHelper(self).navigate('Views')
            self.view_name = 'test_view_ui'
            DdocViewHelper(self).create_view(self.view_name, self.view_name)

    def tearDown(self):
        RestConnection(self.servers[0]).delete_ro_user()
        super(ROuserTests, self).tearDown()

    def test_read_only_user(self):
        username = self.input.param('username', 'myrouser')
        password = self.input.param('password', 'myropass')

        NavigationHelper(self).navigate('Settings')
        SettingsHelper(self).navigate('Account Management')
        SettingsHelper(self).create_user(username, password)
        self.log.info("Login with just created user")
        self.helper.logout()
        self.helper.login(user=username, password=password)
        self.verify_read_only(self.bucket, self.view_name)

    def test_delete_read_only_user(self):
        username = self.input.param('username', 'myrouser')
        password = self.input.param('password', 'myropass')
        time.sleep(2)
        NavigationHelper(self).navigate('Settings')
        SettingsHelper(self).navigate('Account Management')
        SettingsHelper(self).create_user(username, password)
        SettingsHelper(self).delete_user()

        self.helper.logout()
        self.helper.login(user=username, password=password)
        time.sleep(3)
        self.assertTrue(self.helper.controls.error.is_displayed(), "Able to login")
        self.log.info("Unable to login as expected. %s" % self.helper.controls.error.get_text())

    def test_negative_read_only_user(self):
        username = self.input.param('username', 'myrouser')
        password = self.input.param('password', 'myropass')
        verify_password = self.input.param('verify_password', None)
        error = self.input.param('error', '')
        time.sleep(2)
        NavigationHelper(self).navigate('Settings')
        SettingsHelper(self).navigate('Account Management')
        try:
            SettingsHelper(self).create_user(username, password, verify_password)
        except Exception, ex:
            self.assertTrue(str(ex).find(error) != -1, "Error message is incorrect. Expected %s, actual %s" % (error, str(ex)))
        else:
            self.fail("Error %s expected but not appeared" % error)

    def verify_read_only(self, bucket, view):
        navigator = NavigationHelper(self)
        self.log.info("Servers check")
        navigator.navigate('Server Nodes')
        for btn in ServerHelper(self).controls.server_row_controls().failover_btns:
            self.assertFalse(btn.is_displayed(), "There is failover btn")
        for btn in ServerHelper(self).controls.server_row_controls().remove_btns:
            self.assertFalse(btn.is_displayed(), "There is remove btn")
        self.log.info("Bucket check")
        navigator.navigate('Data Buckets')
        BucketHelper(self).controls.bucket_info(bucket.name).arrow.click()
        self.assertFalse(BucketHelper(self).controls.edit_btn().is_displayed(),
                         "Bucket can be edited")
        self.log.info("Views check")
        navigator.navigate('Views')
        DdocViewHelper(self).open_view(view)
        self.assertTrue(DdocViewHelper(self).controls.view_map_reduce_fn().map_fn.get_attribute("class").find("read_only") != -1,
                        "Can edit map fn")
        self.assertTrue(DdocViewHelper(self).controls.view_map_reduce_fn().reduce_fn.get_attribute("class").find("read_only") != -1,
                        "Can edit reduce fn")

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
class ServerTestControls():
    def __init__(self, driver):
        self.helper = ControlsHelper(driver)
        self.add_server_btn = self.helper.find_control('server_nodes', 'add_server_btn')
        self.rebalance_btn = self.helper.find_control('server_nodes', 'rebalance_btn')
        self.num_pend_rebalance = self.helper.find_control('server_nodes', 'num_pend_rebalance',
                                                           parent_locator='pend_rebalance_btn')

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
        return self

    def server_row_controls(self):
        self.failover_btns = self.helper.find_controls('server_nodes','failover_btn')
        self.remove_btns = self.helper.find_controls('server_nodes','remove_btn')
        return self

class BucketTestsControls():
    def __init__(self, driver):
        self.helper = ControlsHelper(driver)
        self.create_bucket_btn = self.helper.find_control('bucket','new_create_btn')

    def bucket_pop_up(self, parent='create_bucket_pop_up'):
        self.parent = parent
        self.create_bucket_pop_up = self.helper.find_control('bucket','create_bucket_pop_up')
        self.name = self.helper.find_control('bucket','name', 'create_bucket_pop_up')
        self.ram_quota = self.helper.find_control('bucket','ram_quota', parent_locator=self.parent)
        self.standart_port_radio = self.helper.find_control('bucket','standart_port_radio',
                                                            parent_locator=self.parent)
        self.dedicated_port_radio = self.helper.find_control('bucket','dedicated_port_radio',
                                                             parent_locator=self.parent)
        self.sasl_password = self.helper.find_control('bucket','sasl_password',
                                                      parent_locator=self.parent)
        self.port = self.helper.find_control('bucket','port', parent_locator=self.parent)
        self.enable_replica_cb = self.helper.find_control('bucket','enable_replica_cb',
                                                          parent_locator=self.parent)
        self.replica_num = self.helper.find_control('bucket','replica_num', parent_locator=self.parent)
        self.index_replica_cb = self.helper.find_control('bucket','index_replica_cb',
                                                         parent_locator=self.parent)
        self.create_btn = self.helper.find_control('bucket','create_btn',
                                                   parent_locator='create_bucket_pop_up')
        return self

    def bucket_info(self, bucket_name):
        self.arrow = self.helper.find_control('bucket_row','arrow', parent_locator='bucket_row',
                                               text=bucket_name)
        self.name = self.helper.find_control('bucket_row','name', parent_locator='bucket_row',
                                              text=bucket_name)
        self.nodes = self.helper.find_control('bucket_row','nodes', parent_locator='bucket_row',
                                               text=bucket_name)
        self.items_count = self.helper.find_control('bucket_row','items_count',
                                                    parent_locator='bucket_row', text=bucket_name)
        self.documents = self.helper.find_control('bucket_row','documents',
                                                  parent_locator='bucket_row', text=bucket_name)
        self.views = self.helper.find_control('bucket_row','views', parent_locator='bucket_row',
                                               text=bucket_name)
        self.health = self.helper.find_first_visible('bucket_row', 'health',
                                                     parent_locator='bucket_row',
                                                     text=bucket_name)
        return self

    def type(self, type):
        return self.helper.find_control('bucket','type', parent_locator='create_bucket_pop_up', text=type)

    def warning_pop_up(self, text):
        return self.helper.find_control('errors', 'warning_pop_up', text=text)

    def edit_btn(self):
        return self.helper.find_control('bucket','edit_btn')

    def bucket_stats(self, stat):
        return self.helper.find_control('bucket_stats', 'value_stat', text=stat)

    def bucket_stat_view_block(self):
        return self.helper.find_control('bucket_stats','view_stats_block')

    def bucket_stat_from_view_block(self, stat):
        return self.helper.find_control('bucket_stats','value_stat', parent_locator='view_stats_block', text=stat)

class NodeInitializeControls():
    def __init__(self, driver):
        self.helper = ControlsHelper(driver)
        self.setup_btn = self.helper.find_control('initialize', 'setup_btn')

    def errors(self):
        self.error_inline = self.helper.find_controls('initialize', 'errors')
        self.error_warn = self.helper.find_control('errors', 'warning_pop_up', text='Error')
        return self

    def main_page(self):
        return self.helper.find_control('initialize', 'main_page')

    def step_screen(self):
        self.current_step = self.helper.find_first_visible('initialize', 'current_step')
        self.next_btn = self.helper.find_first_visible('initialize', 'next_btn')
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
        return self.helper.find_control('step_2','sample', text=sample)

    def step_4(self):
        self.enable_updates = self.helper.find_control('step_4', 'enable_updates')
        self.email = self.helper.find_control('step_4', 'email')
        self.first_name = self.helper.find_control('step_4', 'first_name')
        self.last_name = self.helper.find_control('step_4', 'last_name')
        self.company = self.helper.find_control('step_4', 'company')
        self.agree_terms = self.helper.find_control('step_4', 'agree_terms')
        return self

    def step_5(self):
        self.password_confirm = self.helper.find_control('step_5', 'password_confirm')
        self.user = self.helper.find_control('step_5', 'user')
        self.password = self.helper.find_control('step_5', 'pass')
        return self

class DdocViewControls():
    def __init__(self, driver):
        self.helper = ControlsHelper(driver)
        self.create_view_btn = self.helper.find_control('views_screen', 'create_view_btn')

    def views_screen(self, text=''):
        return self.helper.find_controls('views_screen', 'views_tab', text=text)

    def create_pop_up(self):
        self.pop_up = self.helper.find_control('create_pop_up', 'pop_up')
        self.ddoc_name = self.helper.find_control('create_pop_up', 'ddoc_name',
                                                  parent_locator='pop_up')
        self.view_name = self.helper.find_control('create_pop_up', 'view_name',
                                                  parent_locator='pop_up')
        self.save_btn = self.helper.find_control('create_pop_up', 'save_btn',
                                                  parent_locator='pop_up')
        return self

    def view_row(self, view=''):
        self.row = self.helper.find_control('view_row', 'row', text=view)
        self.name = self.helper.find_control('view_row', 'name', text=view,
                                             parent_locator='row')
        return self

    def view_screen(self):
        self.random_document = self.helper.find_control('view_screen', 'random_doc')
        self.random_doc_name = self.helper.find_control('view_screen', 'random_doc_name',
                                                        parent_locator='random_doc')
        self.random_doc_btn = self.helper.find_control('view_screen', 'random_doc_btn',
                                                        parent_locator='random_doc')
        self.random_doc_content = self.helper.find_control('view_screen', 'random_doc_content',
                                                        parent_locator='random_doc')
        self.random_doc_meta = self.helper.find_control('view_screen', 'random_doc_meta',
                                                        parent_locator='random_doc')
        self.random_doc_edit_btn = self.helper.find_control('view_screen', 'random_doc_edit_btn',
                                                        parent_locator='random_doc')
        return self

    def view_map_reduce_fn(self):
        self.map_fn = self.helper.find_control('view_screen', 'map_fn')
        self.reduce_fn = self.helper.find_control('view_screen', 'reduce_fn')
        return self

class DocumentsControls():
    def __init__(self, driver):
        self.helper = ControlsHelper(driver)
        self.documents_screen = self.helper.find_control('docs_screen', 'screen')
        self.create_doc = self.helper.find_control('docs_screen', 'create_doc_btn')
        self.lookup_input = self.helper.find_control('docs_screen', 'lookup_input')
        self.lookup_btn = self.helper.find_control('docs_screen', 'lookup_btn')

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
                                                      parent_locator='screen')
        self.delete_btn = self.helper.find_control('edit_doc_screen', 'delete_btn',
                                                    parent_locator='screen')
        self.save_btn = self.helper.find_control('edit_doc_screen', 'save_btn',
                                                  parent_locator='screen')
        self.save_as_btn = self.helper.find_control('edit_doc_screen', 'save_as_btn',
                                                     parent_locator='screen')
        self.documents_link = self.helper.find_control('edit_doc_screen', 'documents_link')
        return self

class SettingsTestControls():
    def __init__(self, driver):
        self.helper = ControlsHelper(driver)

    def _settings_tab_link(self, text):
        return self.helper.find_control('settings', 'settings_tab_link',
                                        parent_locator='settings_bar',
                                        text=text)

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

    def user_create_info(self):
        self.username = self.helper.find_control('user', 'username')
        self.password = self.helper.find_control('user', 'password')
        self.verify_password = self.helper.find_control('user', 'verify_password')
        self.create_btn = self.helper.find_control('user', 'create_btn')
        self.delete_btn = self.helper.find_control('user', 'delete_btn')
        return self

    def user_error_msgs(self):
        return self.helper.find_controls('user', 'error_msg')

    def confirmation_user_delete(self):
        self.confirmation_dlg = self.helper.find_control('confirm_delete_ro','dlg')
        self.delete_btn  = self.helper.find_control('confirm_delete_ro','confirm_btn', parent_locator='dlg')
        return self

    def samples_buckets(self, bucket=''):
        self.sample_cb = self.helper.find_control('sample_buckets', 'sample_cb', text=bucket)
        self.installed_sample = self.helper.find_control('sample_buckets', 'installed_sample', text=bucket)
        self.save_btn = self.helper.find_control('sample_buckets','save_btn')
        self.error_msg = self.helper.find_controls('sample_buckets','error')
        return self

'''
Helpers
'''
class NavigationHelper():
    def __init__(self, tc):
        self.tc = tc
        self.controls = NavigationTestControls(tc.driver)
        self.wait = WebDriverWait(tc.driver, timeout=10)


    def _is_tab_selected(self, text):
        return self.controls._navigation_tab(text).get_attribute('class') \
                                                    .find('currentNav') > -1

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

class ServerHelper():
    def __init__(self, tc):
        self.tc = tc
        self.wait = WebDriverWait(tc.driver, timeout=25)
        self.controls = ServerTestControls(tc.driver)

    def _is_btn_enabled(self, btn):
        return btn.get_attribute('class').find('disabled') == -1

    def add(self, input):
        self.tc.log.info("trying add server %s" % (input.param("add_server_ip", "10.1.3.72:8091")))
        self.wait.until(lambda fn: self.controls.add_server_btn.is_displayed(),
                        "Add Server btn is not displayed in %d sec" % (self.wait._timeout))
        self.controls.add_server_btn.click()
        self.wait.until(lambda fn: self.controls.add_server_dialog().add_server_pop_up.is_displayed(),
                        "no reaction for click create new bucket btn in %d sec" % (self.wait._timeout))
        self.fill_server_info(input)
        self.controls.add_server_dialog().add_server_dialog_btn.click()
        self.controls.add_server_dialog().add_server_confirm_btn.click()
        self.wait.until_not(lambda fn:
                            self.controls.add_server_dialog().confirm_server_addition.is_displayed(),
                            "Add server pop up is not closed in %d sec" % self.wait._timeout)
        self.wait.until_not(lambda fn:
                            self.controls.add_server_dialog().add_server_pop_up.is_displayed(),
                            "Add server pop up is not closed in %d sec" % self.wait._timeout)
        self.tc.log.info("added server %s" % (input.param("add_server_ip", "10.1.3.72:8091")))

    def fill_server_info(self, input):
        self.controls.add_server_dialog().ip_address.type(input.param("add_server_ip", "10.1.3.72:8091"))
        self.controls.add_server_dialog().username.type(input.membase_settings.rest_username)
        self.controls.add_server_dialog().password.type(input.membase_settings.rest_password)

    def rebalance(self):
        self.wait.until(lambda fn: self.controls.num_pend_rebalance.is_displayed(),
                        "Number of pending rebalance servers is not displayed in %d sec" % (self.wait._timeout))
        self.wait.until(lambda fn: self._is_btn_enabled(self.controls.rebalance_btn),
                        "Rebalance btn is not enabled in %d sec" % (self.wait._timeout))
        self.controls.rebalance_btn.click()
        self.tc.log.info("Start rebalancing")
        self.wait.until_not(lambda fn: self._is_btn_enabled(self.controls.rebalance_btn),
                            "Rebalance btn is enabled in %d sec" % (self.wait._timeout))
        time.sleep(5)

class BucketHelper():
    def __init__(self, tc):
        self.tc = tc
        self.wait = WebDriverWait(tc.driver, timeout=25)
        self.controls = BucketTestsControls(tc.driver)

    def create(self, bucket):
        self.tc.log.info("trying create bucket '%s' with options %s" % (bucket.name, bucket))
        self.controls.create_bucket_btn.click()
        self.wait.until(lambda fn:
                        self.controls.bucket_pop_up().create_bucket_pop_up.is_displayed() or \
                        self.controls.warning_pop_up('Memory Fully Allocated').is_displayed(),
                        "no reaction for click create new bucket btn in %d sec"
                        % self.wait._timeout)
        self.tc.assertFalse(self.controls.warning_pop_up('Memory Fully Allocated').is_displayed(),
                            "Warning 'Cluster Memory Fully Allocated' appeared")
        self.fill_bucket_info(bucket)
        self.controls.bucket_pop_up().create_btn.click()
        self.tc.log.info("created bucket '%s'" % bucket.name)
        self.wait.until_not(lambda fn:
                            self.controls.bucket_pop_up().create_bucket_pop_up.is_displayed(),
                            "create new bucket pop up is not closed in %d sec" % self.wait._timeout)
        self.wait.until(lambda fn: self.is_bucket_present(bucket),
                         "Bucket '%s' is not displayed" % bucket)
        self.tc.log.info("bucket '%s' is displayed" % bucket)
        self.wait.until(lambda fn: self.is_bucket_helthy(bucket),
                        "Bucket '%s' is not  in healthy state" % bucket)

    def fill_bucket_info(self, bucket, parent='create_bucket_pop_up'):
        if not parent == 'initialize_step':
            self.controls.bucket_pop_up(parent).name.type(bucket.name)
        if bucket.type:
            self.controls.bucket_pop_up(parent).type(bucket.type).click()
        self.controls.bucket_pop_up(parent).ram_quota.type(bucket.ram_quota)
        if bucket.sasl_password:
            self.controls.bucket_pop_up().standart_port_radio.click()
            self.controls.bucket_pop_up().sasl_password.type(bucket.sasl_password)
        if bucket.protocol_port:
            self.controls.bucket_pop_up().dedicated_port_radio.click()
            self.controls.bucket_pop_up().port.type(bucket.protocol_port)
        if bucket.num_replica:
            if bucket.num_replica == 0:
                self.controls.bucket_pop_up(parent).enable_replica_cb.check(setTrue=False)
            else:
                self.controls.bucket_pop_up(parent).enable_replica_cb.check()
                self.controls.bucket_pop_up(parent).replica_num.select(bucket.num_replica)
        if bucket.index_replica is not None:
            self.controls.bucket_pop_up(parent).index_replica_cb.check(setTrue=bucket.index_replica)

    def is_bucket_present(self, bucket):
        try:
            bucket_present = self.controls.bucket_info(bucket.name).name.is_displayed()
            bucket_present &= self.controls.bucket_info(bucket.name).arrow.is_displayed()
            bucket_present &= self.controls.bucket_info(bucket.name).nodes.is_displayed()
            bucket_present &= self.controls.bucket_info(bucket.name).items_count.is_displayed()
            bucket_present &= self.controls.bucket_info(bucket.name).documents.is_displayed()
            bucket_present &= self.controls.bucket_info(bucket.name).views.is_displayed()
            bucket_present &= self.controls.bucket_info(bucket.name).health.is_displayed()
            return bucket_present
        except:
            time.sleep(1)
            return False

    def is_bucket_helthy(self, bucket):
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
        self.controls.bucket_info(bucket.name).documents.click()

    def open_stats(self, bucket):
        self.controls.bucket_info(bucket.name).name.click()
        self.tc.log.info("Stats page is opened")

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
                            self.controls.bucket_stats(stat).is_displayed(),
                            "stat %s is not displayed in %d sec" % (
                                                  stat, self.wait._timeout))
            return self.controls.bucket_stats(stat).get_text()
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
        self.wait = WebDriverWait(tc.driver, timeout=60)
        self.controls = NodeInitializeControls(tc.driver)

    def _get_error(self):
        error_text = ''
        for error in self.controls.errors().error_inline:
            if error.is_displayed():
                if not error.get_text():
                    time.sleep(1)
                error_text += error.get_text()
        if self.controls.errors().error_warn.is_displayed():
            error_text += self.controls.errors().error_warn.get_text()
        return error_text

    def _go_next_step(self, last_step=False):
        step = self._get_current_step_num()
        self.tc.log.info("try to open next step. Now you are on %s" % step)
        self.controls.step_screen().next_btn.click()
        if last_step:
            self.wait.until(lambda fn: NavigationHelper(self.tc)._is_tab_selected('Cluster Overview'),
                                "Main page is not opened")
        else:
            self.wait.until(lambda fn: self._get_current_step_num() == step + 1 or
                                       self._get_error() != '',
                            "no reaction for clicking next btn")
        if self._get_error():
            raise Exception("error '%s' appears" % self._get_error())
        self.tc.log.info("Next step screen is opened")

    def _get_current_step_num(self):
        if self.controls.step_screen().current_step is None:
            return 0
        if self.controls.step_screen().current_step.get_text():
            return int(self.controls.step_screen().current_step.get_text())
        else:
            return 0

    '''
    Following params from test input will be processed:
    db_path
    indeces_path
    ram_quota  - ram quota for starting new cluster
    user_cluster - user for join cluster
    password_cluster - password for user joining cluster
    ip_cluster - ip for joining cluster
    '''
    def _fill_1_step(self, input):
        if input.param("db_path",None):
            self.controls.step_1().db_path.type(input.param("db_path",None))
        if input.param("indeces_path",None):
            self.controls.step_1().indeces_path.type(input.param("indeces_path",None))
        if input.param("ram_quota_node",None):
            self.controls.step_1().new_cluster_cb.click()
            self.controls.step_1().ram_quota.type(input.param("ram_quota_node",None))
        if input.param("user_cluster",None) or input.param("password_cluster",None) \
                                            or input.param("ip_cluster",None):
            self.controls.step_1().join_cluster.click()
            self.controls.step_1().user_cluster.type(input.param("user_cluster",None))
            self.controls.step_1().password_cluster.type(input.param("password_cluster",None))
            self.controls.step_1().ip_cluster.type(input.param("ip_cluster",None))

    def _fill_2_step(self, input):
        if input.param("sample", None):
            self.controls.step_2_sample(input.param("sample", None)).check()
            #TODO successfull loading?

    def _fill_3_step(self, input):
        BucketHelper(self.tc).fill_bucket_info(Bucket(parse_bucket=input),
                                               parent='initialize_step')

    def _fill_4_step(self, input):
        if input.param("enable_updates", None) is not None:
            self.controls.step_4().enable_updates.check(setTrue=input.param("enable_updates", None))
        self.controls.step_4().email.type(input.param("email", None))
        self.controls.step_4().first_name.type(input.param("first_name", None))
        self.controls.step_4().last_name.type(input.param("last_name", None))
        self.controls.step_4().company.type(input.param("company", None))
        if input.param("agree_terms", None) is not None:
            if self.controls.step_4().agree_terms.is_displayed():
                self.controls.step_4().agree_terms.check(setTrue=input.param("agree_terms", None))
            else:
                self.tc.log.info("This version of application doesn't contain agree checkbox(step 4)")

    def _fill_5_step(self, input):
        self.controls.step_5().password_confirm.type(input.membase_settings.rest_password)
        self.controls.step_5().user.type(input.membase_settings.rest_username)
        self.controls.step_5().password.type(input.membase_settings.rest_password)

    def initialize(self, input):
        self.tc.log.info('Starting initializing node')
        self.controls.setup_btn.click()
        self.wait.until(lambda fn: self._get_current_step_num() == 1, "first step screen is not opened")
        for i in xrange(1,6):
            self.tc.log.info('Filling step %d ...' % i)
            getattr(self, '_fill_{0}_step'.format(i))(input)
            self.tc.log.info('Step %d filled in' % i)
            if i == 5:
                self._go_next_step(last_step=True)
            else:
                self._go_next_step()

class DdocViewHelper():
    def __init__(self, tc):
        self.tc = tc
        self.wait = WebDriverWait(tc.driver, timeout=60)
        self.controls = DdocViewControls(tc.driver)

    def create_view(self, ddoc_name, view_name):
        self.tc.log.info('trying create a view %s' % view_name)
        self.controls.create_view_btn.click()
        self.wait.until(lambda fn:
                        self.controls.create_pop_up().ddoc_name.is_displayed(),
                        "Create pop up bucket is not opened")
        self.controls.create_pop_up().ddoc_name.type(ddoc_name)
        self.controls.create_pop_up().view_name.type(view_name)
        self.controls.create_pop_up().save_btn.click()
        self.wait.until(lambda fn:
                        self.is_view_present(view_name),
                        "view %s is not appeared" % view_name)

    def is_view_present(self, view_name):
        return self.controls.create_pop_up().view_row(view_name).row.is_displayed()

    def open_view(self, view_name):
        self.tc.log.info('trying open view %s' % view_name)
        self.controls.create_pop_up().view_row(view_name).name.click()
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

class DocsHelper():
    def __init__(self, tc):
        self.tc = tc
        self.wait = WebDriverWait(tc.driver, timeout=60)
        self.controls = DocumentsControls(tc.driver)

    def create_doc(self, doc):
        self.tc.log.info('trying create a doc %s' % doc.name)
        self.wait.until(lambda fn:
                        self.controls.create_doc.is_displayed())
        self.controls.create_doc.click()
        self.fill_create_doc_pop_up(doc.name)
        self.wait.until(lambda fn:
                        self.controls.edit_document_screen().content is not None and \
                        self.controls.edit_document_screen().content.is_displayed(),
                        "edit doc screen didn't appeared")
        self.tc.log.info("edit doc screen appeared")
        self.fill_edit_doc_screen(doc)
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

    def fill_edit_doc_screen(self, doc, action = 'save'):
        self.tc.log.info('trying to edit doc %s' % doc)
        if doc.content:
            self.controls.edit_document_screen().content.type_native(doc.content)
            if self.get_error():
                raise Exception("Error '%s' appeared" % self.get_error())
        if action == 'save':
            self.controls.edit_document_screen().save_btn.click()
            self.wait.until(lambda fn:
                            self.controls.edit_document_screen()\
                            .save_btn.get_attribute('class').find('disabled') > -1,
                            "Doc %s is not saved" % doc)
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
        self.controls.edit_document_screen().documents_link.click()
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
            opened &= (re.sub(r'\s', '', self.controls.edit_document_screen().content.get_text()) ==
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
        self.tc.assertTrue(self.get_error() is None, "error appears: %s" \
                         % self.get_error())
        self.controls.lookup_btn.click()
        self.wait.until(lambda fn:
                        self.controls.edit_document_screen(doc=doc.name).name.is_displayed(),
                        "Doc %s is not found" % doc.name)

class SettingsHelper():
    def __init__(self, tc):
        self.tc = tc
        self.controls = SettingsTestControls(tc.driver)
        self.wait = WebDriverWait(tc.driver, timeout=10)

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
        self.controls.alerts_info().email_pass.type(input.param("alerts_email_password", None))
        self.controls.alerts_info().email_encrypt.check(setTrue=input.param("alerts_email_encrypt", True))
        self.controls.alerts_info().email_sender.type(input.param("alerts_email_sender", 'qa@couchbase.com'))
        self.controls.alerts_info().email_recipients.type(input.param("alerts_email_recipients", 'chisheng@couchbase.com'))
        self.wait.until(lambda fn: self.controls.alerts_info().test_email_btn.is_displayed(),
                        "Test Mail btn is not displayed in %d sec" % (self.wait._timeout))
        self.controls.alerts_info().test_email_btn.click()
        #        self.wait.until(lambda fn: self.controls.alerts_info().sent_email_btn.is_displayed(),
        #           "Test Mail btn is not selected in %d sec" % (self.wait._timeout))
        self.tc.log.info("Test Mail btn is selected")

        self.wait.until(lambda fn: self.controls.alerts_info().save_btn.is_displayed(),
                        "Save btn is not displayed in %d sec" % (self.wait._timeout))
        self.controls.alerts_info().save_btn.click()
        self.wait.until(lambda fn: self.controls.alerts_info().done_btn.is_displayed() or
                        (self.controls.alerts_info().save_btn.is_displayed() and\
                         self.controls.alerts_info().save_btn.get_attribute('disabled') == 'true'),
                        "Save btn is not selected in %d sec" % (self.wait._timeout))
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
        self.wait.until(lambda fn: self.controls.auto_failover_info().done_btn.is_displayed() or
                        (self.controls.auto_failover_info().save_btn.is_displayed() and\
                         self.controls.auto_failover_info().save_btn.get_attribute('disabled') == 'true'),
                        "Save btn is not selected in %d sec" % (self.wait._timeout))
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

    def is_user_created(self):
        return self.controls.user_create_info().delete_btn.is_displayed()

    def is_error_present(self):
        if self.get_error_msg():
            return True
        return False

    def get_error_msg(self):
        msgs = []
        for control in self.controls.user_error_msgs():
            if control.is_displayed() and control.get_text() != '':
                msgs.append(control.get_text())
        return msgs

    def delete_user(self):
        self.tc.log.info("Delete RO user")
        self.controls.user_create_info().delete_btn.click()
        self.wait.until(lambda fn: self.controls.confirmation_user_delete().delete_btn.is_displayed(),
                        "Confirmation pop up didn't appear in %d sec" % (self.wait._timeout))
        self.controls.confirmation_user_delete().delete_btn.click()
        self.wait.until(lambda fn: self.controls.user_create_info().username.is_displayed(),
                        "Username is not displayed in %d sec" % (self.wait._timeout))
        self.tc.log.info("RO user is deleted")

    def create_user(self, user, pwd, verify_pwd = None):
        if verify_pwd is None:
            verify_pwd = pwd
        self.tc.log.info("Try to create user %s" % user)
        self.wait.until(lambda fn: self.controls.user_create_info().username.is_displayed(),
                        "Username is not displayed in %d sec" % (self.wait._timeout))
        self.controls.user_create_info().username.type(user)
        self.controls.user_create_info().password.type(pwd)
        self.controls.user_create_info().verify_password.type(verify_pwd)
        self.controls.user_create_info().create_btn.click()
        self.wait.until(lambda fn: self.is_user_created() or self.is_error_present(),
                        "No reaction for create btn in %d sec" % (self.wait._timeout))
        if self.is_error_present():
            error = self.get_error_msg()
            self.tc.log.error("User %s not created. error %s" % (user, error))
            raise Exception("ERROR while creating user: %s" % error)
        self.tc.log.info("User %s created" % user)
'''
Objects
'''
class Bucket():
    def __init__(self, name='default', type=None, ram_quota=None, sasl_pwd=None,
                 port=None, replica=None, index_replica=None, parse_bucket=None):
        self.name = name or 'default'
        self.type = type
        self.ram_quota = ram_quota
        self.sasl_password = sasl_pwd
        self.protocol_port = port
        self.num_replica = replica
        self.index_replica = index_replica
        if parse_bucket:
            for param in parse_bucket.test_params:
                if hasattr(self, str(param)):
                   setattr(self, str(param),parse_bucket.test_params[param])
    def __str__(self):
        return '<Bucket: name={0}, type={1}, ram_quota={2}>'.format(self.name,
                                                                    self.type, self.ram_quota)

class Document():
    def __init__(self, name, content=None, bucket='default'):
        self.name = name
        self.content = content
        self.bucket = bucket

    def __str__(self):
        return '<Document: name={0}, content={1}'.format(self.name, self.content)