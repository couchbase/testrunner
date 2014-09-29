import logger
import time
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from couchbase.cluster import Cluster
from uibasetest import *
from uisampletests import Bucket, NavigationHelper, BucketHelper


class XDCRTests(BaseUITestCase):
    def setUp(self):
        super(XDCRTests, self).setUp()
        self.bucket = Bucket()
        self._initialize_nodes()
        RestConnection(self.servers[0]).create_bucket(bucket='default', ramQuotaMB=500)
        RestConnection(self.servers[1]).create_bucket(bucket='default', ramQuotaMB=500)
        self.driver.refresh()
        helper = BaseHelper(self)
        helper.login()

    def tearDown(self):
        super(XDCRTests, self).tearDown()
        if hasattr(self, 'driver') and self.driver:
            self._deinitialize_api()

    def _deinitialize_api(self):
        for server in self.servers:
            try:
                rest = RestConnection(server)
                rest.force_eject_node()
                time.sleep(10)
            except BaseException, e:
                self.fail(e)

    def _initialize_nodes(self):
        for server in self.servers:
            RestConnection(server).init_cluster(self.input.membase_settings.rest_username,
                                                self.input.membase_settings.rest_password, '8091')

    def test_create_reference(self):
        self.assertTrue(len(self.servers) > 1, 'This test requires at least 2 nodes')
        ip = self.input.param('ip_to_replicate', self.servers[1].ip)
        name = self.input.param('name', 'ui_auto')
        user = self.input.param('user', self.input.membase_settings.rest_username)
        passwd = self.input.param('passwd', self.input.membase_settings.rest_password)
        error = self.input.param('error', None)
        helper = XDCRHelper(self)
        NavigationHelper(self).navigate('XDCR')
        try:
            helper.create_cluster_reference(name, ip, user, passwd)
        except Exception, ex:
            self.log.error(str(ex))
            if error:
                self.assertTrue(str(ex).find(error) != -1, 'Error is not expected')
            else:
                raise ex
        else:
            if error:
                raise Exception('Error <%s> was expected' % error)
        self.assertEqual(helper.is_cluster_reference_created(name, ip), not error,
                         'Reference should %s appear' % (('', 'not')[error is None]))
        self.log.info('Test finished as expected')

    def test_create_replication(self):
        self.assertTrue(len(self.servers) > 1, 'This test requires at least 2 nodes')
        ip = self.input.param('ip_to_replicate', self.servers[1].ip)
        name = self.input.param('name', 'ui_auto')
        user = self.input.param('user', self.input.membase_settings.rest_username)
        passwd = self.input.param('passwd', self.input.membase_settings.rest_password)
        src_bucket = self.input.param('src_bucket', self.bucket)
        dest_bucket = self.input.param('dest_bucket', self.bucket)
        dest_cluster = self.input.param('dest_cluster', name)
        error = self.input.param('error', None)
        helper = XDCRHelper(self)
        NavigationHelper(self).navigate('XDCR')
        helper.create_cluster_reference(name, ip, user, passwd)
        try:
            helper.create_replication(dest_cluster, src_bucket, dest_bucket)
        except Exception, ex:
            self.log.error(str(ex))
            if error:
                self.assertTrue(str(ex).find(error) != -1, 'Error is not expected')
            else:
                raise ex
        else:
            if error:
                raise Exception('Error <%s> was expected' % error)
        self.assertEqual(helper.is_replication_created(self.bucket.name, name, self.bucket.name), not error,
                         'Reference should %s appear' % (('', 'not')[error is None]))
        self.log.info('Test finished as expected')

    def test_cancel_create_replication(self):
        self.assertTrue(len(self.servers) > 1, 'This test requires at least 2 nodes')
        ip = self.input.param('ip_to_replicate', self.servers[1].ip)
        name = self.input.param('name', 'ui_auto')
        user = self.input.param('user', self.input.membase_settings.rest_username)
        passwd = self.input.param('passwd', self.input.membase_settings.rest_password)
        helper = XDCRHelper(self)
        NavigationHelper(self).navigate('XDCR')
        helper.create_cluster_reference(name, ip, user, passwd, cancel=True)
        self.assertFalse(helper.is_cluster_reference_created(name, ip),
                         'Reference should not appear')
        self.log.info('Test finished as expected')

    def test_cancel_create_reference(self):
        self.assertTrue(len(self.servers) > 1, 'This test requires at least 2 nodes')
        ip = self.input.param('ip_to_replicate', self.servers[1].ip)
        name = self.input.param('name', 'ui_auto')
        user = self.input.param('user', self.input.membase_settings.rest_username)
        passwd = self.input.param('passwd', self.input.membase_settings.rest_password)
        helper = XDCRHelper(self)
        NavigationHelper(self).navigate('XDCR')
        helper.create_cluster_reference(name, ip, user, passwd)
        helper.create_replication(name, self.bucket.name, self.bucket.name, cancel=True)
        self.assertEqual(helper.is_replication_created(self.bucket.name, ip, self.bucket.name),
                         'Reference should not appear')
        self.log.info('Test finished as expected')

'''
Controls classes for tests
'''

class XDCRControls():
    def __init__(self, driver):
        self.helper = ControlsHelper(driver)
        self.create_cluster_reference_btn = self.helper.find_control('xdcr', 'create_reference_btn')
        self.cluster_references = self.helper.find_control('xdcr', 'references')
        self.create_ongoing_replication_btn = self.helper.find_control('xdcr', 'ongoing_replication_btn')
        self.ongoing_replications = self.helper.find_control('xdcr', 'ongoing_replications')

    def _all_cluster_references(self):
        return self.helper.find_controls('xdcr', 'row_references')

    def _all_ongoing_replications(self):
        return self.helper.find_controls('xdcr', 'row_ongoing_replications')

    def create_reference_pop_up(self):
        self.pop_up = self.helper.find_control('xdcr_create_ref', 'popup')
        self.name = self.helper.find_control('xdcr_create_ref', 'name', parent_locator='popup')
        self.ip = self.helper.find_control('xdcr_create_ref', 'ip', parent_locator='popup')
        self.user = self.helper.find_control('xdcr_create_ref', 'user', parent_locator='popup')
        self.password = self.helper.find_control('xdcr_create_ref', 'password', parent_locator='popup')
        self.create_btn = self.helper.find_control('xdcr_create_ref', 'create_btn', parent_locator='popup')
        self.cancel_btn = self.helper.find_control('xdcr_create_ref', 'cancel_btn', parent_locator='popup')
        return self

    def create_replication_pop_up(self):
        self.pop_up = self.helper.find_control('xdcr_create_repl', 'popup')
        self.cluster = self.helper.find_control('xdcr_create_repl', 'cluster', parent_locator='popup')
        self.bucket = self.helper.find_control('xdcr_create_repl', 'bucket', parent_locator='popup')
        self.remote_cluster = self.helper.find_control('xdcr_create_repl', 'remote_cluster', parent_locator='popup')
        self.remote_bucket = self.helper.find_control('xdcr_create_repl', 'remote_bucket', parent_locator='popup')
        self.replicate_btn = self.helper.find_control('xdcr_create_repl', 'replicate_btn', parent_locator='popup')
        self.cancel_btn = self.helper.find_control('xdcr_create_repl', 'cancel_btn', parent_locator='popup')
        return self

    def error_reference(self):
        return self.helper.find_control('xdcr_create_ref', 'error')

    def error_replica(self):
        return self.helper.find_control('xdcr_create_repl', 'error')


class XDCRHelper():
    def __init__(self, tc):
        self.tc = tc
        self.controls = XDCRControls(tc.driver)
        self.wait = WebDriverWait(tc.driver, timeout=10)

    def create_cluster_reference(self, cluster_name, ip, username, password, cancel=False):
        self.wait.until(lambda fn: self.controls.create_cluster_reference_btn.is_displayed(),
                        "create_cluster_reference_btn is not displayed in %d sec" % (self.wait._timeout))
        time.sleep(3)
        self.tc.log.info("try to create cluster reference with name=%s, ip=%s" % (cluster_name, ip))
        self.controls.create_cluster_reference_btn.click()
        self.wait.until(lambda fn: self.controls.create_reference_pop_up().pop_up.is_displayed(),
                        "create_cluster_reference_ popup is not displayed in %d sec" % (self.wait._timeout))
        if cluster_name:
            self.controls.create_reference_pop_up().name.type(cluster_name)
        if ip:
            self.controls.create_reference_pop_up().ip.type(ip)
        if username:
            self.controls.create_reference_pop_up().user.type(username)
        if password:
            self.controls.create_reference_pop_up().password.type(password)
        if not cancel:
            self.controls.create_reference_pop_up().create_btn.click()
        else:
            self.controls.create_reference_pop_up().cancel_btn.click()
        self.wait.until(lambda fn: self._cluster_reference_pop_up_reaction(),
                        "there is no reaction in %d sec" % (self.wait._timeout))
        if self.controls.error_reference().is_displayed():
            raise Exception('Reference is not created: %s' % self.controls.error_reference().get_text())

    def _cluster_reference_pop_up_reaction(self):
        return (not self.controls.create_reference_pop_up().pop_up.is_displayed()) or self.controls.error_reference().is_displayed()

    def _get_error(self):
        return self.controls.error().get_text()

    def create_replication(self, remote_cluster, bucket, remote_bucket, cancel=False):
        self.wait.until(lambda fn: self.controls.create_ongoing_replication_btn.is_displayed(),
                        "create_cluster_reference_btn is not displayed in %d sec" % (self.wait._timeout))
        self.tc.log.info("try to create cluster replication with cluster=%s, bucket=%s, remote_bucket=%s" % (remote_cluster, bucket, remote_bucket))
        self.controls.create_ongoing_replication_btn.click()
        self.wait.until(lambda fn: self.controls.create_replication_pop_up().pop_up.is_displayed(),
                        "create_cluster_reference_ popup is not displayed in %d sec" % (self.wait._timeout))
        if remote_cluster:
            self.controls.create_replication_pop_up().remote_cluster.select(remote_cluster)
        if remote_bucket:
            self.controls.create_replication_pop_up().remote_bucket.type(remote_bucket.name)
        if bucket:
            self.controls.create_replication_pop_up().bucket.select(bucket.name)
        if not cancel:
            self.controls.create_replication_pop_up().replicate_btn.click()
        else:
            self.controls.create_replication_pop_up.cancel_btn.click()
        self.wait.until(lambda fn: self._cluster_replication_pop_up_reaction(),
                        "there is no reaction in %d sec" % (self.wait._timeout))
        if self.controls.error_replica().is_displayed():
            raise Exception('Reference is not created: %s' % self.controls.error_replica().get_text())

    def _cluster_replication_pop_up_reaction(self):
        return (not self.controls.create_replication_pop_up().pop_up.is_displayed()) or self.controls.error_replica().is_displayed()

    def is_cluster_reference_created(self, name, ip):
        all_ref = self.controls._all_cluster_references()
        for reference in all_ref:
            if reference.get_text().find(name) != -1 and reference.get_text().find(ip) != -1:
                return True
        return False

    def is_replication_created(self, bucket_name, name_remote, bucket_name_remote):
        all_rep = self.controls._all_ongoing_replications()
        for rep in all_rep:
            if rep.get_text().find(bucket_name) != -1 and rep.get_text().find(name_remote) != -1 and rep.get_text().find(bucket_name_remote) != -1:
                return True
        return False