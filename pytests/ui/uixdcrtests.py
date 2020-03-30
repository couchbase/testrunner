from membase.api.rest_client import RestConnection
from .uibasetest import *
from .uisampletests import Bucket, NavigationHelper
from selenium.common.exceptions import StaleElementReferenceException
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper


class XDCRTests(BaseUITestCase):
    def setUp(self):
        super(XDCRTests, self).setUp()
        self.bucket = Bucket()
        self._initialize_nodes()
        self.master = self.servers[0]
        for server in self.servers:
            rest=RestConnection(server)
            cluster_status = rest.cluster_status()
            self.log.info("Initial status of {0} cluster is {1}".format(server.ip,
                                                                        cluster_status['nodes'][0]['status']))
            while cluster_status['nodes'][0]['status'] == 'warmup':
                self.log.info("Waiting for cluster to become healthy")
                self.sleep(5)
                cluster_status = rest.cluster_status()
            self.log.info("current status of {0}  is {1}".format(server.ip,
                                                                 cluster_status['nodes'][0]['status']))
        # Delete all buckets before creating new buckets
        self.log.info("Deleting all existing buckets")
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        self.log.info("Creating new buckets")
        src_bucket = self.input.param('src_bucket', self.bucket)
        dest_bucket = self.input.param('dest_bucket', self.bucket)
        if src_bucket:
            RestConnection(self.servers[0]).create_bucket(bucket='default', ramQuotaMB=500)
        if dest_bucket:
            RestConnection(self.servers[1]).create_bucket(bucket='default', ramQuotaMB=500)
        helper = BaseHelper(self)
        helper.login()


    def tearDown(self):
        super(XDCRTests, self).tearDown()
        if hasattr(self, 'driver') and self.driver:
            self._deinitialize_api()
            self._initialize_nodes()

    def _deinitialize_api(self):
        self.log.info("Cleaning up replications and remote clusters")
        for server in self.servers:
            rest = RestConnection(server)
            rest.remove_all_replications()
            rest.remove_all_remote_clusters()
        self.log.info("Sleeping for 10 seconds after cleaning up replications and remote clusters")
        time.sleep(10)
        ClusterOperationHelper.cleanup_cluster(self.servers, master=self.master)

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
        NavigationHelper(self).navigate('XDCR')
        try:
            XDCRHelper(self).create_cluster_reference(name, ip, user, passwd)
        except Exception as ex:
            self.log.error(str(ex))
            if error:
                self.assertTrue(str(ex).find(error) != -1, 'Error is not expected')
            else:
                raise ex
        else:
            if error:
                raise Exception('Error <%s> was expected' % error)
        self.assertEqual(XDCRHelper(self).is_cluster_reference_created(name, ip), not error,
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
        advanced_settings = self.input.param('advanced_settings', {})
        if advanced_settings:
            adv_s = advanced_settings.split('~')
            advanced_settings = {}
            for setng in adv_s:
                advanced_settings[setng.split(':')[0]] = setng.split(':')[1]
        NavigationHelper(self).navigate('XDCR')
        XDCRHelper(self).create_cluster_reference(name, ip, user, passwd)
        self.sleep(3)
        try:
            XDCRHelper(self).create_replication(dest_cluster, src_bucket, dest_bucket, advanced_settings=advanced_settings)
        except Exception as ex:
            self.log.error(str(ex))
            if error:
                self.assertTrue(str(ex).find(error) != -1, 'Error is not expected')
            else:
                raise ex
        else:
            if error:
                raise Exception('Error <%s> was expected' % error)
        self.assertEqual(XDCRHelper(self).is_replication_created(self.bucket.name, name, self.bucket.name), not error,
                         'Reference should %s appear' % (('', 'not')[error is None]))
        self.log.info('Test finished as expected')

    def test_cancel_create_reference(self):
        self.assertTrue(len(self.servers) > 1, 'This test requires at least 2 nodes')
        ip = self.input.param('ip_to_replicate', self.servers[1].ip)
        name = self.input.param('name', 'ui_auto')
        user = self.input.param('user', self.input.membase_settings.rest_username)
        passwd = self.input.param('passwd', self.input.membase_settings.rest_password)
        NavigationHelper(self).navigate('XDCR')
        XDCRHelper(self).create_cluster_reference(name, ip, user, passwd, cancel=True)
        self.assertFalse(XDCRHelper(self).is_cluster_reference_created(name, ip),
                         'Reference should not appear')
        self.log.info('Test finished as expected')

    def test_cancel_create_replication(self):
        self.assertTrue(len(self.servers) > 1, 'This test requires at least 2 nodes')
        ip = self.input.param('ip_to_replicate', self.servers[1].ip)
        name = self.input.param('name', 'ui_auto')
        user = self.input.param('user', self.input.membase_settings.rest_username)
        passwd = self.input.param('passwd', self.input.membase_settings.rest_password)
        src_bucket = self.input.param('src_bucket', self.bucket)
        dest_bucket = self.input.param('dest_bucket', self.bucket)
        dest_cluster = self.input.param('dest_cluster', name)
        NavigationHelper(self).navigate('XDCR')
        XDCRHelper(self).create_cluster_reference(name, ip, user, passwd)
        XDCRHelper(self).create_replication(dest_cluster, src_bucket, dest_bucket, cancel=True)
        self.assertFalse(XDCRHelper(self).is_replication_created(src_bucket.name, name, dest_bucket.name),
                         'Replication should not appear')
        self.log.info('Test finished as expected')


'''
Controls classes for tests
'''


class XDCRControls:
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
        self.advanced_settings_link = self.helper.find_control('xdcr_create_repl', 'advanced_settings_link', parent_locator='popup')
        return self

    def error_reference(self):
        return self.helper.find_control('xdcr_create_ref', 'error')

    def error_replica(self):
        return self.helper.find_controls('xdcr_create_repl', 'error')

    def advanced_settings(self):
        self.max_replication = self.helper.find_control('xdcr_advanced_settings', 'max_replication')
        self.version = self.helper.find_control('xdcr_advanced_settings', 'version')
        self.source_nozzle_per_node = self.helper.find_control('xdcr_advanced_settings', 'source_nozzle_per_node')
        self.target_nozzle_per_node = self.helper.find_control('xdcr_advanced_settings', 'target_nozzle_per_node')
        self.checkpoint_interval = self.helper.find_control('xdcr_advanced_settings', 'checkpoint_interval')
        self.batch_count = self.helper.find_control('xdcr_advanced_settings', 'batch_count')
        self.batch_size = self.helper.find_control('xdcr_advanced_settings', 'batch_size')
        self.retry_interval = self.helper.find_control('xdcr_advanced_settings', 'retry_interval')
        self.replication_threshold = self.helper.find_control('xdcr_advanced_settings', 'replication_threshold')
        self.collection_interval = self.helper.find_control('xdcr_advanced_settings', 'collection_interval')
        self.logging = self.helper.find_control('xdcr_advanced_settings', 'logging')
        return self

    def errors_advanced_settings(self):
        return self.helper.find_controls('xdcr_advanced_settings', 'error')


class XDCRHelper:
    def __init__(self, tc):
        self.tc = tc
        self.wait = WebDriverWait(tc.driver, timeout=50)
        self.controls = XDCRControls(tc.driver)

    def create_cluster_reference(self, cluster_name, ip, username, password, cancel=False):
        self.wait.until(lambda fn: self.controls.create_cluster_reference_btn.is_displayed(),
                        "create_cluster_reference_btn is not displayed in %d sec" % (self.wait._timeout))
        time.sleep(3)
        self.tc.log.info("try to create cluster reference with name=%s, ip=%s" % (cluster_name, ip))
        self.controls.create_cluster_reference_btn.click()
        BaseHelper(self.tc).wait_ajax_loaded()
        self.wait.until(lambda fn: self.controls.create_reference_pop_up().pop_up.is_displayed(),
                        "create_cluster_reference_ popup is not displayed in %d sec" % self.wait._timeout)
        if cluster_name:
            self.controls.create_reference_pop_up().name.type(cluster_name)
        if ip:
            self.controls.create_reference_pop_up().ip.type(ip)
        if username:
            self.controls.create_reference_pop_up().user.type(username)
        if password:
            self.controls.create_reference_pop_up().password.type(password, is_pwd=True)
        if not cancel:
            self.controls.create_reference_pop_up().create_btn.click()
        else:
            self.controls.create_reference_pop_up().cancel_btn.click()
        time.sleep(4)
        BaseHelper(self.tc).wait_ajax_loaded()
        self.wait.until(lambda fn: self._cluster_reference_pop_up_reaction(),
                        "there is no reaction in %d sec" % self.wait._timeout)
        if self.controls.error_reference().is_displayed():
            raise Exception("Error found:" + self.controls.error_reference().get_text())
        self.tc.log.info("Reference has been created")

    def _cluster_reference_pop_up_reaction(self):
        try:
            if not (self.controls.create_reference_pop_up().pop_up.is_displayed()):
                return not self.controls.create_reference_pop_up().pop_up.is_displayed()
            else:
                return self.controls.error_reference().is_displayed()
        except StaleElementReferenceException:
            return False

    def _get_error(self):
        return self.controls.error().get_text()

    def create_replication(self, remote_cluster, bucket, remote_bucket, cancel=False, advanced_settings={}):
        self.wait.until(lambda fn: self.controls.create_ongoing_replication_btn.is_displayed(),
                        "create_ongoing_replication_btn is not displayed in %d sec" % self.wait._timeout)
        self.tc.log.info("try to create cluster replication with cluster=%s, bucket=%s, remote_bucket=%s" % (remote_cluster, bucket, remote_bucket))
        self.controls.create_ongoing_replication_btn.click()
        self.wait.until(lambda fn: self.controls.create_replication_pop_up().pop_up.is_displayed(),
                        "create_cluster_reference_ popup is not displayed in %d sec" % (self.wait._timeout))
        if remote_cluster:
            self.controls.create_replication_pop_up().remote_cluster.select(remote_cluster)
        if remote_bucket:
            self.controls.create_replication_pop_up().remote_bucket.type(remote_bucket if isinstance(remote_bucket, str) else remote_bucket.name)
        if bucket:
            self.controls.create_replication_pop_up().bucket.select(bucket if isinstance(bucket, str) else bucket.name)
        if advanced_settings:
            self.controls.create_replication_pop_up().advanced_settings_link.click()
            if 'version' in advanced_settings:
                self.controls.advanced_settings().version.select(value=advanced_settings['version'])
            if 'source_nozzle_per_node' in advanced_settings:
                self.controls.advanced_settings().source_nozzle_per_node.type_native(advanced_settings['source_nozzle_per_node'])
            if 'target_nozzle_per_node' in advanced_settings:
                self.controls.advanced_settings().target_nozzle_per_node.type_native(advanced_settings['target_nozzle_per_node'])
            if 'max_replication' in advanced_settings:
                self.controls.advanced_settings().max_replication.type_native(advanced_settings['max_replication'])
            if 'checkpoint_interval' in advanced_settings:
                self.controls.advanced_settings().checkpoint_interval.type_native(advanced_settings['checkpoint_interval'])
            if 'batch_count' in advanced_settings:
                self.controls.advanced_settings().batch_count.type_native(advanced_settings['batch_count'])
            if 'batch_size' in advanced_settings:
                self.controls.advanced_settings().batch_size.type_native(advanced_settings['batch_size'])
            if 'retry_interval' in advanced_settings:
                self.controls.advanced_settings().retry_interval.type_native(advanced_settings['retry_interval'])
            if 'replication_threshold' in advanced_settings:
                self.controls.advanced_settings().replication_threshold.type_native(advanced_settings['replication_threshold'])
            if 'collection_interval' in advanced_settings:
                self.controls.advanced_settings().collection_interval.type_native(advanced_settings['collection_interval'])
            if 'logging' in advanced_settings:
                self.controls.advanced_settings().logging.select(value=advanced_settings['logging'])
            time.sleep(3)
            try:
                errors = [el.get_text() for el in self.controls.errors_advanced_settings() if el.is_displayed() and el.get_text() != '']
            except Exception as e:
                self.tc.log.info("exception when tried to get errors", e)
                errors = [el.get_text() for el in self.controls.errors_advanced_settings() if
                          el.is_displayed() and el.get_text() != '']
            if len(errors):
                raise Exception('Advanced setting error: %s' % str(errors))
        self.tc.log.error("Filled replication popup window")
        if not cancel:
            self.controls.create_replication_pop_up().replicate_btn.click()
        else:
            self.controls.create_replication_pop_up().cancel_btn.click()
        BaseHelper(self.tc).wait_ajax_loaded()
        all_errors = self.controls.error_replica()
        self.tc.log.info("Found errors on replication popup: %s" % str(all_errors))
        try:
            if all_errors:
                for error in all_errors:
                    if error.is_displayed():
                        raise Exception('Replication is not created: %s' % error.get_text())
        except StaleElementReferenceException:
            self.tc.log.error("No error displayed while creating/cancelling a Replication")

        self.wait.until(lambda fn: self._cluster_replication_pop_up_reaction(),
                        "there is no reaction in %d sec" % self.wait._timeout)

    def _cluster_replication_pop_up_reaction(self):
        try:
            if not (self.controls.create_replication_pop_up().pop_up.is_displayed()):
                return not self.controls.create_replication_pop_up().pop_up.is_displayed()
            else:
                return self.controls.error_replica().is_displayed()
        except StaleElementReferenceException as e:
            return False

    def is_cluster_reference_created(self, name, ip):
        all_ref = self.controls._all_cluster_references()
        for reference in all_ref:
            self.wait.until(lambda fn: reference.is_displayed(),
                            "Reference not found")
            if reference.get_text().find(name) != -1 and reference.get_text().find(ip) != -1:
                return True
        return False

    def is_replication_created(self, bucket_name, name_remote, bucket_name_remote):
        all_rep = self.controls._all_ongoing_replications()
        for rep in all_rep:
            self.wait.until(lambda fn: rep.is_displayed(),
                            "Replication not found")
            if rep.get_text().find(bucket_name) != -1 and rep.get_text().find(name_remote) != -1 and rep.get_text().find(bucket_name_remote) != -1:
                return True
        return False