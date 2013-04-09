import re
import testconstants
import gc
import sys
import traceback
from threading import Thread
from basetestcase import BaseTestCase
from mc_bin_client import MemcachedError
from memcached.helper.data_helper import VBucketAwareMemcached
from membase.helper.bucket_helper import BucketOperationHelper
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection
from couchbase.document import DesignDocument, View
from couchbase.documentgenerator import BlobGenerator
from scripts.install import InstallerJob
from builds.build_query import BuildQuery
from pprint import pprint

class NewUpgradeBaseTest(BaseTestCase):
    def setUp(self):
        super(NewUpgradeBaseTest, self).setUp()
        self.product = self.input.param('product', 'couchbase-server')
        self.initial_version = self.input.param('initial_version', '1.8.1-942-rel')
        self.initial_vbuckets = self.input.param('initial_vbuckets', 1024)
        self.upgrade_versions = self.input.param('upgrade_version', '2.0.0-1870-rel')
        self.upgrade_versions = self.upgrade_versions.split(";")
        upgrade_path = self.input.param('upgrade_path', [])
        if upgrade_path:
            upgrade_path = upgrade_path.split(",")
        self.upgrade_versions = upgrade_path + self.upgrade_versions
        self.rest_settings = self.input.membase_settings
        self.rest = None
        self.rest_helper = None
        self.sleep_time = 10
        self.ddocs = []
        self.item_flag = self.input.param('item_flag', 0)
        self.expire_time = self.input.param('expire_time', 0)
        self.wait_expire = self.input.param('wait_expire', False)
        self.default_view_name = "upgrade-test-view"
        self.ddocs_num = self.input.param("ddocs-num", 0)
        self.view_num = self.input.param("view-per-ddoc", 2)
        self.is_dev_ddoc = self.input.param("is-dev-ddoc", False)
        self.during_ops = None
        if "during-ops" in self.input.test_params:
            self.during_ops = self.input.param("during-ops", None).split(",")
        if self.initial_version.startswith("1.6") or self.initial_version.startswith("1.7"):
            self.product = 'membase-server'
        else:
            self.product = 'couchbase-server'
        if self.max_verify is None:
            self.max_verify = min(self.num_items, 100000)
        shell = RemoteMachineShellConnection(self.master)
        type = shell.extract_remote_info().distribution_type
        shell.disconnect()
        if type.lower() == 'windows':
            self.is_linux = False
        else:
            self.is_linux = True

    def tearDown(self):
        test_failed = (hasattr(self, '_resultForDoCleanups') and len(self._resultForDoCleanups.failures or self._resultForDoCleanups.errors)) \
                    or (hasattr(self, '_exc_info') and self._exc_info()[1] is not None)
        if test_failed:
                self.log.warn("CLEANUP WAS SKIPPED DUE TO FAILURES IN UPGRADE TEST")
                self.cluster.shutdown()
                self.log.info("Test Input params were:")
                pprint(self.input.test_params)

                if self.input.param('BUGS', False):
                    self.log.warn("Test failed. Possible reason is: {0}".format(self.input.param('BUGS', False)))
        else:
            try:
                #cleanup only nodes that are in cluster
                #not all servers have been installed
                if self.rest is None:
                    self._new_master(self.master)
                nodes = self.rest.get_nodes()
                temp = []
                for server in self.servers:
                    if server.ip in [node.ip for node in nodes]:
                        temp.append(server)
                self.servers = temp
            except Exception, e:
                self.cluster.shutdown()
                self.fail(e)
            super(NewUpgradeBaseTest, self).tearDown()

    def _install(self, servers):
        params = {}
        params['num_nodes'] = len(servers)
        params['product'] = self.product
        params['version'] = self.initial_version
        params['vbuckets'] = [self.initial_vbuckets]
        InstallerJob().parallel_install(servers, params)
        if self.product in ["couchbase", "couchbase-server", "cb"]:
            success = True
            for server in servers:
                success &= RemoteMachineShellConnection(server).is_couchbase_installed()
                if not success:
                    sys.exit("some nodes were not install successfully!")
        if self.rest is None:
            self._new_master(self.master)


    def operations(self, servers):
        self.quota = self._initialize_nodes(self.cluster, servers, self.disabled_consistent_view,
                                            self.rebalanceIndexWaitingDisabled, self.rebalanceIndexPausingDisabled,
                                            self.maxParallelIndexers, self.maxParallelReplicaIndexers, self.port)
        if self.port and self.port != '8091':
            self.rest = RestConnection(self.master)
            self.rest_helper = RestHelper(self.rest)
        if len(servers) > 1:
            self.cluster.rebalance([servers[0]], servers[1:], [])

        self.buckets = []
        gc.collect()
        self.bucket_size = self._get_bucket_size(self.quota, self.total_buckets)
        self._bucket_creation()
        gen_load = BlobGenerator('upgrade', 'upgrade-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)
        self._wait_for_stats_all_buckets(servers)
        self.change_settings()

    def _get_build(self, server, version, remote, is_amazon=False):
        info = remote.extract_remote_info()
        builds, changes = BuildQuery().get_all_builds()
        self.log.info("finding build %s for machine %s" % (version, server))
        result = re.search('r', version)

        if result is None:
            appropriate_build = BuildQuery().\
                find_membase_release_build('%s-enterprise' % (self.product), info.deliverable_type,
                                           info.architecture_type, version.strip(), is_amazon=is_amazon)
        else:
            appropriate_build = BuildQuery().\
                find_membase_build(builds, '%s-enterprise' % (self.product), info.deliverable_type,
                                   info.architecture_type, version.strip(), is_amazon=is_amazon)
        return appropriate_build

    def _upgrade(self, upgrade_version, server, queue=None, skip_init=False):
        try:
            remote = RemoteMachineShellConnection(server)
            appropriate_build = self._get_build(server, upgrade_version, remote)
            self.assertTrue(appropriate_build.url, msg="unable to find build {0}".format(upgrade_version))
            remote.download_build(appropriate_build)
            remote.membase_upgrade(appropriate_build, save_upgrade_config=False)
            self.log.info("upgrade {0} to version {1} is completed".format(server.ip, upgrade_version))
            remote.disconnect()
            self.sleep(10)
            if self.is_linux:
                self.wait_node_restarted(server, wait_time=testconstants.NS_SERVER_TIMEOUT * 4, wait_if_warmup=True)
            else:
                self.wait_node_restarted(server, wait_time=testconstants.NS_SERVER_TIMEOUT * 10, wait_if_warmup=True, check_service=True)
            if not skip_init:
                self.rest.init_cluster(self.rest_settings.rest_username, self.rest_settings.rest_password)
            self.sleep(self.sleep_time)
        except Exception, e:
            print traceback.extract_stack()
            if queue is not None:
                queue.put(False)
                if not self.is_linux:
                    remote = RemoteMachineShellConnection(server)
                    output, error = remote.execute_command("cmd /c schtasks /Query /FO LIST /TN removeme /V")
                    remote.log_command_output(output, error)
                    output, error = remote.execute_command("cmd /c schtasks /Query /FO LIST /TN installme /V")
                    remote.log_command_output(output, error)
                    output, error = remote.execute_command("cmd /c schtasks /Query /FO LIST /TN upgrademe /V")
                    remote.log_command_output(output, error)
                    remote.disconnect()
                raise e
        if queue is not None:
            queue.put(True)

    def _async_update(self, upgrade_version, servers, queue=None, skip_init=False):
        self.log.info("servers {0} will be upgraded to {1} version".
                      format([server.ip for server in servers], upgrade_version))
        q = queue or self.queue
        upgrade_threads = []
        for server in servers:
            upgrade_thread = Thread(target=self._upgrade,
                                    name="upgrade_thread" + server.ip,
                                    args=(upgrade_version, server, q, skip_init))
            upgrade_threads.append(upgrade_thread)
            upgrade_thread.start()
        return upgrade_threads

    def _new_master(self, server):
        self.master = server
        self.rest = RestConnection(self.master)
        self.rest_helper = RestHelper(self.rest)


    def verification(self, servers, check_items=True):
        if self.master.ip != self.rest.ip or \
           self.master.ip == self.rest.ip and str(self.master.port) != str(self.rest.port):
            if self.port:
                self.master.port = self.port
            self.rest = RestConnection(self.master)
            self.rest_helper = RestHelper(self.rest)
        if self.port and self.port != '8091':
            settings = self.rest.get_cluster_settings()
            if settings and 'port' in settings:
                self.assertTrue(self.port == str(settings['port']),
                                'Expected cluster port is %s, but is %s' % (self.port, settings['port']))
        for bucket in self.buckets:
            if not self.rest_helper.bucket_exists(bucket.name):
                raise Exception("bucket: %s not found" % bucket.name)
        self.verify_cluster_stats(servers, max_verify=self.max_verify, \
                                  timeout=self.wait_timeout * 20, check_items=check_items)

        if self.ddocs:
            self.verify_all_queries()
        if "update_notifications" in self.input.test_params:
            if self.rest.get_notifications() != self.input.param("update_notifications", True):
                self.fail("update notifications settings wasn't saved")
        if "autofailover_timeout" in self.input.test_params:
            if self.rest.get_autofailover_settings().timeout != self.input.param("autofailover_timeout", None):
                self.fail("autofailover settings wasn't saved")
        if "autofailover_alerts" in self.input.test_params:
            alerts = self.rest.get_alerts_settings()
            if alerts["recipients"] != ['couchbase@localhost']:
                self.fail("recipients value wasn't saved")
            if alerts["sender"] != 'root@localhost':
                self.fail("sender value wasn't saved")
            if alerts["emailServer"]["user"] != 'user':
                self.fail("email_username value wasn't saved")
            if alerts["emailServer"]["pass"] != '':
                self.fail("email_password should be empty for security")
        if "autocompaction" in self.input.test_params:
            cluster_status = self.rest.cluster_status()
            if cluster_status["autoCompactionSettings"]["viewFragmentationThreshold"]\
                             ["percentage"] != self.input.param("autocompaction", 50):
                    self.fail("autocompaction settings weren't saved")

    def verify_all_queries(self):
        query = {"connectionTimeout" : 60000}
        expected_rows = self.num_items
        if self.max_verify:
            expected_rows = self.max_verify
            query["limit"] = expected_rows
        if self.input.param("wait_expiration", False):
            expected_rows = 0
        for bucket in self.buckets:
            for ddoc in self.ddocs:
                prefix = ("", "dev_")[ddoc.views[0].dev_view]
                self.perform_verify_queries(len(ddoc.views), prefix, ddoc.name, query, bucket=bucket,
                                           wait_time=self.wait_timeout * 5, expected_rows=expected_rows,
                                           retry_time=10)

    def change_settings(self):
        if "update_notifications" in self.input.test_params:
            self.rest.update_notifications(str(self.input.param("update_notifications", 'true')).lower())
        if "autofailover_timeout" in self.input.test_params:
            self.rest.update_autofailover_settings(True, self.input.param("autofailover_timeout", None))
        if "autofailover_alerts" in self.input.test_params:
            self.rest.set_alerts_settings('couchbase@localhost', 'root@localhost', 'user', 'pwd')
        if "autocompaction" in self.input.test_params:
            self.rest.set_auto_compaction(viewFragmntThresholdPercentage=
                                     self.input.param("autocompaction", 50))

    def warm_up_node(self, warmup_nodes=None):
        if not warmup_nodes:
            warmup_nodes = [self.servers[:self.nodes_init][-1], ]
        for warmup_node in warmup_nodes:
            shell = RemoteMachineShellConnection(warmup_node)
            shell.stop_couchbase()
            shell.disconnect()
        self.sleep(20)
        for warmup_node in warmup_nodes:
            shell = RemoteMachineShellConnection(warmup_node)
            shell.start_couchbase()
            shell.disconnect()
        ClusterOperationHelper.wait_for_ns_servers_or_assert(warmup_nodes, self)

    def start_index(self):
        if self.ddocs:
            query = {"connectionTimeout" : 60000}
            for bucket in self.buckets:
                for ddoc in self.ddocs:
                    prefix = ("", "dev_")[ddoc.views[0].dev_view]
                    self.perform_verify_queries(len(ddoc.views), prefix, ddoc.name, query, bucket=bucket)

    def failover(self):
        rest = RestConnection(self.master)
        nodes = rest.node_statuses()
        nodes = [node for node in nodes
                if node.ip != self.master.ip or str(node.port) != self.master.port]
        self.failover_node = nodes[0]
        rest.fail_over(self.failover_node.id)

    def add_back_failover(self):
        rest = RestConnection(self.master)
        rest.add_back_node(self.failover_node.id)

    def create_ddocs_and_views(self):
        self.default_view = View(self.default_view_name, None, None)
        for bucket in self.buckets:
            for i in xrange(self.ddocs_num):
                views = self.make_default_views(self.default_view_name, self.view_num,
                                               self.is_dev_ddoc, different_map=True)
                ddoc = DesignDocument(self.default_view_name + str(i), views)
                self.ddocs.append(ddoc)
                for view in views:
                    self.cluster.create_view(self.master, ddoc.name, view, bucket=bucket)

    def delete_data(self, servers, paths_to_delete):
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            for path in paths_to_delete:
                output, error = shell.execute_command("rm -rf {0}".format(path))
                shell.log_command_output(output, error)
                #shell._ssh_client.open_sftp().rmdir(path)
            shell.disconnect()

    def check_seqno(self, seqno_expected, comparator='=='):
        for bucket in self.buckets:
            ready = BucketOperationHelper.wait_for_memcached(self.master,
                                                          bucket.name)
            self.assertTrue(ready, "wait_for_memcached failed")
            client = VBucketAwareMemcached(RestConnection(self.master), bucket)
            valid_keys, deleted_keys = bucket.kvs[1].key_set()
            for valid_key in valid_keys:
                try:
                    _, flags, exp, seqno, cas = client.memcached(valid_key).getMeta(valid_key)
                except MemcachedError, e:
                    print e
                    client.reset(RestConnection(self.master))
                    _, flags, exp, seqno, cas = client.memcached(valid_key).getMeta(valid_key)
                self.assertTrue((comparator == '==' and seqno == seqno_expected) or
                                (comparator == '>=' and seqno >= seqno_expected),
                                msg="seqno {0} !{1} {2} for key:{3}".
                                format(seqno, comparator, seqno_expected, valid_key))
            client.done()
