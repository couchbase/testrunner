import re
import time
import testconstants
import gc
import sys
from basetestcase import BaseTestCase
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection
from couchbase.document import DesignDocument, View
from couchbase.documentgenerator import BlobGenerator
from scripts.install import InstallerJob
from builds.build_query import BuildQuery

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
        self.item_flag = self.input.param('item_flag', 4042322160)
        self.expire_time = self.input.param('expire_time', 0)
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

    def tearDown(self):
        if (hasattr(self, '_resultForDoCleanups') and len(self._resultForDoCleanups.failures or self._resultForDoCleanups.errors)):
                self.log.warn("CLEANUP WAS SKIPPED DUE TO FAILURES IN UPGRADE TEST")
                self.cluster.shutdown()
        else:
            try:
                #cleanup only nodes that are in cluster
                #not all servers have been installed
                if self.rest is None:
                    self.rest = RestConnection(self.master)
                    self.rest_helper = RestHelper(self.rest)
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
                    self.log.info("some nodes were not install successfully!")
                    sys.exit(1)
        if self.rest is None:
            self.rest = RestConnection(self.master)
            self.rest_helper = RestHelper(self.rest)


    def operations(self, servers):
        if len(servers) > 1:
            self.cluster.rebalance([servers[0]], servers[1:], [])
        self.quota = self._initialize_nodes(self.cluster, servers, self.disabled_consistent_view,
                                            self.rebalanceIndexWaitingDisabled, self.rebalanceIndexPausingDisabled,
                                            self.maxParallelIndexers, self.maxParallelReplicaIndexers)
        self.buckets = []
        gc.collect()
        self.bucket_size = self._get_bucket_size(self.quota, self.total_buckets)
        self._bucket_creation()
        gen_load = BlobGenerator('upgrade', 'upgrade-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen_load, "create", 0)
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

    def _upgrade(self, upgrade_version, server, queue=None):
        try:
            remote = RemoteMachineShellConnection(server)
            appropriate_build = self._get_build(server, upgrade_version, remote)
            self.assertTrue(appropriate_build.url, msg="unable to find build {0}".format(upgrade_version))
            remote.download_build(appropriate_build)
            remote.membase_upgrade(appropriate_build, save_upgrade_config=False)
            remote.disconnect()
            if not self.rest_helper.is_ns_server_running(testconstants.NS_SERVER_TIMEOUT * 4):
                self.fail("node {0}:{1} is not running after upgrade".format(server.ip, server.port))
            self.rest.init_cluster(self.rest_settings.rest_username, self.rest_settings.rest_password)
            time.sleep(self.sleep_time)
        except Exception, e:
            if queue is not None:
                queue.put(False)
                raise e
        if queue is not None:
            queue.put(True)

    def verification(self, servers, check_items=True):
        for bucket in self.buckets:
            if not self.rest_helper.bucket_exists(bucket.name):
                raise Exception("bucket: %s not found" % bucket.name)
            bucketinfo = self.rest.get_bucket(bucket.name)
            self.log.info("bucket info : %s" % bucketinfo)
        self.verify_cluster_stats(servers, max_verify=self.max_verify, \
                                  timeout=self.wait_timeout * 50, check_items=check_items)

        if self.ddocs:
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

    def warm_up_node(self):
        warmup_node = self.servers[:self.nodes_init][-1]
        shell = RemoteMachineShellConnection(warmup_node)
        shell.stop_couchbase()
        time.sleep(20)
        shell.start_couchbase()
        shell.disconnect()
        ClusterOperationHelper.wait_for_ns_servers_or_assert([warmup_node], self)

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
