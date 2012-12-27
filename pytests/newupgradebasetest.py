import re
import time
import testconstants
import gc
from basetestcase import BaseTestCase
from membase.api.rest_client import RestConnection, Bucket, RestHelper
from remote.remote_util import RemoteMachineShellConnection
from couchbase.documentgenerator import BlobGenerator
from mc_bin_client import MemcachedError
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
        self.rest = RestConnection(self.master)
        self.rest_helper = RestHelper(self.rest)
        self.sleep_time = 10
        self.ddocs = []
        self.data_size = self.input.param('data_size', 1024)
        self.op_types = self.input.param('op_types', 'bucket')
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

    def operations(self, multi_nodes=False):
        if multi_nodes:
            servers_in = [self.servers[i + 1] for i in range(self.initial_num_servers - 1)]
            self.cluster.rebalance(self.servers[:1], servers_in, [])
        self.quota = self._initialize_nodes(self.cluster, self.servers, self.disabled_consistent_view)
        self.buckets = []
        gc.collect()
        self._bucket_creation()
        if self.op_types == "data":
            self._load_data_all_buckets("create")
            if multi_nodes:
                self._wait_for_stats_all_buckets(self.servers[:self.initial_num_servers])
            else:
                self._wait_for_stats_all_buckets([self.master])

    def _load_data_all_buckets(self, op_type='create', start=0):
        loaded = False
        count = 0
        gen_load = BlobGenerator('upgrade-', 'upgrade-', self.data_size, start=start, end=self.num_items)
        while not loaded and count < 60:
            try :
                self._load_all_buckets(self.master, gen_load, op_type, self.expire_time, 1,
                                       self.item_flag, True, batch_size=1000, pause_secs=5, timeout_secs=180)
                loaded = True
            except MemcachedError as error:
                if error.status == 134:
                    loaded = False
                    self.log.error("Memcached error 134, wait for 5 seconds and then try again")
                    count += 1
                    time.sleep(self.sleep_time)

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

    def _upgrade(self, upgrade_version, server):
        remote = RemoteMachineShellConnection(server)
        appropriate_build = self._get_build(server, upgrade_version, remote)
        self.assertTrue(appropriate_build.url, msg="unable to find build {0}".format(upgrade_version))
        remote.download_build(appropriate_build)
        remote.membase_upgrade(appropriate_build, save_upgrade_config=False)
        remote.disconnect()
        if not self.rest_helper.is_ns_server_running(testconstants.NS_SERVER_TIMEOUT * 4):
            self.fail("node {0}:{1} is not running agter upgrade".format(server.ip, server.port))
        self.rest.init_cluster_port(self.rest_settings.rest_username, self.rest_settings.rest_password)
        time.sleep(self.sleep_time)

    def verification(self, servers):
        for bucket in self.buckets:
            if self.rest_helper.bucket_exists(bucket.name):
                continue
            else:
                raise Exception("bucket:- %s not found" % bucket.name)
            if self.op_types == "bucket":
                bucketinfo = self.rest.get_bucket(bucket.name)
                self.log.info("bucket info :- %s" % bucketinfo)
        self.verify_cluster_stats(servers, max_verify=self.max_verify, \
                                  timeout=self.wait_timeout * 50)

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
        if self.input.param("update_notifications", True):
            if self.rest.get_notifications() != self.input.param("update_notifications", True):
                self.log.error("update notifications settings wasn't saved")
        if self.input.param("autofailover_timeout", None):
            if self.rest.get_autofailover_settings() != self.input.param("autofailover_timeout", None):
                self.log.error("autofailover settings wasn't saved")

    def change_settings(self):
        rest = RestConnection(self.master)
        if self.input.param("update_notifications", True):
            rest.update_notifications(self.input.param("update_notifications", True))
        if self.input.param("autofailover_timeout", None):
            rest.update_autofailover_settings(True,
                                              self.input.param("autofailover_timeout", None))
        if self.input.param("autofailover_alerts", None):
            sender = 'couchbase@localhost'
            recipient = 'root@localhost'
            user = pwd = Automation
            rest.enable_autofailover_alerts(self, recipient, sender, user, pwd)
        if self.input.param("autocompaction", 50):
            rest.set_auto_compaction(viewFragmntThresholdPercentage=
                                     self.input.param("autocompaction", 50))

    def warm_up_node(self):
        warmup_node = self.servers[-1]
        shell = RemoteMachineShellConnection(warmup_node)
        shell.stop_couchbase()
        time.sleep(20)
        shell.start_couchbase()
        shell.disconnect()

    def start_index(self):
        if self.ddocs:
            query = {"connectionTimeout" : 60000}
            for bucket in self.buckets:
                for ddoc in self.ddocs:
                    prefix = ("", "dev_")[ddoc.views[0].dev_view]
                    self.perform_verify_queries(len(ddoc.views), prefix, ddoc.name, query, bucket=bucket,
                                                expected_rows=0)

    def failover(self):
        rest = RestConnection(self.master)
        nodes = rest.node_statuses()
        nodes = [node for node in nodes
                if node.ip != self.master.ip or node.port != self.master.port]
        self.failover_node = nodes[0].id
        rest.fail_over(self.failover_node)

    def add_back(self):
        rest = RestConnection(self.master)
        rest.add_back_node(self.failover_node)

    def create_ddocs_and_views(self):
        self.default_view = View(self.default_view_name, None, None)
        for bucket in self.buckets:
            for i in xrange(ddocs_num):
                views = self.make_default_views(self.default_view_name, self.view_num,
                                               self.is_dev_ddoc, different_map=True)
                ddoc = DesignDocument(self.default_view_name + str(i), views)
                self.ddocs.append(ddoc)
                for view in views:
                    self.cluster.create_view(self.master, ddoc.name, [], bucket=bucket.name)
