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
        self.initial_vbuckets = self.input.param('initial_vbuckets', 64)
        self.rest_settings = self.input.membase_settings
        self.rest = RestConnection(self.master)
        self.rest_helper = RestHelper(self.rest)
        self.sleep_time = 10
        self.data_size = self.input.param('data_size', 1024)
        self.op_types = self.input.param('op_types', 'bucket')
        self.item_flag = self.input.param('item_flag', 4042322160)
        self.expire_time = self.input.param('expire_time', 0)

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
        self.quota = self._initialize_nodes(self.cluster, self.servers, self.disabled_consistent_view)
        self.buckets = []
        gc.collect()
        if self.total_buckets > 0:
            self.bucket_size = self._get_bucket_size(self.quota, self.total_buckets)

        if self.default_bucket:
            self.cluster.create_default_bucket(self.master, self.bucket_size, self.num_replicas)
            self.buckets.append(Bucket(name="default", authType="sasl", saslPassword="",
                                       num_replicas=self.num_replicas, bucket_size=self.bucket_size))

        self._create_sasl_buckets(self.master, self.sasl_buckets)
        self._create_standard_buckets(self.master, self.standard_buckets)
        if multi_nodes:
            servers_in = [self.servers[i+1] for i in range(self.initial_num_servers-1)]
            self.cluster.rebalance(self.servers[:1], servers_in, [])
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
                                       self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)
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

    def _upgrade(self, upgrade_version, server, remote):
        appropriate_build = self._get_build(server, upgrade_version, remote)
        self.assertTrue(appropriate_build.url, msg="unable to find build {0}".format(upgrade_version))
        remote.download_build(appropriate_build)
        remote.membase_upgrade(appropriate_build, save_upgrade_config=False)
        self.rest_helper.is_ns_server_running(testconstants.NS_SERVER_TIMEOUT)
        self.rest.init_cluster_port(self.rest_settings.rest_username, self.rest_settings.rest_password)
        time.sleep(self.sleep_time)

    def verification(self, multi_nodes=False):
        for bucket in self.buckets:
            if self.rest_helper.bucket_exists(bucket.name):
                continue
            else:
                raise Exception("bucket:- %s not found" % bucket.name)
            if self.op_types == "bucket":
                bucketinfo = self.rest.get_bucket(bucket.name)
                self.log.info("bucket info :- %s" % bucketinfo)

        if self.op_types == "data":
            if multi_nodes:
                self._wait_for_stats_all_buckets(self.servers[:self.num_servers])
                self._verify_all_buckets(self.master, 1, self.wait_timeout*50, None, True, 1)
                self._verify_stats_all_buckets(self.servers[:self.num_servers])
            else:
                self._wait_for_stats_all_buckets([self.master])
                self._verify_all_buckets(self.master, 1, self.wait_timeout*50, None, True, 1)
                self._verify_stats_all_buckets([self.master])