import re
import testconstants
import gc
import sys, json
import traceback
import queue
from threading import Thread
from basetestcase import BaseTestCase
from mc_bin_client import MemcachedError
from memcached.helper.data_helper import VBucketAwareMemcached, MemcachedClientHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from couchbase_helper.document import DesignDocument, View
from couchbase_helper.documentgenerator import BlobGenerator
from scripts.install import InstallerJob
from builds.build_query import BuildQuery
from couchbase_helper.tuq_generators import JsonGenerator
from pytests.fts.fts_callable import FTSCallable
from pprint import pprint
from testconstants import CB_REPO
from testconstants import MV_LATESTBUILD_REPO
from testconstants import SHERLOCK_BUILD_REPO
from testconstants import COUCHBASE_VERSION_2
from testconstants import COUCHBASE_VERSION_3
from testconstants import COUCHBASE_VERSIONS
from testconstants import SHERLOCK_VERSION
from testconstants import CB_VERSION_NAME
from testconstants import COUCHBASE_FROM_VERSION_3
from testconstants import COUCHBASE_MP_VERSION
from testconstants import CE_EE_ON_SAME_FOLDER


class NewUpgradeBaseTest(BaseTestCase):
    def setUp(self):
        super(NewUpgradeBaseTest, self).setUp()
        self.log.info("==============  NewUpgradeBaseTest setup has started ==============")
        self.released_versions = ["2.0.0-1976-rel", "2.0.1", "2.5.0", "2.5.1",
                                  "2.5.2", "3.0.0", "3.0.1",
                                  "3.0.1-1444", "3.0.2", "3.0.2-1603", "3.0.3",
                                  "3.1.0", "3.1.0-1776", "3.1.1", "3.1.1-1807",
                                  "3.1.2", "3.1.2-1815", "3.1.3", "3.1.3-1823",
                                  "4.0.0", "4.0.0-4051", "4.1.0", "4.1.0-5005",
                                  "4.5.0", "4.5.0-2601", "4.5.1", "4.5.1-2817",
                                  "4.6.0", "4.6.0-3573", '4.6.2', "4.6.2-3905"]
        self.use_hostnames = self.input.param("use_hostnames", False)
        self.product = self.input.param('product', 'couchbase-server')
        self.initial_version = self.input.param('initial_version', '2.5.1-1083')
        self.initial_vbuckets = self.input.param('initial_vbuckets', 1024)
        self.upgrade_versions = self.input.param('upgrade_version', '2.0.1-170-rel')
        self.call_ftsCallable = True
        self.upgrade_versions = self.upgrade_versions.split(";")
        self.skip_cleanup = self.input.param("skip_cleanup", False)
        self.debug_logs = self.input.param("debug_logs", False)
        self.init_nodes = self.input.param('init_nodes', True)

        self.is_downgrade = self.input.param('downgrade', False)
        if self.is_downgrade:
            self.initial_version, self.upgrade_versions = self.upgrade_versions[0], [self.initial_version]

        upgrade_path = self.input.param('upgrade_path', [])
        if upgrade_path:
            upgrade_path = upgrade_path.split(",")
        self.upgrade_versions = upgrade_path + self.upgrade_versions
        if self.input.param('released_upgrade_version', None) is not None:
            self.upgrade_versions = [self.input.param('released_upgrade_version', None)]

        self.initial_build_type = self.input.param('initial_build_type', None)
        self.stop_persistence = self.input.param('stop_persistence', False)
        self.std_vbucket_dist = self.input.param("std_vbucket_dist", None)
        self.cb_bucket_name = self.input.param('cb_bucket_name', 'travel-sample')
        self.cb_server_ip = self.input.param("cb_server_ip", None)
        self.cbas_bucket_name = self.input.param('cbas_bucket_name', 'travel')
        self.cbas_dataset_name = self.input.param("cbas_dataset_name", 'travel_ds')
        self.cbas_dataset_name_invalid = self.input.param('cbas_dataset_name_invalid', self.cbas_dataset_name)
        self.cbas_bucket_name_invalid = self.input.param('cbas_bucket_name_invalid', self.cbas_bucket_name)
        self.num_index_replicas = self.input.param("num_index_replica", 0)
        self.expected_err_msg = self.input.param("expected_err_msg", None)
        self.rest_settings = self.input.membase_settings
        self.multi_nodes_services = False
        self.rest = None
        self.rest_helper = None
        self.is_ubuntu = False
        self.is_rpm = False
        self.is_centos7 = False
        self.sleep_time = 15
        self.ddocs = []
        self.item_flag = self.input.param('item_flag', 0)
        self.expire_time = self.input.param('expire_time', 0)
        self.wait_expire = self.input.param('wait_expire', False)
        self.default_view_name = "upgrade-test-view"
        self.ddocs_num = self.input.param("ddocs_num", 1)
        self.view_num = self.input.param("view_per_ddoc", 2)
        self.is_dev_ddoc = self.input.param("is-dev-ddoc", False)
        self.offline_failover_upgrade = self.input.param("offline_failover_upgrade", False)
        self.during_ops = None
        if "during-ops" in self.input.test_params:
            self.during_ops = self.input.param("during-ops", None).split(",")
        if self.initial_version.startswith("1.6") or self.initial_version.startswith("1.7"):
            self.product = 'membase-server'
        else:
            self.product = 'couchbase-server'
        self.index_replicas = self.input.param("index_replicas", None)
        self.index_kv_store = self.input.param("kvstore", None)
        self.partitions_per_pindex = \
            self.input.param("max_partitions_pindex", 32)
        self.dataset = self.input.param("dataset", "emp")
        self.expiry = self.input.param("expiry", 0)
        self.create_ops_per = self.input.param("create_ops_per", 0)
        self.expiry_ops_per = self.input.param("expiry_ops_per", 0)
        self.delete_ops_per = self.input.param("delete_ops_per", 0)
        self.update_ops_per = self.input.param("update_ops_per", 0)
        self.docs_per_day = self.input.param("doc-per-day", 49)
        self.eventing_log_level = self.input.param('eventing_log_level', 'INFO')
        self.batch_size = self.input.param("batch_size", 1000)
        self.doc_ops = self.input.param("doc_ops", False)
        if self.doc_ops:
            self.ops_dist_map = self.calculate_data_change_distribution(
                create_per=self.create_ops_per, update_per=self.update_ops_per,
                delete_per=self.delete_ops_per, expiry_per=self.expiry_ops_per,
                start=0, end=self.docs_per_day)
            self.log.info(self.ops_dist_map)
            self.docs_gen_map = self.generate_ops_docs(self.docs_per_day, 0)
            #self.full_docs_list_after_ops = self.generate_full_docs_list_after_ops(self.docs_gen_map)
        if self.max_verify is None:
            self.max_verify = min(self.num_items, 100000)
        shell = RemoteMachineShellConnection(self.master)
        type = shell.extract_remote_info().distribution_type
        os_version = shell.extract_remote_info().distribution_version
        shell.disconnect()
        if type.lower() == 'windows':
            self.is_linux = False
        else:
            self.is_linux = True
        if type.lower() == "ubuntu":
            self.is_ubuntu = True
        if type.lower() == "centos":
            self.is_rpm = True
            if os_version.lower() == "centos 7":
                self.is_centos7 = True
        self.queue = queue.Queue()
        self.upgrade_servers = []
        self.index_replicas = self.input.param("index_replicas", None)
        self.partitions_per_pindex = \
            self.input.param("max_partitions_pindex", 32)
        self.index_kv_store = self.input.param("kvstore", None)
        self.fts_obj = None
        self.log.info("==============  NewUpgradeBaseTest setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  NewUpgradeBaseTest tearDown has started ==============")
        test_failed = (hasattr(self, '_resultForDoCleanups') and \
                       len(self._resultForDoCleanups.failures or \
                           self._resultForDoCleanups.errors)) or \
                                 (hasattr(self, '_exc_info') and \
                                  self._exc_info()[1] is not None)
        if test_failed and self.skip_cleanup:
                self.log.warn("CLEANUP WAS SKIPPED DUE TO FAILURES IN UPGRADE TEST")
                self.cluster.shutdown(force=True)
                self.log.info("Test Input params were:")
                pprint(self.input.test_params)

                if self.input.param('BUGS', False):
                    self.log.warn("Test failed. Possible reason is: {0}"
                                           .format(self.input.param('BUGS', False)))
        else:
            if not hasattr(self, 'rest'):
                return
            try:
                # cleanup only nodes that are in cluster
                # not all servers have been installed
                if self.rest is None:
                    self._new_master(self.master)
                nodes = self.rest.get_nodes()
                temp = []
                for server in self.servers:
                    if server.ip in [node.ip for node in nodes]:
                        temp.append(server)
                self.servers = temp
            except Exception as e:
                if e:
                    print("Exception ", e)
                self.cluster.shutdown(force=True)
                self.fail(e)
            super(NewUpgradeBaseTest, self).tearDown()
            if self.upgrade_servers:
                self._install(self.upgrade_servers)
        self.sleep(20, "sleep 20 seconds before run next test")
        self.log.info("==============  NewUpgradeBaseTest tearDown has completed ==============")

    def _install(self, servers):
        params = {}
        params['num_nodes'] = len(servers)
        params['product'] = self.product
        params['version'] = self.initial_version
        params['vbuckets'] = [self.initial_vbuckets]
        params['init_nodes'] = self.init_nodes
        params['debug_logs'] = self.debug_logs
        if self.initial_build_type is not None:
            params['type'] = self.initial_build_type
        if 5 <= int(self.initial_version[:1]) or 5 <= int(self.upgrade_versions[0][:1]):
            params['fts_query_limit'] = 10000000

        self.log.info("will install {0} on {1}"\
                                .format(self.initial_version, [s.ip for s in servers]))
        InstallerJob().parallel_install(servers, params)
        if self.product in ["couchbase", "couchbase-server", "cb"]:
            success = True
            for server in servers:
                shell = RemoteMachineShellConnection(server)
                info = shell.extract_remote_info()
                success &= shell.is_couchbase_installed()
                self.sleep(5, "sleep 5 seconds to let cb up completely")
                ready = RestHelper(RestConnection(server)).is_ns_server_running(60)
                if not ready:
                    if "cento 7" in info.distribution_version.lower():
                        self.log.info("run systemctl daemon-reload")
                        shell.execute_command("systemctl daemon-reload", debug=False)
                        shell.start_server()
                    else:
                        log.error("Couchbase-server did not start...")
                shell.disconnect()
                if not success:
                    sys.exit("some nodes were not install successfully!")
        if self.rest is None:
            self._new_master(self.master)
        if self.use_hostnames:
            for server in self.servers[:self.nodes_init]:
                hostname = RemoteUtilHelper.use_hostname_for_server_settings(server)
                server.hostname = hostname

    def initial_services(self, services=None):
        if services is not None:
            if "-" in services:
                set_services = services.split("-")
            elif "," in services:
                if self.multi_nodes_services:
                    set_services = services.split()
                else:
                    set_services = services.split(",")
        else:
            set_services = services
        return set_services

    def initialize_nodes(self, servers, services=None):
        set_services = self.initial_services(services)
        if 4.5 > float(self.initial_version[:3]):
            self.gsi_type = "forestdb"
        self.quota = self._initialize_nodes(self.cluster, servers,
                                            self.disabled_consistent_view,
                                            self.rebalanceIndexWaitingDisabled,
                                            self.rebalanceIndexPausingDisabled,
                                            self.maxParallelIndexers,
                                            self.maxParallelReplicaIndexers, self.port,
                                            services=set_services)

    def operations(self, servers, services=None):
        set_services = self.initial_services(services)

        if 4.5 > float(self.initial_version[:3]):
            self.gsi_type = "forestdb"
        self.quota = self._initialize_nodes(self.cluster, servers,
                                            self.disabled_consistent_view,
                                            self.rebalanceIndexWaitingDisabled,
                                            self.rebalanceIndexPausingDisabled,
                                            self.maxParallelIndexers,
                                            self.maxParallelReplicaIndexers, self.port,
                                            services=set_services)

        if self.port and self.port != '8091':
            self.rest = RestConnection(self.master)
            self.rest_helper = RestHelper(self.rest)
        if 5.0 <= float(self.initial_version[:3]):
            self.add_built_in_server_user()
        self.sleep(7, "wait to make sure node is ready")
        if len(servers) > 1:
            if services is None:
                self.cluster.rebalance([servers[0]], servers[1:], [],
                                       use_hostnames=self.use_hostnames)
            else:
                for i in range(1, len(set_services)):
                    self.cluster.rebalance([servers[0]], [servers[i]], [],
                                           use_hostnames=self.use_hostnames,
                                           services=[set_services[i]])
                    self.sleep(10)
        self.buckets = []
        gc.collect()
        if self.input.param('extra_verification', False):
            self.total_buckets += 2
            print(self.total_buckets)
        self.bucket_size = self._get_bucket_size(self.quota, self.total_buckets)
        if self.dgm_run:
            self.bucket_size = 256
        self._bucket_creation()
        if self.stop_persistence:
            for server in servers:
                for bucket in self.buckets:
                    client = MemcachedClientHelper.direct_client(server, bucket)
                    client.stop_persistence()
            self.sleep(10)
        gen_load = BlobGenerator('upgrade', 'upgrade-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)
        if not self.stop_persistence:
            self._wait_for_stats_all_buckets(servers)
        else:
            for bucket in self.buckets:
                drain_rate = 0
                for server in servers:
                    client = MemcachedClientHelper.direct_client(server, bucket)
                    drain_rate += int(client.stats()["ep_queue_size"])
                self.sleep(3, "Pause to load all items")
                self.assertEqual(self.num_items * (self.num_replicas + 1), drain_rate,
                                 "Persistence is stopped, drain rate is incorrect %s. Expected %s" % (
                                    drain_rate, self.num_items * (self.num_replicas + 1)))
        self.change_settings()

    def _get_build(self, server, version, remote, is_amazon=False, info=None):
        if info is None:
            info = remote.extract_remote_info()
        build_repo = CB_REPO
        if version[:5] in COUCHBASE_VERSIONS:
            if version[:3] in CB_VERSION_NAME:
                build_repo = CB_REPO + CB_VERSION_NAME[version[:3]] + "/"
            elif version[:5] in COUCHBASE_MP_VERSION:
                build_repo = MV_LATESTBUILD_REPO
        builds, changes = BuildQuery().get_all_builds(version=version, timeout=self.wait_timeout * 5, \
                    deliverable_type=info.deliverable_type, architecture_type=info.architecture_type, \
                    edition_type="couchbase-server-enterprise", repo=build_repo, \
                    distribution_version=info.distribution_version.lower())
        self.log.info("finding build %s for machine %s" % (version, server))

        if re.match(r'[1-9].[0-9].[0-9]-[0-9]+$', version):
            version = version + "-rel"
        if version[:5] in self.released_versions:
            appropriate_build = BuildQuery().\
                find_couchbase_release_build('%s-enterprise' % (self.product),
                                           info.deliverable_type,
                                           info.architecture_type,
                                           version.strip(),
                                           is_amazon=is_amazon,
                                           os_version=info.distribution_version)
        else:
             appropriate_build = BuildQuery().\
                find_build(builds, '%s-enterprise' % (self.product), info.deliverable_type,
                                   info.architecture_type, version.strip())

        if appropriate_build is None:
            self.log.info("builds are: %s \n. Remote is %s, %s. Result is: %s" % (builds, remote.ip, remote.username, version))
            raise Exception("Build %s for machine %s is not found" % (version, server))
        return appropriate_build

    def _upgrade(self, upgrade_version, server, queue=None, skip_init=False, info=None,
                 save_upgrade_config=False, fts_query_limit=None, debug_logs=False):
        try:
            remote = RemoteMachineShellConnection(server)
            appropriate_build = self._get_build(server, upgrade_version, remote, info=info)
            self.assertTrue(appropriate_build.url,
                             msg="unable to find build {0}"\
                             .format(upgrade_version))
            self.assertTrue(remote.download_build(appropriate_build),
                             "Build wasn't downloaded!")
            o, e = remote.couchbase_upgrade(appropriate_build,
                                            save_upgrade_config=False,
                                            forcefully=self.is_downgrade,
                                            fts_query_limit=fts_query_limit,
                                            debug_logs=debug_logs)
            self.log.info("upgrade {0} to version {1} is completed"
                          .format(server.ip, upgrade_version))
            if 5.0 > float(self.initial_version[:3]) and self.is_centos7:
                remote.execute_command("systemctl daemon-reload")
                remote.start_server()
            # TBD: For now adding sleep as the server is not fully up and tests failing during py3
            self.sleep(30)
            self.rest = RestConnection(server)
            if self.is_linux:
                self.wait_node_restarted(server, wait_time=testconstants.NS_SERVER_TIMEOUT * 4, wait_if_warmup=True)
            else:
                self.wait_node_restarted(server, wait_time=testconstants.NS_SERVER_TIMEOUT * 10, wait_if_warmup=True, check_service=True)
            if not skip_init:
                self.rest.init_cluster(self.rest_settings.rest_username, self.rest_settings.rest_password)
            self.sleep(self.sleep_time)
            remote.disconnect()
            self.sleep(30)
            return o, e
        except Exception as e:
            print(traceback.extract_stack())
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
        self.log.info("\n\n*** servers {0} will be upgraded to {1} version ***\n".
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
        if self.use_hostnames:
            for server in servers:
                node_info = RestConnection(server).get_nodes_self()
                new_hostname = node_info.hostname
                self.assertEqual("%s:%s" % (server.hostname, server.port), new_hostname,
                                 "Hostname is incorrect for server %s. Settings are %s" % (server.ip, new_hostname))
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
                    self.log.info("Cluster status: {0}".format(cluster_status))
                    self.fail("autocompaction settings weren't saved")

    def verify_all_queries(self, queue=None):
        query = {"connectionTimeout" : 60000}
        expected_rows = self.num_items
        if self.max_verify:
            expected_rows = self.max_verify
            query["limit"] = expected_rows
        if self.input.param("wait_expiration", False):
            expected_rows = 0
        self.log.info("Start verify queries...")
        for bucket in self.buckets:
            for ddoc in self.ddocs:
                prefix = ("", "dev_")[ddoc.views[0].dev_view]
                try:
                    self.perform_verify_queries(len(ddoc.views), prefix, ddoc.name,
                                                              query, bucket=bucket,
                                                   wait_time=self.wait_timeout * 5,
                                                       expected_rows=expected_rows,
                                                                     retry_time=10)
                except Exception as e:
                    print(e)
                    if queue is not None:
                        queue.put(False)
                if queue is not None:
                    queue.put(True)

    def change_settings(self):
        status = True
        if "update_notifications" in self.input.test_params:
            status &= self.rest.update_notifications(str(self.input.param("update_notifications", 'true')).lower())
        if "autofailover_timeout" in self.input.test_params:
            status &= self.rest.update_autofailover_settings(True, self.input.param("autofailover_timeout", None))
        if "autofailover_alerts" in self.input.test_params:
            status &= self.rest.set_alerts_settings('couchbase@localhost', 'root@localhost', 'user', 'pwd')
        if "autocompaction" in self.input.test_params:
            tmp, _, _ = self.rest.set_auto_compaction(viewFragmntThresholdPercentage=
                                     self.input.param("autocompaction", 50))
            status &= tmp
            if not status:
                self.fail("some settings were not set correctly!")

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

    def create_ddocs_and_views(self, queue=None):
        self.default_view = View(self.default_view_name, None, None)
        for bucket in self.buckets:
            if int(self.ddocs_num) > 0:
                for i in range(int(self.ddocs_num)):
                    views = self.make_default_views(self.default_view_name,
                            self.view_num, self.is_dev_ddoc, different_map=True)
                    ddoc = DesignDocument(self.default_view_name + str(i), views)
                    self.ddocs.append(ddoc)
                    for view in views:
                        try:
                            self.cluster.create_view(self.master, ddoc.name, view,
                                                                   bucket=bucket)
                        except Exception as e:
                            print(e)
                            if queue is not None:
                                queue.put(False)
                        if queue is not None:
                            queue.put(True)
            else:
                self.fail("Check param ddocs_num value")

    def delete_data(self, servers, paths_to_delete):
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            for path in paths_to_delete:
                output, error = shell.execute_command("rm -rf {0}".format(path))
                shell.log_command_output(output, error)
                # shell._ssh_client.open_sftp().rmdir(path)
            shell.disconnect()

    def check_seqno(self, seqno_expected, comparator='=='):
        for bucket in self.buckets:
            if bucket.type == 'memcached':
                continue
            ready = BucketOperationHelper.wait_for_memcached(self.master,
                                                          bucket.name)
            self.assertTrue(ready, "wait_for_memcached failed")
            client = VBucketAwareMemcached(RestConnection(self.master), bucket)
            valid_keys, deleted_keys = bucket.kvs[1].key_set()
            for valid_key in valid_keys:
                try:
                    _, flags, exp, seqno, cas = client.memcached(valid_key).getMeta(valid_key)
                except MemcachedError as e:
                    print(e)
                    client.reset(RestConnection(self.master))
                    _, flags, exp, seqno, cas = client.memcached(valid_key).getMeta(valid_key)
                self.assertTrue((comparator == '==' and seqno == seqno_expected) or
                                (comparator == '>=' and seqno >= seqno_expected),
                                msg="seqno {0} !{1} {2} for key:{3}".
                                format(seqno, comparator, seqno_expected, valid_key))
            client.done()

    def force_reinstall(self, servers):
        for server in servers:
            try:
                remote = RemoteMachineShellConnection(server)
                appropriate_build = self._get_build(server, self.initial_version, remote)
                self.assertTrue(appropriate_build.url, msg="unable to find build {0}".format(self.initial_version))
                remote.download_build(appropriate_build)
                remote.install_server(appropriate_build, force=True)
                self.log.info("upgrade {0} to version {1} is completed".format(server.ip, self.initial_version))
                remote.disconnect()
                self.sleep(10)
                if self.is_linux:
                    self.wait_node_restarted(server, wait_time=testconstants.NS_SERVER_TIMEOUT * 4, wait_if_warmup=True)
                else:
                    self.wait_node_restarted(server, wait_time=testconstants.NS_SERVER_TIMEOUT * 10, wait_if_warmup=True, check_service=True)
            except Exception as e:
                print(traceback.extract_stack())
                if queue is not None:
                    queue.put(False)
                    if not self.is_linux:
                        remote = RemoteMachineShellConnection(server)
                        output, error = remote.execute_command("cmd /c schtasks /Query /FO LIST /TN installme /V")
                        remote.log_command_output(output, error)
                        remote.disconnect()
                    raise e

    def _verify_vbucket_nums_for_swap(self, old_vbs, new_vbs):
        out_servers = set(old_vbs) - set(new_vbs)
        in_servers = set(new_vbs) - set(old_vbs)
        self.assertEqual(len(out_servers), len(in_servers),
                        "Seems like it wasn't swap rebalance. Out %s, in %s" % (
                                                len(out_servers), len(in_servers)))
        for vb_type in ["active_vb", "replica_vb"]:
            self.log.info("Checking %s on nodes that remain in cluster..." % vb_type)
            for server, stats in old_vbs.items():
                if server in new_vbs:
                    self.assertTrue(sorted(stats[vb_type]) == sorted(new_vbs[server][vb_type]),
                    "Server %s Seems like %s vbuckets were shuffled, old vbs is %s, new are %s" %(
                                    server.ip, vb_type, stats[vb_type], new_vbs[server][vb_type]))
            self.log.info("%s vbuckets were not suffled" % vb_type)
            self.log.info("Checking in-out nodes...")
            vbs_servs_out = vbs_servs_in = []
            for srv, stat in old_vbs.items():
                if srv in out_servers:
                    vbs_servs_out.extend(stat[vb_type])
            for srv, stat in new_vbs.items():
                if srv in in_servers:
                    vbs_servs_in.extend(stat[vb_type])
            self.assertTrue(sorted(vbs_servs_out) == sorted(vbs_servs_in),
                            "%s vbuckets seem to be suffled" % vb_type)

    def monitor_dcp_rebalance(self):
        """ released_upgrade_version """
        upgrade_version = ""
        if self.input.param('released_upgrade_version', None) is not None:
            upgrade_version = self.input.param('released_upgrade_version', None)[:5]
        else:
            upgrade_version = self.input.param('upgrade_version', '')[:5]
        if self.input.param('initial_version', '')[:5] in COUCHBASE_VERSION_2 and \
                                       (upgrade_version in COUCHBASE_VERSION_3 or \
                                        upgrade_version in SHERLOCK_VERSION):
            if int(self.initial_vbuckets) >= 512:
                if self.master.ip != self.rest.ip or \
                   self.master.ip == self.rest.ip and \
                   str(self.master.port) != str(self.rest.port):
                    if self.port:
                        self.master.port = self.port
                    self.rest = RestConnection(self.master)
                    self.rest_helper = RestHelper(self.rest)
                if self.rest._rebalance_progress_status() == 'running':
                    self.log.info("Start monitoring DCP upgrade from {0} to {1}"\
                           .format(self.input.param('initial_version', '')[:5], \
                                                                 upgrade_version))
                    status = self.rest.monitorRebalance()
                    if status:
                        self.log.info("Done DCP rebalance upgrade!")
                    else:
                        self.fail("Failed DCP rebalance upgrade")
                elif any ("DCP upgrade completed successfully.\n" \
                                    in list(d.values()) for d in self.rest.get_logs(10)):
                    self.log.info("DCP upgrade is completed")
                else:
                    self.fail("DCP reabalance upgrade is not running")
            else:
                self.fail("Need vbuckets setting >= 256 for upgrade from 2.x.x to 3+")
        else:
            if self.master.ip != self.rest.ip:
                self.rest = RestConnection(self.master)
                self.rest_helper = RestHelper(self.rest)
            self.log.info("No need to do DCP rebalance upgrade")

    def _offline_upgrade(self, skip_init=False):
        try:
            self.log.info("offline_upgrade")
            stoped_nodes = self.servers[:self.nodes_init]
            for upgrade_version in self.upgrade_versions:
                self.sleep(self.sleep_time, "Pre-setup of old version is done. "
                        " Wait for upgrade to {0} version".format(upgrade_version))
                for server in stoped_nodes:
                    remote = RemoteMachineShellConnection(server)
                    remote.stop_server()
                    remote.disconnect()
                self.sleep(self.sleep_time)
                upgrade_threads = self._async_update(upgrade_version, stoped_nodes,
                                                     None, skip_init)
                for upgrade_thread in upgrade_threads:
                    upgrade_thread.join()
                success_upgrade = True
                while not self.queue.empty():
                    success_upgrade &= self.queue.get()
                if not success_upgrade:
                    self.fail("Upgrade failed!")
                self.dcp_rebalance_in_offline_upgrade_from_version2()
            """ set install cb version to upgrade version after done upgrade """
            self.initial_version = self.upgrade_versions[0]
        except Exception as ex:
            self.log.info(ex)
            raise

    def _offline_failover_upgrade(self):
        try:
            self.log.info("offline_failover_upgrade")
            upgrade_version = self.upgrade_versions[0]
            upgrade_nodes = self.servers[:self.nodes_init]
            total_nodes = len(upgrade_nodes)
            for server in upgrade_nodes:
                self.rest.fail_over('ns_1@' + upgrade_nodes[total_nodes - 1].ip,
                                                                  graceful=True)
                self.sleep(timeout=60)
                self.rest.set_recovery_type('ns_1@' + upgrade_nodes[total_nodes - 1].ip,
                                                                                 "full")
                output, error = self._upgrade(upgrade_version,
                                              upgrade_nodes[total_nodes - 1],
                                              fts_query_limit=10000000)
                if "You have successfully installed Couchbase Server." not in output:
                    self.fail("Upgrade failed. See logs above!")
                self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
                total_nodes -= 1
            if total_nodes == 0:
                self.rest = RestConnection(upgrade_nodes[total_nodes])
        except Exception as ex:
            self.log.info(ex)
            raise

    def dcp_rebalance_in_offline_upgrade_from_version2(self):
        if self.input.param('initial_version', '')[:5] in COUCHBASE_VERSION_2 and \
           (self.input.param('upgrade_version', '')[:5] in COUCHBASE_VERSION_3 or \
            self.input.param('upgrade_version', '')[:5] in SHERLOCK_VERSION) and \
            self.input.param('num_stoped_nodes', self.nodes_init) >= self.nodes_init:
            otpNodes = []
            nodes = self.rest.node_statuses()
            for node in nodes:
                otpNodes.append(node.id)
            self.log.info("Start DCP rebalance after complete offline upgrade from {0} to {1}"\
                           .format(self.input.param('initial_version', '')[:5], \
                                   self.input.param('upgrade_version', '')[:5]))
            self.rest.rebalance(otpNodes, [])
            """ verify DCP upgrade in 3.0.0 version """
            self.monitor_dcp_rebalance()
        else:
            self.log.info("No need to do DCP rebalance upgrade")

    def generate_map_nodes_out_dist_upgrade(self, nodes_out_dist):
        self.nodes_out_dist = nodes_out_dist
        self.generate_map_nodes_out_dist()

    """ subdoc base test starts here """
    def generate_json_for_nesting(self):
        json = {
            "not_tested_integer_zero":0,
            "not_tested_integer_big":1038383839293939383938393,
            "not_tested_double_zero":0.0,
            "not_tested_integer":1,
            "not_tested_integer_negative":-1,
            "not_tested_double":1.1,
            "not_tested_double_negative":-1.1,
            "not_tested_float":2.99792458e8,
            "not_tested_float_negative":-2.99792458e8,
            "not_tested_array_numbers_integer" : [1, 2, 3, 4, 5],
            "not_tested_array_numbers_double" : [1.1, 2.2, 3.3, 4.4, 5.5],
            "not_tested_array_numbers_float" : [2.99792458e8, 2.99792458e8, 2.99792458e8],
            "not_tested_array_numbers_mix" : [0, 2.99792458e8, 1.1],
            "not_tested_array_array_mix" : [[2.99792458e8, 2.99792458e8, 2.99792458e8], [0, 2.99792458e8, 1.1], [], [0, 0, 0]],
            "not_tested_simple_string_lower_case":"abcdefghijklmnoprestuvxyz",
            "not_tested_simple_string_upper_case":"ABCDEFGHIJKLMNOPQRSTUVWXZYZ",
            "not_tested_simple_string_empty":"",
            "not_tested_simple_string_datetime":"2012-10-03 15:35:46.461491",
            "not_tested_simple_string_special_chars":"_-+!#@$%&*(){}\][;.,<>?/",
            "not_test_json" : { "not_to_bes_tested_string_field1": "not_to_bes_tested_string"}
        }
        return json

    def generate_simple_data_null(self):
        json = {
            "null":None,
            "null_array":[None, None]
        }
        return json

    def generate_simple_data_boolean(self):
        json = {
            "true":True,
            "false":False,
            "array":[True, False, True, False]
        }
        return json

    def generate_nested_json(self):
        json_data = self.generate_json_for_nesting()
        json = {
            "json_1": { "json_2": {"json_3":json_data}}
        }
        return json

    def generate_simple_data_numbers(self):
        json = {
            "integer_zero": 0,
            "integer_big": 1038383839293939383938393,
            "double_zero": 0.0,
            "integer": 1,
            "integer_negative": -1,
            "double": 1.1,
            "double_negative": -1.1,
            "float": 2.99792458e8,
            "float_negative": -2.99792458e8,
        }
        return json

    def subdoc_direct_client(self, server, bucket, timeout=30):
        # CREATE SDK CLIENT
        if self.use_sdk_client:
            try:
                from sdk_client import SDKClient
                scheme = "couchbase"
                host=self.master.ip
                if self.master.ip == "127.0.0.1":
                    scheme = "http"
                    host="{0}:{1}".format(self.master.ip, self.master.port)
                return SDKClient(scheme=scheme, hosts = [host], bucket = bucket.name)
            except Exception as ex:
                self.log.info("cannot load sdk client due to error {0}".format(str(ex)))
        # USE MC BIN CLIENT WHEN NOT USING SDK CLIENT
        return self.direct_mc_bin_client(server, bucket, timeout= timeout)
    """ subdoc base test ends here """

    def construct_plan_params(self):
        plan_params = {}
        plan_params['numReplicas'] = 0
        if self.index_replicas:
            plan_params['numReplicas'] = self.index_replicas
        if self.partitions_per_pindex:
            plan_params['maxPartitionsPerPIndex'] = self.partitions_per_pindex
        return plan_params

    def construct_cbft_query_json(self, query, fields=None, timeout=None):
        max_matches = TestInputSingleton.input.param("query_max_matches", 10000000)
        query_json = QUERY.JSON
        # query is a unicode dict
        query_json['query'] = query
        query_json['indexName'] = self.name
        if max_matches:
            query_json['size'] = int(max_matches)
        if timeout:
            query_json['timeout'] = int(timeout)
        if fields:
            query_json['fields'] = fields
        return query_json

    def generate_ops_docs(self, num_items, start=0):
        try:
            json_generator = JsonGenerator()
            if self.dataset == "simple":
                return self.generate_ops(num_items, start, json_generator.generate_docs_simple)
            if self.dataset == "sales":
                return self.generate_ops(num_items, start, json_generator.generate_docs_sales)
            if self.dataset == "employee" or self.dataset == "default":
                return self.generate_ops(num_items, start, json_generator.generate_docs_employee)
            if self.dataset == "sabre":
                return self.generate_ops(num_items, start, json_generator.generate_docs_sabre)
            if self.dataset == "bigdata":
                return self.generate_ops(num_items, start, json_generator.generate_docs_bigdata)
            if self.dataset == "array":
                return self.generate_ops(num_items, start, json_generator.generate_docs_array)
        except Exception as ex:
            self.log.info(ex)
            self.fail("There is no dataset %s, please enter a valid one" % self.dataset)

    def generate_ops(self, docs_per_day, start=0, method=None):
        gen_docs_map = {}
        for key in list(self.ops_dist_map.keys()):
            isShuffle = False
            if key == "update":
                isShuffle = True
            if self.dataset != "bigdata":
                gen_docs_map[key] = method(docs_per_day=self.ops_dist_map[key]["end"],
                    start=self.ops_dist_map[key]["start"])
            else:
                gen_docs_map[key] = method(value_size=self.value_size,
                    end=self.ops_dist_map[key]["end"],
                    start=self.ops_dist_map[key]["start"])
        return gen_docs_map

    def _convert_server_map(self, servers):
        map = {}
        for server in servers:
            key  = self._gen_server_key(server)
            map[key] = server
        return map

    def _gen_server_key(self, server):
        return "{0}:{1}".format(server.ip, server.port)

    def generate_docs_simple(self, num_items, start=0):
        from couchbase_helper.tuq_generators import JsonGenerator
        json_generator = JsonGenerator()
        return json_generator.generate_docs_simple(start=start, docs_per_day=self.docs_per_day)

    def generate_docs(self, num_items, start=0):
        try:
            if self.dataset == "simple":
                return self.generate_docs_simple(num_items, start)
            if self.dataset == "array":
                return self.generate_docs_array(num_items, start)
            return getattr(self, 'generate_docs_' + self.dataset)(num_items, start)
        except Exception as ex:
            log.info(str(ex))
            self.fail("There is no dataset %s, please enter a valid one" % self.dataset)

    def create_save_function_body_test(self, appname, appcode, description="Sample Description",
                                  checkpoint_interval=10000, cleanup_timers=False,
                                  dcp_stream_boundary="everything", deployment_status=True,
                                  skip_timer_threshold=86400,
                                  sock_batch_size=1, tick_duration=60000, timer_processing_tick_interval=500,
                                  timer_worker_pool_size=3, worker_count=3, processing_status=True,
                                  cpp_worker_thread_count=1, multi_dst_bucket=False, execution_timeout=3,
                                  data_chan_size=10000, worker_queue_cap=100000, deadline_timeout=6
                                  ):
        body = {}
        body['appname'] = appname
        script_dir = os.path.dirname(__file__)
        abs_file_path = os.path.join(script_dir, appcode)
        fh = open(abs_file_path, "r")
        body['appcode'] = fh.read()
        fh.close()
        body['depcfg'] = {}
        body['depcfg']['buckets'] = []
        body['depcfg']['buckets'].append({"alias": self.dst_bucket_name, "bucket_name": self.dst_bucket_name})
        if multi_dst_bucket:
            body['depcfg']['buckets'].append({"alias": self.dst_bucket_name1, "bucket_name": self.dst_bucket_name1})
        body['depcfg']['metadata_bucket'] = self.metadata_bucket_name
        body['depcfg']['source_bucket'] = self.src_bucket_name
        body['settings'] = {}
        body['settings']['checkpoint_interval'] = checkpoint_interval
        body['settings']['cleanup_timers'] = cleanup_timers
        body['settings']['dcp_stream_boundary'] = dcp_stream_boundary
        body['settings']['deployment_status'] = deployment_status
        body['settings']['description'] = description
        body['settings']['log_level'] = self.eventing_log_level
        body['settings']['skip_timer_threshold'] = skip_timer_threshold
        body['settings']['sock_batch_size'] = sock_batch_size
        body['settings']['tick_duration'] = tick_duration
        body['settings']['timer_processing_tick_interval'] = timer_processing_tick_interval
        body['settings']['timer_worker_pool_size'] = timer_worker_pool_size
        body['settings']['worker_count'] = worker_count
        body['settings']['processing_status'] = processing_status
        body['settings']['cpp_worker_thread_count'] = cpp_worker_thread_count
        body['settings']['execution_timeout'] = execution_timeout
        body['settings']['data_chan_size'] = data_chan_size
        body['settings']['worker_queue_cap'] = worker_queue_cap
        body['settings']['use_memory_manager'] = self.use_memory_manager
        if execution_timeout != 3:
            deadline_timeout = execution_timeout + 1
        body['settings']['deadline_timeout'] = deadline_timeout
        return body

    def _verify_backup_events_definition(self, bk_fxn):
        backup_path = self.backupset.directory + "/backup/{0}/".format(self.backups[0])
        events_file_name = "events.json"
        bk_file_events_dir = "/tmp/backup_events{0}/".format(self.master.ip)
        bk_file_events_path = bk_file_events_dir + events_file_name

        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        self.log.info("create local dir")
        if os.path.exists(bk_file_events_dir):
            shutil.rmtree(bk_file_events_dir)
        os.makedirs(bk_file_events_dir)
        self.log.info("copy eventing definition from remote to local")
        shell.copy_file_remote_to_local(backup_path+events_file_name,
                                        bk_file_events_path)
        local_bk_def = open(bk_file_events_path)
        bk_file_fxn = json.loads(local_bk_def.read())
        for k, v in bk_file_fxn[0]["settings"].items():
            if v != bk_fxn[0]["settings"][k]:
                self.log.info("key {0} has value not match".format(k))
                self.log.info("{0} : {1}".format(v, bk_fxn[0]["settings"][k]))
        self.log.info("remove tmp file in slave")
        if os.path.exists(bk_file_events_dir):
            shutil.rmtree(bk_file_events_dir)

    def _verify_restore_events_definition(self, bk_fxn):
        backup_path = self.backupset.directory + "/backup/{0}/".format(self.backups[0])
        events_file_name = "events.json"
        bk_file_events_dir = "/tmp/backup_events{0}/".format(self.master.ip)
        bk_file_events_path = bk_file_events_dir + events_file_name
        rest = RestConnection(self.backupset.restore_cluster_host)
        rs_fxn = rest.get_all_functions()

        if self.ordered(rs_fxn) != self.ordered(bk_fxn):
            self.fail("Events definition of backup and restore cluster are different")

    def ordered(self, obj):
        if isinstance(obj, dict):
            return sorted((k, ordered(v)) for k, v in list(obj.items()))
        if isinstance(obj, list):
            return sorted(ordered(x) for x in obj)
        else:
            return obj

    def create_fts_index_query_compare(self, queue=None):
        """
        Call before upgrade
        1. creates a default index, one per bucket
        2. Loads fts json data
        3. Runs queries and compares the results against ElasticSearch
        """
        try:
            self.fts_obj = FTSCallable(nodes=self.servers, es_validate=True)
            for bucket in self.buckets:
                self.fts_obj.create_default_index(
                    index_name="index_{0}".format(bucket.name),
                    bucket_name=bucket.name)
            self.fts_obj.load_data(self.num_items)
            self.fts_obj.wait_for_indexing_complete()
            for index in self.fts_obj.fts_indexes:
                self.fts_obj.run_query_and_compare(index=index, num_queries=20)
            return self.fts_obj
        except Exception as ex:
            print(ex)
            if queue is not None:
                queue.put(False)
        if queue is not None:
            queue.put(True)

    def update_delete_fts_data_run_queries(self, queue=None):
        """
        To call after (preferably) upgrade
        :param fts_obj: the FTS object created in create_fts_index_query_compare()
        """
        self.fts_obj.async_perform_update_delete()
        for index in self.fts_obj.fts_indexes:
            self.fts_obj.run_query_and_compare(index)

    def delete_all_fts_artifacts(self, queue=None):
        """
        Call during teardown of upgrade test
        :param fts_obj: he FTS object created in create_fts_index_query_compare()
        """
        self.fts_obj.delete_all()

    def run_fts_query_and_compare(self, queue=None):
        try:
            self.log.info("Verify fts via queries again")
            self.update_delete_fts_data_run_queries(self.fts_obj)
        except Exception as ex:
            print(ex)
            if queue is not None:
                queue.put(False)
        if queue is not None:
            queue.put(True)

    def update_indexes(self, queue=None):
        pre_upgrade_index_type = self.input.param("index_type", "upside_down")
        after_upgrade_index_type = self.input.param("after_upgrade_index_type", pre_upgrade_index_type)
        try:
            for index in self.fts_obj.fts_indexes:
                if index.is_scorch() and after_upgrade_index_type == "scorch":
                    self.log.info("Index type is scorch, now converting to upside_down... ")
                    index.update_index_to_upside_down()
                elif index.is_upside_down() and after_upgrade_index_type == "upside_down":
                    self.log.info("Index type is upside_down, now converting to scorch... ")
                    index.update_index_to_scorch()
                else:
                    self.fail("Index {0} which was {1} pre-upgrade is now automatically "
                              "converted to {2} post upgrade".
                              format(index.name,
                                     pre_upgrade_index_type,
                                     index.get_index_type()))
            self.fts_obj.wait_for_indexing_complete()
        except Exception as ex:
            print(ex)
            if queue is not None:
                queue.put(False)
        if queue is not None:
            queue.put(True)

    def modify_num_pindexes(self, queue=None):
        for index in self.fts_obj.fts_indexes:
            index.update_num_pindexes(8)
        self.fts_obj.wait_for_indexing_complete()

    def modify_num_replicas(self, queue=None):
        for index in self.fts_obj.fts_indexes:
            index.update_num_replicas(1)

    def update_index_to_scorch(self, queue=None):
        try:
            for index in self.fts_obj.fts_indexes:
                if index.is_upside_down() or index.is_type_unspecified():
                    self.log.info("Index type is {0}, now converting to scorch... ".format(index.get_index_type()))
                    index.update_index_to_scorch()
                else:
                    self.fail("FAIL: Index {0} was not meant to be of type {1}".
                              format(index.name,
                                     index.get_index_type()))
            self.fts_obj.wait_for_indexing_complete()
        except Exception as ex:
            print(ex)
            if queue is not None:
                queue.put(False)
        if queue is not None:
            queue.put(True)

    def delete_fts_indexes(self, queue=None):
        rest = RestConnection(self.servers[0])
        for index in self.fts_obj.fts_indexes:
            rest.delete_fts_index(index.name)

    def update_index_to_upside_down(self, queue=None):
        try:
            for index in self.fts_obj.fts_indexes:
                if index.is_scorch() or index.is_type_unspecified():
                    self.log.info("Index type is {0}, now converting to upside_down... ".format(index.get_index_type()))
                    index.update_index_to_upside_down()
                else:
                    self.fail("FAIL: Index {0} was not meant to be of type {1}".
                              format(index.name,
                                     index.get_index_type()))
            self.fts_obj.wait_for_indexing_complete()
        except Exception as ex:
            print(ex)
            if queue is not None:
                queue.put(False)
        if queue is not None:
            queue.put(True)

    def check_index_type(self, queue=None):
        check_index_type = self.input.param("check_index_type", None)
        try:
            for index in self.fts_obj.fts_indexes:
                type = index.get_index_type()
                if type == check_index_type:
                    self.log.info("SUCCESS: Index type check returns {0}".format(type))
                else:
                    self.fail("FAIL: Index {0} of type {1} is expected to be of type {2}".
                              format(index.name,
                                     type,
                                     check_index_type))
        except Exception as ex:
            print(ex)
            if queue is not None:
                queue.put(False)
        if queue is not None:
            queue.put(True)

    """ for cbas test """
    def load_sample_buckets(self, servers=None, bucketName=None, total_items=None,
                                                                        rest=None):
        """ Load the specified sample bucket in Couchbase """
        self.assertTrue(rest.load_sample(bucketName),
                        "Failure while loading sample bucket: {0}".format(bucketName))

        """ check for load data into travel-sample bucket """
        if total_items:
            import time
            end_time = time.time() + 600
            while time.time() < end_time:
                self.sleep(20)
                num_actual = 0
                if not servers:
                    num_actual = self.get_item_count(self.master, bucketName)
                else:
                    for server in servers:
                        num_actual += self.get_item_count(server, bucketName)
                if int(num_actual) == total_items:
                    self.log.info("{0} items are loaded in the {1} bucket"\
                                           .format(num_actual, bucketName))
                    break
                self.log.info("{0} items are loaded in the {1} bucket"\
                                        .format(num_actual, bucketName))
            if int(num_actual) != total_items:
                return False
        else:
            self.sleep(120)

        return True

    def execute_statement_on_cbas_via_rest(self, statement, mode=None, rest=None,
                                           timeout=120, client_context_id=None,
                                           username=None, password=None):
        """
        Executes a statement on CBAS using the REST API using REST Client
        """
        pretty = "true"
        if not rest:
            rest = RestConnection(self.cbas_node)
        try:
            self.log.info("Running query on cbas: {0}".format(statement))
            response = rest.execute_statement_on_cbas(statement, mode, pretty,
                                                      timeout, client_context_id,
                                                      username, password)
            response = json.loads(response)
            if "errors" in response:
                errors = response["errors"]
            else:
                errors = None

            if "results" in response:
                results = response["results"]
            else:
                results = None

            if "handle" in response:
                handle = response["handle"]
            else:
                handle = None

            return response["status"], response[
                "metrics"], errors, results, handle

        except Exception as e:
            raise Exception(str(e))

    def create_bucket_on_cbas(self, cbas_bucket_name, cb_bucket_name,
                              cb_server_ip=None,
                              validate_error_msg=False,
                              username = None, password = None):
        """
        Creates a bucket on CBAS
        """
        if cb_server_ip:
            cmd_create_bucket = "create bucket " + cbas_bucket_name + \
                              " with {\"name\":\"" + cb_bucket_name + \
                              "\",\"nodes\":\"" + cb_server_ip + "\"};"
        else:
            '''DP3 doesn't need to specify cb server ip as cbas node is
               part of the cluster.
            '''
            cmd_create_bucket = "create bucket " + cbas_bucket_name + \
                            " with {\"name\":\"" + cb_bucket_name + "\"};"
        status, metrics, errors, results, _ = \
                   self.execute_statement_on_cbas_via_rest(cmd_create_bucket,
                                                           username=username,
                                                           password=password)

        if validate_error_msg:
            return self.validate_error_in_response(status, errors)
        else:
            if status != "success":
                return False
            else:
                return True

    def create_dataset_on_bucket(self, cbas_bucket_name, cbas_dataset_name,
                                 where_field=None, where_value = None,
                                 validate_error_msg=False, username = None,
                                 password = None):
        """
        Creates a shadow dataset on a CBAS bucket
        """
        cmd_create_dataset = "create shadow dataset {0} on {1};".format(
                                     cbas_dataset_name, cbas_bucket_name)
        if where_field and where_value:
            cmd_create_dataset = "create shadow dataset {0} on {1} WHERE `{2}`=\"{3}\";"\
                                              .format(cbas_dataset_name, cbas_bucket_name,
                                                      where_field, where_value)
        status, metrics, errors, results, _ = \
                        self.execute_statement_on_cbas_via_rest(cmd_create_dataset,
                                                                username=username,
                                                                password=password)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors)
        else:
            if status != "success":
                return False
            else:
                return True

    def test_create_dataset_on_bucket(self):
        # Create bucket on CBAS
        self.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                                   cb_bucket_name=self.cb_bucket_name,
                                   cb_server_ip=self.cb_server_ip)

        # Create dataset on the CBAS bucket
        result = self.create_dataset_on_bucket(
            cbas_bucket_name=self.cbas_bucket_name_invalid,
            cbas_dataset_name=self.cbas_dataset_name,
            validate_error_msg=self.validate_error)
        if not result:
            self.fail("FAIL : Actual error msg does not match the expected")
