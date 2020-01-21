import copy, json
import re
import testconstants
import gc
import sys
import traceback
import queue
from threading import Thread
from basetestcase import BaseTestCase
from mc_bin_client import MemcachedError
from memcached.helper.data_helper import VBucketAwareMemcached, MemcachedClientHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.api.rest_client import RestConnection, RestHelper, Bucket
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from couchbase_helper.document import DesignDocument, View
from couchbase_helper.documentgenerator import BlobGenerator
from query_tests_helper import QueryHelperTests
from couchbase_helper.tuq_helper import N1QLHelper
from scripts.install import InstallerJob
from builds.build_query import BuildQuery
from eventing.eventing_base import EventingBaseTest
from pytests.eventing.eventing_constants import HANDLER_CODE
from random import randrange, randint
from fts.fts_base import FTSIndex, FTSBaseTest
from pytests.fts.fts_callable import FTSCallable
from cbas.cbas_base import CBASBaseTest
from pprint import pprint
from testconstants import CB_REPO
from testconstants import MV_LATESTBUILD_REPO
from testconstants import SHERLOCK_BUILD_REPO
from testconstants import COUCHBASE_VERSION_2
from testconstants import COUCHBASE_VERSION_3
from testconstants import COUCHBASE_VERSIONS
from testconstants import SHERLOCK_VERSION
from testconstants import CB_VERSION_NAME
from testconstants import COUCHBASE_MP_VERSION
from testconstants import CE_EE_ON_SAME_FOLDER
from testconstants import STANDARD_BUCKET_PORT

class NewUpgradeBaseTest(QueryHelperTests, EventingBaseTest, FTSBaseTest):
    def setUp(self):
        super(NewUpgradeBaseTest, self).setUp()
        self.released_versions = ["2.0.0-1976-rel", "2.0.1", "2.5.0", "2.5.1",
                                  "2.5.2", "3.0.0", "3.0.1",
                                  "3.0.1-1444", "3.0.2", "3.0.2-1603", "3.0.3",
                                  "3.1.0", "3.1.0-1776", "3.1.1", "3.1.1-1807",
                                  "3.1.2", "3.1.2-1815", "3.1.3", "3.1.3-1823",
                                  "4.0.0", "4.0.0-4051", "4.1.0", "4.1.0-5005"]
        self.use_hostnames = self.input.param("use_hostnames", False)
        self.product = self.input.param('product', 'couchbase-server')
        self.initial_version = self.input.param('initial_version', '2.5.1-1083')
        self.initial_vbuckets = self.input.param('initial_vbuckets', 1024)
        self.upgrade_versions = self.input.param('upgrade_version', '4.1.0-4963')
        self.upgrade_versions = self.upgrade_versions.split(";")
        self.skip_cleanup = self.input.param("skip_cleanup", False)
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
        self.upgrade_build_type = self.input.param('upgrade_build_type', self.initial_build_type)
        self.stop_persistence = self.input.param('stop_persistence', False)
        self.rest_settings = self.input.membase_settings
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
        self.ddocs_num = self.input.param("ddocs-num", 0)
        self.view_num = self.input.param("view-per-ddoc", 2)
        self.is_dev_ddoc = self.input.param("is-dev-ddoc", False)
        self.offline_failover_upgrade = self.input.param("offline_failover_upgrade", False)
        self.eventing_log_level = self.input.param('eventing_log_level', 'INFO')
        self.src_bucket_name = self.input.param('src_bucket_name', 'src_bucket')
        self.dst_bucket_name = self.input.param('dst_bucket_name', 'dst_bucket')
        self.dst_bucket_name1 = self.input.param('dst_bucket_name1', 'dst_bucket1')
        self.metadata_bucket_name = self.input.param('metadata_bucket_name', 'metadata')
        self.cb_bucket_name = self.input.param('cb_bucket_name', 'travel-sample')
        self.cb_server_ip = self.input.param("cb_server_ip", None)
        self.cbas_bucket_name = self.input.param('cbas_bucket_name', 'travel')
        self.cbas_dataset_name = self.input.param("cbas_dataset_name", 'travel_ds')
        self.cbas_dataset_name_invalid = self.input.param('cbas_dataset_name_invalid',
                                                                self.cbas_dataset_name)
        self.cbas_bucket_name_invalid = self.input.param('cbas_bucket_name_invalid',
                                                                 self.cbas_bucket_name)
        self.use_memory_manager = self.input.param('use_memory_manager', True)
        self.is_fts_in_pre_upgrade = self.input.param('is_fts_in_pre_upgrade', False)
        self.num_index_replicas = self.input.param("num_index_replica", 0)
        self.expected_err_msg = self.input.param("expected_err_msg", None)
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
        if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
            if self.initial_version != self.upgrade_versions:
                self.log.warn(
                    "we can't upgrade from couchbase CE to EE with a different version,defaulting to initial_version")
                self.log.warn("http://developer.couchbase.com/documentation/server/4.0/install/upgrading.html")
                self.upgrade_versions = self.input.param('initial_version', '4.1.0-4963')
                self.upgrade_versions = self.upgrade_versions.split(";")
        self.fts_obj = None
        self.n1ql_helper = None
        self.index_name_prefix = None
        self.flusher_batch_split_trigger = self.input.param("flusher_batch_split_trigger", None)

    def tearDown(self):
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
                self._install(self.upgrade_servers, version=self.initial_version)
        self.sleep(20, "sleep 20 seconds before run next test")

    def _install(self, servers, version=None, community_to_enterprise=False):
        params = {}
        params['num_nodes'] = len(servers)
        params['product'] = self.product
        params['version'] = self.initial_version
        params['vbuckets'] = [self.initial_vbuckets]
        params['init_nodes'] = self.init_nodes
        if 5 <= int(self.initial_version[:1]) or 5 <= int(self.upgrade_versions[0][:1]):
            params['fts_query_limit'] = 10000000
        if version:
            params['version'] = version
        if self.initial_build_type is not None:
            params['type'] = self.initial_build_type
        if community_to_enterprise:
            params['type'] = self.upgrade_build_type
        self.log.info("will install {0} on {1}".format(params['version'], [s.ip for s in servers]))
        InstallerJob().parallel_install(servers, params)
        self.add_built_in_server_user()
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

    def operations(self, servers, services=None):
        if services is not None:
            if "-" in services:
                set_services = services.split("-")
            else:
                set_services = services.split(",")
        else:
            set_services = services

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
        self.sleep(20, "wait to make sure node is ready")
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
#        if not self.total_buckets:
#            self.total_buckets = 1
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
        if self.is_fts_in_pre_upgrade:
            self.create_fts_index_query_compare()
        else:
            self._load_all_buckets(self.master, gen_load, "create", self.expire_time,
                                                             flag=self.item_flag)
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

        if self.upgrade_build_type == "community":
            edition_type = "couchbase-server-community"
        else:
            edition_type = "couchbase-server-enterprise"

        builds, changes = BuildQuery().get_all_builds(version=version, timeout=self.wait_timeout * 5, \
                                                      deliverable_type=info.deliverable_type,
                                                      architecture_type=info.architecture_type, \
                                                      edition_type=edition_type, repo=build_repo, \
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
                            msg="unable to find build {0}".format(upgrade_version))
            self.assertTrue(remote.download_build(appropriate_build),
                                          "Build wasn't downloaded!")
            o, e = remote.couchbase_upgrade(appropriate_build,\
                                            save_upgrade_config=save_upgrade_config,\
                                            forcefully=self.is_downgrade,
                                            fts_query_limit=fts_query_limit, debug_logs=debug_logs)
            self.log.info("upgrade {0} to version {1} is completed".format(server.ip, upgrade_version))
            """ remove this line when bug MB-11807 fixed """
            if self.is_ubuntu:
                remote.start_server()
            """ remove end here """
            if 5.0 > float(self.initial_version[:3]) and self.is_centos7:
                remote.execute_command("systemctl daemon-reload")
                remote.start_server()
            self.rest = RestConnection(server)
            if self.is_linux:
                self.wait_node_restarted(server, wait_time=testconstants.NS_SERVER_TIMEOUT * 4, wait_if_warmup=True)
            else:
                self.wait_node_restarted(server, wait_time=testconstants.NS_SERVER_TIMEOUT * 10, wait_if_warmup=True, check_service=True)
            if not skip_init:
                self.rest.init_cluster(self.rest_settings.rest_username, self.rest_settings.rest_password)
            self.sleep(self.sleep_time)
            remote.disconnect()
            self.sleep(10)
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

    def _async_update(self, upgrade_version, servers, queue=None, skip_init=False,
                      info=None, save_upgrade_config=False,
                      fts_query_limit=None, debug_logs=False):
        self.log.info("servers {0} will be upgraded to {1} version".
                      format([server.ip for server in servers], upgrade_version))
        q = queue or self.queue
        upgrade_threads = []
        for server in servers:
            upgrade_thread = Thread(target=self._upgrade,
                                    name="upgrade_thread" + server.ip,
                                    args=(upgrade_version, server, q, skip_init, info,
                                          save_upgrade_config, fts_query_limit,
                                          debug_logs))
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

    def create_ddocs_and_views(self, server=None):
        server_in_cluster = self.master
        if server is not None:
            self.buckets = RestConnection(server).get_buckets()
            server_in_cluster = server
        self.default_view = View(self.default_view_name, None, None)
        for bucket in self.buckets:
            for i in range(int(self.ddocs_num)):
                views = self.make_default_views(self.default_view_name, self.view_num,
                                               self.is_dev_ddoc, different_map=True)
                ddoc = DesignDocument(self.default_view_name + str(i), views)
                self.ddocs.append(ddoc)
                for view in views:
                    self.cluster.create_view(server_in_cluster, ddoc.name,
                                             view, bucket=bucket)

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
        if self.input.param('initial_version', '')[:5] in COUCHBASE_VERSION_2 and \
           (self.input.param('upgrade_version', '')[:5] in COUCHBASE_VERSION_3 or \
            self.input.param('upgrade_version', '')[:5] in SHERLOCK_VERSION):
            if int(self.initial_vbuckets) >= 256:
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
                                    self.input.param('upgrade_version', '')[:5]))
                    status = self.rest.monitorRebalance()
                    if status:
                        self.log.info("Done DCP rebalance upgrade!")
                    else:
                        self.fail("Failed DCP rebalance upgrade")
                elif self.sleep(5) is None and any ("DCP upgrade completed successfully." \
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

    def pre_upgrade(self, servers):
        if self.rest is None:
            self._new_master(self.master)
        self.ddocs_num = 0
        self.create_ddocs_and_views()
        verify_data = False
        if self.scan_consistency != "request_plus":
            verify_data = True
        self.load(self.gens_load, flag=self.item_flag,
                  verify_data=verify_data, batch_size=self.batch_size)
        rest = RestConnection(servers[0])
        output, rq_content, header = rest.set_auto_compaction(dbFragmentThresholdPercentage=20, viewFragmntThresholdPercentage=20)
        self.assertTrue(output, "Error in set_auto_compaction... {0}".format(rq_content))
        status, content, header = rest.set_indexer_compaction(mode="full", fragmentation=20)
        self.assertTrue(status, "Error in setting Append Only Compaction... {0}".format(content))
        operation_type = self.input.param("pre_upgrade", "")
        self.run_async_index_operations(operation_type)

    def during_upgrade(self, servers):
        self.ddocs_num = 0
        self.create_ddocs_and_views()
        kv_tasks = self.async_run_doc_ops()
        operation_type = self.input.param("during_upgrade", "")
        self.run_async_index_operations(operation_type)
        for task in kv_tasks:
            task.result()

    def post_upgrade(self, servers):
        self.log.info(" Doing post upgrade")
        self.ddocs_num = 0
        self.add_built_in_server_user()
        self.create_ddocs_and_views(servers[0])
        kv_tasks = self.async_run_doc_ops()
        operation_type = self.input.param("post_upgrade", "")
        self.run_async_index_operations(operation_type)
        for task in kv_tasks:
            task.result()
        self.verification(servers, check_items=False)

    def _create_ephemeral_buckets(self):
        create_ephemeral_buckets = self.input.param(
            "create_ephemeral_buckets", False)
        if not create_ephemeral_buckets:
            return
        rest = RestConnection(self.master)
        versions = rest.get_nodes_versions()
        for version in versions:
            if "5" > version:
                self.log.info("Atleast one of the nodes in the cluster is "
                              "pre 5.0 version. Hence not creating ephemeral"
                              "bucket for the cluster.")
                return
        num_ephemeral_bucket = self.input.param("num_ephemeral_bucket", 1)
        server = self.master
        server_id = RestConnection(server).get_nodes_self().id
        ram_size = RestConnection(server).get_nodes_self().memoryQuota
        bucket_size = self._get_bucket_size(ram_size, self.bucket_size +
                                                      num_ephemeral_bucket)
        self.log.info("Creating ephemeral buckets")
        self.log.info("Changing the existing buckets size to accomodate new "
                      "buckets")
        for bucket in self.buckets:
            rest.change_bucket_props(bucket, ramQuotaMB=bucket_size)

        bucket_tasks = []
        bucket_params = copy.deepcopy(
            self.bucket_base_params['membase']['non_ephemeral'])
        bucket_params['size'] = bucket_size
        bucket_params['bucket_type'] = 'ephemeral'
        bucket_params['eviction_policy'] = 'noEviction'
        ephemeral_buckets = []
        self.log.info("Creating ephemeral buckets now")
        for i in range(num_ephemeral_bucket):
            name = 'ephemeral_bucket' + str(i)
            port = STANDARD_BUCKET_PORT + i + 1
            bucket_priority = None
            if self.standard_bucket_priority is not None:
                bucket_priority = self.get_bucket_priority(
                    self.standard_bucket_priority[i])

            bucket_params['bucket_priority'] = bucket_priority
            bucket_tasks.append(
                self.cluster.async_create_standard_bucket(name=name, port=port,
                                                          bucket_params=bucket_params))
            bucket = Bucket(name=name, authType=None, saslPassword=None,
                            num_replicas=self.num_replicas,
                            bucket_size=self.bucket_size,
                            port=port, master_id=server_id,
                            eviction_policy='noEviction', lww=self.lww)
            self.buckets.append(bucket)
            ephemeral_buckets.append(bucket)

        for task in bucket_tasks:
            task.result(self.wait_timeout * 10)

        if self.enable_time_sync:
            self._set_time_sync_on_buckets(
                ['standard_bucket' + str(i) for i in range(
                    num_ephemeral_bucket)])
        load_gen = BlobGenerator('upgrade', 'upgrade-', self.value_size,
                                 end=self.num_items)
        for bucket in ephemeral_buckets:
            self._load_bucket(bucket, self.master, load_gen, "create",
                              self.expire_time)

    def _return_maps(self):
        index_map = self.get_index_map()
        stats_map = self.get_index_stats(perNode=False)
        return index_map, stats_map

    def create_fts_index(self):
        try:
            self.log.info("Checking if index already exists ...")
            name = "default"
            """ test on one bucket """
            for bucket in self.buckets:
                name = bucket.name
                break
            SOURCE_CB_PARAMS = {
                      "authUser": "default",
                      "authPassword": "",
                      "authSaslUser": "",
                      "authSaslPassword": "",
                      "clusterManagerBackoffFactor": 0,
                      "clusterManagerSleepInitMS": 0,
                      "clusterManagerSleepMaxMS": 20000,
                      "dataManagerBackoffFactor": 0,
                      "dataManagerSleepInitMS": 0,
                      "dataManagerSleepMaxMS": 20000,
                      "feedBufferSizeBytes": 0,
                      "feedBufferAckThreshold": 0
                       }
            self.index_type = 'fulltext-index'
            self.index_definition = {
                          "type": "fulltext-index",
                          "name": "",
                          "uuid": "",
                          "params": {},
                          "sourceType": "couchbase",
                          "sourceName": "",
                          "sourceUUID": "",
                          "sourceParams": SOURCE_CB_PARAMS,
                          "planParams": {}
                          }
            self.name = self.index_definition['name'] = \
                                self.index_definition['sourceName'] = name
            fts_node = self.get_nodes_from_services_map("fts", \
                                servers=self.get_nodes_in_cluster_after_upgrade())
            if fts_node:
                rest = RestConnection(fts_node)
                status, _ = rest.get_fts_index_definition(self.name)
                if status != 400:
                    rest.delete_fts_index(self.name)
                self.log.info("Creating {0} {1} on {2}".format(self.index_type,
                                                           self.name, rest.ip))
                rest.create_fts_index(self.name, self.index_definition)
            else:
                raise("No FTS node in cluster")
            self.ops_dist_map = self.calculate_data_change_distribution(
                create_per=self.create_ops_per, update_per=self.update_ops_per,
                delete_per=self.delete_ops_per, expiry_per=self.expiry_ops_per,
                start=0, end=self.docs_per_day)
            self.log.info(self.ops_dist_map)
            self.dataset = "simple"
            self.docs_gen_map = self.generate_ops_docs(self.docs_per_day, 0)
            self.async_ops_all_buckets(self.docs_gen_map, batch_size=100)
        except Exception as ex:
            self.log.info(ex)

    def get_nodes_in_cluster_after_upgrade(self, master_node=None):
        rest = None
        if master_node == None:
            rest = RestConnection(self.master)
        else:
            rest = RestConnection(master_node)
        nodes = rest.node_statuses()
        server_set = []
        for node in nodes:
            for server in self.input.servers:
                if server.ip == node.ip:
                    server_set.append(server)
        return server_set


    def create_eventing_services(self):
        """ Only work after cluster upgrade to 5.5.0 completely """
        try:
            rest = RestConnection(self.master)
            cb_version = rest.get_nodes_version()
            if 5.5 > float(cb_version[:3]):
                self.log.info("This eventing test is only for cb version 5.5 and later.")
                return

            bucket_params = self._create_bucket_params(server=self.master, size=128,
                                                       replicas=self.num_replicas)
            self.cluster.create_standard_bucket(name=self.src_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.buckets = RestConnection(self.master).get_buckets()
            self.src_bucket = RestConnection(self.master).get_buckets()
            self.cluster.create_standard_bucket(name=self.dst_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.cluster.create_standard_bucket(name=self.metadata_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.buckets = RestConnection(self.master).get_buckets()
            self.gens_load = self.generate_docs(self.docs_per_day)
            self.expiry = 3

            self.restServer = self.get_nodes_from_services_map(service_type="eventing")
            """ must be self.rest to pass in deploy_function"""
            self.rest = RestConnection(self.restServer)

            self.load(self.gens_load, buckets=self.buckets, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
            function_name = "Function_{0}_{1}".format(randint(1, 1000000000), self._testMethodName)
            self.function_name = function_name[0:90]
            body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE, worker_count=3)
            bk_events_created = False
            rs_events_created = False
            try:
                self.deploy_function(body)
                bk_events_created = True
                self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
            except Exception as e:
                self.log.error(e)
            finally:
                self.undeploy_and_delete_function(body)
        except Exception as e:
            self.log.info(e)

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

    def create_fts_index_query_compare(self):
        """
        Call before upgrade
        1. creates a default index, one per bucket
        2. Loads fts json data
        3. Runs queries and compares the results against ElasticSearch
        """
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

    def update_delete_fts_data_run_queries(self, fts_obj):
        """
        To call after (preferably) upgrade
        :param fts_obj: the FTS object created in create_fts_index_query_compare()
        """
        fts_obj.async_perform_update_delete()
        for index in fts_obj.fts_indexes:
            fts_obj.run_query_and_compare(index)

    def delete_all_fts_artifacts(self, fts_obj):
        """
        Call during teardown of upgrade test
        :param fts_obj: he FTS object created in create_fts_index_query_compare()
        """
        fts_obj.delete_all()

    def run_fts_query_and_compare(self):
        try:
            self.log.info("Verify fts via queries again")
            self.update_delete_fts_data_run_queries(self.fts_obj)
        except Exception as ex:
            print(ex)

    """ for cbas test """
    def load_sample_buckets(self, servers=None, bucketName=None,
                                  total_items=None, rest=None):
        """ Load the specified sample bucket in Couchbase """
        self.assertTrue(rest.load_sample(bucketName),
                        "Failure while loading sample bucket: {0}".format(bucketName))

        """ check for load data into travel-sample bucket """
        if total_items:
            import time
            end_time = time.time() + 180
            while time.time() < end_time:
                self.sleep(20)
                num_actual = 0
                if not servers:
                    num_actual = self.get_item_count(self.master, bucketName)
                else:
                    bucket_maps = RestConnection(servers[0]).get_buckets_itemCount()
                    num_actual = bucket_maps[bucketName]
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

    def execute_statement_on_cbas_via_rest(self, statement, mode=None, rest=None, timeout=120,\
                                             client_context_id=None, username=None, password=None):
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
            '''DP3 doesn't need to specify cb server ip as cbas node is part of the cluster.'''
            cmd_create_bucket = "create bucket " + cbas_bucket_name + \
                            " with {\"name\":\"" + cb_bucket_name + "\"};"
        status, metrics, errors, results, _ = \
                   self.execute_statement_on_cbas_via_rest(cmd_create_bucket, username=username,
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

    def create_replica_index(self):
        self.log.info("create_index")
        self.index_list = {}
        self._initialize_n1ql_helper()
        try:
            self.n1ql_helper.create_primary_index(using_gsi = True,
                                               server = self.n1ql_server)
            self.log.info("done create_index")
        except Exception as e:
            self.log.info(e)

    def create_index_with_replica_and_query(self):
        """ ,groups=simple,reset_services=True """
        self.log.info("Create index with replica and query")
        self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
        self._initialize_n1ql_helper()
        self.index_name_prefix = "random_index_" + str(randint(100000, 999999))
        create_index_query = "CREATE INDEX " + self.index_name_prefix + \
              " ON default(age) USING GSI WITH {{'num_replica': {0}}};"\
                                        .format(self.num_index_replicas)
        try:
            self.create_replica_index()
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as e:
            self.log.info(e)
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([self.index_name_prefix],
                                                    index_map,
                                                    self.num_index_replicas)

    def verify_index_with_replica_and_query(self):
        index_map = self.get_index_map()
        try:
            self.n1ql_helper.verify_replica_indexes([self.index_name_prefix],
                                                    index_map,
                                                    self.num_index_replicas)
        except Exception as e:
            self.log.info(e)

    def _initialize_n1ql_helper(self):
        if self.n1ql_helper == None:
            self.n1ql_server = self.get_nodes_from_services_map(service_type = \
                                              "n1ql", servers=self.input.servers)
            self.n1ql_helper = N1QLHelper(version = "sherlock", shell = None,
                use_rest = True, max_verify = self.max_verify,
                buckets = self.buckets, item_flag = None,
                n1ql_port = self.n1ql_server.n1ql_port, full_docs_list = [],
                log = self.log, input = self.input, master = self.master)
