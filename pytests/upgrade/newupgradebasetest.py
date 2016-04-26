import re
import testconstants
import gc
import sys
import traceback
import Queue
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
        self.released_versions = ["2.0.0-1976-rel", "2.0.1", "2.5.0", "2.5.1",
                                  "2.5.2", "3.0.0", "3.0.1",
                                  "3.0.1-1444", "3.0.2", "3.0.2-1603", "3.0.3",
                                  "3.1.0", "3.1.1", "3.1.2", "3.1.3"]
        self.use_hostnames = self.input.param("use_hostnames", False)
        self.product = self.input.param('product', 'couchbase-server')
        self.initial_version = self.input.param('initial_version', '2.5.1-1083')
        self.initial_vbuckets = self.input.param('initial_vbuckets', 1024)
        self.upgrade_versions = self.input.param('upgrade_version', '2.0.1-170-rel')
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
        self.stop_persistence = self.input.param('stop_persistence', False)
        self.std_vbucket_dist = self.input.param("std_vbucket_dist", None)
        self.rest_settings = self.input.membase_settings
        self.rest = None
        self.rest_helper = None
        self.is_ubuntu = False
        self.sleep_time = 15
        self.ddocs = []
        self.item_flag = self.input.param('item_flag', 0)
        self.expire_time = self.input.param('expire_time', 0)
        self.wait_expire = self.input.param('wait_expire', False)
        self.default_view_name = "upgrade-test-view"
        self.ddocs_num = self.input.param("ddocs_num", 1)
        self.view_num = self.input.param("view_per_ddoc", 2)
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
        if type.lower() == "ubuntu":
            self.is_ubuntu = True
        self.queue = Queue.Queue()
        self.upgrade_servers = []

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
            except Exception, e:
                if e:
                    print "Exception ", e
                self.cluster.shutdown(force=True)
                self.fail(e)
            super(NewUpgradeBaseTest, self).tearDown()
            if self.upgrade_servers:
                self._install(self.upgrade_servers,version=self.initial_version)
        self.sleep(20, "sleep 20 seconds before run next test")

    def _install(self, servers):
        params = {}
        params['num_nodes'] = len(servers)
        params['product'] = self.product
        params['version'] = self.initial_version
        params['vbuckets'] = [self.initial_vbuckets]
        params['init_nodes'] = self.init_nodes
        if self.initial_build_type is not None:
            params['type'] = self.initial_build_type
        self.log.info("will install {0} on {1}".format(self.initial_version, [s.ip for s in servers]))
        InstallerJob().parallel_install(servers, params)
        if self.product in ["couchbase", "couchbase-server", "cb"]:
            success = True
            for server in servers:
                success &= RemoteMachineShellConnection(server).is_couchbase_installed()
                if not success:
                    sys.exit("some nodes were not install successfully!")
        if self.rest is None:
            self._new_master(self.master)
        if self.use_hostnames:
            for server in self.servers[:self.nodes_init]:
                hostname = RemoteUtilHelper.use_hostname_for_server_settings(server)
                server.hostname = hostname

    def operations(self, servers):
        self.quota = self._initialize_nodes(self.cluster, servers, self.disabled_consistent_view,
                                            self.rebalanceIndexWaitingDisabled, self.rebalanceIndexPausingDisabled,
                                            self.maxParallelIndexers, self.maxParallelReplicaIndexers, self.port)
        if self.port and self.port != '8091':
            self.rest = RestConnection(self.master)
            self.rest_helper = RestHelper(self.rest)
        if len(servers) > 1:
            self.cluster.rebalance([servers[0]], servers[1:], [],
                                   use_hostnames=self.use_hostnames)

        self.buckets = []
        gc.collect()
        if self.input.param('extra_verification', False):
            self.total_buckets += 2
            print self.total_buckets
        self.bucket_size = self._get_bucket_size(self.quota, self.total_buckets)
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

    def _upgrade(self, upgrade_version, server, queue=None, skip_init=False, info=None):
        try:
            remote = RemoteMachineShellConnection(server)
            appropriate_build = self._get_build(server, upgrade_version, remote, info=info)
            self.assertTrue(appropriate_build.url, msg="unable to find build {0}".format(upgrade_version))
            self.assertTrue(remote.download_build(appropriate_build), "Build wasn't downloaded!")
            o, e = remote.couchbase_upgrade(appropriate_build, save_upgrade_config=False, forcefully=self.is_downgrade)
            self.log.info("upgrade {0} to version {1} is completed".format(server.ip, upgrade_version))
            """ remove this line when bug MB-11807 fixed """
            if self.is_ubuntu:
                remote.start_server()
            """ remove end here """
            remote.disconnect()
            self.sleep(10)
            if self.is_linux:
                self.wait_node_restarted(server, wait_time=testconstants.NS_SERVER_TIMEOUT * 4, wait_if_warmup=True)
            else:
                self.wait_node_restarted(server, wait_time=testconstants.NS_SERVER_TIMEOUT * 10, wait_if_warmup=True, check_service=True)
            if not skip_init:
                self.rest.init_cluster(self.rest_settings.rest_username, self.rest_settings.rest_password)
            self.sleep(self.sleep_time)
            return o, e
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
                except Exception, e:
                    print e
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
                for i in xrange(int(self.ddocs_num)):
                    views = self.make_default_views(self.default_view_name,
                            self.view_num, self.is_dev_ddoc, different_map=True)
                    ddoc = DesignDocument(self.default_view_name + str(i), views)
                    self.ddocs.append(ddoc)
                    for view in views:
                        try:
                            self.cluster.create_view(self.master, ddoc.name, view,
                                                                   bucket=bucket)
                        except Exception, e:
                            print e
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
                except MemcachedError, e:
                    print e
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
            except Exception, e:
                print traceback.extract_stack()
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
                                                len(out_servers),len(in_servers)))
        for vb_type in ["active_vb", "replica_vb"]:
            self.log.info("Checking %s on nodes that remain in cluster..." % vb_type)
            for server, stats in old_vbs.iteritems():
                if server in new_vbs:
                    self.assertTrue(sorted(stats[vb_type]) == sorted(new_vbs[server][vb_type]),
                    "Server %s Seems like %s vbuckets were shuffled, old vbs is %s, new are %s" %(
                                    server.ip, vb_type, stats[vb_type], new_vbs[server][vb_type]))
            self.log.info("%s vbuckets were not suffled" % vb_type)
            self.log.info("Checking in-out nodes...")
            vbs_servs_out = vbs_servs_in = []
            for srv, stat in old_vbs.iteritems():
                if srv in out_servers:
                    vbs_servs_out.extend(stat[vb_type])
            for srv, stat in new_vbs.iteritems():
                if srv in in_servers:
                    vbs_servs_in.extend(stat[vb_type])
            self.assertTrue(sorted(vbs_servs_out) == sorted(vbs_servs_in),
                            "%s vbuckets seem to be suffled" % vb_type)

    def monitor_dcp_rebalance(self):
        if self.input.param('initial_version', '')[:5] in COUCHBASE_VERSION_2 and \
           (self.input.param('upgrade_version', '')[:5] in COUCHBASE_VERSION_3 or \
            self.input.param('upgrade_version', '')[:5] in SHERLOCK_VERSION):
            if int(self.initial_vbuckets) >= 512:
                if self.master.ip != self.rest.ip or \
                   self.master.ip == self.rest.ip and \
                   str(self.master.port) != str(self.rest.port):
                    if self.port:
                        self.master.port = self.port
                    self.rest = RestConnection(self.master)
                    self.rest_helper = RestHelper(self.rest)
                if self.rest._rebalance_progress_status() == 'running':
                    self.log.info("Start monitoring DCP rebalance upgrade from {0} to {1}"\
                                  .format(self.input.param('initial_version', '')[:5], \
                                   self.input.param('upgrade_version', '')[:5]))
                    status = self.rest.monitorRebalance()
                else:
                    self.fail("DCP reabalance upgrade is not running")

                if status:
                    self.log.info("Done DCP rebalance upgrade!")
                else:
                    self.fail("Failed DCP rebalance upgrade")
            else:
                self.fail("Need vbuckets setting >= 256 for upgrade from 2.x.x to 3.x.x")
        else:
            if self.master.ip != self.rest.ip:
                self.rest = RestConnection(self.master)
                self.rest_helper = RestHelper(self.rest)
            self.log.info("No need to do DCP rebalance upgrade")

    def dcp_rebalance_in_offline_upgrade_from_version2_to_version3(self):
        if self.input.param('initial_version', '')[:5] in COUCHBASE_VERSION_2 and \
           (self.input.param('upgrade_version', '')[:5] in COUCHBASE_VERSION_3 or \
            self.input.param('upgrade_version', '')[:5] in SHERLOCK_VERSION):
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
            "not_tested_array_numbers_integer" : [1,2,3,4,5],
            "not_tested_array_numbers_double" : [1.1,2.2,3.3,4.4,5.5],
            "not_tested_array_numbers_float" : [2.99792458e8,2.99792458e8,2.99792458e8],
            "not_tested_array_numbers_mix" : [0,2.99792458e8,1.1],
            "not_tested_array_array_mix" : [[2.99792458e8,2.99792458e8,2.99792458e8],[0,2.99792458e8,1.1],[],[0, 0, 0]],
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
            "integer_zero":0,
            "integer_big":1038383839293939383938393,
            "double_zero":0.0,
            "integer":1,
            "integer_negative":-1,
            "double":1.1,
            "double_negative":-1.1,
            "float":2.99792458e8,
            "float_negative":-2.99792458e8,
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
                    host="{0}:{1}".format(self.master.ip,self.master.port)
                return SDKClient(scheme=scheme,hosts = [host], bucket = bucket.name)
            except Exception, ex:
                self.log.info("cannot load sdk client due to error {0}".format(str(ex)))
        # USE MC BIN CLIENT WHEN NOT USING SDK CLIENT
        return self.direct_mc_bin_client(server, bucket, timeout= timeout)
    """ subdoc base test ends here """
