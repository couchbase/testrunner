import queue
import copy
from newupgradebasetest import NewUpgradeBaseTest
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection, RestHelper
from membase.api.exception import RebalanceFailedException
from membase.helper.cluster_helper import ClusterOperationHelper
from memcached.helper.kvstore import KVStore
from testconstants import CB_RELEASE_BUILDS
# from gsi.indexscans_gsi import SecondaryIndexingScanTests
from couchbase.cluster import Cluster
try:
    # For SDK2 (legacy) runs
    from couchbase.cluster import PasswordAuthenticator
    from couchbase.exceptions import CouchbaseError as CouchbaseException
except ImportError:
    # For SDK4 compatible runs
    from couchbase.auth import PasswordAuthenticator
    from couchbase.exceptions import CouchbaseException

from security.rbac_base import RbacBase
from threading import Thread


class SingleNodeUpgradeTests(NewUpgradeBaseTest):
    def setUp(self):
        super(SingleNodeUpgradeTests, self).setUp()
        self.queue = queue.Queue()

    def tearDown(self):
        super(SingleNodeUpgradeTests, self).tearDown()
        if self.input.param("op", None) == "close_port":
            remote = RemoteMachineShellConnection(self.master)
            remote.disable_firewall()

    def test_upgrade(self):
        self._install([self.master])
        self.operations([self.master])
        for upgrade_version in self.upgrade_versions:
            self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for upgrade to {0} version". \
                       format(upgrade_version))
            upgrade_threads = self._async_update(upgrade_version, [self.master])
            # wait upgrade statuses
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            success_upgrade = True
            while not self.queue.empty():
                success_upgrade &= self.queue.get()
            if not success_upgrade:
                self.fail("Upgrade failed!")
            self.add_built_in_server_user(node=self.master)
            self.sleep(self.expire_time)
            #            if not self.is_linux:
            #                self.wait_node_restarted(self.master, wait_time=1200, wait_if_warmup=True, check_service=True)
            remote = RemoteMachineShellConnection(self.master)
            for bucket in self.buckets:
                remote.execute_cbepctl(bucket, "", "set flush_param", "exp_pager_stime", 5)
            remote.disconnect()
            self.sleep(30)
            self.verification([self.master])

    def test_upgrade_negative(self):
        op = self.input.param("op", None)
        error = self.input.param("error", '')
        remote = RemoteMachineShellConnection(self.master)
        if op is None:
            self.fail("operation should be specified")
        if op == "higher_version":
            tmp = self.initial_version
            self.initial_version = self.upgrade_versions[0]
            self.upgrade_versions = [tmp, ]
        info = None
        if op == "wrong_arch":
            info = remote.extract_remote_info()
            info.architecture_type = ('x86_64', 'x86')[info.architecture_type == 'x86']
        self._install([self.master])
        self.operations([self.master])
        try:
            if op == "close_port":
                RemoteUtilHelper.enable_firewall(self.master)
            for upgrade_version in self.upgrade_versions:
                self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for upgrade to {0} version". \
                           format(upgrade_version))
                output, error = self._upgrade(upgrade_version, self.master, info=info)
                if str(output).find(error) != -1 or str(error).find(error) != -1:
                    raise Exception(error)
        except Exception as ex:
            self.log.info("Exception %s appeared as expected" % ex)
            self.log.info("Check that old version is working fine")
            self.verification([self.master])
        else:
            self.fail("Upgrade should fail!")
        remote.disconnect()


class Upgrade_Utils(NewUpgradeBaseTest):
    def setUp(self):
        super(Upgrade_Utils, self).setUp()
        self.nodes_init = self.input.param('nodes_init', 2)
        self.queue = queue.Queue()

    def tearDown(self):
        print("teardown done")

    def add_and_rebalance(self):
        self._install(self.servers[:self.nodes_init])
        self.operations(self.servers[:self.nodes_init])
        self.initial_version = self.upgrade_versions[0]
        self.product = 'couchbase-server'
        self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for online upgrade to {0} version". \
                   format(self.initial_version))
        self._install(self.servers[self.nodes_init:self.num_servers])


class MultiNodesUpgradeTests(NewUpgradeBaseTest):
    def setUp(self):
        super(MultiNodesUpgradeTests, self).setUp()
        self.nodes_init = self.input.param('nodes_init', 2)
        self.queue = queue.Queue()
        self.rate_limit = self.input.param("rate_limit", 100000)
        self.batch_size = self.input.param("batch_size", 1000)
        self.doc_size = self.input.param("doc_size", 100)
        self.loader = self.input.param("loader", "high_doc_ops")
        self.instances = self.input.param("instances", 4)
        self.threads = self.input.param("threads", 5)
        self.use_replica_to = self.input.param("use_replica_to", False)
        self.index_name_prefix = None

    def tearDown(self):
        super(MultiNodesUpgradeTests, self).tearDown()

    def offline_cluster_upgrade(self):
        self._install(self.servers[:self.nodes_init])
        self.operations(self.servers[:self.nodes_init])
        seqno_expected = 1
        if self.ddocs_num:
            self.create_ddocs_and_views()
            if self.input.param('run_index', False):
                self.verify_all_queries()
        if not self.initial_version.startswith("1.") and self.input.param('check_seqno', True):
            self.check_seqno(seqno_expected)
        if self.during_ops:
            for opn in self.during_ops:
                if opn != 'add_back_failover':
                    getattr(self, opn)()
        num_stoped_nodes = self.input.param('num_stoped_nodes', self.nodes_init)
        upgrade_nodes = self.servers[self.nodes_init - num_stoped_nodes:self.nodes_init]
        for upgrade_version in self.upgrade_versions:
            self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for upgrade to {0} version". \
                       format(upgrade_version))
            for server in upgrade_nodes:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                self.sleep(self.sleep_time)
                if self.wait_expire:
                    self.sleep(self.expire_time)
                if self.input.param('remove_manifest_files', False):
                    for file in ['manifest.txt', 'manifest.xml', 'VERSION.txt,']:
                        output, error = remote.execute_command("rm -rf /opt/couchbase/{0}".format(file))
                        remote.log_command_output(output, error)
                if self.input.param('remove_config_files', False):
                    for file in ['config', 'couchbase-server.node', 'ip', 'couchbase-server.cookie']:
                        output, error = remote.execute_command(
                            "rm -rf /opt/couchbase/var/lib/couchbase/{0}".format(file))
                        remote.log_command_output(output, error)
                    self.buckets = []
                remote.disconnect()
            upgrade_threads = self._async_update(upgrade_version, upgrade_nodes)
            # wait upgrade statuses
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            success_upgrade = True
            while not self.queue.empty():
                success_upgrade &= self.queue.get()
            if not success_upgrade:
                self.fail("Upgrade failed. See logs above!")
            self.add_built_in_server_user()
            self.sleep(self.expire_time)
            if self.during_ops:
                if "add_back_failover" in self.during_ops:
                    getattr(self, 'add_back_failover')()
                    self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
                elif "failover" in self.during_ops:
                    self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
                    rem = [server for server in self.servers[:self.nodes_init]
                           if self.failover_node.ip == server.ip and str(self.failover_node.port) == server.port]
                    self.verification(list(set(self.servers[:self.nodes_init]) - set(rem)))
                    return
            self._create_ephemeral_buckets()
            self.verification(self.servers[:self.nodes_init])
            if self.input.param('check_seqno', True):
                self.check_seqno(seqno_expected)

    def offline_cluster_upgrade_and_reboot(self):
        self._install(self.servers[:self.nodes_init])
        self.operations(self.servers[:self.nodes_init])
        if self.ddocs_num:
            self.create_ddocs_and_views()
        if self.during_ops:
            for opn in self.during_ops:
                getattr(self, opn)()
        num_stoped_nodes = self.input.param('num_stoped_nodes', self.nodes_init)
        stoped_nodes = self.servers[self.nodes_init - num_stoped_nodes:self.nodes_init]
        for upgrade_version in self.upgrade_versions:
            self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for upgrade to {0} version". \
                       format(upgrade_version))
            for server in stoped_nodes:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                remote.disconnect()
            self.sleep(self.sleep_time)
            upgrade_threads = self._async_update(upgrade_version, stoped_nodes)
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            success_upgrade = True
            while not self.queue.empty():
                success_upgrade &= self.queue.get()
            if not success_upgrade:
                self.fail("Upgrade failed!")
            self.add_built_in_server_user()
            for server in stoped_nodes:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                self.sleep(5)
                remote.start_couchbase()
                remote.disconnect()
            ClusterOperationHelper.wait_for_ns_servers_or_assert(stoped_nodes, self)
            self.log.info("Need to sleep 15 seconds for couchbase server startup completely")
            self.sleep(15)
            self._create_ephemeral_buckets()
            self.verification(self.servers[:self.nodes_init])

    def offline_cluster_upgrade_and_rebalance(self):
        num_stoped_nodes = self.input.param('num_stoped_nodes', self.nodes_init)
        stoped_nodes = self.servers[self.nodes_init - num_stoped_nodes:self.nodes_init]
        servs_out = self.servers[self.nodes_init - num_stoped_nodes - self.nodes_out:self.nodes_init - num_stoped_nodes]
        servs_in = self.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        self._install(self.servers)
        self.operations(self.servers[:self.nodes_init])
        if self.ddocs_num:
            self.create_ddocs_and_views()
        if self.during_ops:
            for opn in self.during_ops:
                getattr(self, opn)()
        for upgrade_version in self.upgrade_versions:
            self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for upgrade to {0} version". \
                       format(upgrade_version))
            for server in stoped_nodes:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                remote.disconnect()
            upgrade_threads = self._async_update(upgrade_version, stoped_nodes)
            try:
                self.cluster.rebalance(self.servers[:self.nodes_init], servs_in, servs_out)
            except RebalanceFailedException:
                self.log.info("rebalance failed as expected")
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            success_upgrade = True
            while not self.queue.empty():
                success_upgrade &= self.queue.get()
            if not success_upgrade:
                self.fail("Upgrade failed!")
            ClusterOperationHelper.wait_for_ns_servers_or_assert(stoped_nodes, self)
            self.cluster.rebalance(self.servers[:self.nodes_init], [], servs_out)
            self._create_ephemeral_buckets()
            self.verification(list(set(self.servers[:self.nodes_init] + servs_in) - set(servs_out)))

    def offline_cluster_upgrade_non_default_path(self):
        try:
            num_nodes_with_not_default = self.input.param('num_nodes_with_not_default', 1)
            prefix_path = ''
            if not self.is_linux:
                prefix_path = "C:"
            data_path = prefix_path + self.input.param('data_path', '/tmp/data').replace('|', "/")
            index_path = self.input.param('index_path', data_path).replace('|', "/")
            if not self.is_linux and not index_path.startswith("C:"):
                index_path = prefix_path + index_path
            num_nodes_remove_data = self.input.param('num_nodes_remove_data', 0)
            servers_with_not_default = self.servers[:num_nodes_with_not_default]
            old_paths = {}
            for server in servers_with_not_default:
                # to restore servers paths in finally
                old_paths[server.ip] = [server.data_path, server.index_path]
                server.data_path = data_path
                server.index_path = index_path
                shell = RemoteMachineShellConnection(server)
                # shell.remove_folders([data_path, index_path])
                for path in {data_path, index_path}:
                    shell.create_directory(path)
                shell.disconnect()
            self._install(self.servers[:self.nodes_init])
            self.operations(self.servers[:self.nodes_init])
            if self.ddocs_num and not self.input.param('extra_verification', False):
                self.create_ddocs_and_views()
            for upgrade_version in self.upgrade_versions:
                self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for upgrade to {0} version". \
                           format(upgrade_version))
                for server in self.servers[:self.nodes_init]:
                    remote = RemoteMachineShellConnection(server)
                    remote.stop_server()
                    remote.disconnect()
                self.sleep(self.sleep_time)
                # remove data for nodes with non default data paths
                tmp = min(num_nodes_with_not_default, num_nodes_remove_data)
                self.delete_data(self.servers[:tmp], [data_path + "/*", index_path + "/*"])
                # remove data for nodes with default data paths
                self.delete_data(self.servers[tmp: max(tmp, num_nodes_remove_data)],
                                 ["/opt/couchbase/var/lib/couchbase/data/*"])
                upgrade_threads = self._async_update(upgrade_version, self.servers[:self.nodes_init])
                for upgrade_thread in upgrade_threads:
                    upgrade_thread.join()
                success_upgrade = True
                while not self.queue.empty():
                    success_upgrade &= self.queue.get()
                if not success_upgrade:
                    self.fail("Upgrade failed!")
                self.add_built_in_server_user()
                self.sleep(self.expire_time)
                for server in servers_with_not_default:
                    rest = RestConnection(server)
                    node = rest.get_nodes_self()
                    print((node.storage[0]))
                    self.assertEqual(node.storage[0].path.lower(), data_path.lower(),
                                     "Server %s. Data path expected:%s, actual %s." % (
                                         server.ip, data_path, node.storage[0].path))
                    self.assertEqual(node.storage[0].index_path.lower(), index_path.lower(),
                                     "Server %s. Index path expected: %s, actual: %s." % (
                                         server.ip, index_path, node.storage[0].index_path))
            if num_nodes_remove_data:
                for bucket in self.buckets:
                    if self.rest_helper.bucket_exists(bucket):
                        raise Exception("bucket: %s still exists" % bucket.name)
                self.buckets = []

            if self.input.param('extra_verification', False):
                self.bucket_size = 256
                self._create_sasl_buckets(self.master, 1)
                self._create_standard_buckets(self.master, 1)
                if self.ddocs_num:
                    self.create_ddocs_and_views()
                    gen_load = BlobGenerator('upgrade', 'upgrade-', self.value_size, end=self.num_items)
                    self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)
            self._create_ephemeral_buckets()
            self.verification(self.servers[:self.nodes_init], check_items=not num_nodes_remove_data)
        finally:
            for server in servers_with_not_default:
                server.data_path = old_paths[server.ip][0]
                server.index_path = old_paths[server.ip][1]

    def offline_cluster_upgrade_with_reinstall(self):
        self._install(self.servers[:self.nodes_init])
        self.operations(self.servers[:self.nodes_init])
        if self.ddocs_num:
            self.create_ddocs_and_views()
        if self.during_ops:
            for opn in self.during_ops:
                getattr(self, opn)()
        num_nodes_reinstall = self.input.param('num_nodes_reinstall', 1)
        stoped_nodes = self.servers[self.nodes_init - (self.nodes_init - num_nodes_reinstall):self.nodes_init]
        nodes_reinstall = self.servers[:num_nodes_reinstall]
        for upgrade_version in self.upgrade_versions:
            self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for upgrade to {0} version". \
                       format(upgrade_version))
            for server in stoped_nodes:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                remote.disconnect()
            self.sleep(self.sleep_time)
            upgrade_threads = self._async_update(upgrade_version, stoped_nodes)
            self.force_reinstall(nodes_reinstall)
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            success_upgrade = True
            while not self.queue.empty():
                success_upgrade &= self.queue.get()
            if not success_upgrade:
                self.fail("Upgrade failed!")
            self._create_ephemeral_buckets()
            self.verification(self.servers[:self.nodes_init])

    def online_upgrade_rebalance_in_with_ops(self):
        gen_load = BlobGenerator('upgrade', 'upgrade-', self.value_size, end=self.num_items)

        def modify_data():
            tasks_ops = []
            for bucket in self.buckets:
                gen = copy.deepcopy(gen_load)
                tasks_ops.append(self.cluster.async_load_gen_docs(self.master, bucket.name, gen,
                                                                  bucket.kvs[1], "create"))
            for task in tasks_ops:
                task.result()

        self._install(self.servers[:self.nodes_init])
        self.operations(self.servers[:self.nodes_init])
        if self.ddocs_num:
            self.create_ddocs_and_views()
        self.initial_version = self.upgrade_versions[0]
        self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for online upgrade to {0} version". \
                   format(self.initial_version))
        self.product = 'couchbase-server'
        servs_in = self.servers[self.nodes_init:self.nodes_in + self.nodes_init]
        servs_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]

        if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
            self._install(self.servers[self.nodes_init:self.num_servers], community_to_enterprise=True)
        else:
            self._install(self.servers[self.nodes_init:self.num_servers])
        self.sleep(self.sleep_time, "Installation of new version is done. Wait for rebalance")

        modify_data()
        task_reb = self.cluster.async_rebalance(self.servers[:self.nodes_init], servs_in, servs_out)
        self.add_built_in_server_user()
        while task_reb.state != "FINISHED":
            modify_data()
        task_reb.result()
        self._create_ephemeral_buckets()
        self.verification(list((set(self.servers[:self.nodes_init]) | set(servs_in)) - set(servs_out)))

    def online_upgrade_rebalance_in_out(self):
        self._install(self.servers[:self.nodes_init])
        self.operations(self.servers[:self.nodes_init])
        seqno_expected = 1
        seqno_comparator = '>='
        if not self.initial_version.startswith("1.") and self.input.param('check_seqno', True):
            self.check_seqno(seqno_expected)
            seqno_comparator = '=='
        if self.ddocs_num:
            self.create_ddocs_and_views()
        self.initial_version = self.upgrade_versions[0]
        self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for online upgrade to {0} version". \
                   format(self.initial_version))
        self.product = 'couchbase-server'
        if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
            self._install(self.servers[self.nodes_init:self.num_servers], community_to_enterprise=True)
        else:
            self._install(self.servers[self.nodes_init:self.num_servers])
        self.sleep(self.sleep_time, "Installation of new version is done. Wait for rebalance")
        if self.during_ops:
            for opn in self.during_ops:
                getattr(self, opn)()
        if self.wait_expire:
            self.sleep(self.expire_time)
            for bucket in self.buckets:
                bucket.kvs[1] = KVStore()
        self.online_upgrade()
        self.add_built_in_server_user()
        self.sleep(10)

        if self.input.param('initial_version', '')[:5] in CB_RELEASE_BUILDS.keys():
            self.master = self.servers[self.nodes_init: self.num_servers][0]
        """ verify DCP upgrade in 3.0.0 version """
        self.monitor_dcp_rebalance()

        if self.input.param('reboot_cluster', False):
            self.warm_up_node(self.servers[self.nodes_init: self.num_servers])
        self._create_ephemeral_buckets()
        self.verification(self.servers[self.nodes_init: self.num_servers])
        if self.input.param('check_seqno', True):
            self.check_seqno(seqno_expected, comparator=seqno_comparator)

    def online_upgrade_incremental(self):
        self._install(self.servers)
        self.operations(self.servers)
        if self.ddocs_num:
            self.create_ddocs_and_views()
        for server in self.servers[1:]:
            self.cluster.rebalance(self.servers, [], [server])
            self.initial_version = self.upgrade_versions[0]
            self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for online upgrade to {0} version". \
                       format(self.initial_version))
            self.product = 'couchbase-server'
            self._install([server])
            self.sleep(self.sleep_time, "Installation of new version is done. Wait for rebalance")
            self.cluster.rebalance(self.servers, [server], [])
            self.log.info("Rebalanced in upgraded nodes")
            self.sleep(self.sleep_time)
            self.verification(self.servers)
        self._new_master(self.servers[1])
        self.cluster.rebalance(self.servers, [], [self.servers[0]])
        self.log.info("Rebalanced out all old version nodes")
        self.sleep(10)

        """ verify DCP upgrade in 3.0.0 version """
        self.monitor_dcp_rebalance()
        self._create_ephemeral_buckets()
        self.verification(self.servers[1:])

    def online_consequentially_upgrade(self):
        half_node = len(self.servers) // 2
        self._install(self.servers[:half_node])
        self.operations(self.servers[:half_node])
        if self.ddocs_num:
            self.create_ddocs_and_views()
        self.initial_version = self.upgrade_versions[0]
        self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for online upgrade to {0} version". \
                   format(self.initial_version))
        self.product = 'couchbase-server'
        self._install(self.servers[half_node:])
        self.sleep(self.sleep_time, "Installation of new version is done. Wait for rebalance")
        self.log.info("Rebalanced in upgraded nodes and rebalanced out nodes with old version")
        self.cluster.rebalance(self.servers, self.servers[half_node:], self.servers[:half_node])
        self.sleep(10)

        """ verify DCP upgrade in 3.x.x version """
        self.master = self.servers[half_node]
        self.add_built_in_server_user()
        self.monitor_dcp_rebalance()
        self.sleep(self.sleep_time)
        try:
            for server in self.servers[half_node:]:
                if self.port and self.port != '8091':
                    server.port = self.port
            self._new_master(self.servers[half_node])
            self.add_built_in_server_user()
            self.verification(self.servers[half_node:])
            self.log.info("Upgrade nodes of old version")
            upgrade_threads = self._async_update(self.upgrade_versions[0], self.servers[:half_node],
                                                 None, True)
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            success_upgrade = True
            while not self.queue.empty():
                success_upgrade &= self.queue.get()
            if not success_upgrade:
                self.fail("Upgrade failed!")
            self.cluster.rebalance(self.servers[half_node:], self.servers[:half_node], [])
            self.log.info("Rebalanced in all new version nodes")
            self.add_built_in_server_user()
            self.sleep(self.sleep_time)
            self._create_ephemeral_buckets()
            self.verification(self.servers)
        finally:
            for server in self.servers:
                server.port = '8091'

    def online_upgrade_and_rebalance(self):
        self._install(self.servers)
        self.operations(self.servers)
        if self.ddocs_num:
            self.create_ddocs_and_views()
        upgrade_servers = self.servers[self.nodes_init:self.num_servers]
        self.cluster.rebalance(self.servers, [], upgrade_servers)
        self.initial_version = self.upgrade_versions[0]
        self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for online upgrade to {0} version". \
                   format(self.initial_version))
        if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
            self._install(upgrade_servers, community_to_enterprise=True)
        else:
            self._install(upgrade_servers)
        self.sleep(self.sleep_time, "Installation of new version is done. Wait for rebalance")
        self.log.info("Rebalance in new version nodes and rebalance out some nodes")
        self.cluster.rebalance(self.servers, upgrade_servers, self.servers[self.num_servers:])
        self.log.info("Rebalance completed")
        self.log.info("Remove the second old version node")
        self._new_master(self.servers[1])
        self.cluster.rebalance(self.servers, [], [self.servers[0]])
        self.log.info("Rebalance completed")
        self.sleep(10)
        """ verify DCP upgrade in 3.0.0 version """
        self.master = self.servers[self.nodes_init]
        self.monitor_dcp_rebalance()
        self._create_ephemeral_buckets()
        self.verification(self.servers[self.nodes_init: -self.nodes_init])

    def online_upgrade(self, services=None):
        servers_in = self.servers[self.nodes_init:self.num_servers]
        servers_out = self.servers[:self.nodes_init]
        self.cluster.rebalance(self.servers[:self.nodes_init], servers_in, servers_out, services=services)
        self.sleep(60)

        self.log.info("Rebalance in all {0} nodes" \
                      .format(self.input.param("upgrade_version", "")))
        self.log.info("Rebalanced out all old version nodes")

        status, content = ClusterOperationHelper.find_orchestrator(servers_in[0])
        self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}". \
                        format(status, content))
        FIND_MASTER = False
        for new_server in servers_in:
            if content.find(new_server.ip) >= 0:
                self._new_master(new_server)
                FIND_MASTER = True
                self.log.info("%s node %s becomes the master" \
                              % (self.input.param("upgrade_version", ""), new_server.ip))
                break

    def online_upgrade_swap_rebalance(self):
        self._install(self.servers[:self.nodes_init])
        self.operations(self.servers[:self.nodes_init])
        self.initial_version = self.upgrade_versions[0]
        self.product = 'couchbase-server'
        self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for online upgrade to {0} version". \
                   format(self.initial_version))
        if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
            self._install(self.servers[self.nodes_init:self.num_servers], community_to_enterprise=True)
        else:
            self._install(self.servers[self.nodes_init:self.num_servers])
        self.sleep(self.sleep_time, "Installation of new version is done. Wait for rebalance")
        self.swap_num_servers = self.input.param('swap_num_servers', 1)
        old_servers = self.servers[:self.nodes_init]
        new_vb_nums = RestHelper(RestConnection(self.master))._get_vbuckets(old_servers,
                                                                            bucket_name=self.buckets[0].name)
        new_servers = []
        for i in range(self.nodes_init // self.swap_num_servers):
            old_vb_nums = copy.deepcopy(new_vb_nums)
            servers_in = self.servers[(self.nodes_init + i * self.swap_num_servers):
            (self.nodes_init + (i + 1) * self.swap_num_servers)]
            servers_out = self.servers[(i * self.swap_num_servers):((i + 1) * self.swap_num_servers)]
            servers = old_servers + new_servers
            self.log.info("Swap rebalance: rebalance out %s old version nodes, rebalance in %s 2.0 Nodes"
                          % (self.swap_num_servers, self.swap_num_servers))
            self.cluster.rebalance(servers, servers_in, servers_out)
            self.sleep(self.sleep_time)
            old_servers = self.servers[((i + 1) * self.swap_num_servers):self.nodes_init]
            new_servers = new_servers + servers_in
            servers = old_servers + new_servers
            new_vb_nums = RestHelper(RestConnection(self.master))._get_vbuckets(servers,
                                                                                bucket_name=self.buckets[0].name)
            self._verify_vbucket_nums_for_swap(old_vb_nums, new_vb_nums)
            self.log.info("Enable remote eval if its version is 5.5+")
            shell = RemoteMachineShellConnection(self.master)
            shell.enable_diag_eval_on_non_local_hosts()
            shell.disconnect()
            status, content = ClusterOperationHelper.find_orchestrator(servers[0])
            self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}". \
                            format(status, content))
            FIND_MASTER = False
            for new_server in new_servers:
                if content.find(new_server.ip) >= 0:
                    self._new_master(new_server)
                    FIND_MASTER = True
                    self.log.info("3.0 Node %s becomes the master" % (new_server.ip))

        """ verify DCP upgrade in 3.0.0 version """
        self.monitor_dcp_rebalance()
        self.add_built_in_server_user()
        self._create_ephemeral_buckets()
        self.verification(new_servers)

    def online_upgrade_add_services(self):
        half_node = len(self.servers) // 2
        self._install(self.servers[:2])
        self.operations(self.servers[:2])
        if self.ddocs_num:
            self.create_ddocs_and_views()
        self.initial_version = self.upgrade_versions[0]
        self.sleep(self.sleep_time, "Pre-setup of old version is done. "
                                    " Wait for online upgrade to {0} version"
                   .format(self.initial_version))
        self.product = 'couchbase-server'
        self._install(self.servers[2:])
        self.sleep(self.sleep_time, "Installation of new version is done."
                                    " Wait for rebalance")
        self.log.info("Rebalanced in upgraded nodes and rebalanced out "
                      "nodes with old version")
        self.cluster.rebalance(self.servers, self.servers[2:],
                               self.servers[:2])
        self.sleep(10)

        """ verify DCP upgrade in 3.x.x version """
        self.master = self.servers[2]
        self.monitor_dcp_rebalance()
        self.sleep(self.sleep_time)
        try:
            for server in self.servers[2:]:
                if self.port and self.port != '8091':
                    server.port = self.port
            self._new_master(self.servers[2])
            self.verification(self.servers[2:])
            self.log.info("Upgrade nodes of old version")
            upgrade_threads = self._async_update(self.upgrade_versions[0],
                                                 self.servers[:2], None, True)
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            success_upgrade = True
            while not self.queue.empty():
                success_upgrade &= self.queue.get()
            if not success_upgrade:
                self.fail("Upgrade failed!")
            self.cluster.rebalance(self.servers[2:],
                                   self.servers[:2], [])
            self.log.info("Rebalanced in all new version nodes")
            self.sleep(self.sleep_time)
            self._create_ephemeral_buckets()
            self.verification(self.servers)
        finally:
            for server in self.servers:
                server.port = '8091'
        """ init_nodes=False so we could set service on it"""
        self.init_nodes = False
        self._install(self.servers[-1])
        self.cluster.rebalance(self.servers[:4],
                               self.servers[-1], [])
        # SecondaryIndexingScanTests().test_multi_create_query_explain_drop_index()
        # we could add more tests later

    def test_swap_rebalance_with_services(self):
        after_upgrade_services_in = self.input.param("after_upgrade_services_in", False)
        after_upgrade_buckets_in = self.input.param("after_upgrade_buckets_in", False)
        after_upgrade_buckets_out = self.input.param("after_upgrade_buckets_out", False)
        after_upgrade_buckets_flush = self.input.param("after_upgrade_buckets_flush", False)

        self.swap_num_servers = self.input.param('swap_num_servers', 4)
        # Install initial version on the specified nodes
        self._install(self.servers[:self.nodes_init])
        # Configure the nodes with services
        self.operations(self.servers[:self.nodes_init], services="kv,kv,index,n1ql")
        # get the n1ql node which will be used in pre,during and post upgrade for running n1ql commands
        self.n1ql_server = self.get_nodes_from_services_map(
            service_type="n1ql")
        # Run the pre upgrade operations, typically creating index
        self.pre_upgrade(self.servers[:self.nodes_init])
        if self.ddocs_num:
            self.create_ddocs_and_views()
        # set the upgrade version
        self.initial_version = self.upgrade_versions[0]
        self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for online upgrade to {0} version". \
                   format(self.initial_version))
        self.product = 'couchbase-server'
        # install the new version on different set of servers
        if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
            self._install(self.servers[self.nodes_init:self.num_servers], community_to_enterprise=True)
        else:
            self._install(self.servers[self.nodes_init:self.nodes_init + self.swap_num_servers])
        self.sleep(self.sleep_time, "Installation of new version is done. Wait for rebalance")
        if self.during_ops:
            for opn in self.during_ops:
                getattr(self, opn)()
        if self.wait_expire:
            self.sleep(self.expire_time)
            for bucket in self.buckets:
                bucket.kvs[1] = KVStore()
        # swap and rebalance the servers
        self.online_upgrade(services=["kv", "kv", "index", "n1ql"])
        self.n1ql_server = self.get_nodes_from_services_map(
            service_type="n1ql", servers=self.servers[self.nodes_init:self.num_servers])
        # Run the post_upgrade operations
        self._create_ephemeral_buckets()
        self.post_upgrade(self.servers[self.nodes_init:self.num_servers])

        # Add new services after the upgrade
        if after_upgrade_services_in is not False:
            for upgrade_version in self.upgrade_versions:
                if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
                    upgrade_threads = self._async_update(upgrade_version, [self.servers[self.nodes_init]],
                                                         save_upgrade_config=True)
                else:
                    upgrade_threads = self._async_update(upgrade_version, [self.servers[self.nodes_init]])

                for upgrade_thread in upgrade_threads:
                    upgrade_thread.join()
                success_upgrade = True
                while not self.queue.empty():
                    success_upgrade &= self.queue.get()
                if not success_upgrade:
                    self.fail("Upgrade failed. See logs above!")
                self.sleep(self.expire_time)
                self.cluster.rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                       services=["kv", "index", "n1ql"])
        # creating new buckets after upgrade
        if after_upgrade_buckets_in is not False:
            self.bucket_size = 256
            self._create_sasl_buckets(self.master, 1)
            self._create_standard_buckets(self.master, 1)
            if self.ddocs_num:
                self.create_ddocs_and_views()
                gen_load = BlobGenerator('upgrade', 'upgrade-', self.value_size, end=self.num_items)
                self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)
        # deleting buckets after upgrade
        if after_upgrade_buckets_out is not False:
            self._all_buckets_delete(self.master)
        # flushing buckets after upgrade
        if after_upgrade_buckets_flush is not False:
            self._all_buckets_flush()

    def test_regular_rebalance_with_services(self):
        after_upgrade_services_in = self.input.param("after_upgrade_services_in", False)
        after_upgrade_buckets_in = self.input.param("after_upgrade_buckets_in", False)
        after_upgrade_buckets_out = self.input.param("after_upgrade_buckets_out", False)
        after_upgrade_buckets_flush = self.input.param("after_upgrade_buckets_flush", False)
        self.index_quota_percent = self.input.param("index_quota_percent", 30)

        self.swap_num_servers = self.input.param('swap_num_servers', 4)
        # Install initial version on the specified nodes
        self._install(self.servers[:self.nodes_init])
        # Configure the nodes with services
        self.operations(self.servers[:self.nodes_init],
                        services="kv,kv,index,n1ql,kv,kv,index,n1ql")
        # get the n1ql node which will be used in pre,during and post
        # upgrade for running n1ql commands
        self.n1ql_server = self.get_nodes_from_services_map(
            service_type="n1ql")
        # Run the pre upgrade operations, typically creating index
        self.pre_upgrade(self.servers[:self.nodes_init])
        if self.ddocs_num:
            self.create_ddocs_and_views()
        # set the upgrade version
        self.initial_version = self.upgrade_versions[0]
        self.sleep(self.sleep_time, "Pre-setup of old version is done. " \
                                    "Wait for online upgrade to {0} version". \
                   format(self.initial_version))
        upgrade_servers = self.servers[self.swap_num_servers:self.nodes_init]
        upgrade_services = ["kv", "kv", "index", "n1ql"]
        # swap out half of the servers out and rebalance
        self.cluster.rebalance(self.servers, [], upgrade_servers)
        # install the new version on the swapped out servers
        self._install(self.servers[self.swap_num_servers:self.nodes_init])
        self.sleep(20, "wait for server is up after install")
        self.n1ql_server = self.get_nodes_from_services_map(
            service_type="n1ql")
        # Run during upgrade operations
        self.during_upgrade(self.servers[:self.nodes_init])
        # rebalance in the swapped out servers
        for i in range(0, len(upgrade_servers)):
            self.cluster.rebalance(self.servers, [upgrade_servers[i]], [],
                                   services=[upgrade_services[i]])
        self.sleep(30, "wait for rebalance complete")

        self._new_master(self.servers[self.swap_num_servers])
        # swap out the remaining half of the servers and rebalance
        self.log.info("start to remove old version nodes")
        upgrade_servers = self.servers[:self.swap_num_servers]
        reb_status  = self.cluster.rebalance(self.servers, [], upgrade_servers)
        self.assertTrue(reb_status, 'Rebalance out old nodes did not complete')
        # install the new version on the swapped out servers
        self._install(self.servers[:self.swap_num_servers])
        self.sleep(20, "wait for server is up after install")
        for i in range(0, len(upgrade_servers)):
            self.cluster.rebalance(self.servers[self.swap_num_servers:self.nodes_init],
                                   [upgrade_servers[i]], [],
                                   services=[upgrade_services[i]])
        self.sleep(timeout=60)
        self.n1ql_server = self.get_nodes_from_services_map(
            service_type="n1ql")
        # Run the post_upgrade operations
        self._create_ephemeral_buckets()
        self.post_upgrade(self.servers[:self.nodes_init])

        # Add new services after the upgrade
        if after_upgrade_services_in is not False:
            for upgrade_version in self.upgrade_versions:
                if self.initial_build_type == "community" and \
                   self.upgrade_build_type == "enterprise":
                    upgrade_threads = self._async_update(upgrade_version,
                                                         [self.servers[self.nodes_init]],
                                                         save_upgrade_config=True)
                else:
                    upgrade_threads = self._async_update(upgrade_version,
                                                         [self.servers[self.nodes_init]])

                for upgrade_thread in upgrade_threads:
                    upgrade_thread.join()
                success_upgrade = True
                while not self.queue.empty():
                    success_upgrade &= self.queue.get()
                if not success_upgrade:
                    self.fail("Upgrade failed. See logs above!")
                self.sleep(self.expire_time)
                self.cluster.rebalance(self.servers[:self.nodes_init],
                                       [self.servers[self.nodes_init]], [],
                                       services=["kv", "index", "n1ql"])
        # creating new buckets after upgrade
        if after_upgrade_buckets_in is not False:
            self.bucket_size = 256
            self._create_sasl_buckets(self.master, 1)
            self._create_standard_buckets(self.master, 1)
            if self.ddocs_num:
                self.create_ddocs_and_views()
                gen_load = BlobGenerator('upgrade', 'upgrade-',
                                         self.value_size, end=self.num_items)
                self._load_all_buckets(self.master, gen_load, "create",
                                       self.expire_time, flag=self.item_flag)
        # deleting buckets after upgrade
        if after_upgrade_buckets_out is not False:
            self._all_buckets_delete(self.master)
        # flushing buckets after upgrade
        if after_upgrade_buckets_flush is not False:
            self._all_buckets_flush()

    def test_graceful_failover_with_services(self):
        after_upgrade_services_in = self.input.param("after_upgrade_services_in", False)
        after_upgrade_buckets_in = self.input.param("after_upgrade_buckets_in", False)
        after_upgrade_buckets_out = self.input.param("after_upgrade_buckets_out", False)
        after_upgrade_buckets_flush = self.input.param("after_upgrade_buckets_flush", False)

        self.swap_num_servers = self.input.param('swap_num_servers', 4)
        # Install initial version on the specified nodes
        self._install(self.servers[:self.nodes_init])
        # Configure the nodes with services
        self.sleep(30, "wait for servers up")
        self.operations(self.servers[:self.nodes_init], services="kv,index,n1ql,kv,kv,index,n1ql,kv")
        # get the n1ql node which will be used in pre,during and post upgrade for running n1ql commands
        self.n1ql_server = self.get_nodes_from_services_map(
            service_type="n1ql")
        # Run the pre upgrade operations, typically creating index
        self.pre_upgrade(self.servers[:self.nodes_init])
        if self.ddocs_num:
            self.create_ddocs_and_views()
        # set the upgrade version
        self.initial_version = self.upgrade_versions[0]
        self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for online upgrade to {0} version". \
                   format(self.initial_version))

        upgrade_servers = self.servers[self.swap_num_servers:self.nodes_init]
        # do a graceful failover of some nodes
        self.rest.fail_over('ns_1@' + upgrade_servers[0].ip, graceful=True)
        self.sleep(timeout=60)
        self.rest.fail_over('ns_1@' + upgrade_servers[1].ip)
        self.sleep(timeout=60)
        self.rest.fail_over('ns_1@' + upgrade_servers[2].ip)
        self.sleep(timeout=60)

        for i in range(0, len(upgrade_servers) - 1):
            # set recovery type of the failed over nodes
            self.rest.set_recovery_type('ns_1@' + upgrade_servers[i].ip, "full")

        # upgrade the failed over nodes with the new version
        upgrade_threads = self._async_update(self.upgrade_versions[0], upgrade_servers[:len(upgrade_servers) - 1])
        # wait upgrade statuses
        for upgrade_thread in upgrade_threads:
            upgrade_thread.join()
        success_upgrade = True
        while not self.queue.empty():
            success_upgrade &= self.queue.get()
        if not success_upgrade:
            self.fail("Upgrade failed. See logs above!")
        self.sleep(self.expire_time)

        # rebalance in the failed over nodes
        self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
        self.sleep(20, "wait for rebalance complete")

        # failover another node, this is done so that the conditions of graceful failover are met, otherwise
        # hard failover will be implemented
        self.rest.fail_over('ns_1@' + upgrade_servers[3].ip, graceful=True)
        self.sleep(timeout=60)
        # set recovery type of the failed over node
        self.rest.set_recovery_type('ns_1@' + upgrade_servers[3].ip, "full")
        self.sleep(timeout=60)

        # upgrade the failed over node with the new version
        upgrade_threads = self._async_update(self.upgrade_versions[0], [upgrade_servers[len(upgrade_servers) - 1]])
        # wait upgrade statuses
        for upgrade_thread in upgrade_threads:
            upgrade_thread.join()
        success_upgrade = True
        while not self.queue.empty():
            success_upgrade &= self.queue.get()
        if not success_upgrade:
            self.fail("Upgrade failed. See logs above!")
        self.sleep(self.expire_time)

        # rebalance in the failed over nodes
        self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
        self._new_master(self.servers[self.swap_num_servers])
        upgrade_servers = self.servers[:self.swap_num_servers]
        sleep_time = 0
        reb_done = self.rest.monitorRebalance()
        print("\nrebalance status: ", reb_done)
        while not reb_done:
            self.sleep(20, "wait for rebalance done")
            sleep_time += 20
            reb_done = self.rest.monitorRebalance()
            if sleep_time == self.wait_timeout:
                self.fail("Rebalance status does not update more than 60 secs after finish")
        # do a graceful failover of remaining nodes except 1
        self.rest.fail_over('ns_1@' + upgrade_servers[0].ip, graceful=True)
        self.sleep(timeout=60)
        self.rest.fail_over('ns_1@' + upgrade_servers[1].ip)
        self.sleep(timeout=60)
        self.rest.fail_over('ns_1@' + upgrade_servers[2].ip)
        self.sleep(timeout=60)

        for i in range(0, len(upgrade_servers) - 1):
            # set recovery type of the failed over nodes
            self.rest.set_recovery_type('ns_1@' + upgrade_servers[i].ip, "full")

        # upgrade the failed over nodes with the new version
        upgrade_threads = self._async_update(self.upgrade_versions[0], upgrade_servers[:len(upgrade_servers) - 1])
        # wait upgrade statuses
        for upgrade_thread in upgrade_threads:
            upgrade_thread.join()
        success_upgrade = True
        while not self.queue.empty():
            success_upgrade &= self.queue.get()
        if not success_upgrade:
            self.fail("Upgrade failed. See logs above!")
        self.sleep(self.expire_time)

        # rebalance in the failed over nodes
        self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
        self.sleep(timeout=60)

        # failover another node, this is done so that the conditions of graceful failover are met, otherwise
        # hard failover will be implemented
        self.rest.fail_over('ns_1@' + upgrade_servers[3].ip, graceful=True)
        self.sleep(timeout=60)
        # set recovery type of the failed over node
        self.rest.set_recovery_type('ns_1@' + upgrade_servers[3].ip, "full")
        self.sleep(timeout=60)

        # upgrade the failed over node with the new version
        upgrade_threads = self._async_update(self.upgrade_versions[0], [upgrade_servers[len(upgrade_servers) - 1]])
        # wait upgrade statuses
        for upgrade_thread in upgrade_threads:
            upgrade_thread.join()
        success_upgrade = True
        while not self.queue.empty():
            success_upgrade &= self.queue.get()
        if not success_upgrade:
            self.fail("Upgrade failed. See logs above!")
        self.sleep(self.expire_time)
        # rebalance in the failed over node
        self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
        self.n1ql_server = self.get_nodes_from_services_map(
            service_type="n1ql")
        # Run the post_upgrade operations
        self._create_ephemeral_buckets()
        self.post_upgrade(self.servers[:self.nodes_init])
        # Add new services after the upgrade
        if after_upgrade_services_in is not False:
            for upgrade_version in self.upgrade_versions:
                if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
                    upgrade_threads = self._async_update(upgrade_version, [self.servers[self.nodes_init]],
                                                         save_upgrade_config=True)
                else:
                    upgrade_threads = self._async_update(upgrade_version, [self.servers[self.nodes_init]])

                for upgrade_thread in upgrade_threads:
                    upgrade_thread.join()
                success_upgrade = True
                while not self.queue.empty():
                    success_upgrade &= self.queue.get()
                if not success_upgrade:
                    self.fail("Upgrade failed. See logs above!")
                self.sleep(self.expire_time)
                self.cluster.rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                       services=["kv", "index", "n1ql"])
        # creating new buckets after upgrade
        if after_upgrade_buckets_in is not False:
            self.bucket_size = 256
            self._create_sasl_buckets(self.master, 1)
            self._create_standard_buckets(self.master, 1)
            if self.ddocs_num:
                self.create_ddocs_and_views()
                gen_load = BlobGenerator('upgrade', 'upgrade-', self.value_size, end=self.num_items)
                self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)
        # deleting buckets after upgrade
        if after_upgrade_buckets_out is not False:
            self._all_buckets_delete(self.master)
        # flushing buckets after upgrade
        if after_upgrade_buckets_flush is not False:
            self._all_buckets_flush()

    def test_xdcr_upgrade_with_services(self):
        after_upgrade_services_in = self.input.param("after_upgrade_services_in", False)
        after_upgrade_buckets_in = self.input.param("after_upgrade_buckets_in", False)
        after_upgrade_buckets_out = self.input.param("after_upgrade_buckets_out", False)
        after_upgrade_buckets_flush = self.input.param("after_upgrade_buckets_flush", False)

        # Install initial version on the specified nodes
        self._install(self.servers[:self.nodes_init])
        # Configure the nodes with services on cluster1
        self.operations(self.servers[:self.nodes_init], services="kv,kv,index,n1ql")
        # get the n1ql node which will be used in pre,during and post upgrade for running n1ql commands
        self.n1ql_server = self.get_nodes_from_services_map(
            service_type="n1ql")
        # Run the pre upgrade operations, typically creating index
        self.pre_upgrade(self.servers[:self.nodes_init])
        if self.ddocs_num:
            self.create_ddocs_and_views()
        # set the upgrade version
        self.initial_version = self.upgrade_versions[0]
        # install new version on another set of nodes
        self._install(self.servers[self.nodes_init:self.num_servers])
        self.master = self.servers[self.nodes_init]
        # Configure the nodes with services on the other cluster2
        try:
            self.operations(self.servers[self.nodes_init:self.num_servers], services="kv,kv,index,n1ql")
            self.sleep(timeout=300)
        except Exception as ex:
            # do nothing, the bucket is created
            self.log.info("bucket is created")

        # create a xdcr relationship between cluster1 and cluster2
        rest_src = RestConnection(self.servers[0])
        rest_src.add_remote_cluster(self.servers[self.nodes_init].ip, self.servers[self.nodes_init].port,
                                    'Administrator', 'password', "C2")
        repl_id = rest_src.start_replication('continuous', 'default', "C2")
        if repl_id is not None:
            self.log.info("Replication created successfully")
        # Run the post_upgrade operations
        self._create_ephemeral_buckets()
        self.post_upgrade(self.servers[:self.nodes_init])
        # Add new services after the upgrade
        if after_upgrade_services_in is not False:
            for upgrade_version in self.upgrade_versions:
                if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
                    upgrade_threads = self._async_update(upgrade_version, [self.servers[self.nodes_init]],
                                                         save_upgrade_config=True)
                else:
                    upgrade_threads = self._async_update(upgrade_version, [self.servers[self.nodes_init]])

                for upgrade_thread in upgrade_threads:
                    upgrade_thread.join()
                success_upgrade = True
                while not self.queue.empty():
                    success_upgrade &= self.queue.get()
                if not success_upgrade:
                    self.fail("Upgrade failed. See logs above!")
                self.sleep(120)
                self.cluster.rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                       services=["kv", "index", "n1ql"])
        # creating new buckets after upgrade
        if after_upgrade_buckets_in is not False:
            self.bucket_size = 256
            self._create_sasl_buckets(self.master, 1)
            self._create_standard_buckets(self.master, 1)
            if self.ddocs_num:
                self.create_ddocs_and_views()
                gen_load = BlobGenerator('upgrade', 'upgrade-', self.value_size, end=self.num_items)
                self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)
        # deleting buckets after upgrade
        if after_upgrade_buckets_out is not False:
            self._all_buckets_delete(self.master)
        # flushing buckets after upgrade
        if after_upgrade_buckets_flush is not False:
            self._all_buckets_flush()

    def test_offline_upgrade_with_services(self):
        after_upgrade_services_in = self.input.param("after_upgrade_services_in", False)
        after_upgrade_buckets_in = self.input.param("after_upgrade_buckets_in", False)
        after_upgrade_buckets_out = self.input.param("after_upgrade_buckets_out", False)
        after_upgrade_buckets_flush = self.input.param("after_upgrade_buckets_flush", False)

        # Install initial version on the specified nodes
        self._install(self.servers[:self.nodes_init])
        # Configure the nodes with services on cluster
        self.operations(self.servers[:self.nodes_init], services="kv,index,n1ql,kv,kv,index,n1ql,kv")
        # get the n1ql node which will be used in pre,during and post upgrade for running n1ql commands
        self.n1ql_server = self.get_nodes_from_services_map(
            service_type="n1ql")
        # Run the pre upgrade operations, typically creating index
        self.pre_upgrade(self.servers[:self.nodes_init])
        seqno_expected = 1
        if self.ddocs_num:
            self.create_ddocs_and_views()
            if self.input.param('run_index', False):
                self.verify_all_queries()
        if not self.initial_version.startswith("1.") and self.input.param('check_seqno', True):
            self.check_seqno(seqno_expected)
        if self.during_ops:
            for opn in self.during_ops:
                if opn != 'add_back_failover':
                    getattr(self, opn)()
        num_stoped_nodes = self.input.param('num_stoped_nodes', self.nodes_init)
        # upgrade all the nodes in the cluster offline
        upgrade_nodes = self.servers[self.nodes_init - num_stoped_nodes:self.nodes_init]
        for upgrade_version in self.upgrade_versions:
            self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for upgrade to {0} version". \
                       format(upgrade_version))
            for server in upgrade_nodes:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                self.sleep(self.sleep_time)
                if self.wait_expire:
                    self.sleep(self.expire_time)
                if self.input.param('remove_manifest_files', False):
                    for file in ['manifest.txt', 'manifest.xml', 'VERSION.txt,']:
                        output, error = remote.execute_command("rm -rf /opt/couchbase/{0}".format(file))
                        remote.log_command_output(output, error)
                if self.input.param('remove_config_files', False):
                    for file in ['config', 'couchbase-server.node', 'ip', 'couchbase-server.cookie']:
                        output, error = remote.execute_command(
                            "rm -rf /opt/couchbase/var/lib/couchbase/{0}".format(file))
                        remote.log_command_output(output, error)
                    self.buckets = []
                remote.disconnect()
            if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
                upgrade_threads = self._async_update(upgrade_version, upgrade_nodes, save_upgrade_config=True)
            else:
                upgrade_threads = self._async_update(upgrade_version, upgrade_nodes)
            # wait upgrade statuses
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            success_upgrade = True
            while not self.queue.empty():
                success_upgrade &= self.queue.get()
            if not success_upgrade:
                self.fail("Upgrade failed. See logs above!")
            self.sleep(self.expire_time)
        self.n1ql_server = self.get_nodes_from_services_map(
            service_type="n1ql")
        # Run the post_upgrade operations
        self._create_ephemeral_buckets()
        self.post_upgrade(self.servers[:self.nodes_init])
        self.check_seqno(seqno_expected)
        # Add new services after the upgrade
        if after_upgrade_services_in is not False:
            for upgrade_version in self.upgrade_versions:
                if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
                    upgrade_threads = self._async_update(upgrade_version, [self.servers[self.nodes_init]],
                                                         save_upgrade_config=True)
                else:
                    upgrade_threads = self._async_update(upgrade_version, [self.servers[self.nodes_init]])

                for upgrade_thread in upgrade_threads:
                    upgrade_thread.join()
                success_upgrade = True
                while not self.queue.empty():
                    success_upgrade &= self.queue.get()
                if not success_upgrade:
                    self.fail("Upgrade failed. See logs above!")
                self.sleep(self.expire_time)
                self.cluster.rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                       services=["kv", "index", "n1ql"])
        # creating new buckets after upgrade
        if after_upgrade_buckets_in is not False:
            self.bucket_size = 256
            self._create_sasl_buckets(self.master, 1)
            self._create_standard_buckets(self.master, 1)
            if self.ddocs_num:
                self.create_ddocs_and_views()
                gen_load = BlobGenerator('upgrade', 'upgrade-', self.value_size, end=self.num_items)
                self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)
        # deleting buckets after upgrade
        if after_upgrade_buckets_out is not False:
            self._all_buckets_delete(self.master)
        # flushing buckets after upgrade
        if after_upgrade_buckets_flush is not False:
            self._all_buckets_flush()

    def test_offline_upgrade_n1ql_service_in_new_version(self):
        after_upgrade_services_in = self.input.param("after_upgrade_services_in", False)
        after_upgrade_buckets_in = self.input.param("after_upgrade_buckets_in", False)
        after_upgrade_buckets_out = self.input.param("after_upgrade_buckets_out", False)
        after_upgrade_buckets_flush = self.input.param("after_upgrade_buckets_flush", False)

        # Install initial version on the specified nodes
        self._install(self.servers[:3])
        # Configure the nodes with services on cluster
        self.operations(self.servers[:3], services="kv,index,n1ql")
        # get the n1ql node which will be used in pre,during and post upgrade for running n1ql commands
        self.n1ql_server = self.get_nodes_from_services_map(
            service_type="n1ql")
        # Run the pre upgrade operations, typically creating index
        self.pre_upgrade(self.servers[:3])
        seqno_expected = 1
        if self.ddocs_num:
            self.create_ddocs_and_views()
            if self.input.param('run_index', False):
                self.verify_all_queries()
        if not self.initial_version.startswith("1.") and self.input.param('check_seqno', True):
            self.check_seqno(seqno_expected)
        if self.during_ops:
            for opn in self.during_ops:
                if opn != 'add_back_failover':
                    getattr(self, opn)()
        upgrade_nodes = [self.servers[2]]
        # upgrade n1ql node to new version
        for upgrade_version in self.upgrade_versions:
            self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for upgrade to {0} version". \
                       format(upgrade_version))
            for server in upgrade_nodes:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                self.sleep(self.sleep_time)
                if self.wait_expire:
                    self.sleep(self.expire_time)
                if self.input.param('remove_manifest_files', False):
                    for file in ['manifest.txt', 'manifest.xml', 'VERSION.txt,']:
                        output, error = remote.execute_command("rm -rf /opt/couchbase/{0}".format(file))
                        remote.log_command_output(output, error)
                if self.input.param('remove_config_files', False):
                    for file in ['config', 'couchbase-server.node', 'ip', 'couchbase-server.cookie']:
                        output, error = remote.execute_command(
                            "rm -rf /opt/couchbase/var/lib/couchbase/{0}".format(file))
                        remote.log_command_output(output, error)
                    self.buckets = []
                remote.disconnect()

            if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
                upgrade_threads = self._async_update(upgrade_version, upgrade_nodes, save_upgrade_config=True)
            else:
                upgrade_threads = self._async_update(upgrade_version, upgrade_nodes)
            # wait upgrade statuses
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            success_upgrade = True
            while not self.queue.empty():
                success_upgrade &= self.queue.get()
            if not success_upgrade:
                self.fail("Upgrade failed. See logs above!")
            self.sleep(self.expire_time)
        # Run the post_upgrade operations
        self._create_ephemeral_buckets()
        self.post_upgrade(self.servers[:3])
        # Add new services after the upgrade
        if after_upgrade_services_in is not False:
            for upgrade_version in self.upgrade_versions:
                if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
                    upgrade_threads = self._async_update(upgrade_version, [self.servers[self.nodes_init]],
                                                         save_upgrade_config=True)
                else:
                    upgrade_threads = self._async_update(upgrade_version, [self.servers[self.nodes_init]])

                for upgrade_thread in upgrade_threads:
                    upgrade_thread.join()
                success_upgrade = True
                while not self.queue.empty():
                    success_upgrade &= self.queue.get()
                if not success_upgrade:
                    self.fail("Upgrade failed. See logs above!")
                self.sleep(self.expire_time)
                self.cluster.rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                       services=["kv", "index", "n1ql"])
        # creating new buckets after upgrade
        if after_upgrade_buckets_in is not False:
            self.bucket_size = 256
            self._create_sasl_buckets(self.master, 1)
            self._create_standard_buckets(self.master, 1)
            if self.ddocs_num:
                self.create_ddocs_and_views()
                gen_load = BlobGenerator('upgrade', 'upgrade-', self.value_size, end=self.num_items)
                self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)
        # deleting buckets after upgrade
        if after_upgrade_buckets_out is not False:
            self._all_buckets_delete(self.master)
        # flushing buckets after upgrade
        if after_upgrade_buckets_flush is not False:
            self._all_buckets_flush()

    def test_offline_upgrade_index_service_in_new_version(self):
        after_upgrade_services_in = self.input.param("after_upgrade_services_in", False)
        after_upgrade_buckets_in = self.input.param("after_upgrade_buckets_in", False)
        after_upgrade_buckets_out = self.input.param("after_upgrade_buckets_out", False)
        after_upgrade_buckets_flush = self.input.param("after_upgrade_buckets_flush", False)

        # Install initial version on the specified nodes
        self._install(self.servers[:3])
        # Configure the nodes with services on cluster
        self.operations(self.servers[:3], services="kv,n1ql,index")
        # get the n1ql node which will be used in pre,during and post upgrade for running n1ql commands
        self.n1ql_server = self.get_nodes_from_services_map(
            service_type="n1ql")
        # Run the pre upgrade operations, typically creating index
        self.pre_upgrade(self.servers[:3])
        seqno_expected = 1
        if self.ddocs_num:
            self.create_ddocs_and_views()
            if self.input.param('run_index', False):
                self.verify_all_queries()
        if not self.initial_version.startswith("1.") and self.input.param('check_seqno', True):
            self.check_seqno(seqno_expected)
        if self.during_ops:
            for opn in self.during_ops:
                if opn != 'add_back_failover':
                    getattr(self, opn)()
        # upgrade index node to new version
        upgrade_nodes = [self.servers[2]]
        for upgrade_version in self.upgrade_versions:
            self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for upgrade to {0} version". \
                       format(upgrade_version))
            for server in upgrade_nodes:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                self.sleep(self.sleep_time)
                if self.wait_expire:
                    self.sleep(self.expire_time)
                if self.input.param('remove_manifest_files', False):
                    for file in ['manifest.txt', 'manifest.xml', 'VERSION.txt,']:
                        output, error = remote.execute_command("rm -rf /opt/couchbase/{0}".format(file))
                        remote.log_command_output(output, error)
                if self.input.param('remove_config_files', False):
                    for file in ['config', 'couchbase-server.node', 'ip', 'couchbase-server.cookie']:
                        output, error = remote.execute_command(
                            "rm -rf /opt/couchbase/var/lib/couchbase/{0}".format(file))
                        remote.log_command_output(output, error)
                    self.buckets = []
                remote.disconnect()

            if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
                upgrade_threads = self._async_update(upgrade_version, upgrade_nodes, save_upgrade_config=True)
            else:
                upgrade_threads = self._async_update(upgrade_version, upgrade_nodes)
            # wait upgrade statuses
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            success_upgrade = True
            while not self.queue.empty():
                success_upgrade &= self.queue.get()
            if not success_upgrade:
                self.fail("Upgrade failed. See logs above!")
            self.sleep(self.expire_time)
        # Run the post_upgrade operations
        self._create_ephemeral_buckets()
        self.post_upgrade(self.servers[:3])
        # Add new services after the upgrade
        if after_upgrade_services_in is not False:
            for upgrade_version in self.upgrade_versions:
                if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
                    upgrade_threads = self._async_update(upgrade_version, [self.servers[3]], save_upgrade_config=True)
                else:
                    upgrade_threads = self._async_update(upgrade_version, [self.servers[3]])

                for upgrade_thread in upgrade_threads:
                    upgrade_thread.join()
                success_upgrade = True
                while not self.queue.empty():
                    success_upgrade &= self.queue.get()
                if not success_upgrade:
                    self.fail("Upgrade failed. See logs above!")
                self.sleep(self.expire_time)
                self.cluster.rebalance(self.servers[:3], [self.servers[3]], [],
                                       services=["kv", "index", "n1ql"])
        # creating new buckets after upgrade
        if after_upgrade_buckets_in is not False:
            self.bucket_size = 256
            self._create_sasl_buckets(self.master, 1)
            self._create_standard_buckets(self.master, 1)
            if self.ddocs_num:
                self.create_ddocs_and_views()
                gen_load = BlobGenerator('upgrade', 'upgrade-', self.value_size, end=self.num_items)
                self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)
        # deleting buckets after upgrade
        if after_upgrade_buckets_out is not False:
            self._all_buckets_delete(self.master)
        # flushing buckets after upgrade
        if after_upgrade_buckets_flush is not False:
            self._all_buckets_flush()

    def test_offline_upgrade_kv_service_in_new_version(self):
        after_upgrade_services_in = self.input.param("after_upgrade_services_in", False)
        after_upgrade_buckets_in = self.input.param("after_upgrade_buckets_in", False)
        after_upgrade_buckets_out = self.input.param("after_upgrade_buckets_out", False)
        after_upgrade_buckets_flush = self.input.param("after_upgrade_buckets_flush", False)

        # Install initial version on the specified nodes
        self._install(self.servers[:3])
        # Configure the nodes with services
        self.operations(self.servers[:3], services="kv,n1ql,index")
        # get the n1ql node which will be used in pre,during and post upgrade for running n1ql commands
        self.n1ql_server = self.get_nodes_from_services_map(
            service_type="n1ql")
        # Run the pre upgrade operations, typically creating index
        self.pre_upgrade(self.servers[:3])
        seqno_expected = 1
        if self.ddocs_num:
            self.create_ddocs_and_views()
            if self.input.param('run_index', False):
                self.verify_all_queries()
        if not self.initial_version.startswith("1.") and self.input.param('check_seqno', True):
            self.check_seqno(seqno_expected)
        if self.during_ops:
            for opn in self.during_ops:
                if opn != 'add_back_failover':
                    getattr(self, opn)()
        # upgrade kv node to new version
        upgrade_nodes = [self.servers[0]]
        for upgrade_version in self.upgrade_versions:
            self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for upgrade to {0} version". \
                       format(upgrade_version))
            for server in upgrade_nodes:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                self.sleep(self.sleep_time)
                if self.wait_expire:
                    self.sleep(self.expire_time)
                if self.input.param('remove_manifest_files', False):
                    for file in ['manifest.txt', 'manifest.xml', 'VERSION.txt,']:
                        output, error = remote.execute_command("rm -rf /opt/couchbase/{0}".format(file))
                        remote.log_command_output(output, error)
                if self.input.param('remove_config_files', False):
                    for file in ['config', 'couchbase-server.node', 'ip', 'couchbase-server.cookie']:
                        output, error = remote.execute_command(
                            "rm -rf /opt/couchbase/var/lib/couchbase/{0}".format(file))
                        remote.log_command_output(output, error)
                    self.buckets = []
                remote.disconnect()

            if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
                upgrade_threads = self._async_update(upgrade_version, upgrade_nodes, save_upgrade_config=True)
            else:
                upgrade_threads = self._async_update(upgrade_version, upgrade_nodes)
            # wait upgrade statuses
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            success_upgrade = True
            while not self.queue.empty():
                success_upgrade &= self.queue.get()
            if not success_upgrade:
                self.fail("Upgrade failed. See logs above!")
            self.sleep(self.expire_time)
        # Run the post_upgrade operations
        self._create_ephemeral_buckets()
        self.post_upgrade(self.servers[:3])
        # Add new services after the upgrade
        if after_upgrade_services_in is not False:
            for upgrade_version in self.upgrade_versions:
                if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
                    upgrade_threads = self._async_update(upgrade_version, [self.servers[3]], save_upgrade_config=True)
                else:
                    upgrade_threads = self._async_update(upgrade_version, [self.servers[3]])

                for upgrade_thread in upgrade_threads:
                    upgrade_thread.join()
                success_upgrade = True
                while not self.queue.empty():
                    success_upgrade &= self.queue.get()
                if not success_upgrade:
                    self.fail("Upgrade failed. See logs above!")
                self.sleep(self.expire_time)
                self.cluster.rebalance(self.servers[:3], [self.servers[3]], [],
                                       services=["kv", "index", "n1ql"])
        # creating new buckets after upgrade
        if after_upgrade_buckets_in is not False:
            self.bucket_size = 256
            self._create_sasl_buckets(self.master, 1)
            self._create_standard_buckets(self.master, 1)
            if self.ddocs_num:
                self.create_ddocs_and_views()
                gen_load = BlobGenerator('upgrade', 'upgrade-', self.value_size, end=self.num_items)
                self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)
        # deleting buckets after upgrade
        if after_upgrade_buckets_out is not False:
            self._all_buckets_delete(self.master)
        # flushing buckets after upgrade
        if after_upgrade_buckets_flush is not False:
            self._all_buckets_flush()

    def test_offline_upgrade_n1ql_index_service_in_new_version(self):
        after_upgrade_services_in = self.input.param("after_upgrade_services_in", False)
        after_upgrade_buckets_in = self.input.param("after_upgrade_buckets_in", False)
        after_upgrade_buckets_out = self.input.param("after_upgrade_buckets_out", False)
        after_upgrade_buckets_flush = self.input.param("after_upgrade_buckets_flush", False)

        # Install initial version on the specified nodes
        self._install(self.servers[:3])
        # Configure the nodes with services
        self.operations(self.servers[:3], services="kv,n1ql,index")
        # get the n1ql node which will be used in pre,during and post upgrade for running n1ql commands
        self.n1ql_server = self.get_nodes_from_services_map(
            service_type="n1ql")
        # Run the pre upgrade operations, typically creating index
        self.pre_upgrade(self.servers[:3])
        seqno_expected = 1
        if self.ddocs_num:
            self.create_ddocs_and_views()
            if self.input.param('run_index', False):
                self.verify_all_queries()
        if not self.initial_version.startswith("1.") and self.input.param('check_seqno', True):
            self.check_seqno(seqno_expected)
        if self.during_ops:
            for opn in self.during_ops:
                if opn != 'add_back_failover':
                    getattr(self, opn)()
        # upgrade n1ql and index nodes to new version
        upgrade_nodes = self.servers[1:3]
        for upgrade_version in self.upgrade_versions:
            self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for upgrade to {0} version". \
                       format(upgrade_version))
            for server in upgrade_nodes:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                self.sleep(self.sleep_time)
                if self.wait_expire:
                    self.sleep(self.expire_time)
                if self.input.param('remove_manifest_files', False):
                    for file in ['manifest.txt', 'manifest.xml', 'VERSION.txt,']:
                        output, error = remote.execute_command("rm -rf /opt/couchbase/{0}".format(file))
                        remote.log_command_output(output, error)
                if self.input.param('remove_config_files', False):
                    for file in ['config', 'couchbase-server.node', 'ip', 'couchbase-server.cookie']:
                        output, error = remote.execute_command(
                            "rm -rf /opt/couchbase/var/lib/couchbase/{0}".format(file))
                        remote.log_command_output(output, error)
                    self.buckets = []
                remote.disconnect()

            if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
                upgrade_threads = self._async_update(upgrade_version, upgrade_nodes, save_upgrade_config=True)
            else:
                upgrade_threads = self._async_update(upgrade_version, upgrade_nodes)
            # wait upgrade statuses
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            success_upgrade = True
            while not self.queue.empty():
                success_upgrade &= self.queue.get()
            if not success_upgrade:
                self.fail("Upgrade failed. See logs above!")
            self.sleep(self.expire_time)
        # Run the post_upgrade operations
        self._create_ephemeral_buckets()
        self.post_upgrade(self.servers[:3])
        # Add new services after the upgrade
        if after_upgrade_services_in is not False:
            for upgrade_version in self.upgrade_versions:
                if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
                    upgrade_threads = self._async_update(upgrade_version, [self.servers[3]], save_upgrade_config=True)
                else:
                    upgrade_threads = self._async_update(upgrade_version, [self.servers[3]])

                for upgrade_thread in upgrade_threads:
                    upgrade_thread.join()
                success_upgrade = True
                while not self.queue.empty():
                    success_upgrade &= self.queue.get()
                if not success_upgrade:
                    self.fail("Upgrade failed. See logs above!")
                self.sleep(self.expire_time)
                self.cluster.rebalance(self.servers[:3], [self.servers[3]], [],
                                       services=["kv", "index", "n1ql"])
        # creating new buckets after upgrade
        if after_upgrade_buckets_in is not False:
            self.bucket_size = 256
            self._create_sasl_buckets(self.master, 1)
            self._create_standard_buckets(self.master, 1)
            if self.ddocs_num:
                self.create_ddocs_and_views()
                gen_load = BlobGenerator('upgrade', 'upgrade-', self.value_size, end=self.num_items)
                self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)
        # deleting buckets after upgrade
        if after_upgrade_buckets_out is not False:
            self._all_buckets_delete(self.master)
        # flushing buckets after upgrade
        if after_upgrade_buckets_flush is not False:
            self._all_buckets_flush()

    def test_offline_upgrade_kv_index_service_in_new_version(self):
        after_upgrade_services_in = self.input.param("after_upgrade_services_in", False)
        after_upgrade_buckets_in = self.input.param("after_upgrade_buckets_in", False)
        after_upgrade_buckets_out = self.input.param("after_upgrade_buckets_out", False)
        after_upgrade_buckets_flush = self.input.param("after_upgrade_buckets_flush", False)

        # Install initial version on the specified nodes
        self._install(self.servers[:3])
        # Configure the nodes with services
        self.operations(self.servers[:3], services="kv,index,n1ql")
        # get the n1ql node which will be used in pre,during and post upgrade for running n1ql commands
        self.n1ql_server = self.get_nodes_from_services_map(
            service_type="n1ql")
        # Run the pre upgrade operations, typically creating index
        self.pre_upgrade(self.servers[:3])
        seqno_expected = 1
        if self.ddocs_num:
            self.create_ddocs_and_views()
            if self.input.param('run_index', False):
                self.verify_all_queries()
        if not self.initial_version.startswith("1.") and self.input.param('check_seqno', True):
            self.check_seqno(seqno_expected)
        if self.during_ops:
            for opn in self.during_ops:
                if opn != 'add_back_failover':
                    getattr(self, opn)()
        # upgrade kv and index nodes to new version
        upgrade_nodes = self.servers[0:2]
        for upgrade_version in self.upgrade_versions:
            self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for upgrade to {0} version". \
                       format(upgrade_version))
            for server in upgrade_nodes:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                self.sleep(self.sleep_time)
                if self.wait_expire:
                    self.sleep(self.expire_time)
                if self.input.param('remove_manifest_files', False):
                    for file in ['manifest.txt', 'manifest.xml', 'VERSION.txt,']:
                        output, error = remote.execute_command("rm -rf /opt/couchbase/{0}".format(file))
                        remote.log_command_output(output, error)
                if self.input.param('remove_config_files', False):
                    for file in ['config', 'couchbase-server.node', 'ip', 'couchbase-server.cookie']:
                        output, error = remote.execute_command(
                            "rm -rf /opt/couchbase/var/lib/couchbase/{0}".format(file))
                        remote.log_command_output(output, error)
                    self.buckets = []
                remote.disconnect()

            if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
                upgrade_threads = self._async_update(upgrade_version, upgrade_nodes, save_upgrade_config=True)
            else:
                upgrade_threads = self._async_update(upgrade_version, upgrade_nodes)
            # wait upgrade statuses
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            success_upgrade = True
            while not self.queue.empty():
                success_upgrade &= self.queue.get()
            if not success_upgrade:
                self.fail("Upgrade failed. See logs above!")
            self.sleep(self.expire_time)
        # Run the post_upgrade operations
        self._create_ephemeral_buckets()
        self.post_upgrade(self.servers[:3])
        # Add new services after the upgrade
        if after_upgrade_services_in is not False:
            for upgrade_version in self.upgrade_versions:
                if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
                    upgrade_threads = self._async_update(upgrade_version, [self.servers[3]], save_upgrade_config=True)
                else:
                    upgrade_threads = self._async_update(upgrade_version, [self.servers[3]])

                for upgrade_thread in upgrade_threads:
                    upgrade_thread.join()
                success_upgrade = True
                while not self.queue.empty():
                    success_upgrade &= self.queue.get()
                if not success_upgrade:
                    self.fail("Upgrade failed. See logs above!")
                self.sleep(self.expire_time)
                self.cluster.rebalance(self.servers[:3], [self.servers[3]], [],
                                       services=["kv", "index", "n1ql"])
        # creating new buckets after upgrade
        if after_upgrade_buckets_in is not False:
            self.bucket_size = 256
            self._create_sasl_buckets(self.master, 1)
            self._create_standard_buckets(self.master, 1)
            if self.ddocs_num:
                self.create_ddocs_and_views()
                gen_load = BlobGenerator('upgrade', 'upgrade-', self.value_size, end=self.num_items)
                self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)
        # deleting buckets after upgrade
        if after_upgrade_buckets_out is not False:
            self._all_buckets_delete(self.master)
        # flushing buckets after upgrade
        if after_upgrade_buckets_flush is not False:
            self._all_buckets_flush()

    def test_offline_upgrade_kv_n1ql_service_in_new_version(self):
        after_upgrade_services_in = self.input.param("after_upgrade_services_in", False)
        after_upgrade_buckets_in = self.input.param("after_upgrade_buckets_in", False)
        after_upgrade_buckets_out = self.input.param("after_upgrade_buckets_out", False)
        after_upgrade_buckets_flush = self.input.param("after_upgrade_buckets_flush", False)

        # Install initial version on the specified nodes
        self._install(self.servers[:3])
        # Configure the nodes with services
        self.operations(self.servers[:3], services="kv,index,n1ql")
        # get the n1ql node which will be used in pre,during and post upgrade for running n1ql commands
        self.n1ql_server = self.get_nodes_from_services_map(
            service_type="n1ql")
        # Run the pre upgrade operations, typically creating index
        self.pre_upgrade(self.servers[:3])
        seqno_expected = 1
        if self.ddocs_num:
            self.create_ddocs_and_views()
            if self.input.param('run_index', False):
                self.verify_all_queries()
        if not self.initial_version.startswith("1.") and self.input.param('check_seqno', True):
            self.check_seqno(seqno_expected)
        if self.during_ops:
            for opn in self.during_ops:
                if opn != 'add_back_failover':
                    getattr(self, opn)()
        # upgrade kv and n1ql nodes to new version
        upgrade_nodes = [self.servers[0], self.servers[2]]
        for upgrade_version in self.upgrade_versions:
            self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for upgrade to {0} version". \
                       format(upgrade_version))
            for server in upgrade_nodes:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                self.sleep(self.sleep_time)
                if self.wait_expire:
                    self.sleep(self.expire_time)
                if self.input.param('remove_manifest_files', False):
                    for file in ['manifest.txt', 'manifest.xml', 'VERSION.txt,']:
                        output, error = remote.execute_command("rm -rf /opt/couchbase/{0}".format(file))
                        remote.log_command_output(output, error)
                if self.input.param('remove_config_files', False):
                    for file in ['config', 'couchbase-server.node', 'ip', 'couchbase-server.cookie']:
                        output, error = remote.execute_command(
                            "rm -rf /opt/couchbase/var/lib/couchbase/{0}".format(file))
                        remote.log_command_output(output, error)
                    self.buckets = []
                remote.disconnect()

            if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
                upgrade_threads = self._async_update(upgrade_version, upgrade_nodes, save_upgrade_config=True)
            else:
                upgrade_threads = self._async_update(upgrade_version, upgrade_nodes)
            # wait upgrade statuses
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            success_upgrade = True
            while not self.queue.empty():
                success_upgrade &= self.queue.get()
            if not success_upgrade:
                self.fail("Upgrade failed. See logs above!")
            self.sleep(self.expire_time)
        # Run the post_upgrade operations
        self._create_ephemeral_buckets()
        self.post_upgrade(self.servers[:3])
        # Add new services after the upgrade
        if after_upgrade_services_in is not False:
            for upgrade_version in self.upgrade_versions:
                if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
                    upgrade_threads = self._async_update(upgrade_version, [self.servers[3]], save_upgrade_config=True)
                else:
                    upgrade_threads = self._async_update(upgrade_version, [self.servers[3]])

                for upgrade_thread in upgrade_threads:
                    upgrade_thread.join()
                success_upgrade = True
                while not self.queue.empty():
                    success_upgrade &= self.queue.get()
                if not success_upgrade:
                    self.fail("Upgrade failed. See logs above!")
                self.sleep(self.expire_time)
                self.cluster.rebalance(self.servers[:3], [self.servers[3]], [],
                                       services=["kv", "index", "n1ql"])
        # creating new buckets after upgrade
        if after_upgrade_buckets_in is not False:
            self.bucket_size = 256
            self._create_sasl_buckets(self.master, 1)
            self._create_standard_buckets(self.master, 1)
            if self.ddocs_num:
                self.create_ddocs_and_views()
                gen_load = BlobGenerator('upgrade', 'upgrade-', self.value_size, end=self.num_items)
                self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)
        # deleting buckets after upgrade
        if after_upgrade_buckets_out is not False:
            self._all_buckets_delete(self.master)
        # flushing buckets after upgrade
        if after_upgrade_buckets_flush is not False:
            self._all_buckets_flush()

    def test_offline_upgrade_n1ql_index_service_in_new_version_with_multiple_nodes(self):
        after_upgrade_services_in = self.input.param("after_upgrade_services_in", False)
        after_upgrade_buckets_in = self.input.param("after_upgrade_buckets_in", False)
        after_upgrade_buckets_out = self.input.param("after_upgrade_buckets_out", False)
        after_upgrade_buckets_flush = self.input.param("after_upgrade_buckets_flush", False)

        self.bucket_size = 1000
        # Install initial version on the specified nodes
        self._install(self.servers[:6])
        self.init_installed_servers = len(self.servers[:6])
        # Configure the nodes with services
        self.operations(self.servers[:6], services="kv,kv,n1ql,index,n1ql,index")
        # get the n1ql node which will be used in pre,during and post upgrade for running n1ql commands
        self.n1ql_server = self.get_nodes_from_services_map(
            service_type="n1ql")
        # Run the pre upgrade operations, typically creating index
        self.pre_upgrade(self.servers[:6])
        seqno_expected = 1
        if self.ddocs_num:
            self.create_ddocs_and_views()
            if self.input.param('run_index', False):
                self.verify_all_queries()
        if not self.initial_version.startswith("1.") and self.input.param('check_seqno', True):
            self.check_seqno(seqno_expected)
        if self.during_ops:
            for opn in self.during_ops:
                if opn != 'add_back_failover':
                    getattr(self, opn)()
        # upgrade index and n1ql nodes to new version when we have multiple nodes
        upgrade_nodes = self.servers[4:6]
        for upgrade_version in self.upgrade_versions:
            self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for upgrade to {0} version". \
                       format(upgrade_version))
            for server in upgrade_nodes:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                self.sleep(self.sleep_time)
                if self.wait_expire:
                    self.sleep(self.expire_time)
                if self.input.param('remove_manifest_files', False):
                    for file in ['manifest.txt', 'manifest.xml', 'VERSION.txt,']:
                        output, error = remote.execute_command("rm -rf /opt/couchbase/{0}".format(file))
                        remote.log_command_output(output, error)
                if self.input.param('remove_config_files', False):
                    for file in ['config', 'couchbase-server.node', 'ip', 'couchbase-server.cookie']:
                        output, error = remote.execute_command(
                            "rm -rf /opt/couchbase/var/lib/couchbase/{0}".format(file))
                        remote.log_command_output(output, error)
                    self.buckets = []
                remote.disconnect()

            if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
                upgrade_threads = self._async_update(upgrade_version, upgrade_nodes, save_upgrade_config=True)
            else:
                upgrade_threads = self._async_update(upgrade_version, upgrade_nodes)
            # wait upgrade statuses
            self.n1ql_server = self.get_nodes_from_services_map(
                service_type="n1ql")
            self.during_upgrade(self.servers[:6])
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            success_upgrade = True
            while not self.queue.empty():
                success_upgrade &= self.queue.get()
            if not success_upgrade:
                self.fail("Upgrade failed. See logs above!")
            self.sleep(self.expire_time)
        # Run the post_upgrade operations
        self._create_ephemeral_buckets()
        self.post_upgrade(self.servers[:6])
        # Add new services after the upgrade
        if after_upgrade_services_in is not False:
            for upgrade_version in self.upgrade_versions:
                if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
                    upgrade_threads = self._async_update(upgrade_version, [self.servers[6]], save_upgrade_config=True)
                else:
                    if self.init_installed_servers == len(self.servers):
                        upgrade_threads = self._async_update(upgrade_version, [self.servers[6]])
                    else:
                        self._install([self.servers[6]], version=upgrade_version)
                if self.init_installed_servers == len(self.servers):
                    for upgrade_thread in upgrade_threads:
                        upgrade_thread.join()
                    success_upgrade = True
                    while not self.queue.empty():
                        success_upgrade &= self.queue.get()
                    if not success_upgrade:
                        self.fail("Upgrade failed. See logs above!")
                    self.sleep(120)
                self.cluster.rebalance(self.servers[:6], [self.servers[6]], [],
                                       services=["kv", "index", "n1ql"])
        # creating new buckets after upgrade
        if after_upgrade_buckets_in is not False:
            self.bucket_size = 256
            self._create_sasl_buckets(self.master, 1)
            self._create_standard_buckets(self.master, 1)
            if self.ddocs_num:
                self.create_ddocs_and_views()
                gen_load = BlobGenerator('upgrade', 'upgrade-', self.value_size, end=self.num_items)
                self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)
        # deleting buckets after upgrade
        if after_upgrade_buckets_out is not False:
            self._all_buckets_delete(self.master)
        # flushing buckets after upgrade
        if after_upgrade_buckets_flush is not False:
            self._all_buckets_flush()

    def test_offline_upgrade_kv_index_service_in_new_version_with_multiple_nodes(self):
        after_upgrade_services_in = self.input.param("after_upgrade_services_in", False)
        after_upgrade_buckets_in = self.input.param("after_upgrade_buckets_in", False)
        after_upgrade_buckets_out = self.input.param("after_upgrade_buckets_out", False)
        after_upgrade_buckets_flush = self.input.param("after_upgrade_buckets_flush", False)

        self.bucket_size = 1000
        # Install initial version on the specified nodes
        if self.clean_upgrade_install:
            self._install(self.servers[:6])
            self.init_installed_servers = len(self.servers[:6])
        else:
            self._install(self.servers)
            self.init_installed_servers = len(self.servers)
        # Configure the nodes with services
        self.operations(self.servers[:6], services="kv,index,n1ql,n1ql,kv,index")
        # get the n1ql node which will be used in pre,during and post upgrade
        # for running n1ql commands
        self.n1ql_server = self.get_nodes_from_services_map(
            service_type="n1ql")
        # Run the pre upgrade operations, typically creating index
        self.pre_upgrade(self.servers[:6])
        seqno_expected = 1
        if self.ddocs_num:
            self.create_ddocs_and_views()
            if self.input.param('run_index', False):
                self.verify_all_queries()
        if not self.initial_version.startswith("1.") and self.input.param('check_seqno', True):
            self.check_seqno(seqno_expected)
        if self.during_ops:
            for opn in self.during_ops:
                if opn != 'add_back_failover':
                    getattr(self, opn)()
        # upgrade index and kv nodes to new version when we have multiple nodes
        upgrade_nodes = self.servers[4:6]
        for upgrade_version in self.upgrade_versions:
            self.sleep(self.sleep_time, "Pre-setup of old version is done.  "\
                                        "Wait for upgrade to {0} version". \
                                                     format(upgrade_version))
            for server in upgrade_nodes:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                self.sleep(self.sleep_time)
                if self.wait_expire:
                    self.sleep(self.expire_time)
                if self.input.param('remove_manifest_files', False):
                    for file in ['manifest.txt', 'manifest.xml', 'VERSION.txt,']:
                        cmd = "rm -rf /opt/couchbase/{0}".format(file)
                        output, error = remote.execute_command(cmd)
                        remote.log_command_output(output, error)
                if self.input.param('remove_config_files', False):
                    for file in ['config', 'couchbase-server.node', 'ip',\
                                 'couchbase-server.cookie']:
                        cmd = "rm -rf /opt/couchbase/var/lib/couchbase/{0}".format(file)
                        output, error = remote.execute_command(cmd)
                        remote.log_command_output(output, error)
                    self.buckets = []
                remote.disconnect()

            if self.initial_build_type == "community" and \
               self.upgrade_build_type == "enterprise":
                upgrade_threads = self._async_update(upgrade_version,\
                                                     upgrade_nodes, \
                                                     save_upgrade_config=True)
            else:
                upgrade_threads = self._async_update(upgrade_version,\
                                                     upgrade_nodes)
            # wait upgrade statuses
            self.n1ql_server = self.get_nodes_from_services_map(
                service_type="n1ql")
            self.during_upgrade(self.servers[:6])
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            success_upgrade = True
            while not self.queue.empty():
                success_upgrade &= self.queue.get()
            if not success_upgrade:
                self.fail("Upgrade failed. See logs above!")
            self.sleep(self.expire_time)
        # Run the post_upgrade operations
        self._create_ephemeral_buckets()
        self.post_upgrade(self.servers[:6])
        # Add new services after the upgrade
        if after_upgrade_services_in is not False:
            for upgrade_version in self.upgrade_versions:
                if self.initial_build_type == "community" and \
                   self.upgrade_build_type == "enterprise":
                    upgrade_threads = self._async_update(upgrade_version,
                                                         [self.servers[6]],
                                                         save_upgrade_config=True)
                else:
                    if self.init_installed_servers == len(self.servers):
                        self.log.info("It will be upgrade install {0}in new node"
                                                        .format(upgrade_version))
                        upgrade_threads = self._async_update(upgrade_version,
                                                             [self.servers[6]])
                    else:
                        self.log.info("It will be clean install {0} in new node"
                                                        .format(upgrade_version))
                        self._install([self.servers[6]], version=upgrade_version)
                if self.init_installed_servers == len(self.servers):
                    for upgrade_thread in upgrade_threads:
                        upgrade_thread.join()
                    success_upgrade = True
                    while not self.queue.empty():
                        success_upgrade &= self.queue.get()
                    if not success_upgrade:
                        self.fail("Upgrade failed. See logs above!")
                    self.sleep(120)
                self.cluster.rebalance(self.servers[:6], [self.servers[6]], [],
                                       services=["kv", "index", "n1ql"])
        # creating new buckets after upgrade
        if after_upgrade_buckets_in is not False:
            self.bucket_size = 256
            self._create_sasl_buckets(self.master, 1)
            self._create_standard_buckets(self.master, 1)
            if self.ddocs_num:
                self.create_ddocs_and_views()
                gen_load = BlobGenerator('upgrade', 'upgrade-',\
                                          self.value_size, end=self.num_items)
                self._load_all_buckets(self.master, gen_load, "create",\
                                       self.expire_time, flag=self.item_flag)
        # deleting buckets after upgrade
        if after_upgrade_buckets_out is not False:
            self._all_buckets_delete(self.master)
        # flushing buckets after upgrade
        if after_upgrade_buckets_flush is not False:
            self._all_buckets_flush()

    def test_offline_upgrade_kv_n1ql_service_in_new_version_with_multiple_nodes(self):
        after_upgrade_services_in = self.input.param("after_upgrade_services_in", False)
        after_upgrade_buckets_in = self.input.param("after_upgrade_buckets_in", False)
        after_upgrade_buckets_out = self.input.param("after_upgrade_buckets_out", False)
        after_upgrade_buckets_flush = self.input.param("after_upgrade_buckets_flush", False)

        self.bucket_size = 1000
        # Install initial version on the specified nodes
        self._install(self.servers[:6])
        # Configure the nodes with services
        self.operations(self.servers[:6], services="kv,index,n1ql,index,kv,n1ql")
        # get the n1ql node which will be used in pre,during and post upgrade for running n1ql commands
        self.n1ql_server = self.get_nodes_from_services_map(
            service_type="n1ql")
        # Run the pre upgrade operations, typically creating index
        self.pre_upgrade(self.servers[:6])
        seqno_expected = 1
        if self.ddocs_num:
            self.create_ddocs_and_views()
            if self.input.param('run_index', False):
                self.verify_all_queries()
        if not self.initial_version.startswith("1.") and self.input.param('check_seqno', True):
            self.check_seqno(seqno_expected)
        if self.during_ops:
            for opn in self.during_ops:
                if opn != 'add_back_failover':
                    getattr(self, opn)()
        # upgrade n1ql and kv nodes to new version when we have multiple nodes
        upgrade_nodes = self.servers[4:6]
        for upgrade_version in self.upgrade_versions:
            self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for upgrade to {0} version". \
                       format(upgrade_version))
            for server in upgrade_nodes:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                self.sleep(self.sleep_time)
                if self.wait_expire:
                    self.sleep(self.expire_time)
                if self.input.param('remove_manifest_files', False):
                    for file in ['manifest.txt', 'manifest.xml', 'VERSION.txt,']:
                        output, error = remote.execute_command("rm -rf /opt/couchbase/{0}".format(file))
                        remote.log_command_output(output, error)
                if self.input.param('remove_config_files', False):
                    for file in ['config', 'couchbase-server.node', 'ip', 'couchbase-server.cookie']:
                        output, error = remote.execute_command(
                            "rm -rf /opt/couchbase/var/lib/couchbase/{0}".format(file))
                        remote.log_command_output(output, error)
                    self.buckets = []
                remote.disconnect()

            if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
                upgrade_threads = self._async_update(upgrade_version, upgrade_nodes, save_upgrade_config=True)
            else:
                upgrade_threads = self._async_update(upgrade_version, upgrade_nodes)
            # wait upgrade statuses
            self.n1ql_server = self.get_nodes_from_services_map(
                service_type="n1ql")
            self.during_upgrade(self.servers[:6])
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            success_upgrade = True
            while not self.queue.empty():
                success_upgrade &= self.queue.get()
            if not success_upgrade:
                self.fail("Upgrade failed. See logs above!")
            self.sleep(self.expire_time)
        # Run the post_upgrade operations
        self._create_ephemeral_buckets()
        self.post_upgrade(self.servers[:6])
        # Add new services after the upgrade
        if after_upgrade_services_in is not False:
            for upgrade_version in self.upgrade_versions:
                if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
                    upgrade_threads = self._async_update(upgrade_version, [self.servers[6]], save_upgrade_config=True)
                else:
                    upgrade_threads = self._async_update(upgrade_version, [self.servers[6]])

                for upgrade_thread in upgrade_threads:
                    upgrade_thread.join()
                success_upgrade = True
                while not self.queue.empty():
                    success_upgrade &= self.queue.get()
                if not success_upgrade:
                    self.fail("Upgrade failed. See logs above!")
                self.sleep(120)
                self.cluster.rebalance(self.servers[:6], [self.servers[6]], [],
                                       services=["kv", "index", "n1ql"])
        # creating new buckets after upgrade
        if after_upgrade_buckets_in is not False:
            self.bucket_size = 256
            self._create_sasl_buckets(self.master, 1)
            self._create_standard_buckets(self.master, 1)
            if self.ddocs_num:
                self.create_ddocs_and_views()
                gen_load = BlobGenerator('upgrade', 'upgrade-', self.value_size, end=self.num_items)
                self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)
        # deleting buckets after upgrade
        if after_upgrade_buckets_out is not False:
            self._all_buckets_delete(self.master)
        # flushing buckets after upgrade
        if after_upgrade_buckets_flush is not False:
            self._all_buckets_flush()

    def test_offline_upgrade_kv_index_with_index_swap_rebalance(self):
        after_upgrade_services_in = self.input.param(
            "after_upgrade_services_in", False)
        after_upgrade_buckets_in = self.input.param("after_upgrade_buckets_in",
                                                    False)
        after_upgrade_buckets_out = self.input.param(
            "after_upgrade_buckets_out", False)
        after_upgrade_buckets_flush = self.input.param(
            "after_upgrade_buckets_flush", False)

        # Install initial version on the specified nodes
        self._install(self.servers[:3])
        # Configure the nodes with services
        self.operations(self.servers[:3], services="kv,index,n1ql")
        # get the n1ql node which will be used in pre,during and post upgrade for running n1ql commands
        self.n1ql_server = self.get_nodes_from_services_map(
            service_type="n1ql")
        self.index_server = self.get_nodes_from_services_map(
            service_type="index")
        # Run the pre upgrade operations, typically creating index
        self.pre_upgrade(self.servers[:3])
        seqno_expected = 1
        if self.ddocs_num:
            self.create_ddocs_and_views()
            if self.input.param('run_index', False):
                self.verify_all_queries()
        if not self.initial_version.startswith("1.") and self.input.param(
                'check_seqno', True):
            self.check_seqno(seqno_expected)
        if self.during_ops:
            for opn in self.during_ops:
                if opn != 'add_back_failover':
                    getattr(self, opn)()
        # upgrade kv and index nodes to new version
        upgrade_nodes = self.servers[:3]
        for upgrade_version in self.upgrade_versions:
            self.sleep(self.sleep_time,
                       "Pre-setup of old version is done. Wait for upgrade to {0} version". \
                       format(upgrade_version))
            for server in upgrade_nodes:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                self.sleep(self.sleep_time)
                if self.wait_expire:
                    self.sleep(self.expire_time)
                if self.input.param('remove_manifest_files', False):
                    for file in ['manifest.txt', 'manifest.xml',
                                 'VERSION.txt,']:
                        output, error = remote.execute_command(
                            "rm -rf /opt/couchbase/{0}".format(file))
                        remote.log_command_output(output, error)
                if self.input.param('remove_config_files', False):
                    for file in ['config', 'couchbase-server.node', 'ip',
                                 'couchbase-server.cookie']:
                        output, error = remote.execute_command(
                            "rm -rf /opt/couchbase/var/lib/couchbase/{0}".format(
                                file))
                        remote.log_command_output(output, error)
                    self.buckets = []
                remote.disconnect()

            if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
                upgrade_threads = self._async_update(upgrade_version,
                                                     upgrade_nodes,
                                                     save_upgrade_config=True)
            else:
                upgrade_threads = self._async_update(upgrade_version,
                                                     upgrade_nodes)
            # wait upgrade statuses
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            success_upgrade = True
            while not self.queue.empty():
                success_upgrade &= self.queue.get()
            if not success_upgrade:
                self.fail("Upgrade failed. See logs above!")
            self.sleep(self.expire_time)
        # Add new services after the upgrade
        self.log.info("Indexer node : {}".format(self.index_server))
        for upgrade_version in self.upgrade_versions:
            rest = RestConnection(self.master)
            versions = rest.get_nodes_versions()
            for version in versions:
                if "5" > version:
                    self.log.info("Atleast one of the nodes in the cluster is "
                                  "pre 5.0 version. Hence not performing "
                                  "swap rebalance of indexer node")
                    break
            if "5" > upgrade_version:
                break
            if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
                upgrade_threads = self._async_update(upgrade_version,
                                                     [self.servers[3]],
                                                     save_upgrade_config=True)
            else:
                upgrade_threads = self._async_update(upgrade_version,
                                                     [self.servers[3]])

            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            success_upgrade = True
            while not self.queue.empty():
                success_upgrade &= self.queue.get()
            if not success_upgrade:
                self.fail("Upgrade failed. See logs above!")
            self.sleep(self.expire_time)

            self.sleep(180, "Sleep for check")
            map_before_rebalance, stats_map_before_rebalance = \
                self._return_maps()
            index_node = self.get_nodes_from_services_map(service_type="index")
            to_add_nodes = [self.servers[3]]
            to_remove_nodes = [index_node]
            services_in = ["index"]
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init], to_add_nodes, [],
                services=services_in)
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached,
                            "rebalance failed, stuck or did not complete")
            rebalance.result()
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init + 1], [], to_remove_nodes)
            index_server = self.get_nodes_from_services_map(
                service_type="index", get_all_nodes=False)
            for i in range(20):
                output = self.rest.list_indexer_rebalance_tokens(
                    server=index_server)
                if "rebalancetoken" in output:
                    self.log.info(output)
                    break
                self.sleep(2)
            if i == 19 and "rebalancetoken" not in output:
                self.log.warning(
                    "rebalancetoken was not returned by /listRebalanceTokens during gsi rebalance")
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached,
                            "rebalance failed, stuck or did not complete")
            rebalance.result()
            self.sleep(30)
            map_after_rebalance, stats_map_after_rebalance = \
                self._return_maps()
            self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                          stats_map_before_rebalance, stats_map_after_rebalance,
                                                          [self.servers[3]], [self.servers[1]], swap_rebalance=True,
                                                          use_https=self.use_https)
        self.post_upgrade(self.servers[:3])

    def test_offline_upgrade_with_add_new_services(self):
        """
           test run as below.  Change params as needed.
           newupgradetests.MultiNodesUpgradeTests.test_offline_upgrade_with_add_new_services,
           initial_version=4.x.x-xxx,nodes_init=2,ddocs_num=3,pre_upgrade=create_index,
           post_upgrade=drop_index,doc-per-day=1,dataset=default,groups=simple,
           after_upgrade_buckets_flush=True,skip_init_check_cbserver=True,
           gsi_type=memory_optimized,init_nodes=False,upgrade_version=5.5.0-xxx,
           initial-services-setting='kv,index-kv,n1ql',after_upgrade_services_in=eventing,
           dgm_run=True,upgrade_test=True
           if test failover upgrade, add param offline_failover_upgrade=True
        """
        if len(self.servers) < 3 :
            self.fail("This test needs at least 4 servers to run")
        initial_services_setting = self.input.param("initial-services-setting", None)
        after_upgrade_node_in = self.input.param("after_upgrade_node_in", 0)
        after_upgrade_services_in = self.input.param("after_upgrade_services_in", False)
        after_upgrade_buckets_in = self.input.param("after_upgrade_buckets_in", False)
        after_upgrade_buckets_out = self.input.param("after_upgrade_buckets_out", False)
        after_upgrade_buckets_flush = self.input.param("after_upgrade_buckets_flush", False)
        self.is_replica_index_in_pre_upgrade = False

        # Install initial version on the specified nodes
        nodes_init = 2
        fts_obj = None
        if 5 <= int(self.initial_version[:1]) and 5.5 > float(self.initial_version[:3]):
            nodes_init = 3
            if initial_services_setting is not None:
                initial_services_setting += "-kv,fts"
                self.is_fts_in_pre_upgrade = True
        if 5.5 <= float(self.initial_version[:3]) and self.num_index_replicas > 0:
            if len(self.servers) < 5 :
                self.fail("This test needs at least 5 servers to run")
            nodes_init = 4
            if initial_services_setting is not None:
                tmp = initial_services_setting.split("-")
                if nodes_init > len(tmp):
                    if (nodes_init - len(tmp)) == 1:
                        initial_services_setting += "-kv,fts,index"
                    elif (nodes_init - len(tmp)) == 2:
                        initial_services_setting += "-kv,n1ql-kv,fts,index"
                self.is_fts_in_pre_upgrade = True
        self._install(self.servers[:nodes_init])
        # Configure the nodes with services
        self.operations(self.servers[:nodes_init], services=initial_services_setting)
        # get the n1ql node which will be used in pre,during and post upgrade for
        # running n1ql commands
        self.n1ql_server = self.get_nodes_from_services_map(
            service_type="n1ql")
        # Run the pre upgrade operations, typically creating index
        fts_obj = self.create_fts_index_query_compare()

        self.buckets = RestConnection(self.master).get_buckets()
        if 5 <= int(self.initial_version[:1]) and 5.5 > float(self.initial_version[:3]):
            self.pre_upgrade(self.servers[:nodes_init])
        elif 5.5 <= float(self.initial_version[:3]) and self.num_index_replicas > 0:
            self.pre_upgrade(self.servers[:nodes_init])
        seqno_expected = 1
        if self.ddocs_num:
            self.sleep(10)
            self.create_ddocs_and_views()
            if self.input.param('run_index', False):
                self.verify_all_queries()
        """ from 5.5 we support replica index.  Need to test this feature. """
        if 5.5 <= float(self.initial_version[:3]) and self.num_index_replicas > 0:
            self.sleep(10)
            self.create_index_with_replica_and_query()
            self.is_replica_index_in_pre_upgrade = True

        if not self.initial_version.startswith("1.") and self.input.param('check_seqno', True):
            self.check_seqno(seqno_expected)
        if self.during_ops:
            for opn in self.during_ops:
                if opn != 'add_back_failover':
                    getattr(self, opn)()
        upgrade_nodes = self.servers[:nodes_init]
        for upgrade_version in self.upgrade_versions:
            self.sleep(self.sleep_time, \
                "Pre-setup of old version is done. Wait for upgrade to {0} version". \
                       format(upgrade_version))
            if self.offline_failover_upgrade:
                total_nodes = len(upgrade_nodes)
                for server in upgrade_nodes:
                    sleep_time = 0
                    reb_done = self.rest.monitorRebalance()
                    print("\nrebalance status: ", reb_done)
                    while not reb_done:
                        self.sleep(20, "wait for rebalance done")
                        sleep_time += 20
                        reb_done = self.rest.monitorRebalance()
                        if sleep_time == self.wait_timeout:
                            self.fail("Rebalance status does not update more than 60 secs after finish")
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
                    if total_nodes == 1:
                        rest_upgrade = RestConnection(upgrade_nodes[total_nodes])
                        healthy = RestHelper(rest_upgrade).is_cluster_healthy
                        count = 0
                        while not healthy:
                            self.sleep(5, "wait for cluster to update its config")
                            healthy = RestHelper(rest_upgrade).is_cluster_healthy
                            count +=1
                            if count == 5:
                                self.fail("Cluster does not ready after 1 minute")
                    self.cluster.rebalance(self.servers[:nodes_init], [], [])
                    total_nodes -= 1
                if total_nodes == 0:
                    self.rest = RestConnection(upgrade_nodes[total_nodes])
            else:
                for server in upgrade_nodes:
                    remote = RemoteMachineShellConnection(server)
                    remote.stop_server()
                    self.sleep(self.sleep_time)
                    if self.wait_expire:
                        self.sleep(self.expire_time)
                    if self.input.param('remove_manifest_files', False):
                        for file in ['manifest.txt', 'manifest.xml', 'VERSION.txt,']:
                            output, error = remote.execute_command("rm -rf /opt/couchbase/{0}"\
                                                                                 .format(file))
                            remote.log_command_output(output, error)
                    if self.input.param('remove_config_files', False):
                        for file in ['config', 'couchbase-server.node', 'ip',\
                                                   'couchbase-server.cookie']:
                            output, error = remote.execute_command(
                                "rm -rf /opt/couchbase/var/lib/couchbase/{0}".format(file))
                            remote.log_command_output(output, error)
                        self.buckets = []
                    remote.disconnect()

                if self.initial_build_type == "community" and \
                                self.upgrade_build_type == "enterprise":
                    upgrade_threads = self._async_update(upgrade_version, upgrade_nodes,
                                                                save_upgrade_config=True,
                                                                fts_query_limit=10000000)
                else:
                    upgrade_threads = self._async_update(upgrade_version, upgrade_nodes,
                                                               fts_query_limit=10000000)
                # wait upgrade statuses
                for upgrade_thread in upgrade_threads:
                    upgrade_thread.join()
                success_upgrade = True
                while not self.queue.empty():
                    success_upgrade &= self.queue.get()
                if not success_upgrade:
                    self.fail("Upgrade failed. See logs above!")
            self.sleep(self.expire_time)
        # Run the post_upgrade operations
        self._create_ephemeral_buckets()
        self.post_upgrade(self.servers[:nodes_init])
        # Add new services after the upgrade

        if after_upgrade_services_in is not False:
            for upgrade_version in self.upgrade_versions:
                self._install([self.servers[nodes_init]],
                               version=self.upgrade_versions[0])
                self.sleep(self.expire_time)
                try:
                    self.log.info("Add new node {0}after upgrade"\
                                  .format(self.servers[nodes_init].ip))
                    self.cluster.rebalance(self.servers[:(nodes_init + 1)],
                                            [self.servers[nodes_init]], [],
                                             services=[after_upgrade_services_in])
                    self.sleep(15)
                except Exception as e:
                    self.fail(e)

        if not self.is_fts_in_pre_upgrade:
            if after_upgrade_services_in and "fts" in after_upgrade_services_in:
                self.log.info("Create, run and verify fts after upgrade")
                fts_obj = self.create_fts_index_query_compare()
                fts_obj.delete_all()
        elif self.is_fts_in_pre_upgrade:
            self.log.info("Query fts after upgrade to verify")
            for index in fts_obj.fts_indexes:
                fts_obj.run_query_and_compare(index=index, num_queries=20)
            fts_obj.delete_all()
        if self.is_replica_index_in_pre_upgrade:
            self.log.info("verify replica index after upgrade")
            self.verify_index_with_replica_and_query()
        if after_upgrade_services_in:
            if "eventing" in after_upgrade_services_in:
                self.dataset = self.input.param("dataset", "default")
                self.create_eventing_services()
            if "cbas" in after_upgrade_services_in:
                self.validate_error = False
                cbas_node = self.get_nodes_from_services_map(service_type="cbas")
                cbas_rest = RestConnection(self.servers[nodes_init])
                kv_nodes = self.get_nodes_from_services_map(service_type="kv")
                self.cbas_node = cbas_node
                all_buckets = RestConnection(self.master).get_buckets()
                for bucket in all_buckets:
                    if bucket.name == "travel-sample":
                        RestConnection(self.master).delete_bucket("travel-sample")
                        self.sleep(10)
                        break
                items_travel_sample = 63182
                cb_version = RestConnection(self.master).get_nodes_version()
                # Toy build or Greater than CC build
                if float(cb_version[:3]) == 0.0 or float(cb_version[:3]) >= 7.0:
                    items_travel_sample = 63288
                self.load_sample_buckets(servers=self.servers[:nodes_init],
                                         bucketName="travel-sample",
                                         total_items=items_travel_sample,
                                         rest=cbas_rest)
                # self.test_create_dataset_on_bucket()
        if after_upgrade_buckets_in is not False:
            self.bucket_size = 256
            self._create_sasl_buckets(self.master, 1)
            self._create_standard_buckets(self.master, 1)
            if self.ddocs_num:
                self.create_ddocs_and_views()
                gen_load = BlobGenerator('upgrade', 'upgrade-',
                                          self.value_size, end=self.num_items)
                self._load_all_buckets(self.master, gen_load, "create",
                                       self.expire_time, flag=self.item_flag)
        # deleting buckets after upgrade
        if after_upgrade_buckets_out is not False:
            self._all_buckets_delete(self.master)
        # flushing buckets after upgrade
        if after_upgrade_buckets_flush is not False:
            self._all_buckets_flush()

    def online_upgrade_swap_rebalance_with_high_doc_ops(self):
        self.rebalance_quirks = self.input.param('rebalance_quirks', False)
        self.upgrade_version = self.input.param('upgrade_version', '4.6.4-4590')
        self.run_with_views = self.input.param('run_with_views', True)
        self.run_view_query_iterations = self.input.param("run_view_query_iterations", 1)
        self.skip_fresh_install = self.input.param("skip_fresh_install", False)
        self.num_items = self.input.param("num_items", 3000000)
        self.total_items = self.num_items
        # install initial version on the nodes
        self._install(self.servers[:self.nodes_init])
        self.quota = self._initialize_nodes(self.cluster, self.servers,
                                            self.disabled_consistent_view,
                                            self.rebalanceIndexWaitingDisabled,
                                            self.rebalanceIndexPausingDisabled,
                                            self.maxParallelIndexers,
                                            self.maxParallelReplicaIndexers, self.port)
        self.bucket_size = self._get_bucket_size(self.quota, 1)
        self._bucket_creation()
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()[0]
        rest.update_autofailover_settings(False, 60)
        if self.flusher_total_batch_limit:
            self.set_flusher_total_batch_limit(self.flusher_total_batch_limit, [bucket])
        load_thread = Thread(target=self.load_buckets_with_high_ops,
                             name="high_ops_load",
                             args=(self.master, self.buckets[0], self.num_items // 2,
                                   self.batch_size,
                                   1, 0,
                                   1, 0))
        load_thread.start()
        self.sleep(30)
        for i in range(1, self.nodes_init):
            self.cluster.rebalance([self.servers[0]], [self.servers[i]], [])
            self.sleep(30)
        load_thread.join()
        if self.flusher_total_batch_limit:
            try:
                self.check_snap_start_corruption(servers_to_check=self.servers[:self.nodes_init])
            except AssertionError as error:
                self.log.info("Corruption Expected: {0}".format(str(error)))
                if "snap_start and snap_end corruption found" not in str(error):
                    self.fail("Corruption not found as expected")
            else:
                self.fail("Exception did not happen")
        if self.run_with_views:
            self.ddocs_num = self.input.param("ddocs-num", 1)
            self.view_num = self.input.param("view-per-ddoc", 2)
            self.is_dev_ddoc = self.input.param("is-dev-ddoc", False)
            self.create_ddocs_and_views()
        self.initial_version = self.upgrade_versions[0]
        self.product = 'couchbase-server'
        self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for online upgrade to {0} version". \
                   format(self.initial_version))
        if not self.skip_fresh_install:
            # install upgraded versions on the remaning node to be used for swap rebalance
            if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
                self._install(self.servers[self.nodes_init:self.num_servers], community_to_enterprise=True)
            else:
                self._install(self.servers[self.nodes_init:self.num_servers])
        self.sleep(self.sleep_time, "Installation of new version is done. Wait for rebalance")
        self.swap_num_servers = self.input.param('swap_num_servers', 1)
        old_servers = self.servers[:self.nodes_init]
        new_vb_nums = RestHelper(RestConnection(self.master))._get_vbuckets(old_servers,
                                                                            bucket_name=self.buckets[0].name)
        if self.rebalance_quirks:
            for server in self.servers:
                rest = RestConnection(server)
                # rest.diag_eval("[ns_config:set({node, N, extra_rebalance_quirks}, [reset_replicas, trivial_moves]) || N <- ns_node_disco:nodes_wanted()].")
                # rest.diag_eval("ns_config:set(disable_rebalance_quirks, [disable_old_master]).")
                rest.diag_eval("ns_config:set(extra_rebalance_quirks, [disable_old_master]).")
        new_servers = []
        # do online upgrade using swap rebalance for all nodes except the master node
        for i in range(1, self.nodes_init // self.swap_num_servers):
            servers_in = self.servers[(self.nodes_init + i * self.swap_num_servers):
                                      (self.nodes_init + (i + 1) * self.swap_num_servers)]
            servers_out = self.servers[(i * self.swap_num_servers):((i + 1) * self.swap_num_servers)]
            servers = old_servers + new_servers
            self.log.info("Swap rebalance: rebalance out %s old version nodes, rebalance in %s 2.0 Nodes"
                          % (self.swap_num_servers, self.swap_num_servers))
            self.data_load_and_rebalance(self.master, self.total_items, servers, servers_in, servers_out, bucket)
            old_servers = self.servers[((i + 1) * self.swap_num_servers):self.nodes_init]
            new_servers = new_servers + servers_in
            servers = old_servers + new_servers
            self.total_items += self.num_items
        self._new_master(self.servers[self.nodes_init + 1])
        # do final swap rebalance of the node to complete upgrade
        self.data_load_and_rebalance(self.master, self.total_items, servers,
                                     [self.servers[self.nodes_init]], [self.servers[0]], bucket)
        self.total_items += self.num_items
        #         self.add_built_in_server_user()
        self.create_user(self.master)
        # Check after the upgrades complete
        self.check_snap_start_corruption(servers_to_check=self.servers[self.nodes_init:self.nodes_init * 2])
        # After all the upgrades are completed, do a rebalance in of node in new version
        self.data_load_and_rebalance(self.master, self.total_items, servers,
                                     [self.servers[self.nodes_init * 2]], [], bucket)
        self.total_items += self.num_items
        # Check after the rebalance in
        self.check_snap_start_corruption(servers_to_check=self.servers[self.nodes_init:self.nodes_init * 2 + 1])
        # do a rebalance out of node in new version
        self.data_load_and_rebalance(self.master, self.total_items, servers,
                                     [], [self.servers[self.nodes_init * 2]], bucket)
        self.total_items += self.num_items
        # do a swap rebalance of nodes in new version
        self.data_load_and_rebalance(self.master, self.total_items, servers,
                                     [self.servers[self.nodes_init * 2]], [self.servers[self.nodes_init]], bucket)

    def online_upgrade_with_regular_rebalance_with_high_doc_ops(self):
        self.rebalance_quirks = self.input.param('rebalance_quirks', False)
        self.upgrade_version = self.input.param('upgrade_version', '4.6.4-4590')
        self.run_with_views = self.input.param('run_with_views', True)
        self.run_view_query_iterations = self.input.param("run_view_query_iterations", 1)
        self.skip_fresh_install = self.input.param("skip_fresh_install", False)
        self.num_items = self.input.param("num_items", 3000000)
        self.total_items = self.num_items
        # install initial version on the nodes
        self._install(self.servers[:self.nodes_init])
        self.quota = self._initialize_nodes(self.cluster, self.servers,
                                            self.disabled_consistent_view,
                                            self.rebalanceIndexWaitingDisabled,
                                            self.rebalanceIndexPausingDisabled,
                                            self.maxParallelIndexers,
                                            self.maxParallelReplicaIndexers, self.port)
        self.bucket_size = self._get_bucket_size(self.quota, 1)
        self._bucket_creation()
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()[0]
        rest.update_autofailover_settings(False, 60)
        if self.flusher_total_batch_limit:
            self.set_flusher_total_batch_limit(self.flusher_total_batch_limit, [bucket])
        load_thread = Thread(target=self.load_buckets_with_high_ops,
                             name="high_ops_load",
                             args=(self.master, self.buckets[0], self.num_items // 2,
                                   self.batch_size,
                                   1, 0,
                                   1, 0))
        load_thread.start()
        self.sleep(30)
        for i in range(1, self.nodes_init):
            self.cluster.rebalance([self.servers[0]], [self.servers[i]], [])
            self.sleep(30)
        load_thread.join()
        if self.flusher_total_batch_limit:
            try:
                self.check_snap_start_corruption(servers_to_check=self.servers[:self.nodes_init])
            except AssertionError as error:
                self.log.info("Corruption Expected: {0}".format(str(error)))
                if "snap_start and snap_end corruption found" not in str(error):
                    self.fail("Corruption not found as expected")
            else:
                self.fail("Exception did not happen")
        if self.run_with_views:
            self.ddocs_num = self.input.param("ddocs-num", 1)
            self.view_num = self.input.param("view-per-ddoc", 2)
            self.is_dev_ddoc = self.input.param("is-dev-ddoc", False)
            self.create_ddocs_and_views()
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()[0]
        self.initial_version = self.upgrade_versions[0]
        self.product = 'couchbase-server'
        self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for online upgrade to {0} version". \
                   format(self.initial_version))
        if not self.skip_fresh_install:
            # install upgraded versions on the remaning node to be used for swap rebalance
            if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
                self._install(self.servers[self.nodes_init:self.num_servers], community_to_enterprise=True)
            else:
                self._install(self.servers[self.nodes_init:self.num_servers])
        self.sleep(self.sleep_time, "Installation of new version is done. Wait for rebalance")
        self.swap_num_servers = self.input.param('swap_num_servers', 1)
        old_servers = self.servers[:self.nodes_init]
        new_vb_nums = RestHelper(RestConnection(self.master))._get_vbuckets(old_servers,
                                                                            bucket_name=self.buckets[0].name)
        if self.rebalance_quirks:
            for server in self.servers:
                rest = RestConnection(server)
                # rest.diag_eval("[ns_config:set({node, N, extra_rebalance_quirks}, [reset_replicas, trivial_moves]) || N <- ns_node_disco:nodes_wanted()].")
                # rest.diag_eval("ns_config:set(disable_rebalance_quirks, [disable_old_master]).")
                rest.diag_eval("ns_config:set(extra_rebalance_quirks, [disable_old_master]).")
        new_servers = []
        # do online upgrade using swap rebalance for all nodes except the master node
        for i in range(1, self.nodes_init // self.swap_num_servers):
            servers_in = self.servers[(self.nodes_init + i * self.swap_num_servers):
                                      (self.nodes_init + (i + 1) * self.swap_num_servers)]
            servers_out = self.servers[(i * self.swap_num_servers):((i + 1) * self.swap_num_servers)]
            servers = old_servers + new_servers
            self.log.info("Swap rebalance: rebalance out %s old version nodes, rebalance in %s 2.0 Nodes"
                          % (self.swap_num_servers, self.swap_num_servers))
            self.data_load_and_rebalance(self.master, self.total_items, servers, servers_in, servers_out, bucket,
                                         swap=False)
            old_servers = self.servers[((i + 1) * self.swap_num_servers):self.nodes_init]
            new_servers = new_servers + servers_in
            servers = old_servers + new_servers
            self.total_items += self.num_items
        self._new_master(self.servers[self.nodes_init + 1])
        # do final swap rebalance of the node to complete upgrade
        self.data_load_and_rebalance(self.master, self.total_items, servers,
                                     [self.servers[self.nodes_init]], [self.servers[0]], bucket, swap=False)
        self.total_items += self.num_items
        self._new_master(self.servers[self.nodes_init + 1])
        # Check after the upgrades complete
        self.check_snap_start_corruption(servers_to_check=self.servers[self.nodes_init:self.nodes_init * 2])
        #         self.add_built_in_server_user()
        self.create_user(self.master)
        # After all the upgrades are completed, do a rebalance in of node in new version
        self.data_load_and_rebalance(self.master, self.total_items, servers,
                                     [self.servers[self.nodes_init * 2]], [], bucket)
        self.total_items += self.num_items
        # Check after the rebalance in
        self.check_snap_start_corruption(servers_to_check=self.servers[self.nodes_init:self.nodes_init * 2 + 1])
        # do a rebalance out of node in new version
        self.data_load_and_rebalance(self.master, self.total_items, servers,
                                     [], [self.servers[self.nodes_init * 2]], bucket)
        self.total_items += self.num_items
        # do a swap rebalance of nodes in new version
        self.data_load_and_rebalance(self.master, self.total_items, servers,
                                     [self.servers[self.nodes_init * 2]], [self.servers[self.nodes_init]],
                                     bucket)

    def online_upgrade_with_graceful_failover_with_high_doc_ops(self):
        self.rebalance_quirks = self.input.param('rebalance_quirks', False)
        self.upgrade_version = self.input.param('upgrade_version', '4.6.4-4590')
        self.run_with_views = self.input.param('run_with_views', True)
        self.run_view_query_iterations = self.input.param("run_view_query_iterations", 1)
        self.skip_fresh_install = self.input.param("skip_fresh_install", False)
        self.recovery_type = self.input.param("recovery_type", "delta")
        self.num_items = self.input.param("num_items", 3000000)
        self.total_items = self.num_items
        # install initial version on the nodes
        self._install(self.servers[:self.nodes_init])
        self.quota = self._initialize_nodes(self.cluster, self.servers,
                                            self.disabled_consistent_view,
                                            self.rebalanceIndexWaitingDisabled,
                                            self.rebalanceIndexPausingDisabled,
                                            self.maxParallelIndexers,
                                            self.maxParallelReplicaIndexers, self.port)
        self.bucket_size = self._get_bucket_size(self.quota, 1)
        self._bucket_creation()
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()[0]
        rest.update_autofailover_settings(False, 60)
        if self.flusher_total_batch_limit:
            self.set_flusher_total_batch_limit(self.flusher_total_batch_limit, [bucket])
        load_thread = Thread(target=self.load_buckets_with_high_ops,
                             name="high_ops_load",
                             args=(self.master, self.buckets[0], self.num_items // 2,
                                   self.batch_size,
                                   1, 0,
                                   1, 0))
        load_thread.start()
        self.sleep(30)
        for i in range(1, self.nodes_init):
            self.cluster.rebalance([self.servers[0]], [self.servers[i]], [])
            self.sleep(30)
        load_thread.join()
        if self.flusher_total_batch_limit:
            try:
                self.check_snap_start_corruption(servers_to_check=self.servers[:self.nodes_init])
            except AssertionError as error:
                self.log.info("Corruption Expected: {0}".format(str(error)))
                if "snap_start and snap_end corruption found" not in str(error):
                    self.fail("Corruption not found as expected")
            else:
                self.fail("Exception did not happen")
        if self.run_with_views:
            self.ddocs_num = self.input.param("ddocs-num", 1)
            self.view_num = self.input.param("view-per-ddoc", 2)
            self.is_dev_ddoc = self.input.param("is-dev-ddoc", False)
            self.create_ddocs_and_views()
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()[0]
        self.initial_version = self.upgrade_versions[0]
        self.product = 'couchbase-server'
        self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for online upgrade to {0} version". \
                   format(self.initial_version))
        if not self.skip_fresh_install:
            # install upgraded versions on the remaning node to be used for swap rebalance
            if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
                self._install(self.servers[self.nodes_init:self.num_servers], community_to_enterprise=True)
            else:
                self._install(self.servers[self.nodes_init:self.num_servers])
        self.sleep(self.sleep_time, "Installation of new version is done. Wait for rebalance")
        self.swap_num_servers = self.input.param('swap_num_servers', 1)
        old_servers = self.servers[:self.nodes_init]
        new_vb_nums = RestHelper(RestConnection(self.master))._get_vbuckets(old_servers,
                                                                            bucket_name=self.buckets[0].name)
        if self.rebalance_quirks:
            for server in self.servers:
                rest = RestConnection(server)
                # rest.diag_eval("[ns_config:set({node, N, extra_rebalance_quirks}, [reset_replicas, trivial_moves]) || N <- ns_node_disco:nodes_wanted()].")
                # rest.diag_eval("ns_config:set(disable_rebalance_quirks, [disable_old_master]).")
                rest.diag_eval("ns_config:set(extra_rebalance_quirks, [disable_old_master]).")

        new_servers = []
        upgrade_nodes = self.servers[:self.nodes_init]
        # do upgrade using graceful failover/ recovery/ rebalance
        for i in range(1, self.nodes_init // self.swap_num_servers):
            self.failover_upgrade_recovery_with_data_load(self.master, self.total_items, upgrade_nodes,
                                                          [self.servers[i]], self.recovery_type, bucket)
            self.total_items += self.num_items
            self.data_load_and_rebalance(self.master, self.total_items, upgrade_nodes,
                                         [], [], bucket)
            self.total_items += self.num_items
        self.failover_upgrade_recovery_with_data_load(self.master, self.total_items, upgrade_nodes,
                                                      [self.servers[0]], self.recovery_type, bucket)
        self.total_items += self.num_items
        self.data_load_and_rebalance(self.master, self.total_items, upgrade_nodes,
                                     [], [], bucket)
        self.total_items += self.num_items
        # Check after the upgrades complete
        self.check_snap_start_corruption(servers_to_check=self.servers[:self.nodes_init])
        #         self.add_built_in_server_user()
        self.create_user(self.master)
        # After all the upgrades are completed, do a rebalance in of node in new version
        self.data_load_and_rebalance(self.master, self.total_items, upgrade_nodes,
                                     [self.servers[self.nodes_init]], [], bucket)
        self.total_items += self.num_items
        # Check after the rebalance in
        self.check_snap_start_corruption(servers_to_check=self.servers[:self.nodes_init + 1])
        # do a rebalance out of node in new version
        self.data_load_and_rebalance(self.master, self.total_items, upgrade_nodes,
                                     [], [self.servers[self.nodes_init]], bucket)
        self.total_items += self.num_items
        # do a swap rebalance of nodes in new version
        self.data_load_and_rebalance(self.master, self.total_items, upgrade_nodes,
                                     [self.servers[self.nodes_init]], [self.servers[self.nodes_init - 1]],
                                     bucket)

    def offline_upgrade_with_high_doc_ops(self):
        self.rebalance_quirks = self.input.param('rebalance_quirks', False)
        self.upgrade_version = self.input.param('upgrade_version', '4.6.4-4590')
        self.run_with_views = self.input.param('run_with_views', True)
        self.run_view_query_iterations = self.input.param("run_view_query_iterations", 1)
        self.skip_fresh_install = self.input.param("skip_fresh_install", False)
        self.upgrade_order = self.input.param("upgrade_order", "parallel")
        self.num_items = self.input.param("num_items", 3000000)
        self.total_items = self.num_items
        # install initial version on the nodes
        self._install(self.servers[:self.nodes_init])
        self.quota = self._initialize_nodes(self.cluster, self.servers,
                                            self.disabled_consistent_view,
                                            self.rebalanceIndexWaitingDisabled,
                                            self.rebalanceIndexPausingDisabled,
                                            self.maxParallelIndexers,
                                            self.maxParallelReplicaIndexers, self.port)
        self.bucket_size = self._get_bucket_size(self.quota, 1)
        self._bucket_creation()
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()[0]
        rest.update_autofailover_settings(False, 60)
        if self.flusher_total_batch_limit:
            self.set_flusher_total_batch_limit(self.flusher_total_batch_limit, [bucket])
        load_thread = Thread(target=self.load_buckets_with_high_ops,
                             name="high_ops_load",
                             args=(self.master, self.buckets[0], self.num_items // 2,
                                   self.batch_size,
                                   1, 0,
                                   1, 0))
        load_thread.start()
        self.sleep(30)
        for i in range(1, self.nodes_init):
            self.cluster.rebalance([self.servers[0]], [self.servers[i]], [])
            self.sleep(30)
        load_thread.join()
        if self.flusher_total_batch_limit:
            try:
                self.check_snap_start_corruption(servers_to_check=self.servers[:self.nodes_init])
            except AssertionError as error:
                self.log.info("Corruption Expected: {0}".format(str(error)))
                if "snap_start and snap_end corruption found" not in str(error):
                    self.fail("Corruption not found as expected")
            else:
                self.fail("Exception did not happen")
        self.initial_version = self.upgrade_versions[0]
        self.product = 'couchbase-server'
        self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for online upgrade to {0} version". \
                   format(self.initial_version))
        if not self.skip_fresh_install:
            # install upgraded versions on the remaining nodes to be used for rebalance in after upgrade
            if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
                self._install(self.servers[self.nodes_init:self.num_servers], community_to_enterprise=True)
            else:
                self._install(self.servers[self.nodes_init:self.num_servers])
        if self.rebalance_quirks:
            for server in self.servers:
                rest = RestConnection(server)
                # rest.diag_eval("[ns_config:set({node, N, extra_rebalance_quirks}, [reset_replicas, trivial_moves]) || N <- ns_node_disco:nodes_wanted()].")
                # rest.diag_eval("ns_config:set(disable_rebalance_quirks, [disable_old_master]).")
                rest.diag_eval("ns_config:set(extra_rebalance_quirks, [disable_old_master]).")
        upgrade_nodes = self.servers[:self.nodes_init]
        for server in upgrade_nodes:
            remote = RemoteMachineShellConnection(server)
            remote.stop_server()
            self.sleep(self.sleep_time)
            if self.wait_expire:
                self.sleep(self.expire_time)
            if self.input.param('remove_manifest_files', False):
                for file in ['manifest.txt', 'manifest.xml', 'VERSION.txt,']:
                    output, error = remote.execute_command("rm -rf /opt/couchbase/{0}".format(file))
                    remote.log_command_output(output, error)
            if self.input.param('remove_config_files', False):
                for file in ['config', 'couchbase-server.node', 'ip', 'couchbase-server.cookie']:
                    output, error = remote.execute_command(
                        "rm -rf /opt/couchbase/var/lib/couchbase/{0}".format(file))
                    remote.log_command_output(output, error)
                self.buckets = []
            remote.disconnect()
        upgrade_threads = []
        if self.initial_build_type == "community" and self.upgrade_build_type == "enterprise":
            upgrade_threads = self._async_update(self.upgrade_version, upgrade_nodes, save_upgrade_config=True)
        else:
            if self.upgrade_order == "parallel":
                upgrade_threads = self._async_update(self.upgrade_version, upgrade_nodes)
            else:
                for upgrade_node in upgrade_nodes:
                    upgrade_thread = self._async_update(self.upgrade_version, [upgrade_node])
                    upgrade_thread[0].join()
        # wait upgrade statuses
        for upgrade_thread in upgrade_threads:
            upgrade_thread.join()
        success_upgrade = True
        while not self.queue.empty():
            success_upgrade &= self.queue.get()
        if not success_upgrade:
            self.fail("Upgrade failed. See logs above!")
        self.create_user(self.master)
        # Check after the upgrades complete
        self.check_snap_start_corruption(servers_to_check=self.servers[:self.nodes_init])
        # After all the upgrades are completed, do a rebalance in of node in new version
        self.data_load_and_rebalance(self.master, self.total_items, upgrade_nodes,
                                     [self.servers[self.nodes_init]], [], bucket)
        self.total_items += self.num_items
        # Check after the rebalance in
        self.check_snap_start_corruption(servers_to_check=self.servers[:self.nodes_init + 1])
        # do a rebalance out of node in new version
        self.data_load_and_rebalance(self.master, self.total_items, upgrade_nodes,
                                     [], [self.servers[self.nodes_init]], bucket)
        self.total_items += self.num_items
        # do a swap rebalance of nodes in new version
        self.data_load_and_rebalance(self.master, self.total_items, upgrade_nodes,
                                     [self.servers[self.nodes_init]], [self.servers[self.nodes_init - 1]],
                                     bucket)

    def test_print_ops_rate(self):
        array1 = []
        array2 = []
        self.num_items = self.input.param("num_items", 3000000)
        for i in range(1, self.nodes_init):
            self.cluster.rebalance([self.servers[0]], [self.servers[i]], [])
            self.sleep(30)
        rest = RestConnection(self.master)
        self.create_user(self.master)
        self.quota = self._initialize_nodes(self.cluster, self.servers,
                                            self.disabled_consistent_view,
                                            self.rebalanceIndexWaitingDisabled,
                                            self.rebalanceIndexPausingDisabled,
                                            self.maxParallelIndexers,
                                            self.maxParallelReplicaIndexers, self.port)
        self.bucket_size = self._get_bucket_size(self.quota, 1)
        self._bucket_creation()
        load_thread = Thread(target=self.load_using_cbc_pillowfight,
                             name="pillowfight_load",
                             args=(self.master, self.num_items))
        self.log.info('starting the load thread...')
        load_thread.start()
        for i in range(2):
            status, json_parsed1 = rest.get_bucket_stats_json()
            array1.extend(json_parsed1["op"]["samples"]["ep_ops_create"])
            array2.extend(json_parsed1["op"]["samples"]["ep_ops_update"])
            self.sleep(60)
        import numpy as np
        [a for a in array1 if a != 0]
        [a for a in array2 if a != 0]
        self.log.info("array")
        self.log.info(array1)
        self.log.info(array2)
        for i in range(80, 100, 1):
            percentile_ep_ops_create = np.percentile(array1, i)
            percentile_ep_ops_update = np.percentile(array2, i)
            self.log.info(
                "{1} th percentile for creates - Bucket default : {0}".format(percentile_ep_ops_create, i))
            self.log.info(
                "{1} th percentile for updates - Bucket default : {0}".format(percentile_ep_ops_update, i))
        load_thread.join()

    def load_using_cbc_pillowfight(self, server, items, batch=1000, docsize=100):
        self.rate_limit = self.input.param('rate_limit', '100000')
        import subprocess
        import multiprocessing
        num_cores = multiprocessing.cpu_count()
        cmd = "cbc-pillowfight -U couchbase://{0}/default -I {1} -m {4} -M {4} -B {2} --json  " \
              "-t {4} --rate-limit={5} --populate-only".format(server.ip, items, batch, docsize, num_cores // 2,
                                                               self.rate_limit)
        cmd += " -u Administrator -P password"
        self.log.info("Executing '{0}'...".format(cmd))
        rc = subprocess.call(cmd, shell=True)
        if rc != 0:
            cmd = "cbc-pillowfight -U couchbase://{0}/default -I {1} -m {4} -M {4} -B {2} --json  " \
                  "-t {4} --rate-limit={5} --populate-only".format(server.ip, items, batch, docsize, num_cores // 2,
                                                                   self.rate_limit)
            rc = subprocess.call(cmd, shell=True)
            if rc != 0:
                self.fail("Exception running cbc-pillowfight: subprocess module returned non-zero response!")

    def check_dataloss(self, server, bucket, num_items):
        from couchbase.bucket import Bucket
        from couchbase.exceptions import NotFoundError
        from lib.memcached.helper.data_helper import VBucketAwareMemcached
        if RestConnection(server).get_nodes_version()[:5] < '5':
            bkt = Bucket('couchbase://{0}/{1}'.format(server.ip, bucket.name))
        else:
            cluster = Cluster("couchbase://{}".format(server.ip))
            auth = PasswordAuthenticator(server.rest_username,
                                         server.rest_password)
            cluster.authenticate(auth)
            bkt = cluster.open_bucket(bucket.name)
        rest = RestConnection(self.master)
        VBucketAware = VBucketAwareMemcached(rest, bucket.name)
        _, _, _ = VBucketAware.request_map(rest, bucket.name)
        batch_start = 0
        batch_end = 0
        batch_size = 10000
        errors = []
        missing_keys = []
        errors_replica = []
        missing_keys_replica = []
        while num_items > batch_end:
            batch_end = batch_start + batch_size
            keys = []
            for i in range(batch_start, batch_end, 1):
                keys.append(str(i).rjust(20, '0'))
            try:
                bkt.get_multi(keys)
                self.log.info("Able to fetch keys starting from {0} to {1}".format(keys[0], keys[len(keys) - 1]))
            except CouchbaseException as e:
                self.log.error(e)
                ok, fail = e.split_results()
                if fail:
                    for key in fail:
                        try:
                            bkt.get(key)
                        except NotFoundError:
                            vBucketId = VBucketAware._get_vBucket_id(key)
                            errors.append("Missing key: {0}, VBucketId: {1}".
                                          format(key, vBucketId))
                            missing_keys.append(key)
            try:
                bkt.get_multi(keys, replica=True)
                self.log.info(
                    "Able to fetch keys starting from {0} to {1} in replica ".format(keys[0], keys[len(keys) - 1]))
            except CouchbaseException as e:
                self.log.error(e)
                ok, fail = e.split_results()
                if fail:
                    for key in fail:
                        try:
                            bkt.get(key, replica=True)
                        except NotFoundError:
                            vBucketId = VBucketAware._get_vBucket_id(key)
                            errors_replica.append("Missing key in replica: {0}, VBucketId: {1}".
                                                  format(key, vBucketId))
                            missing_keys_replica.append(key)
            batch_start += batch_size
        return errors, missing_keys, errors_replica, missing_keys_replica

    def failover_upgrade_recovery_with_data_load(self, master, items, cluster_nodes, failed_over_node, recovery_type,
                                                 bucket):
        load_thread = Thread(target=self.load_buckets_with_high_ops,
                             name="high_ops_load",
                             args=(master, bucket, items,
                                   self.batch_size,
                                   1, 0,
                                   1, 0))
        load_thread.start()
        # do final graceful failover, upgrade and do a recovery to complete upgrade
        self.cluster.failover(cluster_nodes, failed_over_node, graceful=True)
        self.sleep(300)
        upgrade_thread = self._async_update(self.upgrade_version, failed_over_node)
        upgrade_thread[0].join()
        success_upgrade = True
        while not self.queue.empty():
            success_upgrade &= self.queue.get()
        if not success_upgrade:
            self.fail("Upgrade failed. See logs above!")
        self.rest.set_recovery_type('ns_1@' + failed_over_node[0].ip, recovery_type)

    def data_load_and_rebalance(self, load_host, num_items, servers, servers_in, servers_out, bucket, swap=True):
        self.add_built_in_user = self.input.param('add_built_in_user', True)
        rebalance_fail = False
        self.log.info("inside data_load_and_rebalance")
        rest = RestConnection(load_host)
        self.log.info('starting the view query thread...')
        if self.run_with_views:
            view_query_thread = Thread(target=self.view_queries, name="run_queries",
                                       args=(self.run_view_query_iterations,))
            view_query_thread.start()
        if self.loader == "pillowfight":
            load_thread = Thread(target=self.load_using_cbc_pillowfight,
                                 name="pillowfight_load",
                                 args=(load_host, num_items))
        else:
            load_thread = Thread(target=self.load_buckets_with_high_ops,
                                 name="high_ops_load",
                                 args=(load_host, self.buckets[0], num_items,
                                       self.batch_size,
                                       self.threads, 0,
                                       self.instances, 0))
        self.log.info('starting the load thread...')
        load_thread.start()
        try:
            if swap:
                self.cluster.rebalance(servers, servers_in, servers_out, timeout=10800)
            else:
                self.cluster.rebalance(servers, servers_in, [], timeout=2400)
                self.cluster.rebalance(servers, [], servers_out, timeout=2400)
        except Exception as ex:
            rebalance_fail = True
        finally:
            self.sleep(self.sleep_time)
            load_thread.join()
            if self.run_with_views:
                view_query_thread.join()
            if self.add_built_in_user:
                self.add_built_in_server_user()
            if self.loader == "pillowfight":
                errors, missing_keys, errors_replica, missing_keys_replica = self.check_dataloss(load_host, bucket,
                                                                                                 num_items)
            else:
                errors = self.check_dataloss_for_high_ops_loader(load_host, bucket,
                                                                 num_items,
                                                                 self.batch_size,
                                                                 self.threads,
                                                                 0,
                                                                 False, 0, 0,
                                                                 False, 0)
            if errors:
                self.log.info("Printing missing keys : ")
            for error in errors:
                print(error)
            if errors:
                self.check_dataloss_with_cbc_hash(load_host, bucket, missing_keys)
            if self.loader == "pillowfight":
                if errors_replica:
                    self.log.info("Printing missing keys in replica : ")
                for error in errors_replica:
                    print(error)
                if errors_replica:
                    self.check_dataloss_with_cbc_hash(load_host, bucket, missing_keys_replica)
            if num_items != rest.get_active_key_count(bucket):
                self.fail("FATAL: Data loss detected!! Docs loaded : {0}, docs present: {1}".
                          format(num_items, rest.get_active_key_count(bucket)))
            if num_items != rest.get_replica_key_count(bucket):
                self.fail("FATAL: Data loss detected in replicas !! Docs loaded : {0}, docs present : {1}".
                          format(num_items, rest.get_replica_key_count(bucket)))
            if rebalance_fail:
                self.fail("Rebalance failed")

    def check_dataloss_with_cbc_hash(self, server, bucket, keys):
        import subprocess
        for key in keys:
            cmd = "cbc hash -U couchbase://{0}/{1} {2}".format(server.ip, bucket.name, key)
            self.log.info("Executing '{0}'...".format(cmd))
            output = subprocess.check_output(cmd, shell=True)
            self.log.info("cbc hash output : {0}".format(output))

    def run_view_queries(self):
        view_query_thread = Thread(target=self.view_queries, name="run_queries",
                                   args=(self.run_view_query_iterations,))
        return view_query_thread

    def view_queries(self, iterations):
        query = {"connectionTimeout": 60000}
        for count in range(iterations):
            for i in range(self.view_num):
                self.cluster.query_view(self.master, self.ddocs[0].name,
                                        self.default_view_name + str(i), query,
                                        expected_rows=None, bucket="default", retry_time=2)

    def create_user(self, node):
        self.log.info("inside create_user")
        testuser = [{'id': 'cbadminbucket', 'name': 'cbadminbucket',
                     'password': 'password'}]
        rolelist = [{'id': 'cbadminbucket', 'name': 'cbadminbucket',
                     'roles': 'admin'}]
        self.log.info("before create_user_source")
        RbacBase().create_user_source(testuser, 'builtin', node)
        self.log.info("before add_user_role")
        status = RbacBase().add_user_role(rolelist, RestConnection(node), 'builtin')

    def load_buckets_with_high_ops(self, server, bucket, items, batch=2000,
                                   threads=5, start_document=0, instances=1, ttl=0):
        import subprocess
        cmd_format = "python3 scripts/thanosied.py  --spec couchbase://{0} --bucket {1} --user {2} --password {3} " \
                     "--count {4} --batch_size {5} --threads {6} --start_document {7} --cb_version {8} --workers {9} --ttl {10} --rate_limit {11} " \
                     "--passes 1"
        cb_version = RestConnection(server).get_nodes_version()[:3]
        if self.num_replicas > 0 and self.use_replica_to:
            cmd_format = "{} --replicate_to 1".format(cmd_format)
        cmd = cmd_format.format(server.ip, bucket.name, server.rest_username,
                                server.rest_password,
                                items, batch, threads, start_document,
                                cb_version, instances, ttl, self.rate_limit)
        self.log.info("Running {}".format(cmd))
        result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
        output = result.stdout.read()
        error = result.stderr.read()
        if error:
            # self.log.error(error)
            if "Authentication failed" in error:
                cmd = cmd_format.format(server.ip, bucket.name, server.rest_username,
                                        server.rest_password,
                                        items, batch, threads, start_document,
                                        "4.0", instances, ttl, self.rate_limit)
                self.log.info("Running {}".format(cmd))
                result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                          stderr=subprocess.PIPE)
                output = result.stdout.read()
                error = result.stderr.read()
                if error:
                    self.log.error(error)
                    self.fail("Failed to run the loadgen.")
        if output:
            loaded = output.split('\n')[:-1]
            total_loaded = 0
            for load in loaded:
                total_loaded += int(load.split(':')[1].strip())
            self.assertEqual(total_loaded, items,
                             "Failed to load {} items. Loaded only {} items".format(
                                 items,
                                 total_loaded))

    def check_dataloss_for_high_ops_loader(self, server, bucket, items,
                                           batch=2000, threads=5,
                                           start_document=0,
                                           updated=False, ops=0, ttl=0, deleted=False, deleted_items=0):
        import subprocess
        from lib.memcached.helper.data_helper import VBucketAwareMemcached
        cmd_format = "python3 scripts/thanosied.py  --spec couchbase://{0} --bucket {1} --user {2} --password {3} " \
                     "--count {4} --batch_size {5} --threads {6} --start_document {7} --cb_version {8} --validation 1 --rate_limit {9}  " \
                     "--passes 1"
        cb_version = RestConnection(server).get_nodes_version()[:3]
        if updated:
            cmd_format = "{} --updated --ops {}".format(cmd_format, ops)
        if deleted:
            cmd_format = "{} --deleted --deleted_items {}".format(cmd_format, deleted_items)
        if ttl > 0:
            cmd_format = "{} --ttl {}".format(cmd_format, ttl)
        cmd = cmd_format.format(server.ip, bucket.name, server.rest_username,
                                server.rest_password,
                                int(items), batch, threads, start_document, cb_version, self.rate_limit)
        self.log.info("Running {}".format(cmd))
        result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
        output = result.stdout.read()
        error = result.stderr.read()
        errors = []
        return errors
