from threading import Thread
import re
import copy
import Queue
from datetime import datetime
from membase.api.rest_client import RestConnection, Bucket
from newupgradebasetest import NewUpgradeBaseTest
from xdcrbasetests import XDCRReplicationBaseTest, XDCRConstants
from TestInput import TestInputSingleton
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection, RestHelper
from membase.api.exception import RebalanceFailedException
from membase.helper.cluster_helper import ClusterOperationHelper
from couchbase.documentgenerator import BlobGenerator
from remote.remote_util import RemoteMachineShellConnection
from couchbase.document import DesignDocument, View

class UpgradeTests(NewUpgradeBaseTest, XDCRReplicationBaseTest):
    def setUp(self):
        super(UpgradeTests, self).setUp()
        self.bucket_topology = self.input.param("bucket_topology", "default:1><2").split(";")
        self.src_init = self.input.param('src_init', 2)
        self.dest_init = self.input.param('dest_init', 2)
        self.buckets_on_src = [str(bucket_repl.split(":")[0]) for bucket_repl in self.bucket_topology if re.search('\S+:\S*1', bucket_repl)]
        self.buckets_on_dest = [str(bucket_repl.split(":")[0]) for bucket_repl in self.bucket_topology if re.search('\S+:\S*2', bucket_repl)]
        self.repl_buckets_from_src = [str(bucket_repl.split(":")[0]) for bucket_repl in self.bucket_topology if bucket_repl.find("1>") != -1 ]
        self.repl_buckets_from_dest = [str(bucket_repl.split(":")[0]) for bucket_repl in self.bucket_topology if bucket_repl.find("<2") != -1 ]
        self._override_clusters_structure(self)
        self.queue = Queue.Queue()
        self.rep_type = self.input.param("rep_type", "capi")
        self.upgrade_versions = self.input.param('upgrade_version', '2.2.0-821-rel')
        self.upgrade_versions = self.upgrade_versions.split(";")
        self.ddocs_num_src = self.input.param("ddocs-num-src", 0)
        self.views_num_src = self.input.param("view-per-ddoc-src", 2)
        self.ddocs_num_dest = self.input.param("ddocs-num-dest", 0)
        self.views_num_dest = self.input.param("view-per-ddoc-dest", 2)
        self.post_upgrade_ops = self.input.param("post-upgrade-actions", None)
        self._use_encryption_after_upgrade = self.input.param("use_encryption_after_upgrade", 0)
        self.ddocs_src = []
        self.ddocs_dest = []

    def tearDown(self):
        try:
            XDCRReplicationBaseTest.tearDown(self)
        finally:
            self.cluster.shutdown()

    @staticmethod
    def _override_clusters_structure(self):
        TestInputSingleton.input.clusters[0] = self.servers[:self.src_init]
        TestInputSingleton.input.clusters[1] = self.servers[self.src_init: self.src_init + self.dest_init ]

    @staticmethod
    def _create_buckets(self, nodes):
        master_node = nodes[0]
        if self.src_master.ip in [node.ip for node in nodes]:
            buckets = self.buckets_on_src
        elif self.dest_master.ip in [node.ip for node in nodes]:
            buckets = self.buckets_on_dest

        bucket_size = self._get_bucket_size(self._mem_quota_int, len(buckets))
        rest = RestConnection(master_node)
        master_id = rest.get_nodes_self().id

        sasl_buckets = len([bucket for bucket in buckets if bucket.startswith("bucket")])
        self._create_sasl_buckets(master_node, sasl_buckets, master_id, bucket_size)
        standard_buckets = len([bucket for bucket in buckets if bucket.startswith("standard_bucket")])
        self._create_standard_buckets(master_node, standard_buckets, master_id, bucket_size)
        if "default" in buckets:
            self.cluster.create_default_bucket(master_node, bucket_size, self._num_replicas)
            self.buckets.append(Bucket(name="default", authType="sasl", saslPassword="",
                num_replicas=self._num_replicas, bucket_size=bucket_size, master_id=master_id))
        self.sleep(30)

    @staticmethod
    def _setup_topology_chain(self):
        ord_keys = self._clusters_keys_olst
        ord_keys_len = len(ord_keys)
        dest_key_index = 1
        for src_key in ord_keys:
            if dest_key_index == ord_keys_len:
                break
            dest_key = ord_keys[dest_key_index]
            src_cluster_name = self._cluster_names_dic[src_key]
            dest_cluster_name = self._cluster_names_dic[dest_key]
            UpgradeTests._join_clusters(self, src_cluster_name, self.src_master, dest_cluster_name, self.dest_master)
            dest_key_index += 1

    @staticmethod
    def _set_toplogy_star(self):
        src_master_identified = False
        for key in self._clusters_keys_olst:
            nodes = self._clusters_dic[key]
            if not src_master_identified:
                src_cluster_name = self._cluster_names_dic[key]
                self.src_master = nodes[0]
                src_master_identified = True
                continue
            dest_cluster_name = self._cluster_names_dic[key]
            self.dest_master = nodes[0]
            UpgradeTests._join_clusters(self, src_cluster_name, self.src_master, dest_cluster_name, self.dest_master)
            self.sleep(30)

    @staticmethod
    def _join_clusters(self, src_cluster_name, src_master, dest_cluster_name, dest_master):
        if len(self.repl_buckets_from_src):
            self._link_clusters(src_cluster_name, src_master, dest_cluster_name, dest_master)
        if len(self.repl_buckets_from_dest):
            self._link_clusters(dest_cluster_name, dest_master, src_cluster_name, src_master)

        self._replicate_clusters(self, src_master, dest_cluster_name, self.repl_buckets_from_src)
        self._replicate_clusters(self, dest_master, src_cluster_name, self.repl_buckets_from_dest)

    @staticmethod
    def _replicate_clusters(self, src_master, dest_cluster_name, buckets):
        rest_conn_src = RestConnection(src_master)
        for bucket in buckets:
            (rep_database, rep_id) = rest_conn_src.start_replication(XDCRConstants.REPLICATION_TYPE_CONTINUOUS,
                bucket, dest_cluster_name, self.rep_type)
            self._start_replication_time[bucket] = datetime.now()
            self.sleep(5)
        if self._get_cluster_buckets(src_master):
            self._cluster_state_arr.append((rest_conn_src, dest_cluster_name, rep_database, rep_id))

    @staticmethod
    def _get_bucket(self, bucket_name, server):
            server_id = RestConnection(server).get_nodes_self().id
            for bucket in self.buckets:
                if bucket.name == bucket_name and bucket.master_id == server_id:
                    return bucket
            return None

    def _online_upgrade(self, update_servers, extra_servers, check_newmaster=True):
        self.cluster.rebalance(update_servers + extra_servers, extra_servers, [])
        self.log.info("Rebalance in all 2.0 Nodes")
        self.sleep(self.sleep_time)
        status, content = ClusterOperationHelper.find_orchestrator(update_servers[0])
        self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}".\
                        format(status, content))
        if check_newmaster:
            FIND_MASTER = False
            for new_server in extra_servers:
                if content.find(new_server.ip) >= 0:
                    FIND_MASTER = True
                    self.log.info("2.0 Node %s becomes the master" % (new_server.ip))
                    break
            if not FIND_MASTER:
                raise Exception("After rebalance in 2.0 Nodes, 2.0 doesn't become the master")
        self.log.info("Rebalanced out all old version nodes")
        self.cluster.rebalance(update_servers + extra_servers, [], update_servers)

    def offline_cluster_upgrade(self):
        self._install(self.servers[:self.src_init + self.dest_init ])
        upgrade_nodes = self.input.param('upgrade_nodes', "src").split(";")
        self.cluster.shutdown()
        XDCRReplicationBaseTest.setUp(self)
        self.set_xdcr_param('xdcrFailureRestartInterval', 1)
        self.sleep(60)
        bucket = self._get_bucket(self, 'default', self.src_master)
        self._operations()
        self._load_bucket(bucket, self.src_master, self.gen_create, 'create', exp=0)
        bucket = self._get_bucket(self, 'bucket0', self.src_master)
        self._load_bucket(bucket, self.src_master, self.gen_create, 'create', exp=0)
        bucket = self._get_bucket(self, 'bucket0', self.dest_master)
        gen_create2 = BlobGenerator('loadTwo', 'loadTwo', self._value_size, end=self._num_items)
        self._load_bucket(bucket, self.dest_master, gen_create2, 'create', exp=0)
        nodes_to_upgrade = []
        if "src" in upgrade_nodes :
            nodes_to_upgrade += self.src_nodes
        if "dest" in upgrade_nodes :
            nodes_to_upgrade += self.dest_nodes

        self.sleep(60)
        self._wait_for_replication_to_catchup()
        self._offline_upgrade(nodes_to_upgrade)

        if self._use_encryption_after_upgrade and "src" in upgrade_nodes and "dest" in upgrade_nodes and self.upgrade_versions[0] >= "2.5.0":
            if "src" in self._use_encryption_after_upgrade:
                src_remote_clusters = RestConnection(self.src_master).get_remote_clusters()
                for remote_cluster in src_remote_clusters:
                    self._modify_clusters(None, self.src_master, remote_cluster['name'], self.dest_master, require_encryption=1)
            if "dest" in self._use_encryption_after_upgrade:
                dest_remote_clusters = RestConnection(self.dest_master).get_remote_clusters()
                for remote_cluster in dest_remote_clusters:
                    self._modify_clusters(None, self.dest_master, remote_cluster['name'], self.src_master, require_encryption=1)

        self.set_xdcr_param('xdcrFailureRestartInterval', 1)
        self.sleep(60)
        bucket = self._get_bucket(self, 'bucket0', self.src_master)
        gen_create3 = BlobGenerator('loadThree', 'loadThree', self._value_size, end=self._num_items)
        self._load_bucket(bucket, self.src_master, gen_create3, 'create', exp=0)
        self.do_merge_bucket(self.src_master, self.dest_master, True, bucket)
        bucket = self._get_bucket(self, 'default', self.src_master)
        self._load_bucket(bucket, self.src_master, gen_create2, 'create', exp=0)
        self.do_merge_bucket(self.src_master, self.dest_master, False, bucket)
        self.sleep(60)
        self._post_upgrade_ops()
        self.sleep(60)
        self.verify_xdcr_stats(self.src_nodes, self.dest_nodes, True)
        self._verify(self.gen_create.end + gen_create2.end + gen_create3.end)

    def online_cluster_upgrade(self):
        self._install(self.servers[:self.src_init + self.dest_init ])
        self.initial_version = self.upgrade_versions[0]
        self._install(self.servers[self.src_init + self.dest_init:])
        self.cluster.shutdown()
        XDCRReplicationBaseTest.setUp(self)
        bucket_default = self._get_bucket(self, 'default', self.src_master)
        bucket_sasl = self._get_bucket(self, 'bucket0', self.src_master)
        bucket_standard = self._get_bucket(self, 'standard_bucket0', self.dest_master)

        self._load_bucket(bucket_default, self.src_master, self.gen_create, 'create', exp=0)
        self._load_bucket(bucket_sasl, self.src_master, self.gen_create, 'create', exp=0)
        self._load_bucket(bucket_standard, self.dest_master, self.gen_create, 'create', exp=0)
        gen_create2 = BlobGenerator('loadTwo', 'loadTwo-', self._value_size, end=self._num_items)
        self._load_bucket(bucket_sasl, self.dest_master, gen_create2, 'create', exp=0)

        self._online_upgrade(self.src_nodes, self.servers[self.src_init + self.dest_init:])
        self._install(self.src_nodes)
        self._online_upgrade(self.servers[self.src_init + self.dest_init:], self.src_nodes, False)

        self._load_bucket(bucket_default, self.src_master, self.gen_delete, 'delete', exp=0)
        self._load_bucket(bucket_default, self.src_master, self.gen_update, 'create', exp=self._expires)
        self._load_bucket(bucket_sasl, self.src_master, self.gen_delete, 'delete', exp=0)
        self._load_bucket(bucket_sasl, self.src_master, self.gen_update, 'create', exp=self._expires)
        self.sleep(120)

        self._online_upgrade(self.dest_nodes, self.servers[self.src_init + self.dest_init:])
        self._install(self.dest_nodes)
        self._online_upgrade(self.servers[self.src_init + self.dest_init:], self.dest_nodes, False)

        self._load_bucket(bucket_standard, self.dest_master, self.gen_delete, 'delete', exp=0)
        self._load_bucket(bucket_standard, self.dest_master, self.gen_update, 'create', exp=self._expires)
        self.do_merge_bucket(self.src_master, self.dest_master, True, bucket_sasl)
        bucket_sasl = self._get_bucket(self, 'bucket0', self.dest_master)
        gen_delete2 = BlobGenerator('loadTwo', 'loadTwo-', self._value_size,
            start=int((self._num_items) * (float)(100 - self._percent_delete) / 100), end=self._num_items)
        gen_update2 = BlobGenerator('loadTwo', 'loadTwo-', self._value_size, start=0,
            end=int(self._num_items * (float)(self._percent_update) / 100))
        self._load_bucket(bucket_sasl, self.dest_master, gen_delete2, 'delete', exp=0)
        self._load_bucket(bucket_sasl, self.dest_master, gen_update2, 'create', exp=self._expires)

        self.do_merge_bucket(self.dest_master, self.src_master, False, bucket_sasl)
        self.do_merge_bucket(self.src_master, self.dest_master, False, bucket_default)
        self.do_merge_bucket(self.dest_master, self.src_master, False, bucket_standard)
        self.sleep(120)
        self._post_upgrade_ops()
        self.sleep(120)
        self.verify_xdcr_stats(self.src_nodes, self.dest_nodes, True)
        self.max_verify = None
        if self.ddocs_src:
            for bucket_name in self.buckets_on_src:
                bucket = self._get_bucket(self, bucket_name, self.src_master)
                expected_rows = sum([len(kv_store) for kv_store in bucket.kvs.values()])
                self._verify_ddocs(expected_rows, [bucket_name], self.ddocs_src, self.src_master)

        if self.ddocs_dest:
            for bucket_name in self.buckets_on_dest:
                bucket = self._get_bucket(self, bucket_name, self.dest_master)
                expected_rows = sum([len(kv_store) for kv_store in bucket.kvs.values()])
                self._verify_ddocs(expected_rows, [bucket_name], self.ddocs_dest, self.dest_master)

    def incremental_offline_upgrade(self):
        upgrade_seq = self.input.param("upgrade_seq", "src>dest")

        self._install(self.servers[:self.src_init + self.dest_init ])
        self.cluster.shutdown()
        XDCRReplicationBaseTest.setUp(self)
        self.set_xdcr_param('xdcrFailureRestartInterval', 1)
        self.sleep(60)
        bucket = self._get_bucket(self, 'default', self.src_master)
        self._load_bucket(bucket, self.src_master, self.gen_create, 'create', exp=0)
        bucket = self._get_bucket(self, 'bucket0', self.src_master)
        self._load_bucket(bucket, self.src_master, self.gen_create, 'create', exp=0)
        bucket = self._get_bucket(self, 'bucket0', self.dest_master)
        gen_create2 = BlobGenerator('loadTwo', 'loadTwo', self._value_size, end=self._num_items)
        self._load_bucket(bucket, self.dest_master, gen_create2, 'create', exp=0)
        self.sleep(self._timeout)
        self._wait_for_replication_to_catchup()
        nodes_to_upgrade = []
        if upgrade_seq == "src>dest":
            nodes_to_upgrade = copy.copy(self.src_nodes)
            nodes_to_upgrade.extend(self.dest_nodes)
        elif upgrade_seq == "src<dest":
            nodes_to_upgrade = copy.copy(self.dest_nodes)
            nodes_to_upgrade.extend(self.src_nodes)
        elif upgrade_seq == "src><dest":
            min_cluster = min(len(self.src_nodes), len(self.dest_nodes))
            for i in xrange(min_cluster):
                nodes_to_upgrade.append(self.src_nodes[i])
                nodes_to_upgrade.append(self.dest_nodes[i])

        for _seq, node in enumerate(nodes_to_upgrade):
            self._offline_upgrade([node])
            self.set_xdcr_param('xdcrFailureRestartInterval', 1)
            self.sleep(60)
            bucket = self._get_bucket(self, 'bucket0', self.src_master)
            itemPrefix = "loadThree" + _seq * 'a'
            gen_create3 = BlobGenerator(itemPrefix, itemPrefix, self._value_size, end=self._num_items)
            self._load_bucket(bucket, self.src_master, gen_create3, 'create', exp=0)
            bucket = self._get_bucket(self, 'default', self.src_master)
            itemPrefix = "loadFour" + _seq * 'a'
            gen_create4 = BlobGenerator(itemPrefix, itemPrefix, self._value_size, end=self._num_items)
            self._load_bucket(bucket, self.src_master, gen_create4, 'create', exp=0)
            self.sleep(60)
        bucket = self._get_bucket(self, 'bucket0', self.src_master)
        self.do_merge_bucket(self.src_master, self.dest_master, True, bucket)
        bucket = self._get_bucket(self, 'default', self.src_master)
        self.do_merge_bucket(self.src_master, self.dest_master, False, bucket)
        self.verify_xdcr_stats(self.src_nodes, self.dest_nodes, True)
        self.sleep(self.wait_timeout * 5, "Let clusters work for some time")

    def _operations(self):
        # TODO: there are not tests with views
        if self.ddocs_num_src:
            ddocs = self._create_views(self.ddocs_num_src, self.buckets_on_src,
                                       self.views_num_src, self.src_master)
            self.ddocs_src.extend(ddocs)
        if self.ddocs_num_dest:
            ddocs = self._create_views(self.ddocs_num_dest, self.buckets_on_dest,
                                       self.views_num_dest, self.dest_master)
            self.ddocs_dest.extend(ddocs)

    def _verify(self, expected_rows):
        if self.ddocs_src:
            self._verify_ddocs(expected_rows, self.buckets_on_src, self.ddocs_src, self.src_master)

        if self.ddocs_dest:
            self._verify_ddocs(expected_rows, self.buckets_on_dest, self.ddocs_dest, self.dest_master)

    def _create_views(self, ddocs_num, buckets, views_num, server):
        ddocs = []
        if ddocs_num:
            self.default_view = View(self.default_view_name, None, None)
            for bucket in buckets:
                for i in xrange(ddocs_num):
                    views = self.make_default_views(self.default_view_name, views_num,
                                                    self.is_dev_ddoc, different_map=True)
                    ddoc = DesignDocument(self.default_view_name + str(i), views)
                    bucket_server = self._get_bucket(self, bucket, server)
                    tasks = self.async_create_views(server, ddoc.name, views, bucket=bucket_server)
                    for task in tasks:
                        task.result(timeout=60)
                    ddocs.append(ddoc)
        return ddocs

    def _verify_ddocs(self, expected_rows, buckets, ddocs, server):
        query = {"connectionTimeout" : 60000}
        if self.max_verify:
            expected_rows = self.max_verify
            query["limit"] = expected_rows
        for bucket in buckets:
            for ddoc in ddocs:
                prefix = ("", "dev_")[ddoc.views[0].dev_view]
                bucket_server = self._get_bucket(self, bucket, server)
                self.perform_verify_queries(len(ddoc.views), prefix, ddoc.name, query, bucket=bucket_server,
                                           wait_time=self.wait_timeout * 5, expected_rows=expected_rows,
                                           retry_time=10, server=server)

    def _post_upgrade_ops(self):
        if self.post_upgrade_ops:
            for op_cluster in self.post_upgrade_ops.split(';'):
                cluster, op = op_cluster.split('-')
                if op == 'rebalancein':
                    free_servs = [ser for ser in self.servers
                                  if not (ser in self.src_nodes or ser in self.dest_nodes)]
                    servers_to_add = free_servs[:self.nodes_in]
                    if servers_to_add:
                        temp = self.initial_version
                        self.initial_version = self.upgrade_versions[0]
                        self._install(servers_to_add)
                        self.initial_version = temp
                    if cluster == 'src':
                        self.cluster.rebalance(self.src_nodes, servers_to_add, [])
                        self.src_nodes.extend(servers_to_add)
                    elif cluster == 'dest':
                        self.cluster.rebalance(self.dest_nodes, servers_to_add, [])
                        self.dest_nodes.extend(servers_to_add)
                elif op == 'rebalanceout':
                    if cluster == 'src':
                        rebalance_out_candidates = filter(lambda node: node != self.src_master, self.src_nodes)
                        self.cluster.rebalance(self.src_nodes, [], rebalance_out_candidates[:self.nodes_out])
                        for node in rebalance_out_candidates[:self.nodes_out]:
                            self.src_nodes.remove(node)
                    elif cluster == 'dest':
                        rebalance_out_candidates = filter(lambda node: node != self.dest_master, self.dest_nodes)
                        self.cluster.rebalance(self.dest_nodes, [], rebalance_out_candidates[:self.nodes_out])
                        for node in rebalance_out_candidates[:self.nodes_out]:
                            self.dest_nodes.remove(node)
                if op == 'create_index':
                    ddoc_num = 1
                    views_num = 2
                    if cluster == 'src':
                        ddocs = self._create_views(ddoc_num, self.buckets_on_src,
                                       views_num, self.src_master)
                        self.ddocs_src.extend(ddocs)
                    elif cluster == 'dest':
                        ddocs = self._create_views(ddoc_num, self.buckets_on_dest,
                                       views_num, self.dest_master)
                        self.ddocs_dest.extend(ddocs)

    def _offline_upgrade(self, servers):
        for upgrade_version in self.upgrade_versions:
            for server in servers:
                    remote = RemoteMachineShellConnection(server)
                    remote.stop_server()
                    remote.disconnect()
            upgrade_threads = self._async_update(upgrade_version, servers)
            #wait upgrade statuses
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            success_upgrade = True
            while not self.queue.empty():
                success_upgrade &= self.queue.get()
            if not success_upgrade:
                self.fail("Upgrade failed!")
            self.sleep(self.expire_time)
