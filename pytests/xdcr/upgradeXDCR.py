from threading import Thread
import time
import re
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

class UpgradeTests(NewUpgradeBaseTest, XDCRReplicationBaseTest):
    def setUp(self):
        datetime.now()
        super(UpgradeTests, self).setUp()
        self.bucket_topology = self.input.param("bucket_topology", "default:1><2").split(";")
        self.src_init = self.input.param('src_init', 2)
        self.dest_init = self.input.param('dest_init', 2)
        self.buckets_on_src = [str(bucket_repl.split(":")[0]) for bucket_repl in self.bucket_topology if re.search('\S+:\S*1', bucket_repl)]
        self.buckets_on_dest = [str(bucket_repl.split(":")[0]) for bucket_repl in self.bucket_topology if re.search('\S+:\S*2', bucket_repl)]
        self.repl_buckets_from_src = [str(bucket_repl.split(":")[0]) for bucket_repl in self.bucket_topology if bucket_repl.find("1>") != -1 ]
        self.repl_buckets_from_dest = [str(bucket_repl.split(":")[0]) for bucket_repl in self.bucket_topology if bucket_repl.find("<2") != -1 ]
        self._override_clusters_structure()
        self.queue = Queue.Queue()

    def tearDown(self):
        try:
            XDCRReplicationBaseTest.tearDown(self)
        finally:
            self.cluster.shutdown()

    def _override_clusters_structure(self):
        TestInputSingleton.input.clusters[0] = self.servers[:self.src_init]
        TestInputSingleton.input.clusters[1] = self.servers[self.src_init: self.src_init + self.dest_init ]

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
            self._join_clusters(src_cluster_name, self.src_master, dest_cluster_name, self.dest_master)
            dest_key_index += 1

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
            self._join_clusters(src_cluster_name, self.src_master, dest_cluster_name, self.dest_master)
            self.sleep(30)

    def _join_clusters(self, src_cluster_name, src_master, dest_cluster_name, dest_master):
        if len(self.repl_buckets_from_src):
            self._link_clusters(src_cluster_name, src_master, dest_cluster_name, dest_master)
        if len(self.repl_buckets_from_dest):
            self._link_clusters(dest_cluster_name, dest_master, src_cluster_name, src_master)

        self._replicate_clusters(src_master, dest_cluster_name, self.repl_buckets_from_src)
        self._replicate_clusters(dest_master, src_cluster_name, self.repl_buckets_from_dest)

    def _replicate_clusters(self, src_master, dest_cluster_name, buckets):
        rest_conn_src = RestConnection(src_master)
        for bucket in buckets:
            (rep_database, rep_id) = rest_conn_src.start_replication(XDCRConstants.REPLICATION_TYPE_CONTINUOUS,
                bucket, dest_cluster_name)
            self._start_replication_time[bucket] = datetime.now()
            self.sleep(5)
        if self._get_cluster_buckets(src_master):
            self._cluster_state_arr.append((rest_conn_src, dest_cluster_name, rep_database, rep_id))

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
        bucket = self._get_bucket('default', self.src_master)
        self._load_bucket(bucket, self.src_master, self.gen_create, 'create', exp=0)
        bucket = self._get_bucket('bucket0', self.src_master)
        self._load_bucket(bucket, self.src_master, self.gen_create, 'create', exp=0)
        bucket = self._get_bucket('bucket0', self.dest_master)
        gen_create2 = BlobGenerator('loadTwo', 'loadTwo', self._value_size, end=self._num_items)
        self._load_bucket(bucket, self.dest_master, gen_create2, 'create', exp=0)
        nodes_to_upgrade = []
        if "src" in upgrade_nodes :
            nodes_to_upgrade += self.src_nodes
        if "dest" in upgrade_nodes :
            nodes_to_upgrade += self.dest_nodes

        for upgrade_version in self.upgrade_versions:
            for server in nodes_to_upgrade:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                remote.disconnect()
            upgrade_threads = self._async_update(upgrade_version, nodes_to_upgrade)
            #wait upgrade statuses
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            success_upgrade = True
            while not self.queue.empty():
                success_upgrade &= self.queue.get()
            if not success_upgrade:
                self.fail("Upgrade failed!")
            self.sleep(self.expire_time)

        bucket = self._get_bucket('bucket0', self.src_master)
        gen_create3 = BlobGenerator('loadThree', 'loadThree', self._value_size, end=self._num_items)
        self._load_bucket(bucket, self.src_master, gen_create3, 'create', exp=0)
        self.do_merge_bucket(self.src_master, self.dest_master, True, bucket)
        bucket = self._get_bucket('default', self.src_master)
        self._load_bucket(bucket, self.src_master, gen_create2, 'create', exp=0)
        self.do_merge_bucket(self.src_master, self.dest_master, False, bucket)
        self.verify_xdcr_stats(self.src_nodes, self.dest_nodes, True)

    def online_cluster_upgrade(self):
        self._install(self.servers[:self.src_init + self.dest_init ])
        self.initial_version = self.upgrade_versions[0]
        self._install(self.servers[self.src_init + self.dest_init:])
        self.cluster.shutdown()
        XDCRReplicationBaseTest.setUp(self)
        bucket_default = self._get_bucket('default', self.src_master)
        bucket_sasl = self._get_bucket('bucket0', self.src_master)
        bucket_standard = self._get_bucket('standard_bucket0', self.dest_master)

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

        self._online_upgrade(self.dest_nodes, self.servers[self.src_init + self.dest_init:])
        self._install(self.dest_nodes)
        self._online_upgrade(self.servers[self.src_init + self.dest_init:], self.dest_nodes, False)

        self._load_bucket(bucket_standard, self.dest_master, self.gen_delete, 'delete', exp=0)
        self._load_bucket(bucket_standard, self.dest_master, self.gen_update, 'create', exp=self._expires)
        self.do_merge_bucket(self.src_master, self.dest_master, True, bucket_sasl)
        bucket_sasl = self._get_bucket('bucket0', self.dest_master)
        gen_delete2 = BlobGenerator('loadTwo', 'loadTwo-', self._value_size,
            start=int((self._num_items) * (float)(100 - self._percent_delete) / 100), end=self._num_items)
        gen_update2 = BlobGenerator('loadTwo', 'loadTwo-', self._value_size, start=0,
            end=int(self._num_items * (float)(self._percent_update) / 100))
        self._load_bucket(bucket_sasl, self.dest_master, gen_delete2, 'delete', exp=0)
        self._load_bucket(bucket_sasl, self.dest_master, gen_update2, 'create', exp=self._expires)

        self.do_merge_bucket(self.dest_master, self.src_master, False, bucket_sasl)
        self.do_merge_bucket(self.src_master, self.dest_master, False, bucket_default)
        self.do_merge_bucket(self.dest_master, self.src_master, False, bucket_standard)
        self.verify_xdcr_stats(self.src_nodes, self.dest_nodes, True)
