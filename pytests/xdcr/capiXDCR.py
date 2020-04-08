import json

from couchbase_helper.cluster import Cluster
from couchbase_helper.documentgenerator import DocumentGenerator
from membase.api.esrest_client import EsRestConnection
from membase.api.exception import XDCRCheckpointException
from membase.api.rest_client import RestConnection
from membase.helper.cluster_helper import ClusterOperationHelper
from memcached.helper.data_helper import VBucketAwareMemcached
from newupgradebasetest import NewUpgradeBaseTest
from remote.remote_util import RemoteMachineShellConnection

from .xdcrnewbasetests import REPL_PARAM, NodeHelper
from .xdcrnewbasetests import XDCRNewBaseTest



class Capi(XDCRNewBaseTest, NewUpgradeBaseTest):

    def setUp(self):
        super(Capi, self).setUp()
        self.cluster = Cluster()
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.dest_master = self.dest_cluster.get_master_node()
        self.use_hostnames = self._input.param("use_hostnames", False)
        self.src_init = self._input.param('src_init', 2)
        self.dest_init = self._input.param('dest_init', 1)
        self.product = self._input.param('product', 'couchbase-server')
        self.initial_version = self._input.param('initial_version', '2.5.1-1083')
        self.initial_vbuckets = self._input.param('initial_vbuckets', 1024)
        self.init_nodes = self._input.param('init_nodes', True)
        self.initial_build_type = self._input.param('initial_build_type', None)
        self.upgrade_build_type = self._input.param('upgrade_build_type', self.initial_build_type)
        self.master = self.src_master
        self.rest = RestConnection(self.src_master)
        self.esrest_conn = EsRestConnection(self.dest_master)

    def tearDown(self):
        super(Capi, self).tearDown()

    def _verify_es_results(self, bucket='default'):
        es_docs = self.esrest_conn.all_docs()
        self.log.info("Retrieved ES Docs")
        memcached_conn = VBucketAwareMemcached(self.rest, bucket)
        self.log.info("Comparing CB and ES data")
        for doc in es_docs:
            es_data = doc['doc']
            mc_active = memcached_conn.memcached(str(es_data['_id']))
            cb_flags, cb_cas, cb_data = mc_active.get(str(es_data['_id']))
            self.assertDictEqual(es_data, json.loads(cb_data), "Data mismatch found - es data: {0} cb data: {1}".
                                 format(str(es_data), str(cb_data)))
        self.log.info("Data verified")

    def test_crud_ops_from_cb_to_es(self):
        self.setup_xdcr()

        self.src_cluster.pause_all_replications()

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}', range(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.perform_update_delete()

        self.src_cluster.resume_all_replications()

        self._wait_for_replication_to_catchup()

        self._verify_es_results()

    def test_incr_crud_ops_from_cb_to_es(self):
        self.setup_xdcr()

        self.src_cluster.pause_all_replications()

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}', range(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.async_perform_update_delete()

        self.src_cluster.resume_all_replications()

        self._wait_for_replication_to_catchup()

        self._verify_es_results()

    def test_capi_with_pause_resume(self):
        self.setup_xdcr()

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}', range(100), start=0, end=self._num_items)
        self.src_cluster.async_load_all_buckets_from_generator(gen)

        self.src_cluster.pause_all_replications()

        self.sleep(30)

        self.src_cluster.resume_all_replications()

        self._wait_for_replication_to_catchup()
        self._verify_es_results()

    def test_capi_with_checkpointing(self):
        self.setup_xdcr()

        self.src_cluster.pause_all_replications()

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}', range(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.src_cluster.resume_all_replications()
        self._wait_for_replication_to_catchup()
        self.sleep(120)

        vb0_node = None
        nodes = self.src_cluster.get_nodes()
        ip = VBucketAwareMemcached(self.rest, 'default').vBucketMap[0].split(':')[0]
        for node in nodes:
            if ip == node.ip:
                vb0_node = node
        if not vb0_node:
            raise XDCRCheckpointException("Error determining the node containing active vb0")
        vb0_conn = RestConnection(vb0_node)
        try:
            repl = vb0_conn.get_replication_for_buckets('default', 'default')
            checkpoint_record = vb0_conn.get_recent_xdcr_vb_ckpt(repl['id'])
            self.log.info("Checkpoint record : {0}".format(checkpoint_record))
        except Exception as e:
            raise XDCRCheckpointException("Error retrieving last checkpoint document - {0}".format(e))

        self._verify_es_results()

    def test_capi_with_optimistic_replication(self):
        self.setup_xdcr()

        self.src_cluster.pause_all_replications()

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}', range(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.perform_update_delete()

        self.src_cluster.resume_all_replications()

        self._wait_for_replication_to_catchup()

        self._verify_es_results()

    def test_capi_with_filter(self):

        self.setup_xdcr()

        self.src_cluster.pause_all_replications()

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}', range(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.src_cluster.resume_all_replications()

        self._verify_es_results()

    def test_capi_with_advanced_settings(self):
        batch_count = self._input.param("batch_count", 10)
        batch_size = self._input.param("batch_size", 2048)
        source_nozzle = self._input.param("source_nozzle", 2)
        target_nozzle = self._input.param("target_nozzle", 2)
        enable_firewall = self._input.param("enable_firewall", False)

        capi_data_chan_size_multi = self._input.param("capi_data_chan_size_multi", None)
        if capi_data_chan_size_multi:
            shell = RemoteMachineShellConnection(self.src_master)
            command = "curl -X POST -u Administrator:password http://127.0.0.1:9998/xdcr/internalSettings " + \
                      "-d CapiDataChanSizeMultiplier=" + str(capi_data_chan_size_multi)
            output, error = shell.execute_command(command)
            shell.log_command_output(output, error)

        self.setup_xdcr()

        rest_conn = RestConnection(self.src_master)

        rest_conn.set_xdcr_param('default', 'default', 'workerBatchSize', batch_count)
        rest_conn.set_xdcr_param('default', 'default', 'docBatchSizeKb', batch_size)
        rest_conn.set_xdcr_param('default', 'default', 'sourceNozzlePerNode', source_nozzle)
        rest_conn.set_xdcr_param('default', 'default', 'targetNozzlePerNode', target_nozzle)

        self.src_cluster.pause_all_replications()

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}', range(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.src_cluster.resume_all_replications()

        if enable_firewall:
            NodeHelper.enable_firewall(self.dest_cluster.get_master_node())
            self.sleep(120)
            NodeHelper.disable_firewall(self.dest_cluster.get_master_node())

        self._verify_es_results()

    def test_capi_with_rebalance_in(self):
        self.setup_xdcr()

        self.src_cluster.pause_all_replications()

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}', range(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.src_cluster.resume_all_replications()

        self.src_cluster.rebalance_in()

        self._wait_for_replication_to_catchup(timeout=900)

        self._verify_es_results()

    def test_capi_with_rebalance_out(self):
        self.setup_xdcr()

        self.src_cluster.pause_all_replications()

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}', range(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.src_cluster.resume_all_replications()

        self.src_cluster.rebalance_out()

        self._wait_for_replication_to_catchup(timeout=900)

        self._verify_es_results()

    def test_capi_with_swap_rebalance(self):
        self.setup_xdcr()

        self.src_cluster.pause_all_replications()

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}', range(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.src_cluster.resume_all_replications()

        self.src_cluster.swap_rebalance()

        self._wait_for_replication_to_catchup(timeout=900)

        self._verify_es_results()

    def test_capi_with_failover(self):
        self.setup_xdcr()

        self.src_cluster.pause_all_replications()
        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}', range(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.src_cluster.resume_all_replications()

        graceful = self._input.param("graceful", False)
        self.recoveryType = self._input.param("recoveryType", None)
        self.src_cluster.failover(graceful=graceful)

        self.sleep(30)
        rest_conn = RestConnection(self.src_master)
        if self.recoveryType:
            server_nodes = rest_conn.node_statuses()
            for node in server_nodes:
                if node.ip == self._input.servers[1].ip:
                    rest_conn.set_recovery_type(otpNode=node.id, recoveryType=self.recoveryType)
                    self.sleep(30)
                    rest_conn.add_back_node(otpNode=node.id)
            rebalance = self.cluster.async_rebalance(self.src_cluster.get_nodes(), [], [])
            rebalance.result()

        self._verify_es_results()

    def test_capi_with_malformed_http_resp(self):
        self.setup_xdcr()

        rest_conn = RestConnection(self.src_master)

        rest_conn.set_xdcr_param('default', 'default', 'workerBatchSize', 2000)
        rest_conn.set_xdcr_param('default', 'default', 'docBatchSizeKb', 8096)
        rest_conn.set_xdcr_param('default', 'default', 'targetNozzlePerNode', 64)

        self.src_cluster.pause_all_replications()

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}', range(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.src_cluster.resume_all_replications()

        self._wait_for_replication_to_catchup()

        goxdcr_log = NodeHelper.get_goxdcr_log_dir(self.src_master) \
                     + '/goxdcr.log*'
        for node in self.src_cluster.get_nodes():
            count = NodeHelper.check_goxdcr_log(
                node,
                "malformed HTTP response",
                goxdcr_log)
            self.assertEqual(count, 0, "malformed HTTP response error message found in " + str(node.ip))
            self.log.info("malformed HTTP response error message not found in " + str(node.ip))

        self._verify_es_results()

    def test_capi_with_offline_upgrade(self):
        self._install(self._input.servers[:self.src_init + self.dest_init])
        upgrade_nodes = self.src_cluster.get_nodes()
        upgrade_version = self._input.param("upgrade_version", "5.0.0-1797")

        self.setup_xdcr()
        self.src_cluster.pause_all_replications()

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}', range(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.perform_update_delete()

        self.src_cluster.resume_all_replications()

        self._wait_for_replication_to_catchup()

        self.src_cluster.pause_all_replications()

        self._install(servers=upgrade_nodes, version=upgrade_version)

        self.log.info("######### Upgrade of CB cluster completed ##########")

        self.create_buckets()
        self._join_all_clusters()
        self.src_cluster.pause_all_replications()

        gen = DocumentGenerator('es', '{{"key":"value"}}', range(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.perform_update_delete()

        self.src_cluster.resume_all_replications()

        self._wait_for_replication_to_catchup()

        self._verify_es_results()

    def test_capi_with_online_upgrade(self):
        self._install(self._input.servers[:self.src_init + self.dest_init])
        upgrade_version = self._input.param("upgrade_version", "5.0.0-1797")
        upgrade_nodes = self.src_cluster.get_nodes()
        extra_nodes = self._input.servers[self.src_init + self.dest_init:]

        repl_id = self._start_es_replication()

        rest_conn = RestConnection(self.src_master)
        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}', range(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.perform_update_delete()

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        self._wait_for_es_replication_to_catchup()

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        RestConnection(upgrade_nodes[0]).get_nodes_versions()
        added_versions = RestConnection(extra_nodes[0]).get_nodes_versions()
        self.cluster.rebalance(upgrade_nodes + extra_nodes, extra_nodes, [])
        self.log.info("Rebalance in all {0} nodes completed".format(added_versions[0]))
        RestConnection(upgrade_nodes[0]).get_nodes_versions()
        self.sleep(15)
        status, content = ClusterOperationHelper.find_orchestrator(upgrade_nodes[0])
        self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}". \
                        format(status, content))
        self.log.info("after rebalance in the master is {0}".format(content))
        find_master = False
        for new_server in extra_nodes:
            if content.find(new_server.ip) >= 0:
                find_master = True
                self.log.info("{0} Node {1} becomes the master".format(added_versions[0], new_server.ip))
                break
        if not find_master:
            raise Exception("After rebalance in {0} Nodes, one of them doesn't become the master".
                            format(added_versions[0]))
        self.log.info("Rebalancing out all old version nodes")
        self.cluster.rebalance(upgrade_nodes + extra_nodes, [], upgrade_nodes)
        self.src_master = self._input.servers[self.src_init + self.dest_init]

        self._install(self.src_cluster.get_nodes(), version=upgrade_version)
        upgrade_nodes = self._input.servers[self.src_init + self.dest_init:]
        extra_nodes = self.src_cluster.get_nodes()

        RestConnection(upgrade_nodes[0]).get_nodes_versions()
        added_versions = RestConnection(extra_nodes[0]).get_nodes_versions()
        self.cluster.rebalance(upgrade_nodes + extra_nodes, extra_nodes, [])
        self.log.info("Rebalance in all {0} nodes completed".format(added_versions[0]))
        RestConnection(upgrade_nodes[0]).get_nodes_versions()
        self.sleep(15)
        status, content = ClusterOperationHelper.find_orchestrator(upgrade_nodes[0])
        self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}". \
                        format(status, content))
        self.log.info("after rebalance in the master is {0}".format(content))
        self.log.info("Rebalancing out all old version nodes")
        self.cluster.rebalance(upgrade_nodes + extra_nodes, [], upgrade_nodes)
        self.src_master = self._input.servers[0]

        self.log.info("######### Upgrade of CB cluster completed ##########")

        rest_conn = RestConnection(self.src_master)
        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value"}}', range(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.perform_update_delete()

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        self._wait_for_es_replication_to_catchup()

        self._verify_es_results()

    def test_capi_with_cb_stop_and_start(self):
        self.setup_xdcr()

        self.src_cluster.pause_all_replications()

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}', range(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.src_cluster.resume_all_replications()

        self.async_perform_update_delete()

        conn = RemoteMachineShellConnection(self.src_master)
        conn.stop_couchbase()
        conn.start_couchbase()

        self.sleep(30)

        self._wait_for_replication_to_catchup()

        self._verify_es_results()

    def test_capi_with_erlang_crash(self):
        self.setup_xdcr()

        self.src_cluster.pause_all_replications()

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}', range(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.src_cluster.resume_all_replications()

        self.async_perform_update_delete()

        conn = RemoteMachineShellConnection(self.src_master)
        conn.kill_erlang()
        conn.start_couchbase()

        self.sleep(30)

        self._wait_for_replication_to_catchup()

        self._verify_es_results()

    def test_capi_with_memcached_crash(self):
        self.setup_xdcr()

        self.src_cluster.pause_all_replications()

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}', range(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.src_cluster.resume_all_replications()

        self.async_perform_update_delete()

        conn = RemoteMachineShellConnection(self.src_master)
        conn.pause_memcached()
        conn.unpause_memcached()

        self.sleep(30)

        self._wait_for_replication_to_catchup()

        self._verify_es_results()
