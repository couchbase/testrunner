import json
import Queue

from xdcrnewbasetests import XDCRNewBaseTest, XDCRRemoteClusterRef
from xdcrnewbasetests import REPL_PARAM, Utility, NodeHelper
from couchbase_helper.documentgenerator import DocumentGenerator
from membase.api.rest_client import RestConnection
from membase.api.esrest_client import EsRestConnection
from couchbase_helper.cluster import Cluster
from memcached.helper.data_helper import VBucketAwareMemcached
from membase.api.exception import XDCRCheckpointException
from remote.remote_util import RemoteMachineShellConnection
from membase.helper.cluster_helper import ClusterOperationHelper
from newupgradebasetest import NewUpgradeBaseTest
from testconstants import STANDARD_BUCKET_PORT


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

    def tearDown(self):
        super(Capi, self).tearDown()

    def _start_es_replication(self, bucket='default', xdcr_params={}):
        rest_conn = RestConnection(self.src_cluster.get_master_node())
        if bucket == 'default':
            self.log.info("Creating default bucket")
            rest_conn.create_bucket(bucket='default', ramQuotaMB=100, authType='none', saslPassword='', replicaNumber=1,
                                proxyPort=11211, bucketType='membase', replica_index=1, threadsNumber=3,
                                flushEnabled=1, lww=False)
            self.src_cluster.add_bucket(ramQuotaMB=100, bucket='default', authType='none',
                                   saslPassword='', replicaNumber=1, proxyPort=11211, bucketType='membase',
                                   evictionPolicy='valueOnly')
        elif bucket == 'sasl':
            self.log.info("Creating sasl bucket")
            rest_conn.create_bucket(bucket='sasl', ramQuotaMB=100, authType='sasl', saslPassword='password', replicaNumber=1,
                                proxyPort=11211, bucketType='membase', replica_index=1, threadsNumber=3,
                                flushEnabled=1, lww=False)
            self.src_cluster.add_bucket(ramQuotaMB=100, bucket='sasl', authType='sasl',
                                   saslPassword='password', replicaNumber=1, proxyPort=11211, bucketType='membase',
                                   evictionPolicy='valueOnly')
        elif bucket == 'standard':
            self.log.info("Creating standard bucket")
            rest_conn.create_bucket(bucket='standard', ramQuotaMB=100, authType='none', saslPassword='', replicaNumber=1,
                                proxyPort=STANDARD_BUCKET_PORT, bucketType='membase', replica_index=1, threadsNumber=3,
                                flushEnabled=1, lww=False)
            self.src_cluster.add_bucket(ramQuotaMB=100, bucket='standard', authType='none',
                                   saslPassword='', replicaNumber=1, proxyPort=STANDARD_BUCKET_PORT, bucketType='membase',
                                   evictionPolicy='valueOnly')
        elif bucket== 'lww':
            self.log.info("Creating lww bucket")
            rest_conn.create_bucket(bucket='lww', ramQuotaMB=100, authType='none', saslPassword='', replicaNumber=1,
                                proxyPort=11211, bucketType='membase', replica_index=1, threadsNumber=3,
                                flushEnabled=1, lww=True)
            self.src_cluster.add_bucket(ramQuotaMB=100, bucket='lww', authType='none',
                                   saslPassword='', replicaNumber=1, proxyPort=11211, bucketType='membase',
                                   evictionPolicy='valueOnly')
        esrest_conn = EsRestConnection(self.dest_cluster.get_master_node())
        esrest_conn.create_index(bucket)
        rest_conn.add_remote_cluster(remoteIp=self.dest_master.ip, remotePort=9091, username='Administrator',
                                     password='password', name='es')
        self.src_cluster.get_remote_clusters().append(XDCRRemoteClusterRef(self.src_cluster, self.dest_cluster,
                                                                       Utility.get_rc_name(self.src_cluster.get_name(),
                                                                                        self.dest_cluster.get_name())))
        repl_id = rest_conn.start_replication(replicationType='continuous', fromBucket=bucket, toCluster='es',
                                              rep_type='capi', toBucket=bucket, xdcr_params=xdcr_params)
        return repl_id

    def _verify_es_results(self, bucket='default'):
        esrest_conn = EsRestConnection(self.dest_master)
        es_docs = esrest_conn.all_docs()
        self.log.info("Retrieved ES Docs")
        rest_conn = RestConnection(self.src_master)
        memcached_conn = VBucketAwareMemcached(rest_conn, bucket)
        self.log.info("Comparing CB and ES data")
        for doc in es_docs:
            es_data = doc['doc']
            mc_active = memcached_conn.memcached(str(es_data['_id']))
            cb_flags, cb_cas, cb_data = mc_active.get(str(es_data['_id']))
            self.assertDictEqual(es_data, json.loads(cb_data), "Data mismatch found - es data: {0} cb data: {1}".
                                 format(str(es_data), str(cb_data)))
        self.log.info("Data verified")

    def test_crud_ops_from_cb_to_es(self):
        bucket = self._input.param("bucket", 'default')
        repl_id = self._start_es_replication(bucket=bucket)

        rest_conn = RestConnection(self.src_master)
        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}',  xrange(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.perform_update_delete()

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        self._wait_for_es_replication_to_catchup()

        self._verify_es_results(bucket=bucket)

    def test_incr_crud_ops_from_cb_to_es(self):
        repl_id = self._start_es_replication()

        rest_conn = RestConnection(self.src_master)
        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}',  xrange(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.async_perform_update_delete()

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        self._wait_for_es_replication_to_catchup()

        self._verify_es_results()

    def test_capi_with_pause_resume(self):
        repl_id = self._start_es_replication()

        rest_conn = RestConnection(self.src_master)

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}',  xrange(100), start=0, end=self._num_items)
        self.src_cluster.async_load_all_buckets_from_generator(gen)

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        self.sleep(30)

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        self._wait_for_es_replication_to_catchup()

        self._verify_es_results()

    def test_capi_with_checkpointing(self):
        repl_id = self._start_es_replication(xdcr_params={"checkpointInterval":"60"})

        rest_conn = RestConnection(self.src_master)
        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}',  xrange(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        self.sleep(120)

        vb0_node = None
        nodes = self.src_cluster.get_nodes()
        ip = VBucketAwareMemcached(rest_conn,'default').vBucketMap[0].split(':')[0]
        for node in nodes:
            if ip == node.ip:
                vb0_node = node
        if not vb0_node:
            raise XDCRCheckpointException("Error determining the node containing active vb0")
        vb0_conn = RestConnection(vb0_node)
        try:
            checkpoint_record = vb0_conn.get_recent_xdcr_vb_ckpt(repl_id)
            self.log.info("Checkpoint record : {0}".format(checkpoint_record))
        except Exception as e:
            raise XDCRCheckpointException("Error retrieving last checkpoint document - {0}".format(e))

        self._verify_es_results()

    def test_capi_with_optimistic_replication(self):
        repl_id = self._start_es_replication()

        rest_conn = RestConnection(self.src_master)

        rest_conn.set_xdcr_param('default', 'default', 'optimisticReplicationThreshold', self._optimistic_threshold)

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}',  xrange(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.perform_update_delete()

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        self._wait_for_es_replication_to_catchup()

        self._verify_es_results()

    def test_capi_with_filter(self):
        repl_id = self._start_es_replication(xdcr_params={'filterExpression':'es-5*'})

        rest_conn = RestConnection(self.src_master)

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}',  xrange(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        self._wait_for_es_replication_to_catchup()

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

        repl_id = self._start_es_replication()

        rest_conn = RestConnection(self.src_master)

        rest_conn.set_xdcr_param('default', 'default', 'workerBatchSize', batch_count)
        rest_conn.set_xdcr_param('default', 'default', 'docBatchSizeKb', batch_size)
        rest_conn.set_xdcr_param('default', 'default', 'sourceNozzlePerNode', source_nozzle)
        rest_conn.set_xdcr_param('default', 'default', 'targetNozzlePerNode', target_nozzle)

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}',  xrange(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        if enable_firewall:
            NodeHelper.enable_firewall(self.dest_cluster.get_master_node())
            self.sleep(120)
            NodeHelper.disable_firewall(self.dest_cluster.get_master_node())

        self._verify_es_results()

    def test_capi_with_rebalance_in(self):
        repl_id = self._start_es_replication()

        rest_conn = RestConnection(self.src_master)
        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}',  xrange(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        self.src_cluster.rebalance_in()

        self._wait_for_es_replication_to_catchup(timeout=900)

        self._verify_es_results()

    def test_capi_with_rebalance_out(self):
        repl_id = self._start_es_replication()

        rest_conn = RestConnection(self.src_master)
        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}',  xrange(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        self.src_cluster.rebalance_out()

        self._wait_for_es_replication_to_catchup(timeout=900)

        self._verify_es_results()

    def test_capi_with_swap_rebalance(self):
        repl_id = self._start_es_replication()

        rest_conn = RestConnection(self.src_master)
        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}',  xrange(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        self.src_cluster.swap_rebalance()

        self._wait_for_es_replication_to_catchup(timeout=600)

        self._verify_es_results()

    def test_capi_with_failover(self):
        repl_id = self._start_es_replication()

        rest_conn = RestConnection(self.src_master)
        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}',  xrange(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        graceful = self._input.param("graceful", False)
        self.recoveryType = self._input.param("recoveryType", None)
        self.src_cluster.failover(graceful=graceful)

        self.sleep(30)

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
        repl_id = self._start_es_replication(xdcr_params={'workerBatchSize':'2000',
                                                          'docBatchSizeKb':'8096',
                                                          'targetNozzlePerNode':'64'})

        rest_conn = RestConnection(self.src_master)

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}',  xrange(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        self._wait_for_es_replication_to_catchup()

        goxdcr_log = NodeHelper.get_goxdcr_log_dir(self.src_master)\
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

        repl_id = self._start_es_replication()

        rest_conn = RestConnection(self.src_master)
        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}',  xrange(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.perform_update_delete()

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        self._wait_for_es_replication_to_catchup()

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        self._install(servers=upgrade_nodes, version=upgrade_version)

        self.log.info("######### Upgrade of CB cluster completed ##########")

        repl_id = self._start_es_replication()

        rest_conn = RestConnection(self.src_master)
        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value"}}',  xrange(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.perform_update_delete()

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        self._wait_for_es_replication_to_catchup()

        self._verify_es_results()

    def test_capi_with_online_upgrade(self):
        self._install(self._input.servers[:self.src_init + self.dest_init])
        upgrade_version = self._input.param("upgrade_version", "5.0.0-1797")
        upgrade_nodes = self.src_cluster.get_nodes()
        extra_nodes = self._input.servers[self.src_init + self.dest_init:]

        repl_id = self._start_es_replication()

        rest_conn = RestConnection(self.src_master)
        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}',  xrange(100), start=0, end=self._num_items)
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
        self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}".\
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
        self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}".\
                        format(status, content))
        self.log.info("after rebalance in the master is {0}".format(content))
        self.log.info("Rebalancing out all old version nodes")
        self.cluster.rebalance(upgrade_nodes + extra_nodes, [], upgrade_nodes)
        self.src_master = self._input.servers[0]

        self.log.info("######### Upgrade of CB cluster completed ##########")

        rest_conn = RestConnection(self.src_master)
        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value"}}',  xrange(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.perform_update_delete()

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        self._wait_for_es_replication_to_catchup()

        self._verify_es_results()

    def test_capi_with_cb_stop_and_start(self):
        bucket = self._input.param("bucket", 'default')
        repl_id = self._start_es_replication(bucket=bucket)

        rest_conn = RestConnection(self.src_master)
        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}',  xrange(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        self.async_perform_update_delete()

        conn = RemoteMachineShellConnection(self.src_master)
        conn.stop_couchbase()
        conn.start_couchbase()

        self.sleep(30)

        self._wait_for_es_replication_to_catchup()

        self._verify_es_results(bucket=bucket)

    def test_capi_with_erlang_crash(self):
        bucket = self._input.param("bucket", 'default')
        repl_id = self._start_es_replication(bucket=bucket)

        rest_conn = RestConnection(self.src_master)
        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}',  xrange(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        self.async_perform_update_delete()

        conn = RemoteMachineShellConnection(self.src_master)
        conn.kill_erlang()
        conn.start_couchbase()

        self.sleep(30)

        self._wait_for_es_replication_to_catchup()

        self._verify_es_results(bucket=bucket)

    def test_capi_with_memcached_crash(self):
        bucket = self._input.param("bucket", 'default')
        repl_id = self._start_es_replication(bucket=bucket)

        rest_conn = RestConnection(self.src_master)
        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value","mutated":0}}',  xrange(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        self.async_perform_update_delete()

        conn = RemoteMachineShellConnection(self.src_master)
        conn.pause_memcached()
        conn.unpause_memcached()

        self.sleep(30)

        self._wait_for_es_replication_to_catchup()

        self._verify_es_results(bucket=bucket)