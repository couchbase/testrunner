import json

from xdcrnewbasetests import XDCRNewBaseTest, XDCRRemoteClusterRef
from xdcrnewbasetests import REPL_PARAM, Utility
from couchbase_helper.documentgenerator import DocumentGenerator
from membase.api.rest_client import RestConnection
from membase.api.esrest_client import EsRestConnection
from couchbase_helper.cluster import Cluster
from memcached.helper.data_helper import VBucketAwareMemcached
from membase.api.exception import XDCRCheckpointException


class Capi(XDCRNewBaseTest):

    def setUp(self):
        super(Capi, self).setUp()
        self.cluster = Cluster()
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.dest_master = self.dest_cluster.get_master_node()

    def tearDown(self):
        super(Capi, self).tearDown()

    def _start_es_replication(self, xdcr_params={}):
        rest_conn = RestConnection(self.src_cluster.get_master_node())
        rest_conn.create_bucket(bucket='default', ramQuotaMB=100, authType='none', saslPassword='', replicaNumber=1,
                                proxyPort=11211, bucketType='membase', replica_index=1, threadsNumber=3,
                                flushEnabled=1, lww=False)
        self.src_cluster.add_bucket(ramQuotaMB=100, bucket='default', authType='none',
                                   saslPassword='', replicaNumber=1, proxyPort=11211, bucketType='membase',
                                   evictionPolicy='valueOnly')
        esrest_conn = EsRestConnection(self.dest_cluster.get_master_node())
        esrest_conn.create_index('default')
        rest_conn.add_remote_cluster(remoteIp=self.dest_master.ip, remotePort=9091, username='Administrator',
                                     password='password', name='es')
        self.src_cluster.get_remote_clusters().append(XDCRRemoteClusterRef(self.src_cluster, self.dest_cluster,
                                                                       Utility.get_rc_name(self.src_cluster.get_name(),
                                                                                        self.dest_cluster.get_name())))
        repl_id = rest_conn.start_replication(replicationType='continuous', fromBucket='default', toCluster='es',
                                              rep_type='capi', toBucket='default', xdcr_params=xdcr_params)
        return repl_id

    def _verify_es_results(self):
        esrest_conn = EsRestConnection(self.dest_master)
        es_docs = esrest_conn.all_docs()
        self.log.info("Retrieved ES Docs")
        rest_conn = RestConnection(self.src_master)
        memcached_conn = VBucketAwareMemcached(rest_conn, 'default')
        self.log.info("Comparing CB and ES data")
        for doc in es_docs:
            es_data = doc['doc']
            mc_active = memcached_conn.memcached(str(es_data['_id']))
            cb_flags, cb_cas, cb_data = mc_active.get(str(es_data['_id']))
            self.assertDictEqual(es_data, json.loads(cb_data), "Data mismatch found - es data: {0} cb data: {1}".
                                 format(str(es_data), str(cb_data)))
        self.log.info("Data verified")

    def test_crud_ops_from_cb_to_es(self):
        repl_id = self._start_es_replication()

        rest_conn = RestConnection(self.src_master)
        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value"}}',  xrange(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.perform_update_delete()

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        self._wait_for_es_replication_to_catchup()

        self._verify_es_results()

    def test_incr_crud_ops_from_cb_to_es(self):
        repl_id = self._start_es_replication()

        rest_conn = RestConnection(self.src_master)
        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value"}}',  xrange(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.async_perform_update_delete()

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        self._wait_for_es_replication_to_catchup()

        self._verify_es_results()

    def test_capi_with_pause_resume(self):
        repl_id = self._start_es_replication()

        rest_conn = RestConnection(self.src_master)

        gen = DocumentGenerator('es', '{{"key":"value"}}',  xrange(100), start=0, end=self._num_items)
        self.src_cluster.async_load_all_buckets_from_generator(gen)

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        self.sleep(30)

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        self._wait_for_es_replication_to_catchup()

        self._verify_es_results()

    def test_capi_with_checkpointing(self):
        repl_id = self._start_es_replication()

        rest_conn = RestConnection(self.src_master)
        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value"}}',  xrange(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        self._wait_for_es_replication_to_catchup()

        self.sleep(60)

        vb0_node = None
        nodes = self.src_cluster.get_nodes()
        ip = VBucketAwareMemcached(rest_conn,'default').vBucketMap[0].split(':')[0]
        for node in nodes:
            if ip == node.ip:
                vb0_node = node
        if not vb0_node:
            raise XDCRCheckpointException("Error determining the node containing active vb0")
        vb0_conn = RestConnection(vb0_node)
        repl = vb0_conn.get_replication_for_buckets('default', 'default')
        try:
            checkpoint_record = vb0_conn.get_recent_xdcr_vb_ckpt(repl['id'])
            self.log.info("Checkpoint record : {0}".format(checkpoint_record))
        except Exception as e:
            raise XDCRCheckpointException("Error retrieving last checkpoint document - {0}".format(e))

        self._verify_es_results()

    def test_capi_with_optimistic_replication(self):
        repl_id = self._start_es_replication()

        rest_conn = RestConnection(self.src_master)

        rest_conn.set_xdcr_param('default', 'default', 'optimisticReplicationThreshold', self._optimistic_threshold)

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value"}}',  xrange(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.perform_update_delete()

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        self._wait_for_es_replication_to_catchup()

        self._verify_es_results()

    def test_capi_with_filter(self):
        repl_id = self._start_es_replication(xdcr_params={'filterExpression':'es-5*'})

        rest_conn = RestConnection(self.src_master)

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value"}}',  xrange(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        self._wait_for_es_replication_to_catchup()

        self._verify_es_results()

    def test_capi_with_advanced_settings(self):
        repl_id = self._start_es_replication(xdcr_params={'workerBatchSize':'2000',
                                                          'docBatchSizeKb':'8096',
                                                          'targetNozzlePerNode':'64'})

        rest_conn = RestConnection(self.src_master)

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value"}}',  xrange(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        self._wait_for_es_replication_to_catchup()

        self._verify_es_results()

    def test_capi_with_rebalance_in(self):
        repl_id = self._start_es_replication()

        rest_conn = RestConnection(self.src_master)
        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value"}}',  xrange(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        self.src_cluster.rebalance_in()

        self._wait_for_es_replication_to_catchup(timeout=600)

        self._verify_es_results()

    def test_capi_with_rebalance_out(self):
        repl_id = self._start_es_replication()

        rest_conn = RestConnection(self.src_master)
        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value"}}',  xrange(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        self.src_cluster.rebalance_out()

        self._wait_for_es_replication_to_catchup(timeout=600)

        self._verify_es_results()

    def test_capi_with_swap_rebalance(self):
        repl_id = self._start_es_replication()

        rest_conn = RestConnection(self.src_master)
        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value"}}',  xrange(100), start=0, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'false')

        self.src_cluster.swap_rebalance()

        self._wait_for_es_replication_to_catchup(timeout=600)

        self._verify_es_results()

    def test_capi_with_failover(self):
        repl_id = self._start_es_replication()

        rest_conn = RestConnection(self.src_master)
        rest_conn.pause_resume_repl_by_id(repl_id, REPL_PARAM.PAUSE_REQUESTED, 'true')

        gen = DocumentGenerator('es', '{{"key":"value"}}',  xrange(100), start=0, end=self._num_items)
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

        self._wait_for_es_replication_to_catchup(timeout=600)

        self._verify_es_results()