from .xdcrnewbasetests import XDCRNewBaseTest, TOPOLOGY
from remote.remote_util import RemoteMachineShellConnection, RestConnection
from couchbase_helper.documentgenerator import BlobGenerator
from couchbase_helper.cluster import Cluster
import json, time

class compression(XDCRNewBaseTest):
    def setUp(self):
        super(compression, self).setUp()
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.dest_master = self.dest_cluster.get_master_node()
        self.chain_length = self._input.param("chain_length", 2)
        self.topology = self._input.param("ctopology", "chain")
        if self.chain_length > 2:
            self.c3_cluster = self.get_cb_cluster_by_name('C3')
            self.c3_master = self.c3_cluster.get_master_node()
        self.cluster = Cluster()

    def tearDown(self):
        super(compression, self).tearDown()

    def _set_compression_type(self, cluster, bucket_name, compression_type="None"):
        repls = cluster.get_remote_clusters()[0].get_replications()
        for repl in repls:
            if bucket_name in str(repl):
                repl_id = repl.get_repl_id()
        shell = RemoteMachineShellConnection(cluster.get_master_node())
        repl_id = str(repl_id).replace('/', '%2F')
        base_url = "http://" + cluster.get_master_node().ip + ":8091/settings/replications/" + repl_id
        command = "curl -X POST -u Administrator:password " + base_url + " -d compressionType=" + str(compression_type)
        output, error = shell.execute_command(command)
        shell.log_command_output(output, error)
        shell.disconnect()
        return output, error

    def _verify_compression(self, cluster, compr_bucket_name="", uncompr_bucket_name="",
                            compression_type="None", repl_time=0):
        repls = cluster.get_remote_clusters()[0].get_replications()
        for repl in repls:
            if compr_bucket_name in str(repl):
                compr_repl_id = repl.get_repl_id()
            elif uncompr_bucket_name in str(repl):
                uncompr_repl_id = repl.get_repl_id()

        compr_repl_id = str(compr_repl_id).replace('/', '%2F')
        uncompr_repl_id = str(uncompr_repl_id).replace('/', '%2F')

        base_url = "http://" + cluster.get_master_node().ip + ":8091/settings/replications/" + compr_repl_id
        shell = RemoteMachineShellConnection(cluster.get_master_node())
        command = "curl -u Administrator:password " + base_url
        output, error = shell.execute_command(command)
        shell.log_command_output(output, error)
        self.assertTrue('"compressionType":"Snappy"' in output[0],
                        "Compression Type for replication " + compr_repl_id + " is not Snappy")
        self.log.info("Compression Type for replication " + compr_repl_id + " is Snappy")

        base_url = "http://" + cluster.get_master_node().ip + ":8091/pools/default/buckets/" + compr_bucket_name + \
                   "/stats/replications%2F" + compr_repl_id + "%2Fdata_replicated?haveTStamp=" + str(repl_time)
        command = "curl -u Administrator:password " + base_url
        output, error = shell.execute_command(command)
        shell.log_command_output(output, error)
        output = json.loads(output[0])
        compressed_data_replicated = 0
        for node in cluster.get_nodes():
            items = output["nodeStats"]["{0}:8091".format(node.ip)]
            for item in items:
                compressed_data_replicated += item
        self.log.info("Compressed data for replication {0} is {1}".format(compr_repl_id, compressed_data_replicated))

        base_url = "http://" + cluster.get_master_node().ip + ":8091/pools/default/buckets/" + uncompr_bucket_name + \
                   "/stats/replications%2F" + uncompr_repl_id + "%2Fdata_replicated?haveTStamp=" + str(repl_time)
        command = "curl -u Administrator:password " + base_url
        output, error = shell.execute_command(command)
        shell.log_command_output(output, error)
        output = json.loads(output[0])
        uncompressed_data_replicated = 0
        for node in cluster.get_nodes():
            items = output["nodeStats"]["{0}:8091".format(node.ip)]
            for item in items:
                uncompressed_data_replicated += item
        self.log.info("Uncompressed data for replication {0} is {1}".format(uncompr_repl_id, uncompressed_data_replicated))

        self.assertTrue(uncompressed_data_replicated > compressed_data_replicated,
                        "Compression did not work as expected")
        self.log.info("Compression worked as expected")

        shell.disconnect()

    def test_compression_with_unixdcr_incr_load(self):
        bucket_prefix = self._input.param("bucket_prefix", "standard_bucket_")
        self.setup_xdcr()
        repl_time = int(time.time())
        self.sleep(60)
        compression_type = self._input.param("compression_type", "Snappy")
        self._set_compression_type(self.src_cluster, bucket_prefix + "1", compression_type)
        self._set_compression_type(self.src_cluster, bucket_prefix + "2")
        if self.chain_length > 2 and self.topology == TOPOLOGY.CHAIN:
            self._set_compression_type(self.dest_cluster, bucket_prefix + "1", compression_type)
            self._set_compression_type(self.dest_cluster, bucket_prefix + "2")
        if self.chain_length > 2 and self.topology == TOPOLOGY.RING:
            self._set_compression_type(self.dest_cluster, bucket_prefix + "1", compression_type)
            self._set_compression_type(self.dest_cluster, bucket_prefix + "2")
            self._set_compression_type(self.c3_cluster, bucket_prefix + "1", compression_type)
            self._set_compression_type(self.c3_cluster, bucket_prefix + "2")

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.perform_update_delete()

        self._wait_for_replication_to_catchup()

        self._verify_compression(cluster=self.src_cluster,
                                 compr_bucket_name=bucket_prefix + "1",
                                 uncompr_bucket_name=bucket_prefix + "2",
                                 compression_type=compression_type,
                                 repl_time=repl_time)
        if self.chain_length > 2 and self.topology == TOPOLOGY.CHAIN:
            self._verify_compression(cluster=self.dest_cluster,
                                     compr_bucket_name=bucket_prefix + "1",
                                     uncompr_bucket_name=bucket_prefix + "2",
                                     compression_type=compression_type,
                                     repl_time=repl_time)
        if self.chain_length > 2 and self.topology == TOPOLOGY.RING:
            self._verify_compression(cluster=self.dest_cluster,
                                     compr_bucket_name=bucket_prefix + "1",
                                     uncompr_bucket_name=bucket_prefix + "2",
                                     compression_type=compression_type,
                                     repl_time=repl_time)
            self._verify_compression(cluster=self.c3_cluster,
                                     compr_bucket_name=bucket_prefix + "1",
                                     uncompr_bucket_name=bucket_prefix + "2",
                                     compression_type=compression_type,
                                     repl_time=repl_time)
        self.verify_results()

    def test_compression_with_unixdcr_backfill_load(self):
        self.setup_xdcr()
        repl_time = int(time.time())
        self.sleep(60)
        compression_type = self._input.param("compression_type", "Snappy")
        self._set_compression_type(self.src_cluster, "standard_bucket_1", compression_type)
        self._set_compression_type(self.src_cluster, "standard_bucket_2")

        self.src_cluster.pause_all_replications()

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.resume_all_replications()

        self.perform_update_delete()

        self._wait_for_replication_to_catchup()

        self._verify_compression(cluster=self.src_cluster,
                                 compr_bucket_name="standard_bucket_1",
                                 uncompr_bucket_name="standard_bucket_2",
                                 compression_type=compression_type,
                                 repl_time=repl_time)
        self.verify_results()

    def test_compression_with_bixdcr_incr_load(self):
        self.setup_xdcr()
        repl_time = int(time.time())
        self.sleep(60)
        compression_type = self._input.param("compression_type", "Snappy")
        self._set_compression_type(self.src_cluster, "standard_bucket_1", compression_type)
        self._set_compression_type(self.src_cluster, "standard_bucket_2")
        self._set_compression_type(self.dest_cluster, "standard_bucket_1", compression_type)
        self._set_compression_type(self.dest_cluster, "standard_bucket_2")

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)
        gen_create = BlobGenerator('comprTwo-', 'comprTwo-', self._value_size, end=self._num_items)
        self.dest_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.perform_update_delete()

        self._wait_for_replication_to_catchup()

        self._verify_compression(cluster=self.src_cluster,
                                 compr_bucket_name="standard_bucket_1",
                                 uncompr_bucket_name="standard_bucket_2",
                                 compression_type=compression_type,
                                 repl_time=repl_time)
        self._verify_compression(cluster=self.dest_cluster,
                                 compr_bucket_name="standard_bucket_1",
                                 uncompr_bucket_name="standard_bucket_2",
                                 compression_type=compression_type,
                                 repl_time=repl_time)
        self.verify_results()

    def test_compression_with_bixdcr_backfill_load(self):
        self.setup_xdcr()
        repl_time = int(time.time())
        self.sleep(60)
        compression_type = self._input.param("compression_type", "Snappy")
        self._set_compression_type(self.src_cluster, "standard_bucket_1", compression_type)
        self._set_compression_type(self.src_cluster, "standard_bucket_2")
        self._set_compression_type(self.dest_cluster, "standard_bucket_1", compression_type)
        self._set_compression_type(self.dest_cluster, "standard_bucket_2")

        self.src_cluster.pause_all_replications()
        self.dest_cluster.pause_all_replications()

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)
        gen_create = BlobGenerator('comprTwo-', 'comprTwo-', self._value_size, end=self._num_items)
        self.dest_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.resume_all_replications()
        self.dest_cluster.resume_all_replications()

        self.perform_update_delete()

        self._wait_for_replication_to_catchup()

        self._verify_compression(cluster=self.src_cluster,
                                 compr_bucket_name="standard_bucket_1",
                                 uncompr_bucket_name="standard_bucket_2",
                                 compression_type=compression_type,
                                 repl_time=repl_time)
        self._verify_compression(cluster=self.dest_cluster,
                                 compr_bucket_name="standard_bucket_1",
                                 uncompr_bucket_name="standard_bucket_2",
                                 compression_type=compression_type,
                                 repl_time=repl_time)
        self.verify_results()

    def test_compression_with_pause_resume(self):
        repeat = self._input.param("repeat", 5)
        self.setup_xdcr()
        repl_time = int(time.time())
        self.sleep(60)
        compression_type = self._input.param("compression_type", "Snappy")
        self._set_compression_type(self.src_cluster, "standard_bucket_1", compression_type)
        self._set_compression_type(self.src_cluster, "standard_bucket_2")

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.async_perform_update_delete()

        for i in range(0, repeat):
            self.src_cluster.pause_all_replications()
            self.sleep(30)
            self.src_cluster.resume_all_replications()

        self._wait_for_replication_to_catchup()

        self.verify_results()

    def test_compression_with_optimistic_threshold_change(self):
        self.setup_xdcr()
        repl_time = int(time.time())
        self.sleep(60)
        compression_type = self._input.param("compression_type", "Snappy")
        self._set_compression_type(self.src_cluster, "standard_bucket_1", compression_type)
        self._set_compression_type(self.src_cluster, "standard_bucket_2")

        src_conn = RestConnection(self.src_cluster.get_master_node())
        src_conn.set_xdcr_param('standard_bucket_1', 'standard_bucket_1', 'optimisticReplicationThreshold',
                                self._optimistic_threshold)
        src_conn.set_xdcr_param('standard_bucket_2', 'standard_bucket_2', 'optimisticReplicationThreshold',
                                self._optimistic_threshold)

        self.src_cluster.pause_all_replications()

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.resume_all_replications()

        self.async_perform_update_delete()

        self._wait_for_replication_to_catchup()

        self._verify_compression(cluster=self.src_cluster,
                                 compr_bucket_name="standard_bucket_1",
                                 uncompr_bucket_name="standard_bucket_2",
                                 compression_type=compression_type,
                                 repl_time=repl_time)
        self.verify_results()

    def test_compression_with_advanced_settings(self):
        batch_count = self._input.param("batch_count", 10)
        batch_size = self._input.param("batch_size", 2048)
        source_nozzle = self._input.param("source_nozzle", 2)
        target_nozzle = self._input.param("target_nozzle", 2)

        self.setup_xdcr()
        repl_time = int(time.time())
        self.sleep(60)
        compression_type = self._input.param("compression_type", "Snappy")
        self._set_compression_type(self.src_cluster, "standard_bucket_1", compression_type)
        self._set_compression_type(self.src_cluster, "standard_bucket_2")

        src_conn = RestConnection(self.src_cluster.get_master_node())
        src_conn.set_xdcr_param('standard_bucket_1', 'standard_bucket_1', 'workerBatchSize', batch_count)
        src_conn.set_xdcr_param('standard_bucket_1', 'standard_bucket_1', 'docBatchSizeKb', batch_size)
        src_conn.set_xdcr_param('standard_bucket_1', 'standard_bucket_1', 'sourceNozzlePerNode', source_nozzle)
        src_conn.set_xdcr_param('standard_bucket_1', 'standard_bucket_1', 'targetNozzlePerNode', target_nozzle)
        src_conn.set_xdcr_param('standard_bucket_2', 'standard_bucket_2', 'workerBatchSize', batch_count)
        src_conn.set_xdcr_param('standard_bucket_2', 'standard_bucket_2', 'docBatchSizeKb', batch_size)
        src_conn.set_xdcr_param('standard_bucket_2', 'standard_bucket_2', 'sourceNozzlePerNode', source_nozzle)
        src_conn.set_xdcr_param('standard_bucket_2', 'standard_bucket_2', 'targetNozzlePerNode', target_nozzle)

        self.src_cluster.pause_all_replications()

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.resume_all_replications()

        self.async_perform_update_delete()

        self._wait_for_replication_to_catchup()

        self._verify_compression(cluster=self.src_cluster,
                                 compr_bucket_name="standard_bucket_1",
                                 uncompr_bucket_name="standard_bucket_2",
                                 compression_type=compression_type,
                                 repl_time=repl_time)
        self.verify_results()

    def test_compression_with_capi(self):
        self.setup_xdcr()
        self.sleep(60)
        compression_type = self._input.param("compression_type", "Snappy")
        output, error = self._set_compression_type(self.src_cluster, "default", compression_type)
        self.assertTrue("The value can not be specified for CAPI replication" in output[0], "Compression enabled for CAPI")
        self.log.info("Compression not enabled for CAPI as expected")

    def test_compression_with_rebalance_in(self):
        self.setup_xdcr()
        repl_time = int(time.time())
        self.sleep(60)
        compression_type = self._input.param("compression_type", "Snappy")
        self._set_compression_type(self.src_cluster, "standard_bucket_1", compression_type)
        self._set_compression_type(self.src_cluster, "standard_bucket_2")

        self.src_cluster.pause_all_replications()

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.resume_all_replications()

        self.async_perform_update_delete()

        self.src_cluster.rebalance_in()

        self._wait_for_replication_to_catchup()

        self.verify_results()

    def test_compression_with_rebalance_out(self):
        self.setup_xdcr()
        repl_time = int(time.time())
        self.sleep(60)
        compression_type = self._input.param("compression_type", "Snappy")
        self._set_compression_type(self.src_cluster, "standard_bucket_1", compression_type)
        self._set_compression_type(self.src_cluster, "standard_bucket_2")

        self.src_cluster.pause_all_replications()

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.resume_all_replications()

        self.async_perform_update_delete()

        self.src_cluster.rebalance_out()

        self._wait_for_replication_to_catchup()

        self._verify_compression(cluster=self.src_cluster,
                                 compr_bucket_name="standard_bucket_1",
                                 uncompr_bucket_name="standard_bucket_2",
                                 compression_type=compression_type,
                                 repl_time=repl_time)
        self.verify_results()

    def test_compression_with_swap_rebalance(self):
        self.setup_xdcr()
        repl_time = int(time.time())
        self.sleep(60)
        compression_type = self._input.param("compression_type", "Snappy")
        self._set_compression_type(self.src_cluster, "standard_bucket_1", compression_type)
        self._set_compression_type(self.src_cluster, "standard_bucket_2")

        self.src_cluster.pause_all_replications()

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.resume_all_replications()

        self.async_perform_update_delete()

        self.src_cluster.swap_rebalance()

        self._wait_for_replication_to_catchup()

        self._verify_compression(cluster=self.src_cluster,
                                 compr_bucket_name="standard_bucket_1",
                                 uncompr_bucket_name="standard_bucket_2",
                                 compression_type=compression_type,
                                 repl_time=repl_time)
        self.verify_results()

    def test_compression_with_failover(self):
        self.setup_xdcr()
        repl_time = int(time.time())
        self.sleep(60)
        compression_type = self._input.param("compression_type", "Snappy")
        self._set_compression_type(self.src_cluster, "standard_bucket_1", compression_type)
        self._set_compression_type(self.src_cluster, "standard_bucket_2")

        self.src_cluster.pause_all_replications()

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.resume_all_replications()

        self.async_perform_update_delete()

        src_conn = RestConnection(self.src_cluster.get_master_node())
        graceful = self._input.param("graceful", False)
        self.recoveryType = self._input.param("recoveryType", None)
        self.src_cluster.failover(graceful=graceful)

        self.sleep(30)

        if self.recoveryType:
            server_nodes = src_conn.node_statuses()
            for node in server_nodes:
                if node.ip == self._input.servers[1].ip:
                    src_conn.set_recovery_type(otpNode=node.id, recoveryType=self.recoveryType)
                    self.sleep(30)
                    src_conn.add_back_node(otpNode=node.id)
            rebalance = self.cluster.async_rebalance(self.src_cluster.get_nodes(), [], [])
            rebalance.result()

        self._wait_for_replication_to_catchup()

        self._verify_compression(cluster=self.src_cluster,
                                 compr_bucket_name="standard_bucket_1",
                                 uncompr_bucket_name="standard_bucket_2",
                                 compression_type=compression_type,
                                 repl_time=repl_time)
        self.verify_results()

    def test_compression_with_replication_delete_and_create(self):
        self.setup_xdcr()
        repl_time = int(time.time())
        self.sleep(60)

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.async_perform_update_delete()

        rest_conn = RestConnection(self.src_master)
        rest_conn.remove_all_replications()
        rest_conn.remove_all_remote_clusters()

        self.src_cluster.get_remote_clusters()[0].clear_all_replications()
        self.src_cluster.clear_all_remote_clusters()

        self.setup_xdcr()

        compression_type = self._input.param("compression_type", "Snappy")
        self._set_compression_type(self.src_cluster, "standard_bucket_1", compression_type)
        self._set_compression_type(self.src_cluster, "standard_bucket_2")

        self._wait_for_replication_to_catchup()

        self.verify_results()

    def test_compression_with_bixdcr_and_compression_one_way(self):
        self.setup_xdcr()
        self.sleep(60)
        compression_type = self._input.param("compression_type", "Snappy")
        self._set_compression_type(self.src_cluster, "default", compression_type)

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)
        gen_create = BlobGenerator('comprTwo-', 'comprTwo-', self._value_size, end=self._num_items)
        self.dest_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.perform_update_delete()

        self._wait_for_replication_to_catchup()

        self.verify_results()

    def test_compression_with_enabling_later(self):
        self.setup_xdcr()
        self.sleep(60)

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.async_perform_update_delete()
        self.sleep(10)

        compression_type = self._input.param("compression_type", "Snappy")
        self._set_compression_type(self.src_cluster, "default", compression_type)

        self._wait_for_replication_to_catchup()

        self.verify_results()

    def test_compression_with_disabling_later(self):
        self.setup_xdcr()
        self.sleep(60)
        compression_type = self._input.param("compression_type", "Snappy")
        self._set_compression_type(self.src_cluster, "default", compression_type)

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.async_perform_update_delete()
        self.sleep(10)

        self._set_compression_type(self.src_cluster, "default", "None")

        self._wait_for_replication_to_catchup()

        self.verify_results()

    def test_compression_with_rebalance_out_target_and_disabling(self):
        self.setup_xdcr()
        self.sleep(60)
        compression_type = self._input.param("compression_type", "Snappy")
        self._set_compression_type(self.src_cluster, "default", compression_type)

        self.src_cluster.pause_all_replications()

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.resume_all_replications()

        self.async_perform_update_delete()

        self.dest_cluster.rebalance_out()

        self._set_compression_type(self.src_cluster, "default", "None")
        self.sleep(5)
        self._set_compression_type(self.src_cluster, "default", compression_type)

        self._wait_for_replication_to_catchup()

        self.verify_results()

    def test_compression_with_rebalance_out_src_and_disabling(self):
        self.setup_xdcr()
        self.sleep(60)
        compression_type = self._input.param("compression_type", "Snappy")
        self._set_compression_type(self.src_cluster, "default", compression_type)

        self.src_cluster.pause_all_replications()

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.resume_all_replications()

        self.async_perform_update_delete()

        self.src_cluster.rebalance_out()

        self._set_compression_type(self.src_cluster, "default", "None")
        self.sleep(5)
        self._set_compression_type(self.src_cluster, "default", compression_type)

        self._wait_for_replication_to_catchup()

        self.verify_results()
