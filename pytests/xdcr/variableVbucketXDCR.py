from pytests.xdcr.xdcrnewbasetests import XDCRNewBaseTest, NodeHelper
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from lib.couchbase_helper.documentgenerator import BlobGenerator


class VariableVbucketXDCR(XDCRNewBaseTest):
    """
    Tests XDCR replication between clusters with different vbucket configurations
    and storage backends (magma vs couchstore) with goxdcr process restart scenarios.
    """

    def setUp(self):
        super().setUp()
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_master = self.dest_cluster.get_master_node()
        self.src_rest = RestConnection(self.src_master)
        self.dest_rest = RestConnection(self.dest_master)
        self.kill_count = self._input.param("kill_count", 2)
        self.kill_interval = self._input.param("kill_interval", 30)

    def _kill_goxdcr(self, cluster, wait_to_recover=True):
        """Kill goxdcr process on all nodes in a cluster."""
        for node in cluster.get_nodes():
            shell = RemoteMachineShellConnection(node)
            try:
                cmd = 'pkill -9 goxdcr || true'
                shell.execute_command(cmd, use_channel=True)
                self.log.info(f"Killed goxdcr on {node.ip}")
            except Exception as e:
                self.log.error(f"Error killing goxdcr on {node.ip}: {e}")
            finally:
                shell.disconnect()

        if wait_to_recover:
            self.sleep(15, "Waiting for goxdcr to restart")

    def _kill_goxdcr_both_clusters(self, wait_to_recover=True):
        """Kill goxdcr on both source and destination clusters."""
        self._kill_goxdcr(self.src_cluster, wait_to_recover=False)
        self._kill_goxdcr(self.dest_cluster, wait_to_recover=False)
        if wait_to_recover:
            self.sleep(15, "Waiting for goxdcr to restart on both clusters")

    def test_variable_vbucket_with_goxdcr_restart(self):
        """
        Test replication between clusters with different vbucket configurations
        (magma vs couchstore) while periodically killing and restarting goxdcr.
        
        The variable_vbucket_test param enables random vbucket selection from:
        magma_128vbs, magma_1024vbs, couchstore
        """
        self.setup_xdcr_and_load()
        
        for i in range(self.kill_count):
            self.log.info(f"Killing goxdcr {i+1}/{self.kill_count}")
            self._kill_goxdcr_both_clusters(wait_to_recover=True)
            
            gen = BlobGenerator(f"vb-kill{i}-", f"vb-kill{i}-", self._value_size, end=self._num_items // self.kill_count)
            self.src_cluster.load_all_buckets_from_generator(gen)
            self.log.info(f"Loaded additional documents after kill {i+1}")
            
            self.sleep(self.kill_interval, f"Wait after kill {i+1}")
        
        self._wait_for_replication_to_catchup(timeout=600)

    def test_magma_to_couchstore_high_load(self):
        """
        Test replication from magma storage backend to couchstore with high load
        and goxdcr restarts.
        """
        self.setup_xdcr_and_load()
        
        for i in range(self.kill_count):
            self.log.info(f"Killing goxdcr on source {i+1}/{self.kill_count}")
            self._kill_goxdcr(self.src_cluster, wait_to_recover=True)
            
            gen = BlobGenerator(f"m2c-{i}-", f"m2c-{i}-", self._value_size, end=self._num_items // self.kill_count)
            self.src_cluster.load_all_buckets_from_generator(gen)
            self.log.info(f"Loaded additional documents after kill {i+1}")
            
            self.sleep(self.kill_interval, f"Wait after kill {i+1}")
        
        self._wait_for_replication_to_catchup(timeout=600)

    def test_couchstore_to_magma_high_load(self):
        """
        Test replication from couchstore storage backend to magma with high load
        and goxdcr restarts.
        """
        self.setup_xdcr_and_load()
        
        for i in range(self.kill_count):
            self.log.info(f"Killing goxdcr on destination {i+1}/{self.kill_count}")
            self._kill_goxdcr(self.dest_cluster, wait_to_recover=True)
            
            gen = BlobGenerator(f"c2m-{i}-", f"c2m-{i}-", self._value_size, end=self._num_items // self.kill_count)
            self.src_cluster.load_all_buckets_from_generator(gen)
            self.log.info(f"Loaded additional documents after kill {i+1}")
            
            self.sleep(self.kill_interval, f"Wait after kill {i+1}")
        
        self._wait_for_replication_to_catchup(timeout=600)

    def test_bidirectional_variable_vbucket(self):
        """
        Test bidirectional replication between clusters with different vbucket
        configurations while goxdcr is periodically killed.
        """
        self.setup_xdcr_and_load()
        
        for i in range(self.kill_count):
            self.log.info(f"Killing goxdcr on both clusters {i+1}/{self.kill_count}")
            self._kill_goxdcr_both_clusters(wait_to_recover=True)
            
            gen_src = BlobGenerator(f"src-k{i}-", f"src-k{i}-", self._value_size, end=self._num_items // (self.kill_count * 2))
            gen_dest = BlobGenerator(f"dest-k{i}-", f"dest-k{i}-", self._value_size, end=self._num_items // (self.kill_count * 2))
            self.src_cluster.load_all_buckets_from_generator(gen_src)
            self.dest_cluster.load_all_buckets_from_generator(gen_dest)
            self.log.info(f"Loaded documents on both clusters after kill {i+1}")
            
            self.sleep(self.kill_interval, f"Wait after kill {i+1}")
        
        self._wait_for_replication_to_catchup(timeout=900)

    def test_variable_vbucket_rapid_kills(self):
        """
        Test replication stability with rapid goxdcr kills during high load.
        """
        self.setup_xdcr_and_load()
        
        rapid_kill_count = self._input.param("rapid_kill_count", 5)
        rapid_kill_interval = self._input.param("rapid_kill_interval", 10)
        
        for i in range(rapid_kill_count):
            self.log.info(f"Rapid kill {i+1}/{rapid_kill_count}")
            self._kill_goxdcr(self.src_cluster, wait_to_recover=False)
            self.sleep(rapid_kill_interval, "Wait between rapid kills")
        
        self.sleep(30, "Wait for goxdcr to stabilize after rapid kills")
        
        gen = BlobGenerator("post-rapid-", "post-rapid-", self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)
        self.log.info("Loaded documents after rapid kills")
        
        self._wait_for_replication_to_catchup(timeout=600)

    def test_variable_vbucket_with_mutations(self):
        """
        Test replication with updates and deletes across different vbucket
        configurations while goxdcr is periodically killed.
        """
        self.setup_xdcr_and_load()
        
        self._wait_for_replication_to_catchup(timeout=300)
        
        gen_update = BlobGenerator("loadOne", "loadOne-updated-", self._value_size, end=self._num_items // 3)
        self.src_cluster.load_all_buckets_from_generator(gen_update)
        self.log.info("Updated first third of documents")
        
        self._kill_goxdcr_both_clusters(wait_to_recover=True)
        
        gen_new = BlobGenerator("newdocs-", "newdocs-", self._value_size, end=self._num_items // 4)
        self.src_cluster.load_all_buckets_from_generator(gen_new)
        self.log.info("Loaded new documents")
        
        self._kill_goxdcr_both_clusters(wait_to_recover=True)
        
        self._wait_for_replication_to_catchup(timeout=600)
