import re
from lib.couchbase_helper.documentgenerator import BlobGenerator
from lib.membase.api.rest_client import RestConnection
from pytests.xdcr.xdcrnewbasetests import XDCRNewBaseTest, FloatingServers
from lib.remote.remote_util import RemoteMachineShellConnection

class MemoryTestXDCR(XDCRNewBaseTest):
    def setUp(self):
        super().setUp()
        self.src_cluster = self.get_cb_cluster_by_name("C1")
        self.dest_cluster = self.get_cb_cluster_by_name("C2")
        self.src_master = self.src_cluster.get_master_node()
        self.dest_master = self.dest_cluster.get_master_node()
        self.iteration_count = self._input.param("iteration_count", 100)
        self.leak_threshold = self._input.param("leak_threshold", 15)
        self.pause_resume_interval = self._input.param("pause_resume_interval", 10)
        self.src_rest = RestConnection(self.src_master)
        self.dest_rest = RestConnection(self.dest_master)
    def get_goroutines(self, node):
        shell = RemoteMachineShellConnection(node)
        output, err, status_code = shell.execute_command(f"curl -s http://localhost:9998/debug/pprof/goroutine?debug=1",
                                                         get_exit_code=True, timeout=5, use_channel=True)
        match = re.search(r'goroutine profile: total (\d+)', output[0])
        if match:
            return int(match.group(1))
        else:
            raise ValueError("Could not find goroutine total in pprof output")
    def test_goroutine_leak(self):
        max_goroutines = 0
        increase_count = 0
        threshold = 7
        self.src_cluster.set_internal_setting("TopologyChangeCheckInterval", 10)
        self.src_cluster.set_internal_setting("MaxTopologyChangeCountBeforeRestart", 3)
        self.src_cluster.set_internal_setting("MaxTopologyStableCountBeforeRestart", 3)
        self.src_cluster.set_internal_setting("TopologySvcCooldownPeriodSec", 1)
        gen = BlobGenerator("doc-", "doc-", 300, 0, 2000)
        self.src_cluster.load_all_buckets_from_generator(gen)
        self.sleep(10, "waiting after inserting data")
        for i in range(3):
            self.src_cluster.load_all_buckets_from_generator(gen)
            self.src_cluster.pause_all_replications(verify=True)
            self.sleep(20, "Waiting for pausing")
            self.src_cluster.resume_all_replications(verify=True)
            self.sleep(20, "Waiting for resuming")
        self.sleep(5, "Waiting before rebalance")
        node_to_monitor = FloatingServers._serverlist[-1]
        self.src_cluster.rebalance_in(1)
        for i in range(5):
            self.src_cluster.load_all_buckets_from_generator(gen)
            self.src_cluster.pause_all_replications(verify=True)
            self.sleep(5, "Waiting after pausing")
            self.src_cluster.resume_all_replications(verify=True)
        for i in range(100):
            self.log.info(f"Cycle {i + 1} of 100")
            try:
                self.src_cluster.pause_all_replications(verify=True)
                self.sleep(5, "Waiting after pausing replication in cycle")
                self.src_cluster.resume_all_replications(verify=True)
                self.sleep(5, "Waiting after resuming replication in cycle")
            except Exception as e:
                self.fail(f"Pause/resume failed on iteration {i + 1}: {e}")
            goroutines = self.get_goroutines(node_to_monitor)
            self.log.info(f"Goroutines Count : {goroutines}")
            if goroutines > max_goroutines:
                max_goroutines = goroutines
                increase_count += 1
                self.log.info(f"Max increased to: {max_goroutines}")
            self.log.info(f"Current max goroutines: {max_goroutines}")
            self.log.info(f"Increase count so far: {increase_count}")
            if increase_count > threshold:
                self.fail(f"Goroutine count increased {increase_count} times — leak detected")
            self.sleep(1, "Waiting after goroutine check")
        self.log.info("No goroutine leak detected")

    def test_backfill_manager_manifest_restoration_after_restart(self):
        "MB-65354: To ensure backfill manager's manifest cache is populated properly post restart"
        bucket_name = "travel-sample"
        self.src_rest.load_sample(bucket_name)
        self.dest_rest.load_sample(bucket_name)
        self.sleep(10, "Sleeping after loading sample bucket")
        self.dest_rest.delete_collection(bucket_name, "inventory", "airport")
        self.dest_rest.delete_collection(bucket_name, "inventory", "route")
        self.src_rest.add_remote_cluster(self.dest_master.ip, 8091, self.dest_master.rest_username, self.dest_master.rest_password,"C2")
        self.src_rest.start_replication("continuous", bucket_name,"C2", rep_type='xmem', toBucket=bucket_name)
        self.sleep(30+self._checkpoint_interval, f"Waiting for checkpoint to be saved, checkpoint interval: {self._checkpoint_interval}")
        self.src_cluster.pause_all_replications()
        self.src_rest.delete_collection(bucket_name, "inventory", "airport")
        self.src_rest.delete_collection(bucket_name, "inventory", "route")
        shell_conn = RemoteMachineShellConnection(self.src_master)
        shell_conn.kill_goxdcr()
        self.sleep(30, "waiting for goxdcr process to restart")
        errors = self.check_errors_in_goxdcr_logs()
        self.log.info(f"errors in logs: {errors}")
        if len(errors)>=1:
            self.fail("Error in goxdcr")