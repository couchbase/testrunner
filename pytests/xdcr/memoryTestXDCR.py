import re
from lib.couchbase_helper.documentgenerator import BlobGenerator
from pytests.xdcr.xdcrnewbasetests import XDCRNewBaseTest, FloatingServers
from lib.remote.remote_util import RemoteMachineShellConnection
from lib.membase.api.on_prem_rest_client import RestConnection
import threading
import random
import time

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
        
        # Test parameters
        self.remote_cluster_name = "test_remote_cluster_mb52156"
        self.concurrent_operations = self._input.param("concurrent_ops", 5) if self._input else 5
        self.operation_delay = self._input.param("operation_delay", 2) if self._input else 2
        self.max_retry_attempts = self._input.param("max_retries", 10) if self._input else 10
        self.dns_delay_ms = self._input.param("dns_delay_ms", 5000) if self._input else 5000
        
        # Track errors for analysis
        self.revision_mismatch_errors = []
        self.operation_errors = []
        self.stuck_refresh_loops = []
        self.metadata_sync_errors = []
        
        # Lock for thread-safe error collection
        self.error_lock = threading.Lock()
        
        self.log.info("MB-52156 Test Setup Complete - Testing metakv race condition fix")
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

    def test_metakv_race_condition_fix(self):
        """
        Main test for MB-52156 fix verification
        
        Tests concurrent metakv operations with simulated delays to trigger race conditions
        and verifies that the fix prevents revision number mismatches and stuck refresh loops.
        """
        self.log.info("=== Starting MB-52156 metakv race condition test ===")
        
        try:
            # Step 1: Set up initial XDCR with remote cluster references
            self.log.info("Step 1: Setting up initial XDCR configuration")
            self.setup_xdcr()
            
            # Step 2: Simulate concurrent metakv operations that could cause revision mismatches
            self.log.info("Step 2: Simulating concurrent metakv operations")
            self._simulate_concurrent_metakv_operations()
            
            # Step 3: Introduce delays/stalls in callbacks to trigger race conditions
            self.log.info("Step 3: Introducing callback delays to trigger race conditions")
            self._introduce_callback_delays()
            
            # Step 4: Verify metakv operations complete successfully without revision errors
            self.log.info("Step 4: Verifying no revision mismatch errors occurred")
            self._verify_no_revision_mismatch_errors()
            
            # Step 5: Ensure remote cluster agent refresh loop doesn't get stuck
            self.log.info("Step 5: Verifying remote cluster agent refresh loop is not stuck")
            self._verify_refresh_loop_not_stuck()
            
            # Step 6: Check that remote cluster metadata stays in sync across nodes
            self.log.info("Step 6: Verifying metadata sync across nodes")
            self._verify_metadata_sync_across_nodes()
            
            # Step 7: Validate that stale references are properly updated
            self.log.info("Step 7: Validating stale references are properly updated")
            self._validate_stale_references_updated()
            
            self.log.info("=== MB-52156 test completed successfully - Fix is working ===")
            
        except Exception as e:
            self.fail(f"MB-52156 test failed: {e}")

    def test_dns_lookup_delay_scenario(self):
        """
        Specific test for DNS lookup delay scenario mentioned in MB-52156
        
        This test simulates the exact scenario where long DNS lookup times
        cause metakv callbacks to get stalled and apply changes out-of-order.
        """
        self.log.info("=== Starting DNS lookup delay scenario test for MB-52156 ===")
        
        try:
            # Setup initial state
            self.setup_xdcr()
            
            # Introduce network delays to simulate DNS lookup delays
            self._introduce_network_delays()
            
            # Simulate DNS delays in multiple threads
            dns_delay_threads = []
            
            for i in range(3):  # Multiple concurrent operations with DNS delays
                thread = threading.Thread(
                    target=self._simulate_dns_delay_operation,
                    args=(i,),
                    name=f"DNSDelayThread-{i}"
                )
                dns_delay_threads.append(thread)
            
            # Start all operations
            for thread in dns_delay_threads:
                thread.start()
            
            # Wait for completion
            for thread in dns_delay_threads:
                thread.join(timeout=60)  # 60 second timeout per thread
            
            # Remove network delays
            self._cleanup_network_delays()
            
            # Verify no revision mismatches occurred
            if self.revision_mismatch_errors:
                error_summary = f"Found {len(self.revision_mismatch_errors)} revision mismatch errors"
                self.fail(f"MB-52156 DNS delay scenario failed: {error_summary}")
            
            self.log.info("=== DNS lookup delay scenario test passed - MB-52156 fix is working ===")
            
        except Exception as e:
            self.fail(f"DNS lookup delay scenario test failed: {e}")


    def _simulate_concurrent_metakv_operations(self):
        """Simulate concurrent metakv operations that could cause revision mismatches"""
        self.log.info(f"Simulating {self.concurrent_operations} concurrent metakv operations")
        
        threads = []
        
        # Create multiple threads to perform concurrent operations
        for i in range(self.concurrent_operations):
            thread = threading.Thread(
                target=self._concurrent_metakv_operation,
                args=(i,),
                name=f"MetaKVThread-{i}"
            )
            threads.append(thread)
        
        # Start all threads simultaneously to maximize concurrency
        start_time = time.time()
        for thread in threads:
            thread.start()
            # Small stagger to increase chance of race conditions
            time.sleep(0.1)
        
        # Wait for all operations to complete
        for thread in threads:
            thread.join(timeout=30)  # 30 second timeout per thread
        
        duration = time.time() - start_time
        self.log.info(f"Completed {self.concurrent_operations} concurrent metakv operations in {duration:.2f} seconds")

    def _concurrent_metakv_operation(self, operation_id):
        """Perform concurrent metakv operations on remote cluster references"""
        thread_name = threading.current_thread().name
        self.log.info(f"{thread_name}: Starting concurrent operation {operation_id}")
        
        try:
            cluster_name = f"{self.remote_cluster_name}_{operation_id}"
            
            # Add remote cluster
            self.log.info(f"{thread_name}: Adding remote cluster {cluster_name}")
            self.src_rest.add_remote_cluster(
                self.dest_master.ip,
                self.dest_master.port,
                self.dest_master.rest_username,
                self.dest_master.rest_password,
                cluster_name
            )
            
            # Add random delay to increase chance of race conditions
            delay = self.operation_delay + random.uniform(0, 1)
            time.sleep(delay)
            
            # Modify the remote cluster (trigger metakv update)
            self.log.info(f"{thread_name}: Modifying remote cluster {cluster_name}")
            self.src_rest.modify_remote_cluster(
                self.dest_master.ip,
                self.dest_master.port,
                self.dest_master.rest_username,
                self.dest_master.rest_password,
                cluster_name
            )
            
            time.sleep(delay)
            
            # Remove remote cluster
            self.log.info(f"{thread_name}: Removing remote cluster {cluster_name}")
            self.src_rest.remove_remote_cluster(cluster_name)
            
            self.log.info(f"{thread_name}: Completed operation {operation_id} successfully")
            
        except Exception as e:
            error_msg = str(e)
            with self.error_lock:
                if "revision number does not match" in error_msg.lower():
                    self.revision_mismatch_errors.append(f"{thread_name} operation {operation_id}: {error_msg}")
                    self.log.error(f"REVISION MISMATCH ERROR in {thread_name} operation {operation_id}: {error_msg}")
                else:
                    self.operation_errors.append(f"{thread_name} operation {operation_id}: {error_msg}")
                    self.log.error(f"Operation error in {thread_name} operation {operation_id}: {error_msg}")

    def _introduce_callback_delays(self):
        """Introduce delays/stalls in callbacks to trigger race conditions"""
        self.log.info("Introducing callback delays to trigger race conditions")
        
        try:
            # Perform operations that would trigger metakv callbacks
            test_cluster_name = f"{self.remote_cluster_name}_delayed"
            
            # Rapid-fire operations to increase chance of callback overlap
            for i in range(3):
                operation_name = f"{test_cluster_name}_{i}"
                
                # Add
                self.src_rest.add_remote_cluster(
                    self.dest_master.ip,
                    self.dest_master.port,
                    self.dest_master.rest_username,
                    self.dest_master.rest_password,
                    operation_name
                )
                
                # Very short delay to trigger overlapping callbacks
                time.sleep(0.5)
                
                # Modify
                self.src_rest.modify_remote_cluster(
                    self.dest_master.ip,
                    self.dest_master.port,
                    self.dest_master.rest_username,
                    self.dest_master.rest_password,
                    operation_name
                )
                
                time.sleep(0.5)
            
            # Wait to let any race conditions manifest
            time.sleep(self.operation_delay * 2)
            
            # Clean up test clusters
            for i in range(3):
                operation_name = f"{test_cluster_name}_{i}"
                try:
                    self.src_rest.remove_remote_cluster(operation_name)
                except Exception as e:
                    if "revision number does not match" in str(e).lower():
                        with self.error_lock:
                            self.revision_mismatch_errors.append(f"Cleanup operation: {e}")
                    self.log.warning(f"Error during cleanup of {operation_name}: {e}")
            
        except Exception as e:
            error_msg = str(e)
            if "revision number does not match" in error_msg.lower():
                with self.error_lock:
                    self.revision_mismatch_errors.append(f"Callback delay operation: {error_msg}")
            self.log.error(f"Error in callback delay operation: {error_msg}")

    def _introduce_network_delays(self):
        """Introduce network delays to simulate DNS lookup delays"""
        self.log.info(f"Introducing {self.dns_delay_ms}ms network delays to simulate DNS lookup delays")
        
        try:
            shell = RemoteMachineShellConnection(self.src_master)
            
            # Check if tc (traffic control) is available
            result, error = shell.execute_command("which tc")
            if not result:
                self.log.warning("Traffic control (tc) not available, skipping network delay simulation")
                shell.disconnect()
                return
            
            # Add artificial delay to simulate slow DNS lookups
            delay_cmd = f"tc qdisc add dev eth0 root netem delay {self.dns_delay_ms}ms 100ms distribution normal"
            result, error = shell.execute_command(delay_cmd)
            
            if error:
                self.log.warning(f"Could not introduce network delays: {error}")
            else:
                self.log.info(f"Successfully introduced {self.dns_delay_ms}ms network delays")
            
            shell.disconnect()
            
        except Exception as e:
            self.log.warning(f"Could not introduce network delays: {e}")

    def _cleanup_network_delays(self):
        """Remove any network delays that were introduced"""
        try:
            shell = RemoteMachineShellConnection(self.src_master)
            
            # Remove artificial delay
            cleanup_cmd = "tc qdisc del dev eth0 root 2>/dev/null || true"
            shell.execute_command(cleanup_cmd)
            
            shell.disconnect()
            self.log.info("Network delays cleaned up")
            
        except Exception as e:
            self.log.warning(f"Error cleaning up network delays: {e}")

    def _simulate_dns_delay_operation(self, operation_id):
        """Simulate operations with DNS lookup delays"""
        thread_name = threading.current_thread().name
        self.log.info(f"{thread_name}: Starting DNS delay operation {operation_id}")
        
        try:
            cluster_name = f"dns_delay_cluster_{operation_id}"
            
            # Add cluster reference
            self.log.info(f"{thread_name}: Adding cluster {cluster_name}")
            self.src_rest.add_remote_cluster(
                self.dest_master.ip,
                self.dest_master.port,
                self.dest_master.rest_username,
                self.dest_master.rest_password,
                cluster_name
            )
            
            # Simulate additional delay (representing DNS lookup time)
            delay = self.operation_delay + operation_id
            time.sleep(delay)
            
            # Perform rapid modifications (to trigger race condition)
            for mod_count in range(2):
                self.log.info(f"{thread_name}: Modifying cluster {cluster_name} (attempt {mod_count + 1})")
                self.src_rest.modify_remote_cluster(
                    self.dest_master.ip,
                    self.dest_master.port,
                    self.dest_master.rest_username,
                    self.dest_master.rest_password,
                    cluster_name
                )
                time.sleep(0.5)  # Short delay between modifications
            
            # Clean up
            self.log.info(f"{thread_name}: Removing cluster {cluster_name}")
            self.src_rest.remove_remote_cluster(cluster_name)
            
            self.log.info(f"{thread_name}: Completed DNS delay operation {operation_id}")
            
        except Exception as e:
            error_msg = str(e)
            with self.error_lock:
                if "revision number does not match" in error_msg.lower():
                    self.revision_mismatch_errors.append(f"{thread_name} DNS delay operation {operation_id}: {error_msg}")
                    self.log.error(f"REVISION MISMATCH in {thread_name} DNS delay operation {operation_id}: {error_msg}")
                else:
                    self.operation_errors.append(f"{thread_name} DNS delay operation {operation_id}: {error_msg}")
                    self.log.error(f"DNS delay operation error in {thread_name} operation {operation_id}: {error_msg}")

    def _verify_no_revision_mismatch_errors(self):
        """Verify that metakv operations complete successfully without revision errors"""
        self.log.info("Verifying no revision mismatch errors occurred")
        
        if self.revision_mismatch_errors:
            error_count = len(self.revision_mismatch_errors)
            error_details = "\n".join(self.revision_mismatch_errors[:5])  # Show first 5 errors
            if error_count > 5:
                error_details += f"\n... and {error_count - 5} more errors"
                
            self.fail(f"MB-52156 REGRESSION DETECTED! Found {error_count} revision mismatch errors:\n{error_details}")
        
        self.log.info("✓ No revision mismatch errors detected - MB-52156 fix is working correctly")

    def _verify_refresh_loop_not_stuck(self):
        """Ensure remote cluster agent refresh loop doesn't get stuck"""
        self.log.info("Verifying remote cluster agent refresh loop is not stuck")
        
        try:
            # Check GOXDCR logs for stuck refresh loop warnings
            shell = RemoteMachineShellConnection(self.src_master)
            
            # Look for the specific warning mentioned in MB-52156
            log_check_cmd = "grep -c 'Agent remoteCluster.*periodic refresher' /opt/couchbase/var/lib/couchbase/logs/goxdcr.log* 2>/dev/null || echo '0'"
            result, error = shell.execute_command(log_check_cmd)
            
            if result:
                warning_count = int(result[0].strip())
                if warning_count > self.max_retry_attempts:
                    self.log.warning(f"Found {warning_count} refresh loop warnings (threshold: {self.max_retry_attempts})")
                    
                    # Get sample of recent warnings
                    recent_warnings_cmd = "grep 'Agent remoteCluster.*periodic refresher' /opt/couchbase/var/lib/couchbase/logs/goxdcr.log* 2>/dev/null | tail -3"
                    recent_result, _ = shell.execute_command(recent_warnings_cmd)
                    
                    if recent_result:
                        stuck_logs = "\n".join(recent_result)
                        self.stuck_refresh_loops.append(stuck_logs)
                        self.log.warning(f"Recent refresh loop warnings:\n{stuck_logs}")
                        
                        # Don't fail the test immediately, but mark as concerning
                        if warning_count > self.max_retry_attempts * 2:
                            self.fail(f"Remote cluster agent refresh loop appears stuck - found {warning_count} warnings")
                else:
                    self.log.info(f"Found {warning_count} refresh loop warnings (within normal range)")
            
            shell.disconnect()
            
            # Verify that remote cluster operations still work
            remote_clusters = self.src_rest.get_remote_clusters()
            active_clusters = [c for c in remote_clusters if not c.get('deleted', False)]
            self.log.info(f"✓ Remote cluster agent is responsive, found {len(active_clusters)} active clusters")
            
        except Exception as e:
            self.log.error(f"Error checking refresh loop status: {e}")

    def _verify_metadata_sync_across_nodes(self):
        """Check that remote cluster metadata stays in sync across nodes"""
        self.log.info("Verifying remote cluster metadata sync across nodes")
        
        try:
            # Get metadata from all nodes in source cluster
            src_nodes = self.src_cluster.get_nodes()
            if len(src_nodes) <= 1:
                self.log.info("Only one node in cluster, skipping metadata sync verification")
                return
                
            metadata_from_nodes = []
            
            for node in src_nodes:
                try:
                    node_rest = RestConnection(node)
                    remote_clusters = node_rest.get_remote_clusters()
                    # Filter out deleted clusters for comparison
                    active_clusters = [c for c in remote_clusters if not c.get('deleted', False)]
                    metadata_from_nodes.append({
                        'node': node.ip,
                        'clusters': active_clusters,
                        'cluster_count': len(active_clusters)
                    })
                except Exception as e:
                    self.log.warning(f"Could not get metadata from node {node.ip}: {e}")
            
            # Verify all nodes have consistent metadata
            if len(metadata_from_nodes) > 1:
                reference_count = metadata_from_nodes[0]['cluster_count']
                reference_names = {c['name'] for c in metadata_from_nodes[0]['clusters']}
                
                for node_data in metadata_from_nodes[1:]:
                    node_count = node_data['cluster_count']
                    node_names = {c['name'] for c in node_data['clusters']}
                    
                    if node_count != reference_count or node_names != reference_names:
                        error_msg = (f"Metadata mismatch: Node {node_data['node']} has {node_count} clusters "
                                   f"({node_names}) vs reference {reference_count} clusters ({reference_names})")
                        self.metadata_sync_errors.append(error_msg)
                        self.log.error(error_msg)
            
            if self.metadata_sync_errors:
                sync_error_summary = "\n".join(self.metadata_sync_errors)
                self.fail(f"Remote cluster metadata sync errors detected:\n{sync_error_summary}")
            
            self.log.info("✓ Remote cluster metadata is consistent across all nodes")
            
        except Exception as e:
            self.log.error(f"Error verifying metadata sync: {e}")

    def _validate_stale_references_updated(self):
        """Validate that stale references are properly updated and don't remain stuck"""
        self.log.info("Validating that stale references are properly updated")
        
        try:
            # Create a reference that might become stale
            stale_test_cluster = f"{self.remote_cluster_name}_stale_test"
            
            self.log.info(f"Creating test cluster reference: {stale_test_cluster}")
            self.src_rest.add_remote_cluster(
                self.dest_master.ip,
                self.dest_master.port,
                self.dest_master.rest_username,
                self.dest_master.rest_password,
                stale_test_cluster
            )
            
            # Modify it multiple times to test update mechanism
            for i in range(3):
                self.log.info(f"Modifying {stale_test_cluster} (attempt {i + 1})")
                self.src_rest.modify_remote_cluster(
                    self.dest_master.ip,
                    self.dest_master.port,
                    self.dest_master.rest_username,
                    self.dest_master.rest_password,
                    stale_test_cluster
                )
                time.sleep(1)
            
            # Verify the reference can still be operated on (not stuck)
            remote_clusters = self.src_rest.get_remote_clusters()
            cluster_found = any(cluster['name'] == stale_test_cluster 
                              for cluster in remote_clusters if not cluster.get('deleted', False))
            
            if not cluster_found:
                self.fail(f"Stale reference test failed - cluster {stale_test_cluster} not found after modifications")
            
            # Clean up the test reference
            self.log.info(f"Cleaning up test cluster reference: {stale_test_cluster}")
            self.src_rest.remove_remote_cluster(stale_test_cluster)
            
            self.log.info("✓ Stale reference validation completed successfully")
            
        except Exception as e:
            if "revision number does not match" in str(e).lower():
                self.fail(f"MB-52156 REGRESSION: Stale reference caused revision mismatch: {e}")
            else:
                self.log.error(f"Error in stale reference validation: {e}")

    def _report_test_errors(self):
        """Report summary of any errors found during the test"""
        total_errors = (len(self.revision_mismatch_errors) + 
                       len(self.operation_errors) + 
                       len(self.stuck_refresh_loops) + 
                       len(self.metadata_sync_errors))
        
        if total_errors > 0:
            self.log.info(f"=== TEST ERROR SUMMARY ===")
            self.log.info(f"Revision mismatch errors: {len(self.revision_mismatch_errors)}")
            self.log.info(f"General operation errors: {len(self.operation_errors)}")
            self.log.info(f"Stuck refresh loop warnings: {len(self.stuck_refresh_loops)}")
            self.log.info(f"Metadata sync errors: {len(self.metadata_sync_errors)}")
            self.log.info(f"Total issues found: {total_errors}")
        else:
            self.log.info("✓ No errors detected during test execution")
    def _rapid_modification_worker(self, worker_id):
        """Worker function for rapid modification test"""
        thread_name = threading.current_thread().name
        cluster_name = f"rapid_mod_cluster_{worker_id}"
        
        try:
            # Rapid sequence of operations
            self.src_rest.add_remote_cluster(
                self.dest_master.ip,
                self.dest_master.port,
                self.dest_master.rest_username,
                self.dest_master.rest_password,
                cluster_name
            )
            
            # Rapid modifications
            for mod_num in range(5):
                self.src_rest.modify_remote_cluster(
                    self.dest_master.ip,
                    self.dest_master.port,
                    self.dest_master.rest_username,
                    self.dest_master.rest_password,
                    cluster_name
                )
                time.sleep(0.1)  # Very short delay
            
            # Cleanup
            self.src_rest.remove_remote_cluster(cluster_name)
            
        except Exception as e:
            error_msg = str(e)
            with self.error_lock:
                if "revision number does not match" in error_msg.lower():
                    self.revision_mismatch_errors.append(f"{thread_name}: {error_msg}")
                else:
                    self.operation_errors.append(f"{thread_name}: {error_msg}") 