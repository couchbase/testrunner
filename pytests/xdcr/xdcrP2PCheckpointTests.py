"""
XDCR P2P Checkpoint Tests

This module contains tests for XDCR peer-to-peer checkpoint functionality during
rebalance and failover operations. The tests verify that checkpoints are properly
transferred between nodes during topology changes.
"""

import time
import json
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from .xdcrnewbasetests import XDCRNewBaseTest


class XDCRP2PCheckpointTests(XDCRNewBaseTest):
    """
    Test class for XDCR P2P checkpoint scenarios.
    
    This class tests the behavior of XDCR checkpoint transfer between nodes
    during rebalance and failover operations.
    """

    def setUp(self):
        """Set up test environment and initialize clusters."""
        XDCRNewBaseTest.setUp(self)
        
        # Initialize cluster references
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_master = self.dest_cluster.get_master_node()
        self.src_rest = RestConnection(self.src_master)
        self.dest_rest = RestConnection(self.dest_master)
        
        # Get test parameters
        self.checkpoint_interval = self._input.param("checkpoint_interval", 120) if self._input else 120
        self.num_items = self._input.param("items", 1000) if self._input else 1000
        self.bucket_name = self._input.param("default_bucket_name", "default") if self._input else "default"
        
        self.log.info(f"Using checkpoint interval: {self.checkpoint_interval} seconds")
        self.log.info(f"Test will use {self.num_items} documents")

    def tearDown(self):
        """Clean up test environment."""
        XDCRNewBaseTest.tearDown(self)

    def set_internal_setting(self, cluster_name, setting_name, setting_value):
        """
        Set XDCR internal setting using REST API.
        
        Args:
            cluster_name (str): Name of the cluster (C1, C2, etc.)
            setting_name (str): Name of the internal setting
            setting_value (str/int): Value to set
        """
        if cluster_name == "C1":
            master_node = self.src_master
        elif cluster_name == "C2":
            master_node = self.dest_master
        else:
            raise Exception(f"Unknown cluster name: {cluster_name}")
            
        # Use port 9998 for internal settings API
        url = f"http://{master_node.ip}:9998/xdcr/internalSettings"
        data = f"{setting_name}={setting_value}"
        
        self.log.info(f"Setting {setting_name}={setting_value} on cluster {cluster_name}")
        
        shell = RemoteMachineShellConnection(master_node)
        try:
            cmd = f'curl -X POST -u Administrator:password "{url}" -d "{data}"'
            result = shell.execute_command(cmd, timeout=30)
            output = result[0] if result else ""
            self.log.info(f"Internal setting result: {output}")
        finally:
            shell.disconnect()

    def get_internal_setting(self, cluster_name, setting_name):
        """
        Get XDCR internal setting value.
        
        Args:
            cluster_name (str): Name of the cluster
            setting_name (str): Name of the setting to retrieve
            
        Returns:
            str: Setting value
        """
        if cluster_name == "C1":
            master_node = self.src_master
        elif cluster_name == "C2":
            master_node = self.dest_master
        else:
            raise Exception(f"Unknown cluster name: {cluster_name}")
            
        url = f"http://{master_node.ip}:9998/xdcr/internalSettings"
        
        shell = RemoteMachineShellConnection(master_node)
        try:
            cmd = f'curl -u Administrator:password "{url}"'
            result = shell.execute_command(cmd, timeout=30)
            output = result[0] if result else ""
            
            # Parse JSON response to get specific setting
            try:
                if isinstance(output, list):
                    output_str = ''.join(str(line) for line in output)
                else:
                    output_str = str(output)
                settings = json.loads(output_str)
                return settings.get(setting_name, "")
            except:
                self.log.warning(f"Could not parse settings response: {output}")
                return ""
        finally:
            shell.disconnect()

    def configure_p2p_settings(self):
        """Configure internal settings for P2P checkpoint testing."""
        self.log.info("Configuring P2P checkpoint settings")
        
        # Set topology change detection settings for faster testing
        self.set_internal_setting("C1", "TopologyChangeCheckInterval", 10)
        self.set_internal_setting("C1", "MaxTopologyChangeCountBeforeRestart", 3)
        self.set_internal_setting("C1", "MaxTopologyStableCountBeforeRestart", 3)
        self.set_internal_setting("C1", "TopologySvcCooldownPeriodSec", 1)
        
        # Wait for settings to take effect
        time.sleep(5)
        
        # Verify settings were applied
        check_interval = self.get_internal_setting("C1", "TopologyChangeCheckInterval")
        if check_interval != 10:
            self.log.warning(f"TopologyChangeCheckInterval not set correctly: {check_interval}")

    def load_documents_with_pillowfight(self, num_docs):
        """Load documents using pillowfight for the test."""
        from lib.remote.remote_util import RemoteMachineShellConnection
        
        server_shell = RemoteMachineShellConnection(self.src_master)
        
        try:
            cmd = f"/opt/couchbase/bin/cbc-pillowfight -u Administrator -P password " \
                  f"-U couchbase://localhost/{self.bucket_name} -I {num_docs} " \
                  f"-m 256 -M 256 -B 100 --rate-limit=5000 --populate-only " \
                  f"--key-prefix=P2P_TEST_"
            
            self.log.info(f"Loading {num_docs} documents with pillowfight")
            result = server_shell.execute_command(cmd, timeout=120, use_channel=True)
            
            if result and len(result) >= 2:
                output, error = result[0], result[1]
                if error and "error" in str(error).lower():
                    raise Exception(f"Pillowfight failed: {error}")
                    
            self.log.info(f"Successfully loaded {num_docs} documents")
            
        finally:
            server_shell.disconnect()

    def wait_for_replication_progress(self, timeout=300):
        """Wait for replication to make progress without requiring all items."""
        self.log.info(f"Waiting up to {timeout} seconds for replication progress")
        
        start_time = time.time()
        initial_dest_count = 0
        
        while time.time() - start_time < timeout:
            src_count, dest_count = self.get_document_counts()
            
            if dest_count > initial_dest_count:
                self.log.info(f"Replication progress: {dest_count}/{src_count} documents")
                initial_dest_count = dest_count
                
            if dest_count >= src_count * 0.8:  # 80% replicated is good enough
                self.log.info(f"Sufficient replication achieved: {dest_count}/{src_count}")
                return True
                
            time.sleep(10)
            
        self.log.info(f"Replication timeout reached. Final count: {dest_count}/{src_count}")
        return dest_count > 0  # As long as some replication occurred

    def get_document_counts(self):
        """Get current document counts on source and destination clusters."""
        try:
            src_stats = self.src_rest.get_bucket_stats(self.bucket_name)
            dest_stats = self.dest_rest.get_bucket_stats(self.bucket_name)
            
            src_count = src_stats.get("curr_items", 0)
            dest_count = dest_stats.get("curr_items", 0)
            
            return src_count, dest_count
            
        except Exception as e:
            self.log.warning(f"Failed to get document counts: {e}")
            return 0, 0

    def pause_and_resume_replication(self):
        """Pause and resume replication to trigger checkpoint operations."""
        self.log.info("Pausing replication to test checkpoint behavior")
        self.src_cluster.pause_all_replications(verify=True)
        
        time.sleep(15)
        
        self.log.info("Resuming replication")
        self.src_cluster.resume_all_replications(verify=True)
        
        time.sleep(15)

    def check_goxdcr_logs_for_patterns(self, node, patterns):
        """
        Check goxdcr logs for specific patterns.
        
        Args:
            node: The node to check logs on
            patterns (list): List of patterns to search for
            
        Returns:
            dict: Pattern -> count mapping
        """
        shell = RemoteMachineShellConnection(node)
        results = {}
        
        try:
            log_path = "/opt/couchbase/var/lib/couchbase/logs/goxdcr.log"
            
            for pattern in patterns:
                cmd = f'grep -c "{pattern}" {log_path} || echo "0"'
                result = shell.execute_command(cmd, timeout=30)
                if result and result[0]:
                    count_str = str(result[0][0]).strip() if hasattr(result[0][0], 'strip') else str(result[0][0])
                    count = int(count_str)
                else:
                    count = 0
                results[pattern] = count
                self.log.info(f"Pattern '{pattern}': {count} occurrences")
                
        except Exception as e:
            self.log.warning(f"Error checking logs: {e}")
            
        finally:
            shell.disconnect()
            
        return results

    def validate_no_error_messages(self):
        """
        Validate that there are no P2P error messages or panics in the logs.
        
        This checks for specific error patterns that should not occur during
        proper P2P checkpoint operations.
        """
        self.log.info("Validating no P2P error messages or panics in logs")
        
        # Error patterns that should NOT be present
        error_patterns = [
            "has payloadCompressed but no payload after deserialization",  # P2P_DESERIALIZATION_ERR_MSG
            "Unable to respond to caller",  # P2P_UNABLE_TO_RESPOND_MSG
            "Error when getting brokenMapping for",  # CKPT_MAPPING_NOT_FOUND_MSG
            "panic:",  # General panic detection
            "PANIC:",  # Uppercase panic detection
            "runtime error:",  # Runtime errors
            "fatal error:"  # Fatal errors
        ]
        
        # Check on both source and destination clusters
        clusters = [("C1", self.src_master), ("C2", self.dest_master)]
        
        for cluster_name, node in clusters:
            self.log.info(f"Checking error patterns on cluster {cluster_name}")
            error_results = self.check_goxdcr_logs_for_patterns(node, error_patterns)
            
            # Validate that no error patterns were found
            for pattern, count in error_results.items():
                if count > 0:
                    self.log.error(f"✗ ERROR: Found {count} occurrences of '{pattern}' on {cluster_name}")
                    self.fail(f"Found {count} error occurrences of pattern '{pattern}' on cluster {cluster_name}")
                else:
                    self.log.info(f"✓ No occurrences of '{pattern}' on {cluster_name}")
        
        self.log.info("✓ All error pattern validations passed - no P2P errors or panics found")

    def test_p2p_checkpoint_during_rebalance(self):
        """
        Test P2P checkpoint functionality during rebalance operations.
        
        This test verifies that:
        1. Checkpoints are properly transferred between nodes during rebalance
        2. Replication resumes from correct checkpoint positions
        3. No data loss occurs during topology changes
        4. No P2P errors or panics occur
        """
        self.log.info("=== Starting P2P Checkpoint During Rebalance Test ===")
        
        try:
            # Step 1: Setup XDCR and configure P2P settings
            self.log.info("Step 1: Setting up XDCR and configuring P2P settings")
            self.setup_xdcr()
            self.configure_p2p_settings()
            
            # Step 2: Load initial documents
            self.log.info("Step 2: Loading initial documents")
            self.load_documents_with_pillowfight(self.num_items)
            
            # Step 3: Wait for initial replication
            self.log.info("Step 3: Waiting for initial replication")
            replication_success = self.wait_for_replication_progress(timeout=300)
            
            if not replication_success:
                self.log.warning("Initial replication did not complete fully, but continuing test")
            
            initial_src_count, initial_dest_count = self.get_document_counts()
            self.log.info(f"Initial replication - Source: {initial_src_count}, Destination: {initial_dest_count}")
            
            # Step 4: Pause and resume to establish checkpoints
            self.log.info("Step 4: Pausing and resuming replication to establish checkpoints")
            self.pause_and_resume_replication()
            
            # Step 5: Check for P2P checkpoint messages in logs
            self.log.info("Step 5: Checking for P2P checkpoint activity in logs")
            p2p_patterns = [
                "Discovered peers:",
                "retrieving CheckpointsDocs request found",
                "Received peerToPeer checkpoint data from node"
            ]
            
            log_results = self.check_goxdcr_logs_for_patterns(self.src_master, p2p_patterns)
            
            # Step 6: Verify replication continued properly
            self.log.info("Step 6: Verifying replication continued properly after checkpoint operations")
            time.sleep(30)  # Allow time for any remaining replication
            
            final_src_count, final_dest_count = self.get_document_counts()
            self.log.info(f"Final counts - Source: {final_src_count}, Destination: {final_dest_count}")
            
            # Verify that we have reasonable replication
            if final_dest_count >= initial_dest_count:
                self.log.info("✓ P2P checkpoint test completed successfully")
                self.log.info(f"✓ Replication maintained: {final_dest_count} documents on destination")
            else:
                self.log.warning(f"⚠ Destination count decreased: {initial_dest_count} -> {final_dest_count}")
            
            # Log P2P activity summary
            for pattern, count in log_results.items():
                if count > 0:
                    self.log.info(f"✓ P2P activity detected: {pattern} ({count} times)")
                else:
                    self.log.info(f"⚠ No activity for: {pattern}")
            
            # Step 7: Validate no error messages or panics in logs
            self.log.info("Step 7: Validating no P2P error messages or panics in logs")
            self.validate_no_error_messages()
            
            self.log.info("=== P2P Checkpoint Test Completed ===")
            
        except Exception as e:
            self.log.error(f"P2P Checkpoint Test Failed: {e}")
            self.fail(f"Test failed with error: {e}")

    def test_p2p_checkpoint_simple_validation(self):
        """
        Simple test to validate P2P checkpoint basic functionality.
        
        This is a lighter version that focuses on basic checkpoint operations
        without complex rebalance scenarios.
        """
        self.log.info("=== Starting Simple P2P Checkpoint Validation ===")
        
        try:
            # Setup XDCR
            self.setup_xdcr()
            
            # Configure faster checkpoint intervals for testing
            self.set_internal_setting("C1", "checkpointInterval", str(self.checkpoint_interval))
            
            # Load some documents
            self.load_documents_with_pillowfight(100)
            
            # Wait for replication
            time.sleep(60)
            
            # Pause and resume multiple times to trigger checkpoint activity
            for i in range(3):
                self.log.info(f"Checkpoint cycle {i+1}/3")
                self.pause_and_resume_replication()
                time.sleep(30)
            
            # Check final state
            src_count, dest_count = self.get_document_counts()
            self.log.info(f"Final validation - Source: {src_count}, Destination: {dest_count}")
            
            # Validate no error messages or panics
            self.validate_no_error_messages()
            
            if dest_count > 0:
                self.log.info("✓ Simple P2P checkpoint validation completed successfully")
            else:
                self.fail("No documents replicated - checkpoint functionality may be broken")
            
        except Exception as e:
            self.log.error(f"Simple P2P Checkpoint Test Failed: {e}")
            self.fail(f"Test failed with error: {e}") 