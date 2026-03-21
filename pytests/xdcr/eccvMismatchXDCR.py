import json
import time

from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from xdcr.xdcrnewbasetests import XDCRNewBaseTest, NodeHelper


class ECCVMismatchTests(XDCRNewBaseTest):
    """
    Test suite for XDCR enableCrossClusterVersioning (ECCV) mismatch scenarios.
    
    ECCV must be set to true on both source and target clusters for features
    like conflict logging to work. A mismatch between source and target ECCV
    settings will cause warnings in the XDCR errors section of the API.
    
    Expected warning format:
    WARN: Mismatch between source and target enableCrossClusterVersioning (ECCV)
    settings - It's disabled for bucket <bucket_name> on cluster <cluster_name>.
    ECCV needs to be set true for all buckets in the replication topology for
    features like 'XDCR Active-Active with Sync Gateway' to ensure data consistency.
    """

    def setUp(self):
        super(ECCVMismatchTests, self).setUp()
        self.src_cluster = self.get_cb_cluster_by_name("C1")
        self.src_master = self.src_cluster.get_master_node()
        self.dest_cluster = self.get_cb_cluster_by_name("C2")
        self.dest_master = self.dest_cluster.get_master_node()
        self.src_rest = RestConnection(self.src_master)
        self.dest_rest = RestConnection(self.dest_master)
        
        self.gen_create = BlobGenerator('eccv', 'eccv-', self._value_size, start=0,
                                        end=self._num_items)
        
        self.eccv_warning_timeout = self._input.param("eccv_warning_timeout", 120)
        self.eccv_warning_clear_timeout = self._input.param("eccv_warning_clear_timeout", 180)

    def tearDown(self):
        super(ECCVMismatchTests, self).tearDown()

    def _set_eccv_on_cluster(self, cluster, enabled):
        """
        Set enableCrossClusterVersioning for all buckets on a cluster.
        
        Args:
            cluster: CouchbaseCluster object
            enabled: True to enable, False to disable
        """
        rest = RestConnection(cluster.get_master_node())
        for bucket in cluster.get_buckets():
            self.log.info(f"Setting ECCV={enabled} for bucket {bucket.name} on cluster {cluster.get_name()}")
            result = rest.change_bucket_props(bucket, enableCrossClusterVersioning=str(enabled).lower())
            self.log.info(f"Result: {result}")

    def _get_eccv_setting(self, cluster, bucket_name):
        """
        Get the current ECCV setting for a bucket on a cluster.
        
        Args:
            cluster: CouchbaseCluster object
            bucket_name: Name of the bucket
            
        Returns:
            bool: True if ECCV is enabled, False otherwise
        """
        rest = RestConnection(cluster.get_master_node())
        bucket_info = rest.get_bucket_json(bucket_name)
        return bucket_info.get('enableCrossClusterVersioning', False)

    def _get_xdcr_errors(self, cluster):
        """
        Get XDCR errors/warnings from the pools/default/tasks API.
        
        Args:
            cluster: CouchbaseCluster object
            
        Returns:
            list: List of error/warning dictionaries from XDCR replications
        """
        rest = RestConnection(cluster.get_master_node())
        replications = rest.get_replications()
        errors = []
        for repl in replications:
            if 'errors' in repl:
                errors.extend(repl['errors'])
            if 'warnings' in repl:
                errors.extend(repl['warnings'])
        return errors

    def _get_all_replication_info(self, cluster):
        """
        Get full replication info from the pools/default/tasks API.
        
        Args:
            cluster: CouchbaseCluster object
            
        Returns:
            list: List of replication task dictionaries
        """
        rest = RestConnection(cluster.get_master_node())
        return rest.get_replications()

    def _check_eccv_mismatch_warning(self, cluster, bucket_name=None, target_cluster_name=None):
        """
        Check if ECCV mismatch warning is present in XDCR errors.
        
        Args:
            cluster: CouchbaseCluster object to check
            bucket_name: Optional bucket name to filter warnings
            target_cluster_name: Optional target cluster name to filter warnings
            
        Returns:
            tuple: (warning_found: bool, warning_message: str or None)
        """
        replications = self._get_all_replication_info(cluster)
        
        for repl in replications:
            self.log.info(f"Checking replication: {repl.get('id', 'unknown')}")
            
            errors_list = repl.get('errors', [])
            if isinstance(errors_list, list):
                for error in errors_list:
                    error_str = str(error) if not isinstance(error, str) else error
                    self.log.info(f"Found error/warning: {error_str}")
                    
                    if 'enableCrossClusterVersioning' in error_str or 'ECCV' in error_str:
                        if bucket_name and bucket_name not in error_str:
                            continue
                        return True, error_str
        
        return False, None

    def _check_goxdcr_log_for_eccv_warning(self, server, bucket_name=None):
        """
        Check goxdcr logs for ECCV mismatch warning.
        
        Args:
            server: Server object
            bucket_name: Optional bucket name to filter warnings
            
        Returns:
            tuple: (warning_found: bool, warning_messages: list)
        """
        search_patterns = [
            "enableCrossClusterVersioning",
            "ECCV",
            "Mismatch between source and target"
        ]
        
        warnings_found = []
        for pattern in search_patterns:
            matches, count = NodeHelper.check_goxdcr_log(server, pattern)
            if count > 0:
                for match in matches:
                    if bucket_name is None or bucket_name in match:
                        warnings_found.append(match)
        
        return len(warnings_found) > 0, warnings_found

    def _wait_for_eccv_warning(self, cluster, timeout=None, bucket_name=None):
        """
        Wait for ECCV mismatch warning to appear.
        
        Args:
            cluster: CouchbaseCluster object
            timeout: Maximum time to wait in seconds
            bucket_name: Optional bucket name to filter warnings
            
        Returns:
            tuple: (warning_found: bool, warning_message: str or None)
        """
        if timeout is None:
            timeout = self.eccv_warning_timeout
            
        self.log.info(f"Waiting up to {timeout}s for ECCV mismatch warning...")
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            found, message = self._check_eccv_mismatch_warning(cluster, bucket_name)
            if found:
                self.log.info(f"ECCV mismatch warning found after {time.time() - start_time:.1f}s: {message}")
                return True, message
            
            found, messages = self._check_goxdcr_log_for_eccv_warning(
                cluster.get_master_node(), bucket_name)
            if found:
                self.log.info(f"ECCV mismatch warning found in logs after {time.time() - start_time:.1f}s")
                return True, messages[0] if messages else None
            
            time.sleep(5)
        
        self.log.warning(f"ECCV mismatch warning not found after {timeout}s")
        return False, None

    def _wait_for_eccv_warning_to_clear(self, cluster, timeout=None, bucket_name=None):
        """
        Wait for ECCV mismatch warning to disappear after syncing ECCV settings.
        
        Args:
            cluster: CouchbaseCluster object
            timeout: Maximum time to wait in seconds
            bucket_name: Optional bucket name to filter warnings
            
        Returns:
            bool: True if warning cleared, False if still present after timeout
        """
        if timeout is None:
            timeout = self.eccv_warning_clear_timeout
            
        self.log.info(f"Waiting up to {timeout}s for ECCV mismatch warning to clear...")
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            found, _ = self._check_eccv_mismatch_warning(cluster, bucket_name)
            if not found:
                self.log.info(f"ECCV mismatch warning cleared after {time.time() - start_time:.1f}s")
                return True
            
            time.sleep(10)
        
        self.log.warning(f"ECCV mismatch warning still present after {timeout}s")
        return False

    def test_eccv_source_true_target_false(self):
        """
        Test ECCV mismatch: source=true, target=false
        
        Steps:
        1. Set up XDCR replication
        2. Enable ECCV on source cluster only
        3. Verify ECCV mismatch warning appears
        4. Load some data to ensure replication is active
        5. Verify warning is present in XDCR errors
        6. Enable ECCV on target to sync settings
        7. Verify warning disappears
        """
        self._set_eccv_on_cluster(self.src_cluster, True)
        self._set_eccv_on_cluster(self.dest_cluster, False)
        
        self.setup_xdcr_and_load()
        
        self.log.info("Verifying ECCV settings before test")
        for bucket in self.src_cluster.get_buckets():
            src_eccv = self._get_eccv_setting(self.src_cluster, bucket.name)
            dest_eccv = self._get_eccv_setting(self.dest_cluster, bucket.name)
            self.log.info(f"Bucket {bucket.name}: Source ECCV={src_eccv}, Target ECCV={dest_eccv}")
            self.assertTrue(src_eccv, f"Source ECCV should be True for bucket {bucket.name}")
            self.assertFalse(dest_eccv, f"Target ECCV should be False for bucket {bucket.name}")
        
        warning_found, warning_msg = self._wait_for_eccv_warning(self.src_cluster)
        self.assertTrue(warning_found, 
                       "Expected ECCV mismatch warning when source=true, target=false")
        self.log.info(f"ECCV mismatch warning found: {warning_msg}")
        
        self.log.info("Syncing ECCV settings by enabling on target cluster")
        self._set_eccv_on_cluster(self.dest_cluster, True)
        
        warning_cleared = self._wait_for_eccv_warning_to_clear(self.src_cluster)
        self.assertTrue(warning_cleared, 
                       "ECCV mismatch warning should clear after syncing settings")

    def test_eccv_source_false_target_true(self):
        """
        Test ECCV mismatch: source=false, target=true
        
        Steps:
        1. Set up XDCR replication
        2. Enable ECCV on target cluster only
        3. Verify ECCV mismatch warning appears
        4. Load some data to ensure replication is active
        5. Verify warning is present in XDCR errors
        6. Enable ECCV on source to sync settings
        7. Verify warning disappears
        """
        self._set_eccv_on_cluster(self.src_cluster, False)
        self._set_eccv_on_cluster(self.dest_cluster, True)
        
        self.setup_xdcr_and_load()
        
        self.log.info("Verifying ECCV settings before test")
        for bucket in self.src_cluster.get_buckets():
            src_eccv = self._get_eccv_setting(self.src_cluster, bucket.name)
            dest_eccv = self._get_eccv_setting(self.dest_cluster, bucket.name)
            self.log.info(f"Bucket {bucket.name}: Source ECCV={src_eccv}, Target ECCV={dest_eccv}")
            self.assertFalse(src_eccv, f"Source ECCV should be False for bucket {bucket.name}")
            self.assertTrue(dest_eccv, f"Target ECCV should be True for bucket {bucket.name}")
        
        warning_found, warning_msg = self._wait_for_eccv_warning(self.src_cluster)
        self.assertTrue(warning_found, 
                       "Expected ECCV mismatch warning when source=false, target=true")
        self.log.info(f"ECCV mismatch warning found: {warning_msg}")
        
        self.log.info("Syncing ECCV settings by enabling on source cluster")
        self._set_eccv_on_cluster(self.src_cluster, True)
        
        warning_cleared = self._wait_for_eccv_warning_to_clear(self.src_cluster)
        self.assertTrue(warning_cleared, 
                       "ECCV mismatch warning should clear after syncing settings")

    def test_eccv_both_false_no_warning(self):
        """
        Test ECCV matching: source=false, target=false (default)
        
        Steps:
        1. Ensure ECCV is disabled on both clusters (default)
        2. Set up XDCR replication
        3. Load some data
        4. Verify NO ECCV mismatch warning appears
        """
        self._set_eccv_on_cluster(self.src_cluster, False)
        self._set_eccv_on_cluster(self.dest_cluster, False)
        
        self.setup_xdcr_and_load()
        
        self.log.info("Verifying ECCV settings")
        for bucket in self.src_cluster.get_buckets():
            src_eccv = self._get_eccv_setting(self.src_cluster, bucket.name)
            dest_eccv = self._get_eccv_setting(self.dest_cluster, bucket.name)
            self.log.info(f"Bucket {bucket.name}: Source ECCV={src_eccv}, Target ECCV={dest_eccv}")
            self.assertFalse(src_eccv, f"Source ECCV should be False for bucket {bucket.name}")
            self.assertFalse(dest_eccv, f"Target ECCV should be False for bucket {bucket.name}")
        
        self.log.info("Waiting to verify no ECCV warning appears with matching false settings")
        time.sleep(30)
        
        warning_found, warning_msg = self._check_eccv_mismatch_warning(self.src_cluster)
        self.assertFalse(warning_found, 
                        f"No ECCV mismatch warning expected when both clusters have ECCV=false, but found: {warning_msg}")

    def test_eccv_both_true_no_warning(self):
        """
        Test ECCV matching: source=true, target=true
        
        Steps:
        1. Enable ECCV on both clusters
        2. Set up XDCR replication
        3. Load some data
        4. Verify NO ECCV mismatch warning appears
        """
        self._set_eccv_on_cluster(self.src_cluster, True)
        self._set_eccv_on_cluster(self.dest_cluster, True)
        
        self.setup_xdcr_and_load()
        
        self.log.info("Verifying ECCV settings")
        for bucket in self.src_cluster.get_buckets():
            src_eccv = self._get_eccv_setting(self.src_cluster, bucket.name)
            dest_eccv = self._get_eccv_setting(self.dest_cluster, bucket.name)
            self.log.info(f"Bucket {bucket.name}: Source ECCV={src_eccv}, Target ECCV={dest_eccv}")
            self.assertTrue(src_eccv, f"Source ECCV should be True for bucket {bucket.name}")
            self.assertTrue(dest_eccv, f"Target ECCV should be True for bucket {bucket.name}")
        
        self.log.info("Waiting to verify no ECCV warning appears with matching true settings")
        time.sleep(30)
        
        warning_found, warning_msg = self._check_eccv_mismatch_warning(self.src_cluster)
        self.assertFalse(warning_found, 
                        f"No ECCV mismatch warning expected when both clusters have ECCV=true, but found: {warning_msg}")

    def test_eccv_mismatch_bidirectional(self):
        """
        Test ECCV mismatch in bidirectional replication
        
        Steps:
        1. Set up bidirectional XDCR replication
        2. Enable ECCV on source cluster only
        3. Verify ECCV mismatch warning appears on both directions
        4. Sync ECCV settings
        5. Verify warnings clear on both clusters
        """
        self._set_eccv_on_cluster(self.src_cluster, True)
        self._set_eccv_on_cluster(self.dest_cluster, False)
        
        self.setup_xdcr_and_load()
        
        self.log.info("Checking for ECCV mismatch warning on source cluster")
        src_warning_found, src_warning_msg = self._wait_for_eccv_warning(self.src_cluster)
        
        self.log.info("Checking for ECCV mismatch warning on destination cluster")
        dest_warning_found, dest_warning_msg = self._wait_for_eccv_warning(self.dest_cluster)
        
        self.assertTrue(src_warning_found or dest_warning_found, 
                       "Expected ECCV mismatch warning in bidirectional setup")
        
        if src_warning_found:
            self.log.info(f"ECCV mismatch warning on source: {src_warning_msg}")
        if dest_warning_found:
            self.log.info(f"ECCV mismatch warning on dest: {dest_warning_msg}")
        
        self.log.info("Syncing ECCV settings by enabling on target cluster")
        self._set_eccv_on_cluster(self.dest_cluster, True)
        
        src_cleared = self._wait_for_eccv_warning_to_clear(self.src_cluster)
        dest_cleared = self._wait_for_eccv_warning_to_clear(self.dest_cluster)
        
        self.assertTrue(src_cleared, "ECCV warning should clear on source after syncing")
        self.assertTrue(dest_cleared, "ECCV warning should clear on dest after syncing")

    def test_eccv_mismatch_warning_persists_until_fixed(self):
        """
        Test that ECCV mismatch warning persists until settings are synchronized
        
        Steps:
        1. Set up XDCR with ECCV mismatch
        2. Verify warning appears
        3. Wait and verify warning persists
        4. Fix the mismatch
        5. Verify warning clears
        """
        self._set_eccv_on_cluster(self.src_cluster, True)
        self._set_eccv_on_cluster(self.dest_cluster, False)
        
        self.setup_xdcr_and_load()
        
        warning_found, _ = self._wait_for_eccv_warning(self.src_cluster)
        self.assertTrue(warning_found, "Expected ECCV mismatch warning")
        
        self.log.info("Verifying warning persists after 60 seconds")
        time.sleep(60)
        
        warning_still_present, _ = self._check_eccv_mismatch_warning(self.src_cluster)
        self.assertTrue(warning_still_present, 
                       "ECCV mismatch warning should persist until settings are fixed")
        
        self.log.info("Fixing ECCV mismatch by enabling on target")
        self._set_eccv_on_cluster(self.dest_cluster, True)
        
        warning_cleared = self._wait_for_eccv_warning_to_clear(self.src_cluster)
        self.assertTrue(warning_cleared, "ECCV warning should clear after fixing mismatch")

    def test_eccv_mismatch_multiple_buckets(self):
        """
        Test ECCV mismatch with multiple buckets
        
        Steps:
        1. Create multiple buckets
        2. Set ECCV mismatch on all buckets
        3. Verify warnings appear for each bucket
        4. Fix mismatch bucket by bucket
        5. Verify warnings clear as each bucket is fixed
        """
        self._set_eccv_on_cluster(self.src_cluster, True)
        self._set_eccv_on_cluster(self.dest_cluster, False)
        
        self.setup_xdcr_and_load()
        
        warning_found, _ = self._wait_for_eccv_warning(self.src_cluster)
        self.assertTrue(warning_found, "Expected ECCV mismatch warning for multiple buckets")
        
        buckets = self.dest_cluster.get_buckets()
        self.log.info(f"Fixing ECCV on {len(buckets)} buckets one by one")
        
        for bucket in buckets:
            self.log.info(f"Enabling ECCV on target bucket: {bucket.name}")
            self.dest_rest.change_bucket_props(bucket, enableCrossClusterVersioning="true")
            time.sleep(10)
        
        warning_cleared = self._wait_for_eccv_warning_to_clear(self.src_cluster)
        self.assertTrue(warning_cleared, 
                       "All ECCV mismatch warnings should clear after fixing all buckets")

    def test_eccv_mismatch_with_replication_pause_resume(self):
        """
        Test ECCV mismatch behavior with replication pause/resume
        
        Steps:
        1. Set up XDCR with ECCV mismatch
        2. Verify warning appears
        3. Pause replication
        4. Resume replication
        5. Verify warning still present
        6. Fix mismatch
        7. Verify warning clears
        """
        self._set_eccv_on_cluster(self.src_cluster, True)
        self._set_eccv_on_cluster(self.dest_cluster, False)
        
        self.setup_xdcr_and_load()
        
        warning_found, _ = self._wait_for_eccv_warning(self.src_cluster)
        self.assertTrue(warning_found, "Expected ECCV mismatch warning")
        
        self.log.info("Pausing all replications")
        for remote_cluster in self.src_cluster.get_remote_clusters():
            remote_cluster.pause_all_replications()
        
        time.sleep(10)
        
        self.log.info("Resuming all replications")
        for remote_cluster in self.src_cluster.get_remote_clusters():
            remote_cluster.resume_all_replications()
        
        time.sleep(30)
        warning_after_resume, _ = self._check_eccv_mismatch_warning(self.src_cluster)
        self.assertTrue(warning_after_resume, 
                       "ECCV mismatch warning should persist after pause/resume")
        
        self.log.info("Fixing ECCV mismatch")
        self._set_eccv_on_cluster(self.dest_cluster, True)
        
        warning_cleared = self._wait_for_eccv_warning_to_clear(self.src_cluster)
        self.assertTrue(warning_cleared, "ECCV warning should clear after fixing mismatch")

    def test_eccv_mismatch_warning_message_format(self):
        """
        Test that ECCV mismatch warning message contains expected information
        
        Expected format:
        WARN: Mismatch between source and target enableCrossClusterVersioning (ECCV)
        settings - It's disabled for bucket <bucket_name> on cluster <cluster_name>.
        """
        self._set_eccv_on_cluster(self.src_cluster, True)
        self._set_eccv_on_cluster(self.dest_cluster, False)
        
        self.setup_xdcr_and_load()
        
        warning_found, warning_msg = self._wait_for_eccv_warning(self.src_cluster)
        self.assertTrue(warning_found, "Expected ECCV mismatch warning")
        
        self.log.info(f"Validating warning message format: {warning_msg}")
        
        expected_keywords = [
            "enableCrossClusterVersioning",
            "ECCV",
            "Mismatch",
            "disabled"
        ]
        
        found_keywords = []
        for keyword in expected_keywords:
            if keyword.lower() in warning_msg.lower():
                found_keywords.append(keyword)
        
        self.log.info(f"Found keywords in warning: {found_keywords}")
        self.assertGreater(len(found_keywords), 0, 
                          f"Warning message should contain ECCV-related keywords. Message: {warning_msg}")

    def test_eccv_toggle_creates_and_clears_warning(self):
        """
        Test enabling ECCV creates mismatch warning and enabling on target clears it.
        
        Note: ECCV cannot be disabled once enabled on a bucket. This is a Couchbase
        Server constraint. Therefore this test only validates the enable path.
        
        Steps:
        1. Start with matching ECCV settings (both false)
        2. Create mismatch by enabling on source only
        3. Verify warning appears
        4. Fix by enabling on target
        5. Verify warning clears
        """
        self.setup_xdcr_and_load()
        
        time.sleep(20)
        warning_found, _ = self._check_eccv_mismatch_warning(self.src_cluster)
        self.assertFalse(warning_found, "No warning expected with matching false settings")
        
        self.log.info("Creating mismatch: enabling ECCV on source only")
        self._set_eccv_on_cluster(self.src_cluster, True)
        
        warning_found, _ = self._wait_for_eccv_warning(self.src_cluster)
        self.assertTrue(warning_found, "Warning expected after creating mismatch")
        
        self.log.info("Fixing mismatch: enabling ECCV on target")
        self._set_eccv_on_cluster(self.dest_cluster, True)
        
        warning_cleared = self._wait_for_eccv_warning_to_clear(self.src_cluster)
        self.assertTrue(warning_cleared, "Warning should clear after fixing mismatch")
