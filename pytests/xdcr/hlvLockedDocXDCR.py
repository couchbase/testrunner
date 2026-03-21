"""
HLV Locked Document XDCR Tests

Tests for XDCR replication behavior when target documents are locked and contain
HLV (Hybrid Logical Vector) information. This test suite validates the fix for
the issue where source XDCR should retry until the target document is unlocked
and ensure proper conflict resolution takes place.

"""

import time

from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from xdcr.xdcrnewbasetests import XDCRNewBaseTest

try:
    from sdk_client3 import SDKClient
except:
    from lib.sdk_client3 import SDKClient


class HLVLockedDocTests(XDCRNewBaseTest):
    def setUp(self):
        super(HLVLockedDocTests, self).setUp()
        self.src_cluster = self.get_cb_cluster_by_name("C1")
        self.src_master = self.src_cluster.get_master_node()
        self.dest_cluster = self.get_cb_cluster_by_name("C2")
        self.dest_master = self.dest_cluster.get_master_node()
        self.src_rest = RestConnection(self.src_master)
        self.dest_rest = RestConnection(self.dest_master)

        self.gen_create = BlobGenerator('hlvlock', 'hlvlock-', self._value_size, start=0,
                                        end=self._num_items)

        self.lock_timeout = self._input.param("lock_timeout", 15)
        self.replication_wait_timeout = self._input.param("replication_wait_timeout", 120)

        self.src_master_shell = RemoteMachineShellConnection(self.src_master)
        self.init_xdcr_differ(
            self.src_cluster,
            self.src_master,
            self.src_master_shell,
            yaml_conf_path="/tmp/xdcr_differ_hlv_params.yaml",
            output_dir="/tmp/xdcr_differ_hlv_outputs"
        )

    def tearDown(self):
        self._cleanup_xdcr_differ_output()
        if hasattr(self, 'src_master_shell'):
            self.src_master_shell.disconnect()
        super(HLVLockedDocTests, self).tearDown()

    def _create_sdk_client(self, cluster, bucket_name):
        """       
        Args:
            cluster: CouchbaseCluster object
            bucket_name: Name of the bucket to connect to
            
        Returns:
            SDKClient: Connected SDK client
        """
        master = cluster.get_master_node()
        return SDKClient(
            bucket=bucket_name,
            hosts=[master.ip],
            username=master.rest_username,
            password=master.rest_password
        )

    def _create_doc_with_hlv(self, cluster, bucket_name, key, value):
        """
        Args:
            cluster: CouchbaseCluster object
            bucket_name: Bucket name
            key: Document key
            value: Document value (dict)
        """
        client = self._create_sdk_client(cluster, bucket_name)
        try:
            client.upsert(key, value)
            self.log.info(f"Created document {key} with HLV on cluster {cluster.get_name()}")
        finally:
            client.close()

    def _lock_document(self, cluster, bucket_name, key, lock_time):
        """
        Args:
            cluster: CouchbaseCluster object
            bucket_name: Bucket name
            key: Document key
            lock_time: Lock duration in seconds
            
        Returns:
            tuple: (cas, content) from the locked document
        """
        client = self._create_sdk_client(cluster, bucket_name)
        try:
            cas, content = client.get_and_lock(key, lock_time)
            self.log.info(f"Locked document {key} for {lock_time}s on cluster {cluster.get_name()}, CAS: {cas}")
            return (cas, content)
        finally:
            client.close()

    def _unlock_document(self, cluster, bucket_name, key, cas):
        """
        Args:
            cluster: CouchbaseCluster object
            bucket_name: Bucket name
            key: Document key
            cas: CAS value from lock operation
        """
        client = self._create_sdk_client(cluster, bucket_name)
        try:
            client.unlock_with_cas(key, cas)
            self.log.info(f"Unlocked document {key} on cluster {cluster.get_name()}")
        finally:
            client.close()

    def _get_document(self, cluster, bucket_name, key):
        """
        Args:
            cluster: CouchbaseCluster object
            bucket_name: Bucket name
            key: Document key
            
        Returns:
            dict: Document value or None if not found
        """
        client = self._create_sdk_client(cluster, bucket_name)
        try:
            # Access the default_collection directly to get SDK 4.x GetResult
            result = client.default_collection.get(key)
            # SDK 4.x GetResult has content_as property to get the document value
            if hasattr(result, 'content_as'):
                return result.content_as[dict]
            elif hasattr(result, 'content'):
                return result.content
            elif hasattr(result, 'value'):
                return result.value
            else:
                return result
        except Exception as e:
            self.log.warning(f"Failed to get document {key}: {e}")
            return None
        finally:
            client.close()

    def _update_document(self, cluster, bucket_name, key, value):
        """
        Args:
            cluster: CouchbaseCluster object
            bucket_name: Bucket name
            key: Document key
            value: New document value
        """
        client = self._create_sdk_client(cluster, bucket_name)
        try:
            client.upsert(key, value)
            self.log.info(f"Updated document {key} on cluster {cluster.get_name()}")
        finally:
            client.close()

    def _wait_for_replication(self, timeout=120):
        """
        Args:
            timeout: Maximum time to wait in seconds
        """
        self.log.info(f"Waiting for replication to complete (timeout: {timeout}s)")
        self._wait_for_replication_to_catchup(timeout=timeout)

    def test_hlv_locked_doc_replication(self):
        lock_duration = self._input.param("lock_duration", 15)
        bucket_name = self.src_cluster.get_buckets()[0].name
        test_key = f"hlv_lock_test_{lock_duration}s"
        initial_value = {"test": "initial", "version": 1}
        updated_value = {"test": "updated", "version": 2}

        self.setup_xdcr_and_load()

        self._create_doc_with_hlv(self.src_cluster, bucket_name, test_key, initial_value)

        time.sleep(5)
        self._wait_for_replication(timeout=60)

        dest_doc = self._get_document(self.dest_cluster, bucket_name, test_key)
        self.assertIsNotNone(dest_doc, "Document should exist on target after initial replication")

        self.log.info(f"Locking document on target for {lock_duration} seconds")
        lock_result = self._lock_document(self.dest_cluster, bucket_name, test_key, lock_duration)

        self.log.info("Updating document on source while target is locked")
        self._update_document(self.src_cluster, bucket_name, test_key, updated_value)

        self.log.info(f"Waiting for lock to expire ({lock_duration}s) and replication to complete")
        time.sleep(lock_duration + 10)
        self._wait_for_replication(timeout=self.replication_wait_timeout + lock_duration)

        final_dest_doc = self._get_document(self.dest_cluster, bucket_name, test_key)
        self.assertIsNotNone(final_dest_doc, "Document should exist on target after replication")

        self.log.info(f"Final document value on target: {final_dest_doc}")

        if isinstance(final_dest_doc, dict):
            self.assertEqual(final_dest_doc.get("version"), 2,
                           "Source document should win conflict resolution after lock expires")

        self._verify_no_data_loss_with_differ(bucket_name)
        self.log.info("Test passed: XDCR correctly replicated after lock expired, no data loss")

    def test_hlv_locked_doc_multiple_updates_during_lock(self):
        lock_duration = self._input.param("lock_duration", 20)
        bucket_name = self.src_cluster.get_buckets()[0].name
        test_key = "hlv_multi_update_lock_test"
        initial_value = {"test": "initial", "version": 1}
        
        # ECCV is enabled via enable_cross_cluster_versioning=True in conf file
        self.setup_xdcr_and_load()
        
        self._create_doc_with_hlv(self.src_cluster, bucket_name, test_key, initial_value)
        
        time.sleep(5)
        self._wait_for_replication(timeout=60)
        
        self.log.info(f"Locking document on target for {lock_duration} seconds")
        lock_result = self._lock_document(self.dest_cluster, bucket_name, test_key, lock_duration)
        
        self.log.info("Performing multiple updates on source while target is locked")
        for i in range(2, 6):
            updated_value = {"test": f"update_{i}", "version": i}
            self._update_document(self.src_cluster, bucket_name, test_key, updated_value)
            time.sleep(1)
        
        final_src_value = {"test": "final", "version": 10}
        self._update_document(self.src_cluster, bucket_name, test_key, final_src_value)
        
        self.log.info(f"Waiting for lock to expire ({lock_duration}s) and replication to complete")
        time.sleep(lock_duration + 10)
        self._wait_for_replication(timeout=self.replication_wait_timeout)
        
        final_dest_doc = self._get_document(self.dest_cluster, bucket_name, test_key)
        self.assertIsNotNone(final_dest_doc, "Document should exist on target after replication")
            
        self.log.info(f"Final document value on target: {final_dest_doc}")
        
        if isinstance(final_dest_doc, dict):
            self.assertEqual(final_dest_doc.get("version"), 10, 
                           "Latest source document should be replicated after lock expires")
        
        self._verify_no_data_loss_with_differ(bucket_name)
        self.log.info("Test passed: Latest source update replicated after lock expired, no data loss")

    def test_hlv_locked_doc_concurrent_locks(self):
        lock_duration = self._input.param("lock_duration", 15)
        bucket_name = self.src_cluster.get_buckets()[0].name
        num_docs = self._input.param("num_locked_docs", 5)
        
        # ECCV is enabled via enable_cross_cluster_versioning=True in conf file
        self.setup_xdcr_and_load()
        
        self.log.info(f"Creating {num_docs} documents with HLV")
        for i in range(num_docs):
            key = f"hlv_concurrent_lock_{i}"
            value = {"doc_id": i, "test": "initial", "version": 1}
            self._create_doc_with_hlv(self.src_cluster, bucket_name, key, value)
        
        time.sleep(5)
        self._wait_for_replication(timeout=60)
        
        self.log.info(f"Locking {num_docs} documents on target for {lock_duration} seconds")
        lock_results = []
        for i in range(num_docs):
            key = f"hlv_concurrent_lock_{i}"
            lock_result = self._lock_document(self.dest_cluster, bucket_name, key, lock_duration)
            lock_results.append((key, lock_result))
        
        self.log.info(f"Updating all {num_docs} documents on source while target locks are held")
        for i in range(num_docs):
            key = f"hlv_concurrent_lock_{i}"
            value = {"doc_id": i, "test": "updated", "version": 2}
            self._update_document(self.src_cluster, bucket_name, key, value)
        
        self.log.info(f"Waiting for locks to expire ({lock_duration}s) and replication to complete")
        time.sleep(lock_duration + 10)
        self._wait_for_replication(timeout=self.replication_wait_timeout)
        
        self.log.info("Verifying all documents were replicated correctly")
        for i in range(num_docs):
            key = f"hlv_concurrent_lock_{i}"
            final_dest_doc = self._get_document(self.dest_cluster, bucket_name, key)
            self.assertIsNotNone(final_dest_doc, f"Document {key} should exist on target")
                
            if isinstance(final_dest_doc, dict):
                self.assertEqual(final_dest_doc.get("version"), 2, 
                               f"Document {key} should have updated version after lock expires")
        
        self._verify_no_data_loss_with_differ(bucket_name)
        self.log.info("Test passed: All locked documents correctly replicated after locks expired, no data loss")

    def test_hlv_locked_doc_early_unlock(self):
        lock_duration = self._input.param("lock_duration", 60)
        unlock_after = self._input.param("unlock_after", 10)
        bucket_name = self.src_cluster.get_buckets()[0].name
        test_key = "hlv_early_unlock_test"
        initial_value = {"test": "initial", "version": 1}
        updated_value = {"test": "updated", "version": 2}
        
        # ECCV is enabled via enable_cross_cluster_versioning=True in conf file
        self.setup_xdcr_and_load()
        
        self._create_doc_with_hlv(self.src_cluster, bucket_name, test_key, initial_value)
        
        time.sleep(5)
        self._wait_for_replication(timeout=60)
        
        self.log.info(f"Locking document on target for {lock_duration} seconds")
        lock_result = self._lock_document(self.dest_cluster, bucket_name, test_key, lock_duration)
        
        self.log.info("Updating document on source while target is locked")
        self._update_document(self.src_cluster, bucket_name, test_key, updated_value)
        
        self.log.info(f"Waiting {unlock_after}s before manually unlocking")
        time.sleep(unlock_after)
        
        if isinstance(lock_result, tuple) and len(lock_result) > 0:
            # lock_result is (cas, content) - cas is at index 0
            cas = lock_result[0]
            self.log.info(f"Manually unlocking document with CAS: {cas}")
            try:
                self._unlock_document(self.dest_cluster, bucket_name, test_key, cas)
            except Exception as e:
                self.log.warning(f"Failed to manually unlock (may have been unlocked by XDCR): {e}")
        
        self._wait_for_replication(timeout=self.replication_wait_timeout)
        
        final_dest_doc = self._get_document(self.dest_cluster, bucket_name, test_key)
        self.assertIsNotNone(final_dest_doc, "Document should exist on target after replication")
            
        self.log.info(f"Final document value on target: {final_dest_doc}")
        
        if isinstance(final_dest_doc, dict):
            self.assertEqual(final_dest_doc.get("version"), 2, 
                           "Source document should be replicated after early unlock")
        
        self._verify_no_data_loss_with_differ(bucket_name)
        self.log.info("Test passed: XDCR correctly replicated after early unlock, no data loss")

    def test_hlv_locked_doc_bidirectional_conflict(self):
        lock_duration = self._input.param("lock_duration", 15)
        bucket_name = self.src_cluster.get_buckets()[0].name
        test_key = "hlv_bidir_conflict_test"
        
        # ECCV is enabled via enable_cross_cluster_versioning=True in conf file
        self.setup_xdcr_and_load()
        
        initial_value = {"test": "initial", "cluster": "source", "version": 1}
        self._create_doc_with_hlv(self.src_cluster, bucket_name, test_key, initial_value)
        
        time.sleep(5)
        self._wait_for_replication(timeout=60)
        
        self.log.info(f"Locking document on target for {lock_duration} seconds")
        lock_result = self._lock_document(self.dest_cluster, bucket_name, test_key, lock_duration)
        
        self.log.info("Updating document on source to create conflict")
        src_updated = {"test": "conflict", "cluster": "source", "version": 2, "timestamp": time.time()}
        self._update_document(self.src_cluster, bucket_name, test_key, src_updated)
        
        self.log.info(f"Waiting for lock to expire ({lock_duration}s)")
        time.sleep(lock_duration + 5)
        
        self.log.info("Updating document on destination to create bidirectional conflict")
        dest_updated = {"test": "conflict", "cluster": "dest", "version": 3, "timestamp": time.time()}
        self._update_document(self.dest_cluster, bucket_name, test_key, dest_updated)
        
        self._wait_for_replication(timeout=self.replication_wait_timeout)
        
        src_final = self._get_document(self.src_cluster, bucket_name, test_key)
        dest_final = self._get_document(self.dest_cluster, bucket_name, test_key)
        
        self.log.info(f"Source final: {src_final}")
        self.log.info(f"Dest final: {dest_final}")
        
        self.assertIsNotNone(src_final, "Document should exist on source after conflict resolution")
        self.assertIsNotNone(dest_final, "Document should exist on destination after conflict resolution")
        
        self._verify_no_data_loss_with_differ(bucket_name)
        self.log.info("Test passed: Bidirectional conflict resolution completed with locked document, no data loss")

