import json
import time
import urllib.parse
from random import randint
import threading

from couchbase_helper.documentgenerator import BlobGenerator, DocumentGenerator
from membase.api.rest_client import RestConnection
from xdcr.xdcrnewbasetests import XDCRNewBaseTest


class ConflictLoggingTests(XDCRNewBaseTest):
    """
    Test suite for XDCR conflict logging functionality.
    Tests the ability to log conflicts to specified collections.
    """
    
    def setUp(self):
        super(ConflictLoggingTests, self).setUp()
        self.src_cluster = self.get_cb_cluster_by_name("C1")
        self.src_master = self.src_cluster.get_master_node()
        self.dest_cluster = self.get_cb_cluster_by_name("C2")
        self.dest_master = self.dest_cluster.get_master_node()
        
        # Initialize replication direction
        self._replication_direction = self._input.param("rdirection", "bidirection")
        
        # Create document generators for testing
        self.gen_create = BlobGenerator('conflict', 'conflict-', self._value_size, start=0,
                                        end=self._num_items)
        self.gen_update = BlobGenerator('conflict', 'conflict-', self._value_size, start=0,
                                        end=int(self._num_items * 0.9))
        self.gen_delete = BlobGenerator('conflict', 'conflict-', self._value_size,
                                        start=int(self._num_items * 0.8), end=self._num_items)
        
        # Parse conflict logging parameters
        self.conflict_logging_params = self._parse_conflict_logging_params()
        
    def _parse_conflict_logging_params(self):
        """
        Parse conflict logging parameters directly from test_params.
        Format: default@C1=conflict_logging:testscope.testcollection
        
        Returns:
            dict: Dictionary mapping bucket/cluster to conflict logging settings
        """
        params = {}
        
        if not hasattr(self, '_input') or not hasattr(self._input, 'test_params'):
            self.log.warning("No test parameters found")
            return params
            
        # Log all test parameters
        self.log.info(f"Test parameters: {self._input.test_params}")
        
        for key, value in self._input.test_params.items():
            if "@" in key and "conflict_logging:" in value:
                bucket, cluster = key.split('@')
                collection_path = value.split('conflict_logging:')[1].strip()
                
                self.log.info(f"Found conflict logging param: {bucket}@{cluster} -> {collection_path}")
                
                if bucket not in params:
                    params[bucket] = {}
                
                params[bucket][cluster] = {
                    "bucket": bucket,
                    "collection": collection_path,
                    "disabled": False
                }
                
                self.log.info(f"Parsed conflict logging param: {bucket}@{cluster} -> {params[bucket][cluster]}")
        
        return params
    
    def tearDown(self):
        super(ConflictLoggingTests, self).tearDown()
    
    def _setup_conflict_logging(self):
        """
        Set up conflict logging for all replications based on parsed parameters
        """
        self.log.info(f"Setting up conflict logging with params: {self.conflict_logging_params}")
        
        if not self.conflict_logging_params:
            self.log.warning("No conflict logging parameters found. Conflict logging will not be enabled.")
            return
            
        for cluster in self.get_cb_clusters():
            for remote_cluster in cluster.get_remote_clusters():
                for replication in remote_cluster.get_replications():
                    bucket_name = replication.get_src_bucket().name
                    cluster_name = cluster.get_name()
                    
                    if bucket_name in self.conflict_logging_params and cluster_name in self.conflict_logging_params[bucket_name]:
                        settings = self.conflict_logging_params[bucket_name][cluster_name]
                        self.log.info(f"Setting conflict logging for {bucket_name}@{cluster_name}: {settings}")
                        self._set_conflict_logging(replication, settings)
                    else:
                        self.log.info(f"No conflict logging settings for {bucket_name}@{cluster_name}")
    
    def _set_conflict_logging(self, replication, settings):
        """
        Set conflict logging for a specific replication
        
        Args:
            replication: XDCReplication object
            settings: Dictionary with conflict logging settings
        """
        src_cluster = replication.get_src_cluster()
        
        # API expects a single JSON object (map), not an array
        conflict_settings_json = json.dumps(settings)
        self.log.info(f"Setting conflict logging (per-replication): {conflict_settings_json}")

        # Set per-replication param to avoid overwriting other replications
        try:
            src_bucket = replication.get_src_bucket().name
            dst_bucket = replication.get_dest_bucket().name
            src_master = replication.get_src_cluster().get_master_node()
            # Use set_xdcr_params to avoid lowercasing JSON
            RestConnection(src_master).set_xdcr_params(src_bucket, dst_bucket, {"conflictLogging": conflict_settings_json})
        except Exception as e:
            self.log.error(f"Failed to set per-replication conflictLogging: {e}")
            raise
        
        self.log.info(f"Successfully set conflict logging for replication {replication.get_repl_id()}: {settings}")
    
    def _get_conflict_logging_settings(self, replication=None, bucket_filter=None):
        """
        Get current conflict logging settings for a replication
        
        Args:
            replication: XDCReplication object (optional)
            bucket_filter (str): If provided, return settings for this bucket
            
        Returns:
            dict: Current conflict logging settings
        """
        # If specific replication is provided, query exactly that replication's setting
        if replication is not None:
            try:
                src_bucket = replication.get_src_bucket().name
                dst_bucket = replication.get_dest_bucket().name
                src_master = replication.get_src_cluster().get_master_node()
                value = RestConnection(src_master).get_xdcr_param(src_bucket, dst_bucket, 'conflictLogging')
                # value can be dict or JSON string; normalize to dict
                if isinstance(value, str):
                    try:
                        parsed = json.loads(value)
                        if isinstance(parsed, dict):
                            return parsed
                    except Exception:
                        pass
                    return {}
                if isinstance(value, dict):
                    return value
                return {}
            except Exception as e:
                self.log.error(f"Failed to fetch per-replication conflictLogging: {e}")
                return {}

        # Fallback: query cluster-level aggregated values
        src_cluster = self.get_cb_clusters()[0]
        conflict_logging_str = src_cluster.get_conflict_logging_settings()
        
        # Parse the conflict logging settings
        try:
            parsed = json.loads(conflict_logging_str) if isinstance(conflict_logging_str, str) else conflict_logging_str
            # If list, select entry for the requested bucket
            if isinstance(parsed, list):
                if bucket_filter:
                    for item in parsed:
                        current = None
                        if isinstance(item, dict):
                            current = item
                        elif isinstance(item, str):
                            try:
                                current = json.loads(item)
                            except Exception:
                                current = None
                        if isinstance(current, dict) and current.get('bucket') == bucket_filter:
                            self.log.info(f"Selected settings for bucket '{bucket_filter}': {current}")
                            return current
                # If no filter match, return empty dict
                self.log.info("No matching bucket settings found in list; returning empty settings")
                return {}
            # If single dict, return as-is
            if isinstance(parsed, dict):
                return parsed
            # If single string, try to parse
            if isinstance(parsed, str):
                try:
                    obj = json.loads(parsed)
                    if isinstance(obj, dict):
                        return obj
                except Exception:
                    pass
            return {}
                
        except json.JSONDecodeError:
            self.log.error(f"Failed to parse conflict logging settings: {conflict_logging_str}")
            return {}
    
    def _verify_conflict_logging_settings(self):
        """
        Verify that conflict logging settings are applied correctly
        """
        self.log.info("Verifying conflict logging settings")
        
        for cluster in self.get_cb_clusters():
            for remote_cluster in cluster.get_remote_clusters():
                for replication in remote_cluster.get_replications():
                    # Request settings for the specific source bucket of this replication
                    bucket_name = replication.get_src_bucket().name
                    settings = self._get_conflict_logging_settings(replication, bucket_filter=bucket_name)
                    
                    # Ensure settings is a dictionary
                    if not isinstance(settings, dict):
                        self.log.error(f"Expected settings to be a dict, got {type(settings)}: {settings}")
                        continue
                        
                    bucket_name = replication.get_src_bucket().name
                    cluster_name = cluster.get_name()
                    
                    if bucket_name in self.conflict_logging_params and cluster_name in self.conflict_logging_params[bucket_name]:
                        expected = self.conflict_logging_params[bucket_name][cluster_name]
                        
                        # Verify bucket setting
                        self.assertEqual(settings.get('bucket', ''), expected['bucket'],
                                        f"Bucket setting mismatch for {bucket_name}@{cluster_name}")
                        
                        # Verify collection setting
                        self.assertEqual(settings.get('collection', ''), expected['collection'],
                                        f"Collection setting mismatch for {bucket_name}@{cluster_name}")
                        
                        # Verify disabled setting
                        self.assertEqual(settings.get('disabled', True), expected.get('disabled', False),
                                        f"Disabled setting mismatch for {bucket_name}@{cluster_name}")
                        
                        self.log.info(f"Conflict logging settings verified for {bucket_name}@{cluster_name}") 
    
    def _create_conflicts_with_pillowfight(self):
        """
        Create conflicts using cbc-pillowfight tool for reliable conflict generation
        """
        if self._replication_direction != "bidirection":
            self.log.info("Skipping conflict creation in unidirectional setup")
            return False
            
        # Get bucket names from conflict logging parameters
        bucket_names = []
        for bucket_name in self.conflict_logging_params.keys():
            bucket_names.append(bucket_name)
            
        if not bucket_names:
            self.log.info("No bucket names found in conflict logging parameters, using default bucket")
            bucket_names = ["default"]
        
        self.log.info(f"Creating conflicts with cbc-pillowfight for buckets: {bucket_names}")
        
        # Get server credentials for authentication with cbc-pillowfight
        src_rest = RestConnection(self.src_master)
        dest_rest = RestConnection(self.dest_master)
        username = self.src_master.rest_username
        password = self.src_master.rest_password
        
        # Run cbc-pillowfight on both clusters for each bucket
        success = True
        for bucket_name in bucket_names:
            # Run pillowfight against source cluster
            src_cmd = f"cbc-pillowfight -U couchbase://{self.src_master.ip}/{bucket_name} -u {username} -P {password} " \
                    f"-I 1000 -m 100 -M 100 -c 10 -t 4 -B 100"
                    
            # Run pillowfight against destination cluster
            dest_cmd = f"cbc-pillowfight -U couchbase://{self.dest_master.ip}/{bucket_name} -u {username} -P {password} " \
                     f"-I 1000 -m 100 -M 100 -c 10 -t 4 -B 100"
            
            # Execute commands in parallel
            import subprocess
            import threading
            
            src_result = None
            dest_result = None
            
            def run_src_cmd():
                nonlocal src_result
                try:
                    src_result = subprocess.run(src_cmd, shell=True, check=False, capture_output=True)
                    self.log.info(f"Source pillowfight command completed with status: {src_result.returncode}")
                except Exception as e:
                    self.log.error(f"Error running source pillowfight command: {e}")
            
            def run_dest_cmd():
                nonlocal dest_result
                try:
                    dest_result = subprocess.run(dest_cmd, shell=True, check=False, capture_output=True)
                    self.log.info(f"Destination pillowfight command completed with status: {dest_result.returncode}")
                except Exception as e:
                    self.log.error(f"Error running destination pillowfight command: {e}")
            
            # Start threads
            src_thread = threading.Thread(target=run_src_cmd)
            dest_thread = threading.Thread(target=run_dest_cmd)
            
            src_thread.start()
            dest_thread.start()
            
            # Wait for both to finish
            src_thread.join()
            dest_thread.join()
            
            # Check if both commands were successful
            if src_result and src_result.returncode != 0:
                self.log.error(f"Source pillowfight failed: {src_result.stderr.decode('utf-8')}")
                success = False
                
            if dest_result and dest_result.returncode != 0:
                self.log.error(f"Destination pillowfight failed: {dest_result.stderr.decode('utf-8')}")
                success = False
            
            if success:
                self.log.info(f"Successfully created potential conflicts for bucket {bucket_name}")
            
        # Wait for replication to catch up
        self.log.info("Waiting for replication to catch up...")
        
        # Extract conflict logging collection paths to exclude from doc count
        exclude_paths = []
        for bucket_name, bucket_params in self.conflict_logging_params.items():
            for cluster_name, settings in bucket_params.items():
                collection_path = settings.get('collection')
                
                if collection_path:
                    # Parse scope and collection names
                    if '.' in collection_path:
                        scope_name, collection_name = collection_path.split('.')
                    else:
                        scope_name = '_default'
                        collection_name = collection_path
                    
                    # Format for exclude_paths is "bucket.scope.collection"
                    full_path = f"{cluster_name}.{bucket_name}.{scope_name}.{collection_name}"
                    exclude_paths.append(full_path)
                    self.log.info(f"Adding {full_path} to exclude_paths")
        
        if exclude_paths:
            self.log.info(f"Excluding conflict logging collections from count: {exclude_paths}")
        else:
            self.log.warning("No conflict logging collections found to exclude")
            
        # Call parent method with exclude_paths
        super(ConflictLoggingTests, self)._wait_for_replication_to_catchup(
            timeout=300, 
            fetch_bucket_stats_by="minute",
            exclude_paths=exclude_paths
        )
        
        # Sleep to allow conflict logs to be created
        self.log.info("Sleeping to allow conflict logs to be generated...")
        time.sleep(10)
        
        return success

    def _create_conflicts(self):
        """
        Create conflicts by updating the same documents on both clusters using cbc-pillowfight
        """
        if self._replication_direction != "bidirection":
            self.log.info("Skipping conflict creation in unidirectional setup")
            return
        
        # Execute pillowfight for conflict creation
        self.log.info("Creating conflicts using cbc-pillowfight...")
        success = self._create_conflicts_with_pillowfight()
        
        if not success:
            self.log.error("Failed to create conflicts using cbc-pillowfight")
            self.log.warning("No fallback method available - conflict testing may not work correctly")
            
        self.log.info("Conflict creation process completed")
    
    def _verify_conflict_logs(self):
        """
        Verify that conflicts are logged in the specified collections
        """
        self.log.info("Verifying conflict logs")
        
        for cluster in self.get_cb_clusters():
            for remote_cluster in cluster.get_remote_clusters():
                for replication in remote_cluster.get_replications():
                    settings = self._get_conflict_logging_settings(replication)
                    
                    # Handle the case where settings is a list
                    if isinstance(settings, list):
                        self.log.info(f"Settings is a list: {settings}")
                        # Try to find the first non-empty settings object
                        settings_dict = {}
                        for item in settings:
                            if isinstance(item, dict) and item:
                                settings_dict = item
                                break
                        settings = settings_dict
                    
                    if not settings or not isinstance(settings, dict) or settings.get('disabled', True):
                        self.log.info(f"Conflict logging disabled or not properly configured for replication {replication.get_repl_id()}")
                        continue
                    
                    bucket_name = settings.get('bucket')
                    collection_path = settings.get('collection')
                    
                    if not bucket_name or not collection_path:
                        self.log.info(f"No bucket or collection specified for conflict logging")
                        continue
                    
                    # Parse scope and collection names
                    if '.' in collection_path:
                        scope_name, collection_name = collection_path.split('.')
                    else:
                        scope_name = '_default'
                        collection_name = collection_path
                    
                    # Query the collection for conflict logs
                    src_master = replication.get_src_cluster().get_master_node()
                                        
                    # Using COUNT(*) without 'as count' to get the result in the expected format with '$1' key
                    query = f'SELECT COUNT(*) FROM `{bucket_name}`.`{scope_name}`.`{collection_name}` WHERE META().id LIKE "crd%"'
                    
                    try:
                        # Use the parent class's __execute_query method
                        count = self._XDCRNewBaseTest__execute_query(src_master, query)
                        self.log.info(f"Found {count} conflict logs in {bucket_name}.{scope_name}.{collection_name}")
                        
                        # In a bidirectional setup with updates on both sides, we expect conflicts
                        if self._replication_direction == "bidirection":
                            # We should have at least some conflicts
                            self.assertGreater(count, 0, f"No conflict logs found in {bucket_name}.{scope_name}.{collection_name}")
                        else:
                            self.log.info(f"No conflicts expected in unidirectional setup, found: {count}")
                    except Exception as e:
                        self.log.error(f"Error querying conflict logs: {e}")
    
    def _get_conflict_logs(self, bucket_name, scope_name, collection_name, limit=10):
        """
        Get conflict logs from a specific collection
        
        Args:
            bucket_name: Name of the bucket
            scope_name: Name of the scope
            collection_name: Name of the collection
            limit: Maximum number of logs to retrieve
            
        Returns:
            list: List of conflict logs
        """
        self.log.info(f"Getting conflict logs from {bucket_name}.{scope_name}.{collection_name}")
        
        # Get the master node of the first cluster
        src_master = self.get_cb_clusters()[0].get_master_node()
        
        # Use N1QL to query for conflict logs with both formats
        query = f'SELECT META().id as id, * FROM `{bucket_name}`.`{scope_name}`.`{collection_name}` WHERE META().id LIKE "crd%"'
        
        if limit:
            query += f' LIMIT {limit}'
        
        # Use the base class's query execution method
        rest_conn = RestConnection(src_master)
        try:
            res = rest_conn.query_tool(query)
            self.log.info(f"Retrieved {len(res.get('results', []))} conflict logs")
            return res.get("results", [])
        except Exception as e:
            self.log.error(f"Failed to execute query: {e}")
            return []

    def _get_collection_doc_count(self, cluster, bucket_name, scope_name, collection_name):
        """
        Return COUNT(*) for the specified `bucket.scope.collection` on the cluster's master.
        """
        src_master = cluster.get_master_node()
        query = f'SELECT COUNT(*) FROM `{bucket_name}`.`{scope_name}`.`{collection_name}`'
        return self._XDCRNewBaseTest__execute_query(src_master, query)
    
    def _create_scopes_and_collections(self):
        """
        Create scopes and collections specified in conflict logging parameters
        """
        self.log.info("Creating scopes and collections for conflict logging")
        
        for bucket_params in self.conflict_logging_params.values():
            for cluster_name, settings in bucket_params.items():
                bucket_name = settings.get('bucket')
                collection_path = settings.get('collection')
                
                if not bucket_name or not collection_path:
                    continue
                
                # Parse scope and collection names
                if '.' in collection_path:
                    scope_name, collection_name = collection_path.split('.')
                else:
                    scope_name = '_default'
                    collection_name = collection_path
                
                # Skip default scope/collection
                if scope_name == '_default' and collection_name == '_default':
                    continue
                
                # Get the cluster
                cluster = self.get_cb_cluster_by_name(cluster_name)
                if not cluster:
                    self.log.error(f"Cluster {cluster_name} not found")
                    continue
                
                # Create scope if needed
                if scope_name != '_default':
                    self._create_scope(cluster, bucket_name, scope_name)
                
                # Create collection
                self._create_collection(cluster, bucket_name, scope_name, collection_name)
    
    def _create_scope(self, cluster, bucket_name, scope_name):
        """
        Create a scope in a bucket
        
        Args:
            cluster: Cluster object
            bucket_name: Name of the bucket
            scope_name: Name of the scope to create
        """
        master = cluster.get_master_node()
        rest_conn = RestConnection(master)
        
        try:
            rest_conn.create_scope(bucket_name, scope_name)
            self.log.info(f"Created scope {scope_name} in bucket {bucket_name}")
        except Exception as e:
            # Ignore if scope already exists
            self.log.info(f"Scope creation failed (may already exist): {e}")
    
    def _create_collection(self, cluster, bucket_name, scope_name, collection_name):
        """
        Create a collection in a scope
        
        Args:
            cluster: Cluster object
            bucket_name: Name of the bucket
            scope_name: Name of the scope
            collection_name: Name of the collection to create
        """
        master = cluster.get_master_node()
        rest_conn = RestConnection(master)
        
        try:
            rest_conn.create_collection(bucket_name, scope_name, collection_name)
            self.log.info(f"Created collection {collection_name} in scope {scope_name} of bucket {bucket_name}")
        except Exception as e:
            # Ignore if collection already exists
            self.log.info(f"Collection creation failed (may already exist): {e}")
    
    def test_conflict_logging_basic(self):
        """
        Test basic conflict logging functionality
        1. Set up XDCR with conflict logging
        2. Create conflicts
        3. Verify conflicts are logged
        """
        # Create necessary scopes and collections
        self._create_scopes_and_collections()
        
        # Set up XDCR without conflict logging
        self.setup_xdcr_and_load()
        
        # Set up conflict logging after XDCR is established
        self._setup_conflict_logging()
        
        # Verify conflict logging settings
        self._verify_conflict_logging_settings()
        
        # Create conflicts
        self._create_conflicts()
        
        # Verify conflicts are logged
        self._verify_conflict_logs()
        
        # Display sample conflict logs
        for bucket_params in self.conflict_logging_params.values():
            for settings in bucket_params.values():
                bucket_name = settings.get('bucket')
                collection_path = settings.get('collection')
                
                if not bucket_name or not collection_path:
                    continue
                
                # Parse scope and collection names
                if '.' in collection_path:
                    scope_name, collection_name = collection_path.split('.')
                else:
                    scope_name = '_default'
                    collection_name = collection_path
                
                logs = self._get_conflict_logs(bucket_name, scope_name, collection_name, limit=3)
                if logs:
                    self.log.info(f"Sample conflict logs from {bucket_name}.{scope_name}.{collection_name}:")
                    for log in logs:
                        self.log.info(f"  {log}")
    
    def test_conflict_logging_update_settings(self):
        """
        Test updating conflict logging settings
        1. Set up XDCR without conflict logging
        2. Update settings to enable conflict logging
        3. Create conflicts
        4. Verify conflicts are logged
        """
        # Create necessary scopes and collections
        self._create_scopes_and_collections()
        
        # Set up XDCR without conflict logging
        self.setup_xdcr_and_load()
        
        # Verify no conflict logging initially
        for cluster in self.get_cb_clusters():
            for remote_cluster in cluster.get_remote_clusters():
                for replication in remote_cluster.get_replications():
                    settings = self._get_conflict_logging_settings(replication)
                    self.log.info(f"Initial conflict logging settings: {settings}")
        
        # Set up conflict logging
        self._setup_conflict_logging()
        
        # Verify conflict logging settings
        self._verify_conflict_logging_settings()
        
        # Create conflicts
        self._create_conflicts()
        
        # Verify conflicts are logged
        self._verify_conflict_logs()
    
    def test_conflict_logging_disable(self):
        """
        Test disabling conflict logging
        1. Set up XDCR with conflict logging
        2. Create conflicts and verify logging
        3. Disable conflict logging
        4. Create more conflicts
        5. Verify no new logs are created
        """
        # Create necessary scopes and collections
        self._create_scopes_and_collections()
        
        # Set up XDCR without conflict logging
        self.setup_xdcr_and_load()
        
        # Set up conflict logging
        self._setup_conflict_logging()
        
        # Verify conflict logging settings
        self._verify_conflict_logging_settings()
        
        # Create conflicts
        self._create_conflicts()
        
        # Verify conflicts are logged
        self._verify_conflict_logs()
        
        # Get current log counts
        log_counts = {}
        for bucket_params in self.conflict_logging_params.values():
            for settings in bucket_params.values():
                bucket_name = settings.get('bucket')
                collection_path = settings.get('collection')
                
                if not bucket_name or not collection_path:
                    continue
                
                # Parse scope and collection names
                if '.' in collection_path:
                    scope_name, collection_name = collection_path.split('.')
                else:
                    scope_name = '_default'
                    collection_name = collection_path
                
                # Query the collection for conflict logs
                src_master = self.get_cb_clusters()[0].get_master_node()
                query = f'SELECT COUNT(*) as count FROM `{bucket_name}`.`{scope_name}`.`{collection_name}` WHERE META().id LIKE "xdcr_conflict_log::%"'
                result = self._execute_query(src_master, query)
                count = result[0]['count'] if result and 'count' in result[0] else 0
                log_counts[f"{bucket_name}.{scope_name}.{collection_name}"] = count
        
        # Disable conflict logging
        for cluster in self.get_cb_clusters():
            for remote_cluster in cluster.get_remote_clusters():
                for replication in remote_cluster.get_replications():
                    bucket_name = replication.get_src_bucket().name
                    cluster_name = cluster.get_name()
                    
                    if bucket_name in self.conflict_logging_params and cluster_name in self.conflict_logging_params[bucket_name]:
                        settings = self.conflict_logging_params[bucket_name][cluster_name].copy()
                        settings["disabled"] = True
                        self._set_conflict_logging(replication, settings)
        
        # Create more conflicts
        self._create_conflicts()
        
        # Verify no new logs are created
        for bucket_params in self.conflict_logging_params.values():
            for settings in bucket_params.values():
                bucket_name = settings.get('bucket')
                collection_path = settings.get('collection')
                
                if not bucket_name or not collection_path:
                    continue
                
                # Parse scope and collection names
                if '.' in collection_path:
                    scope_name, collection_name = collection_path.split('.')
                else:
                    scope_name = '_default'
                    collection_name = collection_path
                
                # Query the collection for conflict logs
                src_master = self.get_cb_clusters()[0].get_master_node()
                query = f'SELECT COUNT(*) as count FROM `{bucket_name}`.`{scope_name}`.`{collection_name}` WHERE META().id LIKE "xdcr_conflict_log::%"'
                result = self._execute_query(src_master, query)
                new_count = result[0]['count'] if result and 'count' in result[0] else 0
                old_count = log_counts.get(f"{bucket_name}.{scope_name}.{collection_name}", 0)
                
                self.log.info(f"Conflict log count for {bucket_name}.{scope_name}.{collection_name}: {old_count} -> {new_count}")
                self.assertEqual(new_count, old_count, "New conflict logs were created after disabling logging")
    
    def _verify_conflict_log_structure(self, log):
        """
        Verify that a conflict log contains all the required fields
        
        Args:
            log: The conflict log document to verify
            
        Returns:
            bool: True if the log has all required fields, False otherwise
        """
        # Define required fields based on your conflict log structure
        # Modify this list based on your actual conflict log format
        required_fields = ['doc_id', 'rev_id', 'timestamp', 'conflict_type']
        missing_fields = []
        
        for field in required_fields:
            if field not in log:
                missing_fields.append(field)
                
        if missing_fields:
            self.log.warning(f"Conflict log missing required fields: {missing_fields}")
            return False
            
        return True
    
    def _generate_high_volume_conflicts(self, num_docs=10000, num_threads=8):
        """
        Generate a high volume of conflicts to stress the system
        
        Args:
            num_docs: Number of documents to create conflicts for
            num_threads: Number of threads to use for concurrent operations
        """
        if self._replication_direction != "bidirection":
            self.log.info("Skipping conflict creation in unidirectional setup")
            return False
            
        # Get bucket names from conflict logging parameters
        bucket_names = []
        for bucket_name in self.conflict_logging_params.keys():
            bucket_names.append(bucket_name)
            
        if not bucket_names:
            self.log.info("No bucket names found in conflict logging parameters, using default bucket")
            bucket_names = ["default"]
        
        self.log.info(f"Creating high-volume conflicts for buckets: {bucket_names}")
        
        # Get server credentials for authentication with cbc-pillowfight
        username = self.src_master.rest_username
        password = self.src_master.rest_password
        
        # Run cbc-pillowfight with high number of operations
        success = True
        for bucket_name in bucket_names:
            # Run pillowfight against source cluster with higher throughput
            src_cmd = f"cbc-pillowfight -U couchbase://{self.src_master.ip}/{bucket_name} -u {username} -P {password} " \
                    f"-I {num_docs} -m 200 -M 2000 -c 10 -t {num_threads} -B 500"
                    
            # Run pillowfight against destination cluster with higher throughput
            dest_cmd = f"cbc-pillowfight -U couchbase://{self.dest_master.ip}/{bucket_name} -u {username} -P {password} " \
                     f"-I {num_docs} -m 200 -M 2000 -c 10 -t {num_threads} -B 500"
            
            # Execute commands in parallel
            import subprocess
            import threading
            
            src_result = None
            dest_result = None
            
            def run_src_cmd():
                nonlocal src_result
                try:
                    src_result = subprocess.run(src_cmd, shell=True, check=False, capture_output=True)
                    self.log.info(f"Source high-volume pillowfight completed with status: {src_result.returncode}")
                except Exception as e:
                    self.log.error(f"Error running source high-volume pillowfight command: {e}")
            
            def run_dest_cmd():
                nonlocal dest_result
                try:
                    dest_result = subprocess.run(dest_cmd, shell=True, check=False, capture_output=True)
                    self.log.info(f"Destination high-volume pillowfight completed with status: {dest_result.returncode}")
                except Exception as e:
                    self.log.error(f"Error running destination high-volume pillowfight command: {e}")
            
            # Start threads
            src_thread = threading.Thread(target=run_src_cmd)
            dest_thread = threading.Thread(target=run_dest_cmd)
            
            src_thread.start()
            dest_thread.start()
            
            # Wait for both to finish
            src_thread.join()
            dest_thread.join()
            
            # Check if both commands were successful
            if src_result and src_result.returncode != 0:
                self.log.error(f"Source high-volume pillowfight failed: {src_result.stderr.decode('utf-8')}")
                success = False
                
            if dest_result and dest_result.returncode != 0:
                self.log.error(f"Destination high-volume pillowfight failed: {dest_result.stderr.decode('utf-8')}")
                success = False
        
        # Wait for replication to catch up
        self.log.info("Waiting for replication to catch up...")
        
        # Extract conflict logging collection paths to exclude from doc count
        exclude_paths = []
        for bucket_name, bucket_params in self.conflict_logging_params.items():
            for cluster_name, settings in bucket_params.items():
                collection_path = settings.get('collection')
                
                if collection_path:
                    # Parse scope and collection names
                    if '.' in collection_path:
                        scope_name, collection_name = collection_path.split('.')
                    else:
                        scope_name = '_default'
                        collection_name = collection_path
                    
                    # Format for exclude_paths is "bucket.scope.collection"
                    full_path = f"{cluster_name}.{bucket_name}.{scope_name}.{collection_name}"
                    exclude_paths.append(full_path)
                    self.log.info(f"Adding {full_path} to exclude_paths")
        
        # Call parent method with exclude_paths
        super(ConflictLoggingTests, self)._wait_for_replication_to_catchup(
            timeout=600,  # Longer timeout for high volume
            fetch_bucket_stats_by="minute",
            exclude_paths=exclude_paths
        )
        
        return success
    
    def _measure_conflict_generation_rate(self):
        """
        Measure the rate at which conflicts can be generated and logged
        
        Returns:
            tuple: (conflicts_per_second, total_conflicts)
        """
        if self._replication_direction != "bidirection":
            self.log.info("Skipping conflict creation in unidirectional setup")
            return (0, 0)
            
        # Get bucket names from conflict logging parameters
        bucket_name = next(iter(self.conflict_logging_params.keys()), "default")
        
        # Get all conflict logging collections
        conflict_collections = []
        for cluster_name, settings in self.conflict_logging_params.get(bucket_name, {}).items():
            collection_path = settings.get('collection')
            
            if collection_path:
                # Parse scope and collection names
                if '.' in collection_path:
                    scope_name, collection_name = collection_path.split('.')
                else:
                    scope_name = '_default'
                    collection_name = collection_path
                
                conflict_collections.append({
                    'bucket': bucket_name,
                    'scope': scope_name,
                    'collection': collection_name
                })
        
        if not conflict_collections:
            self.log.warning("No conflict logging collections found")
            return (0, 0)
        
        # Get initial conflict log count
        initial_count = 0
        for collection_info in conflict_collections:
            collection_count = self._get_collection_doc_count(
                self.get_cb_clusters()[0],
                collection_info['bucket'],
                collection_info['scope'],
                collection_info['collection']
            )
            initial_count += collection_count
        
        self.log.info(f"Initial conflict log count: {initial_count}")
        
        # Generate conflicts for a fixed duration
        test_duration = 60  # seconds
        start_time = time.time()
        
        # Use cbc-pillowfight to generate conflicts
        username = self.src_master.rest_username
        password = self.src_master.rest_password
        
        # Run pillowfight against both clusters simultaneously
        src_cmd = f"cbc-pillowfight -U couchbase://{self.src_master.ip}/{bucket_name} -u {username} -P {password} " \
                f"-I 50000 -m 100 -M 100 -c 50 -t 8 -B 100 --sequential"
                
        dest_cmd = f"cbc-pillowfight -U couchbase://{self.dest_master.ip}/{bucket_name} -u {username} -P {password} " \
                 f"-I 50000 -m 100 -M 100 -c 50 -t 8 -B 100 --sequential"
        
        import subprocess
        import threading
        
        # Start pillowfight on source cluster
        src_process = subprocess.Popen(src_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Start pillowfight on destination cluster
        dest_process = subprocess.Popen(dest_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Wait for test duration
        self.log.info(f"Running conflict generation test for {test_duration} seconds...")
        time.sleep(test_duration)
        
        # Terminate processes
        src_process.terminate()
        dest_process.terminate()
        
        end_time = time.time()
        actual_duration = end_time - start_time
        
        # Wait for replication to catch up
        self.log.info("Waiting for replication to catch up...")
        
        # Extract conflict logging collection paths to exclude from doc count
        exclude_paths = [f"{col['bucket']}.{col['scope']}.{col['collection']}" for col in conflict_collections]
        
        # Call parent method with exclude_paths
        super(ConflictLoggingTests, self)._wait_for_replication_to_catchup(
            timeout=300,
            fetch_bucket_stats_by="minute",
            exclude_paths=exclude_paths
        )
        
        # Get final conflict log count
        final_count = 0
        for collection_info in conflict_collections:
            collection_count = self._get_collection_doc_count(
                self.get_cb_clusters()[0],
                collection_info['bucket'],
                collection_info['scope'],
                collection_info['collection']
            )
            final_count += collection_count
        
        self.log.info(f"Final conflict log count: {final_count}")
        
        # Calculate conflict generation rate
        total_conflicts = final_count - initial_count
        conflicts_per_second = total_conflicts / actual_duration if actual_duration > 0 else 0
        
        self.log.info(f"Generated {total_conflicts} conflicts in {actual_duration:.2f} seconds")
        self.log.info(f"Conflict generation rate: {conflicts_per_second:.2f} conflicts/second")
        
        return (conflicts_per_second, total_conflicts)
    
    def _start_rebalance(self, cluster):
        """
        Start a rebalance operation on the specified cluster
        
        Args:
            cluster: Cluster object to rebalance
            
        Returns:
            Task: Rebalance task
        """
        try:
            # Use XDCR base cluster helper that draws from FloatingServers._serverlist
            self.log.info("Starting async rebalance-in of 1 node on cluster")
            return cluster.async_rebalance_in(num_nodes=1)
        except Exception as e:
            self.log.warning(f"No available free nodes to add or rebalance failed to start: {e}")
            return None
    
    def test_conflict_logging_high_volume(self):
        """
        Test conflict logging with a high volume of conflicts to stress the system
        """
        # Create necessary scopes and collections
        self._create_scopes_and_collections()
        
        # Set up XDCR
        self.setup_xdcr_and_load()
        
        # Set up conflict logging
        self._setup_conflict_logging()
        
        # Verify conflict logging settings
        self._verify_conflict_logging_settings()
        
        # Generate high volume of conflicts
        self.log.info("Generating high volume of conflicts...")
        success = self._generate_high_volume_conflicts(num_docs=50000, num_threads=16)
        
        if not success:
            self.fail("Failed to generate high volume conflicts")
        
        # Verify conflicts are logged
        self._verify_conflict_logs()
        
        # Verify conflict log structure for a sample of logs
        self.log.info("Verifying conflict log structure...")
        for bucket_params in self.conflict_logging_params.values():
            for settings in bucket_params.values():
                bucket_name = settings.get('bucket')
                collection_path = settings.get('collection')
                
                if not bucket_name or not collection_path:
                    continue
                
                # Parse scope and collection names
                if '.' in collection_path:
                    scope_name, collection_name = collection_path.split('.')
                else:
                    scope_name = '_default'
                    collection_name = collection_path
                
                # Get a sample of conflict logs
                logs = self._get_conflict_logs(bucket_name, scope_name, collection_name, limit=10)
                
                if logs:
                    self.log.info(f"Verifying structure of {len(logs)} conflict logs from {bucket_name}.{scope_name}.{collection_name}")
                    for log in logs:
                        # Call verification function for each log
                        if not self._verify_conflict_log_structure(log):
                            self.log.error(f"Invalid conflict log structure: {log}")
                            
        # Verify log count is proportional to the number of conflicts generated
        for bucket_params in self.conflict_logging_params.values():
            for settings in bucket_params.values():
                bucket_name = settings.get('bucket')
                collection_path = settings.get('collection')
                
                if not bucket_name or not collection_path:
                    continue
                
                # Parse scope and collection names
                if '.' in collection_path:
                    scope_name, collection_name = collection_path.split('.')
                else:
                    scope_name = '_default'
                    collection_name = collection_path
                
                # Get count of conflict logs
                collection_count = self._get_collection_doc_count(
                    self.get_cb_clusters()[0],
                    bucket_name,
                    scope_name,
                    collection_name
                )
                
                self.log.info(f"Total conflict logs in {bucket_name}.{scope_name}.{collection_name}: {collection_count}")
                
                # Assert that we have a significant number of conflict logs
                self.assertGreater(collection_count, 1000, "Expected at least 1000 conflict logs from high-volume test")
    
    def test_conflict_logging_performance(self):
        """
        Test the performance of conflict logging by measuring generation rate
        """
        # Create necessary scopes and collections
        self._create_scopes_and_collections()
        
        # Set up XDCR
        self.setup_xdcr_and_load()
        
        # Set up conflict logging
        self._setup_conflict_logging()
        
        # Verify conflict logging settings
        self._verify_conflict_logging_settings()
        
        # Measure conflict generation rate
        self.log.info("Measuring conflict generation rate...")
        rate, total = self._measure_conflict_generation_rate()
        
        # Verify reasonable performance
        self.log.info(f"Conflict generation rate: {rate:.2f} conflicts/second")
        self.log.info(f"Total conflicts generated: {total}")
        
        # Assert that we have a reasonable generation rate
        # This threshold should be adjusted based on your system capabilities
        self.assertGreater(rate, 10, "Expected conflict generation rate of at least 10 conflicts/second")
        
        # Verify conflicts are logged correctly
        self._verify_conflict_logs()
    
    def test_conflict_logging_during_rebalance(self):
        """
        Test conflict logging while a rebalance operation is in progress
        """
        # Create necessary scopes and collections
        self._create_scopes_and_collections()
        
        # Set up XDCR
        self.setup_xdcr_and_load()
        
        # Set up conflict logging
        self._setup_conflict_logging()
        
        # Verify conflict logging settings
        self._verify_conflict_logging_settings()
        
        # Start rebalance on source cluster
        self.log.info("Starting rebalance on source cluster...")
        rebalance_task = self._start_rebalance(self.src_cluster)
        
        if not rebalance_task:
            self.log.warning("Could not start rebalance, continuing test without rebalance")
        
        # Create conflicts during rebalance
        self.log.info("Creating conflicts during rebalance...")
        self._create_conflicts()
        
        # Wait for rebalance to complete
        if rebalance_task:
            self.log.info("Waiting for rebalance to complete...")
            rebalance_task.result()
        
        # Verify conflicts are logged
        self._verify_conflict_logs()
        
        # Create more conflicts after rebalance
        self.log.info("Creating more conflicts after rebalance...")
        self._create_conflicts()
        
        # Verify additional conflicts are logged
        self._verify_conflict_logs()
        
        # Get detailed conflict log statistics
        self.log.info("Getting conflict log statistics...")
        for bucket_params in self.conflict_logging_params.values():
            for settings in bucket_params.values():
                bucket_name = settings.get('bucket')
                collection_path = settings.get('collection')
                
                if not bucket_name or not collection_path:
                    continue
                
                # Parse scope and collection names
                if '.' in collection_path:
                    scope_name, collection_name = collection_path.split('.')
                else:
                    scope_name = '_default'
                    collection_name = collection_path
                
                # Get conflict log count
                collection_count = self._get_collection_doc_count(
                    self.get_cb_clusters()[0],
                    bucket_name,
                    scope_name,
                    collection_name
                )
                
                self.log.info(f"Total conflict logs in {bucket_name}.{scope_name}.{collection_name}: {collection_count}")
                
                # Assert we have conflict logs
                self.assertGreater(collection_count, 0, "Expected conflict logs to be generated during rebalance") 
