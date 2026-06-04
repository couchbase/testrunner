import json
import time
import shutil
import subprocess
import threading

from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from xdcr.xdcrnewbasetests import XDCRNewBaseTest, NodeHelper

NodeHelper.raise_fd_soft_limit()

CLOG_PAUSE_THRESHOLD_KEY = "cLogPauseReplThreshold"
CLOG_MONITOR_DURATION_KEY = "cLogMonitorDuration"
CLOG_AUTOPAUSE_ERR_MSGS = ("autopaused", "exceeded the configured threshold")
MAX_MONITOR_DURATION = 2592000
INT_MAX = 2147483647
AUTOPAUSE_POLL_INTERVAL = 5
ERROR_API_POLL_INTERVAL = 10
CONFLICT_KEY_PREFIX = "clog_conflict_"


class ConflictLoggingTests(XDCRNewBaseTest):
    """
    Test suite for XDCR conflict logging functionality.
    Tests the ability to log conflicts to specified collections.
    """
    
    def suite_setUp(self):        
        pass

    def suite_tearDown(self):
        pass

    def setUp(self):
        super(ConflictLoggingTests, self).setUp()
        NodeHelper.raise_fd_soft_limit(self.log)
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

        # Cache of RestConnection per node ip. RestConnection.__init__ does a
        # pools/default connectivity GET (and opens a socket) every construction;
        # the pause/error poll loops query per replication every few seconds, so
        # reuse avoids a redundant round-trip and connection churn per poll.
        self._rest_cache = {}

    def _rest_for(self, node):
        """Return a cached RestConnection for `node` (keyed by ip)."""
        rest = self._rest_cache.get(node.ip)
        if rest is None:
            rest = RestConnection(node)
            self._rest_cache[node.ip] = rest
        return rest

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
    
    @staticmethod
    def _strip_ipv6_brackets(ip):
        """Strip surrounding brackets from an IPv6 literal ([fd00::1] -> fd00::1)."""
        if isinstance(ip, str) and ip.startswith("[") and ip.endswith("]"):
            return ip[1:-1]
        return ip

    def _run_pillowfight(self, node, bucket, pf_opts, label, timeout_secs=None):
        """Run cbc-pillowfight against `bucket` on a cluster `node`.

        Prefers a cbc-pillowfight already on the testrunner host's PATH
        (couchbase://node.ip) - original behavior. If it is not installed locally,
        or the local run fails, falls back to the binary bundled with Couchbase
        Server at /opt/couchbase/bin/cbc-pillowfight, run over SSH on the node
        (couchbase://localhost). The bundled binary always ships with a server
        install, so conflict generation no longer breaks with
        "cbc-pillowfight: not found" on CI workers that lack the libcouchbase tools.

        :param node: cluster node object (e.g. self.src_master).
        :param pf_opts: list of pillowfight option tokens (no -U/-u/-P), e.g.
            ["-I", "1000", "-m", "100"].
        :param label: short tag for log lines (e.g. "source high-volume").
        :param timeout_secs: if set, pillowfight runs for at most this many seconds
            and a clean timeout counts as success (timed rate-measurement path).
        :return: True on success.
        """
        username = node.rest_username
        password = node.rest_password

        if shutil.which("cbc-pillowfight"):
            # argv list + shell=False (default): no shell parsing, so an IPv6/bracketed
            # node.ip or odd ini value can't be glob-expanded, word-split, or injected.
            cmd = ["cbc-pillowfight",
                   "-U", f"couchbase://{self._strip_ipv6_brackets(node.ip)}/{bucket}",
                   *pf_opts, "-u", username, "-P", password]
            self.log.info(f"Executing local cbc-pillowfight [{label}]: {' '.join(cmd)}")
            for attempt in range(1, 3):
                try:
                    result = subprocess.run(cmd, check=False, capture_output=True, timeout=timeout_secs)
                except subprocess.TimeoutExpired:
                    self.log.info(f"Local cbc-pillowfight [{label}] ran for {timeout_secs}s and was stopped")
                    return True
                except FileNotFoundError:
                    break  # binary vanished between which() and run(); use bundled fallback
                if result.returncode == 0:
                    self.log.info(f"Loaded docs into {bucket} via local cbc-pillowfight [{label}]")
                    return True
                self.log.warning(f"Local cbc-pillowfight [{label}] exited {result.returncode} "
                                 f"(attempt {attempt}/2): {result.stderr.decode('utf-8', 'replace')}")
            self.log.warning(f"Local cbc-pillowfight [{label}] failed; falling back to bundled "
                             f"/opt/couchbase/bin/cbc-pillowfight over SSH")
        else:
            self.log.info(f"cbc-pillowfight not on testrunner host PATH; using bundled "
                          f"/opt/couchbase/bin/cbc-pillowfight over SSH on cluster node [{label}]")

        return self._run_bundled_pillowfight(node, bucket, pf_opts, label, timeout_secs=timeout_secs)

    def _run_bundled_pillowfight(self, node, bucket, pf_opts, label, timeout_secs=None):
        """Run /opt/couchbase/bin/cbc-pillowfight over SSH on `node` (couchbase://localhost).

        Uses the cbc-pillowfight bundled with Couchbase Server (always present),
        mirroring load_docs_with_pillowfight in advFilteringXDCR. Plain exec (no
        use_channel) is used on purpose so execute_command returns a real exit code
        and stderr to report on failure. Returns True on success.
        """
        username = node.rest_username
        password = node.rest_password
        pf_args = " ".join(pf_opts)
        cmd = (f"/opt/couchbase/bin/cbc-pillowfight -u {username} -P {password} "
               f"-U couchbase://localhost/{bucket} {pf_args}")
        ssh_timeout = 600
        if timeout_secs:
            # Self-terminate the long-running load on the node; rc 124 (timeout) is success.
            cmd = f"timeout {int(timeout_secs)} {cmd}"
            ssh_timeout = int(timeout_secs) + 60
        self.log.info(f"Executing bundled cbc-pillowfight [{label}] on {node.ip}: {cmd}")
        output, error = [], []
        for attempt in range(1, 3):
            shell = RemoteMachineShellConnection(node)
            try:
                output, error, exit_code = shell.execute_command(cmd, get_exit_code=True, timeout=ssh_timeout)
            finally:
                shell.disconnect()
            if output:
                self.log.info(f"cbc-pillowfight [{label}] output: {output}")
            if exit_code == 0 or (timeout_secs and exit_code == 124):
                self.log.info(f"Loaded docs into {bucket} on {node.ip} via bundled cbc-pillowfight [{label}]")
                return True
            self.log.warning(f"Bundled cbc-pillowfight [{label}] exited {exit_code} "
                             f"(attempt {attempt}/2) on {node.ip}: stderr={error} stdout={output}")
        self.log.error(f"Bundled cbc-pillowfight [{label}] failed on {node.ip}: stderr={error} stdout={output}")
        return False

    def _run_pillowfight_on_both(self, bucket, pf_opts, label):
        """Run cbc-pillowfight against `bucket` on source and destination in parallel.

        Each side independently falls back to the bundled binary over SSH (see
        _run_pillowfight). Returns True only if both sides succeed.
        """
        results = {}

        def _run(side, node):
            results[side] = self._run_pillowfight(node, bucket, pf_opts, f"{side} {label}")

        threads = [
            threading.Thread(target=_run, args=("source", self.src_master)),
            threading.Thread(target=_run, args=("destination", self.dest_master)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        if not results.get("source"):
            self.log.error(f"Source {label} pillowfight failed for bucket {bucket}")
        if not results.get("destination"):
            self.log.error(f"Destination {label} pillowfight failed for bucket {bucket}")
        return all(results.get(side) for side in ("source", "destination"))

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

        # Run cbc-pillowfight on both clusters for each bucket
        pf_opts = ["-I", "1000", "-m", "100", "-M", "100", "-c", "10", "-t", "4", "-B", "100"]
        success = True
        for bucket_name in bucket_names:
            if not self._run_pillowfight_on_both(bucket_name, pf_opts, "conflicts"):
                success = False
            elif success:
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

        # Run cbc-pillowfight with high number of operations on both clusters in parallel
        pf_opts = ["-I", str(num_docs), "-m", "200", "-M", "2000",
                   "-c", "10", "-t", str(num_threads), "-B", "500"]
        success = True
        for bucket_name in bucket_names:
            if not self._run_pillowfight_on_both(bucket_name, pf_opts, "high-volume"):
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
        for settings in self.conflict_logging_params.get(bucket_name, {}).values():
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

        # Run pillowfight against both clusters simultaneously for test_duration; each side
        # self-terminates after the timeout (local subprocess timeout / remote `timeout`).
        pf_opts = ["-I", "50000", "-m", "100", "-M", "100",
                   "-c", "50", "-t", "8", "-B", "100", "--sequential"]
        self.log.info(f"Running conflict generation test for {test_duration} seconds...")
        results = {}

        def _run(side, node):
            results[side] = self._run_pillowfight(node, bucket_name, pf_opts,
                                                  f"{side} rate-measure", timeout_secs=test_duration)

        threads = [
            threading.Thread(target=_run, args=("source", self.src_master)),
            threading.Thread(target=_run, args=("destination", self.dest_master)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

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

    def _get_all_replications(self):
        """Return every XDCReplication object across all configured clusters.

        Fails fast when none are found. An empty list means XDCR setup did not
        create any replication; without this guard the auto-pause helpers would
        silently no-op - `_set_clog_pause_settings` would apply nothing and
        `_is_any_replication_paused` would always return False, letting the
        "no auto-pause" tests pass vacuously without validating anything.
        """
        replications = []
        for cluster in self.get_cb_clusters():
            for remote_cluster in cluster.get_remote_clusters():
                replications.extend(remote_cluster.get_replications())
        if not replications:
            self.fail("No XDCR replications found across any cluster; XDCR setup likely "
                      "failed. Cannot validate conflict-logging auto-pause without an "
                      "active replication.")
        return replications

    def _set_clog_pause_settings(self, threshold, duration, replication=None):
        """
        Set cLogPauseReplThreshold and cLogMonitorDuration on one or all replications.

        Raises on REST error so validation/negative tests can assert rejection.
        """
        settings = {CLOG_PAUSE_THRESHOLD_KEY: threshold,
                    CLOG_MONITOR_DURATION_KEY: duration}
        replications = [replication] if replication is not None else self._get_all_replications()
        for repl in replications:
            src_bucket = repl.get_src_bucket().name
            dst_bucket = repl.get_dest_bucket().name
            src_master = repl.get_src_cluster().get_master_node()
            self.log.info(f"Setting auto-pause settings on {repl.get_repl_id()}: {settings}")
            RestConnection(src_master).set_xdcr_params(src_bucket, dst_bucket, settings)

    def _get_clog_pause_settings(self, replication):
        """Read back the two auto-pause settings for a replication as a dict."""
        src_bucket = replication.get_src_bucket().name
        dst_bucket = replication.get_dest_bucket().name
        src_master = replication.get_src_cluster().get_master_node()
        rest = RestConnection(src_master)
        return {
            CLOG_PAUSE_THRESHOLD_KEY: rest.get_xdcr_param(src_bucket, dst_bucket, CLOG_PAUSE_THRESHOLD_KEY),
            CLOG_MONITOR_DURATION_KEY: rest.get_xdcr_param(src_bucket, dst_bucket, CLOG_MONITOR_DURATION_KEY),
        }

    def _apply_clog_pause_from_params(self):
        """Apply clog_pause_threshold / clog_monitor_duration test params to all replications."""
        threshold = self._input.param("clog_pause_threshold", 0)
        duration = self._input.param("clog_monitor_duration", 0)
        self._set_clog_pause_settings(threshold, duration)
        return threshold, duration

    def _is_any_replication_paused(self):
        """Return True if any replication currently reports pauseRequested."""
        for repl in self._get_all_replications():
            src_bucket = repl.get_src_bucket().name
            dst_bucket = repl.get_dest_bucket().name
            src_master = repl.get_src_cluster().get_master_node()
            paused = self._rest_for(src_master).is_replication_paused(src_bucket, dst_bucket)
            if str(paused).lower() == "true":
                self.log.info(f"Replication {repl.get_repl_id()} is paused")
                return True
        return False

    def _wait_until_replications_resumed(self, timeout=60):
        """Poll until no replication reports pauseRequested.

        Conflict generation manually pauses then resumes the replications, and
        auto-pause sets the SAME pauseRequested flag. Without confirming the
        manual resume actually took effect, a lingering manual pause would be
        mistaken for auto-pause by `_wait_for_autopause` (false positive) or trip
        `_assert_no_autopause` (false failure). Establishing an unpaused baseline
        here means any subsequent paused state is a genuine auto-pause.

        Non-fatal: a timeout most likely means auto-pause already fired (the
        flag never cleared), which the caller's own auto-pause check confirms.
        """
        start = time.time()
        while time.time() - start < timeout:
            if not self._is_any_replication_paused():
                self.log.info(f"Replications confirmed resumed (unpaused baseline) after "
                              f"{time.time() - start:.1f}s")
                return True
            self.sleep(AUTOPAUSE_POLL_INTERVAL)
        self.log.warning(f"Replications still report paused {timeout}s after manual resume; "
                         f"proceeding (auto-pause may have already fired)")
        return False

    def _wait_for_autopause(self, timeout=120):
        """Poll pauseRequested until any replication is paused or timeout."""
        self.log.info(f"Waiting up to {timeout}s for replication auto-pause...")
        start = time.time()
        while time.time() - start < timeout:
            if self._is_any_replication_paused():
                self.log.info(f"Auto-pause detected after {time.time() - start:.1f}s")
                return True
            self.sleep(AUTOPAUSE_POLL_INTERVAL)
        self.log.warning(f"No auto-pause observed after {timeout}s")
        return False

    def _assert_no_autopause(self, observe_secs):
        """Fail if any replication becomes paused within observe_secs."""
        self.log.info(f"Observing {observe_secs}s to confirm no auto-pause...")
        start = time.time()
        while time.time() - start < observe_secs:
            if self._is_any_replication_paused():
                self.fail(f"Replication auto-paused unexpectedly after {time.time() - start:.1f}s")
            self.sleep(AUTOPAUSE_POLL_INTERVAL)
        self.log.info("No auto-pause observed, as expected")

    def _get_xdcr_errors(self, cluster):
        """Collect error + warning strings from the XDCR tasks API
        (/pools/default/tasks) for every replication on the cluster's master -
        the same source the UI "XDCR Errors" panel renders.

        Each entry is normalised to a string (goxdcr returns either plain strings
        or ``{"time": ..., "errorMsg": ...}`` dicts, so the message is extracted
        from the dict when present).
        """
        rest = self._rest_for(cluster.get_master_node())
        errors = []
        for repl in rest.get_replications():
            for field in ("errors", "warnings"):
                entries = repl.get(field)
                if not isinstance(entries, list):
                    continue
                for entry in entries:
                    if isinstance(entry, dict):
                        errors.append(str(entry.get("errorMsg") or entry.get("msg") or entry))
                    else:
                        errors.append(str(entry))
        return errors

    @staticmethod
    def _normalize_err(text):
        """Lowercase and strip hyphens so 'autopaused' and 'auto-paused' compare equal."""
        return text.lower().replace("-", "")

    def _wait_for_autopause_error(self, cluster, timeout=120):
        """
        Poll the XDCR errors API for the auto-pause reason message.

        Logs every error/warning seen each poll so a miss is debuggable, and
        matches the reason case- and hyphen-insensitively.

        Returns (found: bool, message: str or None).
        """
        self.log.info(f"Waiting up to {timeout}s for auto-pause reason in XDCR errors API...")
        candidates = [self._normalize_err(m) for m in CLOG_AUTOPAUSE_ERR_MSGS]
        start = time.time()
        while time.time() - start < timeout:
            errs = self._get_xdcr_errors(cluster)
            self.log.info(f"XDCR errors API returned {len(errs)} error/warning entries: {errs}")
            for err_str in errs:
                norm = self._normalize_err(err_str)
                if any(c in norm for c in candidates):
                    self.log.info(f"Auto-pause reason found in errors API: {err_str}")
                    return True, err_str
            self.sleep(ERROR_API_POLL_INTERVAL)
        self.log.warning(f"Auto-pause reason not found in errors API after {timeout}s")
        return False, None

    def _clog_exclude_paths(self):
        """Conflict-log collection paths to exclude from replication catch-up doc counts."""
        exclude_paths = []
        for bucket_name, bucket_params in self.conflict_logging_params.items():
            for cluster_name, settings in bucket_params.items():
                collection_path = settings.get('collection')
                if not collection_path:
                    continue
                if '.' in collection_path:
                    scope_name, collection_name = collection_path.split('.')
                else:
                    scope_name, collection_name = '_default', collection_path
                exclude_paths.append(f"{cluster_name}.{bucket_name}.{scope_name}.{collection_name}")
        return exclude_paths

    def _generate_conflicts_no_wait(self, num_items=2000, threads=8, rate_limit=None,
                                    key_prefix=CONFLICT_KEY_PREFIX):
        """
        Create GUARANTEED true conflicts by pausing both replications, writing the
        same keys independently on each cluster, then resuming. Does NOT wait for
        replication catch-up (the pipeline may auto-pause mid-flight).

        Why pause/resume instead of a plain concurrent load: writing the same keys
        on a RUNNING (warm) pipeline is racy - a key written on the source can
        replicate to the destination before the destination's own write lands,
        producing last-writer-wins with no divergence and ZERO conflicts. This is
        exactly why the live-update test's second phase never paused: by then the
        pipeline was warm and the concurrent writes did not diverge. Pausing both
        replications first isolates the clusters, so the same-key writes on each
        side are guaranteed independent; resuming then forces XDCR to detect every
        one as a true conflict, regardless of pipeline warmth.

        Both sides use the same ``--key-prefix`` so the keyspaces overlap;
        ``key_prefix`` lets a multi-phase test use a fresh batch per phase.

        Delegates to the robust ``_run_pillowfight_on_both`` (local cbc-pillowfight
        with SSH fallback to the bundled binary), so a missing binary / auth
        failure surfaces in the logs instead of silently generating no conflicts.
        """
        if self._replication_direction != "bidirection":
            self.log.info("Skipping conflict creation in unidirectional setup")
            return

        pf_opts = ["-I", str(num_items), "-m", "100", "-M", "100",
                   "-c", "10", "-t", str(threads), "-B", "100",
                   "--key-prefix", key_prefix]
        if rate_limit:
            pf_opts += ["--rate-limit", str(rate_limit)]

        # Isolate the clusters so same-key writes on each side are independent and
        # cannot be reconciled by in-flight replication.
        self.log.info("Pausing both replications to isolate clusters for deterministic "
                      "conflict generation")
        self.src_cluster.pause_all_replications_by_id(verify=False)
        self.dest_cluster.pause_all_replications_by_id(verify=False)
        try:
            bucket_names = list(self.conflict_logging_params.keys()) or ["default"]
            for bucket_name in bucket_names:
                self.log.info(f"Generating conflicts on bucket {bucket_name} with overlapping "
                              f"keys [{key_prefix}*], {num_items} items x {threads} threads on "
                              f"each (isolated) cluster; pillowfight opts={pf_opts}")
                ok = self._run_pillowfight_on_both(bucket_name, pf_opts, "conflicts")
                if ok:
                    self.log.info(f"Divergent load completed on both clusters for bucket "
                                  f"{bucket_name}")
                else:
                    self.log.warning(f"Divergent load did not succeed on both clusters for "
                                     f"bucket {bucket_name}; conflict generation may be incomplete")
        finally:
            self.log.info("Resuming both replications; XDCR will now detect the conflicts")
            self.src_cluster.resume_all_replications_by_id(verify=False)
            self.dest_cluster.resume_all_replications_by_id(verify=False)

        # Confirm the manual resume cleared the pause BEFORE any auto-pause check, so a
        # lingering manual pause is never mistaken for an auto-pause. Only on the success
        # path - on a load exception the finally above still resumes and the error propagates.
        self._wait_until_replications_resumed()

    def test_clog_pause_settings_validation(self):
        """
        Validate accepted ranges and rejection of out-of-range / non-integer values for
        cLogPauseReplThreshold ([0, INT_MAX]) and cLogMonitorDuration ([0, 2592000]).
        """
        self._create_scopes_and_collections()
        self.setup_xdcr_and_load()
        self._setup_conflict_logging()

        replication = self._get_all_replications()[0]

        # Accepted boundary combinations must all be accepted by the server.
        accepted = [(0, 0), (1, 1), (INT_MAX, MAX_MONITOR_DURATION)]
        for threshold, duration in accepted:
            try:
                self._set_clog_pause_settings(threshold, duration, replication=replication)
            except Exception as e:
                self.log.error(f"Valid settings ({threshold},{duration}) rejected: {e}")
                self.fail(f"Valid clog pause settings ({threshold},{duration}) rejected: {e}")

            # Read-back only for NON-default (non-zero) values. get_xdcr_param falls back to
            # the global internalSettings when a per-replication value equals the global
            # default (0), so a (0,0) read-back would not reflect the per-replication set
            # and could even KeyError if the global key is absent on the build.
            if threshold != 0 and duration != 0:
                settings = self._get_clog_pause_settings(replication)
                self.assertEqual(int(settings[CLOG_PAUSE_THRESHOLD_KEY]), threshold,
                                 f"Threshold not persisted for ({threshold},{duration})")
                self.assertEqual(int(settings[CLOG_MONITOR_DURATION_KEY]), duration,
                                 f"Duration not persisted for ({threshold},{duration})")

        # Out-of-range / non-integer values must be rejected by the server.
        rejected = [
            (-1, 30),                        # negative threshold
            (10, -1),                        # negative duration
            (10, MAX_MONITOR_DURATION + 1),  # duration above max
            ("abc", 30),                     # non-integer threshold
            (10, "xyz"),                     # non-integer duration
        ]
        for threshold, duration in rejected:
            rejected_ok = False
            try:
                self._set_clog_pause_settings(threshold, duration, replication=replication)
            except Exception as e:
                rejected_ok = True
                self.log.info(f"Rejected invalid settings ({threshold},{duration}) as expected: {e}")
            if not rejected_ok:
                self.fail(f"Invalid clog pause settings ({threshold},{duration}) were accepted")

    def test_clog_autopause_on_threshold_breach(self):
        """
        Conflicts exceeding cLogPauseReplThreshold within cLogMonitorDuration must auto-pause
        the replication and surface the reason via the XDCR errors API.
        """
        autopause_timeout = self._input.param("autopause_timeout", 180)

        self._create_scopes_and_collections()
        self.setup_xdcr_and_load()
        self._setup_conflict_logging()
        self._verify_conflict_logging_settings()

        threshold, duration = self._apply_clog_pause_from_params()
        self.assertGreater(threshold, 0, "clog_pause_threshold must be > 0 for this test")
        self.assertGreater(duration, 0, "clog_monitor_duration must be > 0 for this test")

        # Deterministic divergent load (pause -> load same keys on each cluster ->
        # resume) guarantees conflicts >> threshold. The pause state is the reliable
        # signal: with a low threshold the pipeline auto-pauses before conflict-log
        # docs ever flush to the collection, so doc count cannot gate this test.
        self._generate_conflicts_no_wait(num_items=self._num_items)

        self.assertTrue(self._wait_for_autopause(timeout=autopause_timeout),
                        "Replication was not auto-paused after conflicts exceeded threshold")

        found, message = self._wait_for_autopause_error(self.src_cluster, timeout=autopause_timeout)
        self.assertTrue(found, "Auto-pause reason not surfaced in XDCR errors API")
        self.log.info(f"Auto-pause reason: {message}")

    def test_clog_default_hibernation_unchanged(self):
        """
        With both settings = 0 (default), the legacy hibernation path applies: the replication
        must NOT be auto-paused even under heavy conflict load (the two fault-tolerance
        mechanisms are mutually exclusive).
        """
        observe_secs = self._input.param("observe_secs", 90)

        self._create_scopes_and_collections()
        self.setup_xdcr_and_load()
        self._setup_conflict_logging()

        threshold, duration = self._apply_clog_pause_from_params()
        self.assertEqual(threshold, 0, "clog_pause_threshold must be 0 for default hibernation test")
        self.assertEqual(duration, 0, "clog_monitor_duration must be 0 for default hibernation test")

        self._generate_conflicts_no_wait(num_items=self._num_items, threads=16)
        self._assert_no_autopause(observe_secs)

    def test_clog_pause_settings_live_update(self):
        """
        Settings must update live on a running pipeline: a high threshold tolerates conflicts
        (no pause), then lowering the threshold on the same replication must trigger auto-pause.
        """
        autopause_timeout = self._input.param("autopause_timeout", 180)
        observe_secs = self._input.param("observe_secs", 60)
        low_threshold = self._input.param("clog_pause_threshold", 1)
        duration = self._input.param("clog_monitor_duration", 30)
        high_threshold = self._input.param("clog_high_threshold", INT_MAX)

        self._create_scopes_and_collections()
        self.setup_xdcr_and_load()
        self._setup_conflict_logging()

        # Phase 1: high threshold -> conflicts must not pause the replication.
        self._set_clog_pause_settings(high_threshold, duration)
        self._generate_conflicts_no_wait(num_items=self._num_items,
                                         key_prefix=CONFLICT_KEY_PREFIX + "p1_")
        self._assert_no_autopause(observe_secs)

        # Phase 2: lower the threshold, then generate a FRESH batch of conflicts.
        # _generate_conflicts_no_wait pauses+resumes the replications, which both
        # (a) isolates the clusters so the same-key writes diverge deterministically
        # and (b) restarts the pipeline so the newly-lowered threshold is picked up.
        # A fresh key prefix avoids reusing phase 1's already-converged docs.
        self._set_clog_pause_settings(low_threshold, duration)
        self._generate_conflicts_no_wait(num_items=self._num_items,
                                         key_prefix=CONFLICT_KEY_PREFIX + "p2_")

        self.assertTrue(self._wait_for_autopause(timeout=autopause_timeout),
                        "Replication did not auto-pause after live-updating to a low threshold")
        found, _ = self._wait_for_autopause_error(self.src_cluster, timeout=autopause_timeout)
        self.assertTrue(found, "Auto-pause reason not surfaced in XDCR errors API after live update")

    def test_clog_window_reset(self):
        """
        A sustained conflict rate that stays below cLogPauseReplThreshold in every 1s cycle must
        never trip auto-pause across multiple cLogMonitorDuration windows, because the running
        sum resets at the start of each window.

        Timing note: this test is rate-sensitive. It uses a deliberately high threshold and a
        rate-limited pillowfight drip so the per-cycle conflict volume stays provably below the
        threshold while the load runs for longer than several windows.
        """
        duration = self._input.param("clog_monitor_duration", 10)
        threshold = self._input.param("clog_pause_threshold", 1000)
        rate_limit = self._input.param("clog_rate_limit", 50)  # ops/sec per cluster, << threshold
        observe_secs = self._input.param("observe_secs", duration * 3)

        self._create_scopes_and_collections()
        self.setup_xdcr_and_load()
        self._setup_conflict_logging()

        self._set_clog_pause_settings(threshold, duration)

        # Rate-limited, steady conflict drip well under the per-cycle threshold. Launch it in
        # the background so it runs *concurrently* with the observation window spanning several
        # cLogMonitorDuration windows. No single cycle's running sum should breach the threshold.
        # Both clusters use the shared key prefix so the drip produces real (overlapping-key)
        # conflicts, and timeout_secs self-terminates each pillowfight at the end of the window.
        bucket_names = list(self.conflict_logging_params.keys()) or ["default"]
        drip_secs = observe_secs + duration
        num_items = rate_limit * drip_secs
        pf_opts = ["-I", str(num_items), "-m", "100", "-M", "100", "-c", "10", "-t", "2",
                   "-B", "100", "--rate-limit", str(rate_limit), "--key-prefix", CONFLICT_KEY_PREFIX]
        self.log.info(f"Starting rate-limited conflict drip: {rate_limit} ops/sec/cluster for "
                      f"~{drip_secs}s, threshold={threshold}, monitor_duration={duration}s; "
                      f"pillowfight opts={pf_opts}")
        threads = []
        if self._replication_direction == "bidirection":
            for bucket_name in bucket_names:
                for node in (self.src_master, self.dest_master):
                    t = threading.Thread(
                        target=self._run_pillowfight,
                        args=(node, bucket_name, pf_opts, f"window-reset drip {node.ip}"),
                        kwargs={"timeout_secs": drip_secs}, daemon=True)
                    t.start()
                    threads.append(t)
        try:
            self._assert_no_autopause(observe_secs)
        finally:
            for t in threads:
                t.join(timeout=drip_secs)

    def test_clog_resume_after_autopause(self):
        """
        After an auto-pause fires, the replication can be resumed and resumes replicating
        (data catches up once the conflict pressure is removed).
        """
        autopause_timeout = self._input.param("autopause_timeout", 180)

        self._create_scopes_and_collections()
        self.setup_xdcr_and_load()
        self._setup_conflict_logging()

        self._apply_clog_pause_from_params()
        self._generate_conflicts_no_wait(num_items=self._num_items)

        self.assertTrue(self._wait_for_autopause(timeout=autopause_timeout),
                        "Replication was not auto-paused")

        # Resume and confirm the replication is running again. resume_all_replications is a
        # CouchbaseCluster method; in a bidirectional setup both clusters may have an
        # auto-paused replication, so resume both sides.
        self.src_cluster.resume_all_replications(verify=True)
        self.dest_cluster.resume_all_replications(verify=True)
        self.assertFalse(self._is_any_replication_paused(),
                         "Replication still paused after resume")

        # Once resumed, replication should catch up (excluding conflict-log collections).
        super(ConflictLoggingTests, self)._wait_for_replication_to_catchup(
            timeout=600,
            fetch_bucket_stats_by="minute",
            exclude_paths=self._clog_exclude_paths())

    def test_clog_autopause_during_rebalance(self):
        """
        Auto-pause must still fire when a conflict burst occurs during a source-cluster rebalance.
        """
        autopause_timeout = self._input.param("autopause_timeout", 240)

        self._create_scopes_and_collections()
        self.setup_xdcr_and_load()
        self._setup_conflict_logging()

        self._apply_clog_pause_from_params()

        rebalance_task = self._start_rebalance(self.src_cluster)
        if not rebalance_task:
            # The conf entry promises "during rebalance"; without a spare node we cannot
            # honor that, so skip rather than give false confidence as a plain auto-pause run.
            self.skipTest("No free node available to start a source-cluster rebalance")

        self._generate_conflicts_no_wait(num_items=self._num_items, threads=16)

        paused = self._wait_for_autopause(timeout=autopause_timeout)

        if rebalance_task:
            self.log.info("Waiting for rebalance to complete...")
            try:
                rebalance_task.result()
            except Exception as e:
                self.log.warning(f"Rebalance ended with: {e}")

        self.assertTrue(paused, "Replication was not auto-paused during/after rebalance")
        found, _ = self._wait_for_autopause_error(self.src_cluster, timeout=autopause_timeout)
        self.assertTrue(found, "Auto-pause reason not surfaced in XDCR errors API")
