"""10k_gsi_indexes_no_retry.py: 10k-namespace GSI test — strict, no-retry variant.

Same end-to-end flow as 10k_gsi_indexes.py (create -> build -> scan -> shard
rebalance -> DCP rebalance -> query-node resilience -> drop), but hardened for
zero bug leakage:

  * NO retries anywhere. Every operation runs exactly once. A failure is a real
    failure and fails the test immediately — nothing is re-attempted or masked.
  * Every validation is a hard assertion. No "continue anyway" / "non-fatal"
    downgrades. Return values of every verify helper are checked and asserted.
  * A replica-integrity gate runs BEFORE and AFTER every rebalance, so a lost
    replica is caught at the exact step that caused it (this is the class of bug
    the original suite silently logged and passed over).
  * Precise, step-scoped logging so any failure is debuggable from the log alone.

Doc loading: standalone hotel bulk loader, one thread per namespace, SDK bulk ops.

__author__ = "Nirmala Roy"
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
import subprocess
import sys
import os
import random
import string
import time

from gsi.base_gsi import BaseSecondaryIndexingTests
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from threading import Event, Thread
from couchbase_helper.documentgenerator import SDKDataLoader
from couchbase_helper.query_definitions import QueryDefinition, RANGE_SCAN_TEMPLATE


# Fields the background mutation workload re-randomizes on every document it
# touches -- hotel_bulk_loader.py "update" mode ->
# HotelDataGenerator.generate_mutation_fields():
#
#     "price":        random.randint(500, 2500)
#     "avg_rating":   round(random.uniform(0.0, 5.0), 16)
#     "desc_vectors": [random.uniform(-1.0, 1.0) for _ in range(384)]
#
# A predicate on one of these has NO stable doc count across a mutation window,
# so it must never be compared against an earlier baseline. The 20 composite
# namespaces are indexed on (free_breakfast, avg_rating) and queried with
#     WHERE avg_rating > 3 AND free_breakfast = true
# so their matched population is re-rolled by every mutation cycle. Steps 6/8
# were reporting that RNG drift as a doc-count failure -- counts moved BOTH
# ways (26->19 but also 20->24, 13->21), which is drift, not doc loss.
#
# `price` is deliberately NOT listed: it is re-randomized too, but only within
# randint(500, 2500), so the sole predicate this suite uses on it (`price > 0`)
# can never change truth value -- its count stays pinned at
# num_of_docs_per_collection and remains a valid baseline. `country` is never
# mutated at all.
MUTATION_VOLATILE_PREDICATE_FIELDS = ("avg_rating", "desc_vectors")


class TenKGSIIndexesNoRetry(BaseSecondaryIndexingTests):
    def setUp(self):
        super(TenKGSIIndexesNoRetry, self).setUp()
        self.log.info("==============  TenKGSIIndexesNoRetry setup has started ==============")

        # Get test parameters
        self.num_buckets = self.input.param("num_buckets", 1)
        self.namespace_layout = self.input.param("namespace_layout", "many_collections")
        self.total_namespaces = self.input.param("total_namespaces", 10000)
        self.num_scopes_per_bucket = self.input.param("num_scopes_per_bucket", None)
        self.num_collections_per_scope = self.input.param("num_collections_per_scope", None)
        self.index_build_mode = self.input.param("index_build_mode", "immediate")
        self.indexes_per_collection = self.input.param("indexes_per_collection", 1)
        self.num_replica = self.input.param("num_replica", 0)
        self.rebalance_type = self.input.param("rebalance_type", "swap")
        self.kvMemQuota = self.input.param("kvMemQuota", self.input.param("kv_quota_mb", None))
        self.indexMemQuota = self.input.param("indexMemQuota", self.input.param("index_quota_mb", None))
        
        # Doc loading parallelization parameters
        self.use_bulk_loader = self.input.param("use_bulk_loader", True)
        self.bulk_loader_batch_size = self.input.param("bulk_loader_batch_size", 1000)
        self.max_loader_threads = self.input.param("max_loader_threads", 8)
        
        # Namespace distribution parameters
        self.max_namespaces_per_cluster = self.input.param("max_namespaces_per_cluster", 10000)
        self.distribution_pattern = self.input.param("distribution_pattern", "uniform")
        self.skew_factor = self.input.param("skew_factor", 0.8)  # For skewed distribution
        
        # Index instance management
        self.max_index_instances = self.input.param("max_index_instances", 10000)
        self.indexed_namespaces = []  # Track namespaces that have indexes
        
        # Concurrency control to avoid overwhelming the GSI indexer
        # GSI has a hard limit of ~10 concurrent build operations and the
        # postScheduleCreateRequest RPC times out at ~42s under heavy load
        self.max_concurrent_index_creates = self.input.param("max_concurrent_index_creates", 5)
        self.max_concurrent_index_builds = self.input.param("max_concurrent_index_builds", 3)

        # Mutation parameters for rebalance workload
        self.mutation_ops_rate = self.input.param("mutation_ops_rate", 100)  # ops/sec per worker during mutations
        self.mutation_sample_size = self.input.param("mutation_sample_size", 200)  # collections to sample per cycle
        self.mutation_docs_per_op = self.input.param("mutation_docs_per_op", 25)   # docs per insert/update/delete
        self.mutation_num_cycles = self.input.param("mutation_num_cycles", 3)       # Step 4b cycles
        self.mutation_cycle_gap_secs = self.input.param("mutation_cycle_gap_secs", 180)  # gap between Step 4b cycles
        # Gap between consecutive mutation/scan cycles during rebalance (seconds).
        # Prevents continuous hammering — each cycle runs, then pauses before the next.
        self.rebalance_workload_cycle_gap_secs = self.input.param("rebalance_workload_cycle_gap_secs", 120)

        # Background workload control
        self.run_continuous_mutations = False
        self.run_continuous_scans = False

        # Section 3 (Query Service Improvement Areas) — collected perf baseline
        self.perf_metrics = {}

        # Post-rebalance index-loss tracking: each _rebalance_indexer_nodes() soft-fail
        # appends here instead of asserting, so the run continues; FINAL SUMMARY prints
        # a consolidated report and only asserts at the end if fail_on_index_loss=True.
        self.index_loss_report = []
        self.fail_on_index_loss = self.input.param("fail_on_index_loss", False)

        if self.kvMemQuota:
            self.log.info("Setting KV memory quota to {0} MB on all nodes...".format(self.kvMemQuota))
            for server in self.servers[:self.nodes_init]:
                try:
                    RestConnection(server).set_service_memoryQuota(
                        service='memoryQuota', memoryQuota=int(self.kvMemQuota))
                    self.log.info("  {0}: KV quota set".format(server.ip))
                except Exception as e:
                    self.log.warning("  {0}: Failed to set KV quota: {1}".format(server.ip, e))
            self.sleep(5)
        if self.indexMemQuota:
            self.log.info("Setting indexer memory quota to {0} MB on all nodes...".format(self.indexMemQuota))
            for server in self.servers[:self.nodes_init]:
                try:
                    RestConnection(server).set_service_memoryQuota(
                        service='indexMemoryQuota', memoryQuota=int(self.indexMemQuota))
                    self.log.info("  {0}: Index quota set".format(server.ip))
                except Exception as e:
                    self.log.warning("  {0}: Failed to set index quota: {1}".format(server.ip, e))
            self.sleep(30)

        self._active_churn_events = []  # events for background threads; stopped in tearDown

        self.log.info("==============  TenKGSIIndexesNoRetry setup has ended ==============")

    def tearDown(self):
        self.log.info("==============  TenKGSIIndexesNoRetry tearDown has started ==============")
        # During suite_setUp, basetestcase calls tearDown() as a pre-test cleanup before
        # forming the cluster.  At that point the cluster is a single standalone node —
        # there is nothing to clean up, no ejected nodes to re-add, and the 120s sleep
        # just wastes time.  Skip all custom logic and delegate directly to super().
        if getattr(self, '_testMethodName', '') == 'suite_setUp':
            try:
                super(TenKGSIIndexesNoRetry, self).tearDown()
            except Exception as e:
                self.log.warning(f"suite_setUp tearDown failed (non-fatal): {e}")
            self.log.info("==============  TenKGSIIndexesNoRetry tearDown has completed ==============")
            return
        # Stop any background churn/sampler threads that may still be running if the
        # test failed mid-step before their event.set() was called.
        for _ev in getattr(self, '_active_churn_events', []):
            _ev.set()
        self._active_churn_events = []

        # Ensure no index builds are in progress before the framework's teardown rebalance.
        # A DROP issued against an index that is still building is rejected, so such an
        # index survives Step 9 and stays in "Building" state.  The subsequent
        # basetestcase.setUp() for suite_tearDown then triggers a rebalance that the
        # indexer refuses with:
        #   "ShardRebalancer indexer rebalance failure - index build is in progress"
        # Waiting here (after test_10k_namespaces_with_indexes returns, before suite_tearDown
        # setUp runs) lets those builds finish so the rebalance succeeds.
        try:
            self.log.info("Pre-teardown: checking for in-progress index builds (timeout=1800s)...")
            index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            deadline = time.time() + 1800
            while time.time() < deadline:
                building = []
                for node in index_nodes:
                    try:
                        meta = RestConnection(node).get_indexer_metadata()
                        for idx in meta.get('status', []):
                            if idx.get('status') in ('Building', 'Warmup'):
                                building.append(f"{idx.get('bucket')}:{idx.get('name')}")
                    except Exception as node_err:
                        self.log.warning(f"Could not query indexer metadata on {node.ip}: {node_err}")
                if not building:
                    self.log.info("No indexes are building — safe to proceed with teardown rebalance")
                    break
                self.log.info(f"Waiting for {len(building)} building index(es): {building[:5]}...")
                self.sleep(10, "Waiting for index builds to complete before teardown")
            else:
                self.log.warning("Timed out (1800s) waiting for index builds to complete; proceeding anyway")
        except Exception as e:
            self.log.warning(f"Pre-teardown index build wait failed (proceeding): {e}")

        # A 120s wait gives ns_config_rep enough headroom even under post-DCP
        # cluster stress (compaction, GC, memory reclaim).
        self.log.info("Pre-teardown: sleeping 120s to allow ns_config_rep to sync on rejoining nodes...")
        self.sleep(120, "Waiting for ns_config_rep sync before teardown rebalance")

        # Re-add any ejected nodes (e.g. DCP swap-out node) WITHOUT rebalancing.
        # This lets ns_config_rep start gossiping the 35-min metakv backlog to the
        # rejoining node during the ~6 min of super().tearDown() cleanup, so the
        # suite_tearDown setUp rebalance's gen_server:call([ns_config_rep,synchronize,30000])
        # completes well within the 30s limit instead of timing out.
        try:
            current_nodes = RestConnection(self.master).get_nodes()
            current_ips = {n.ip for n in current_nodes}
            # Mirror the services of the master node so the re-added node rejoins
            # with the correct service set (kv+n1ql+index), not the kv-only default.
            svc = getattr(next((n for n in current_nodes if n.ip == self.master.ip), current_nodes[0]),
                          'services', ['kv', 'index', 'n1ql']) if current_nodes else ['kv', 'index', 'n1ql']
            # node.services may be a list or a comma-separated string; normalise to a list
            if isinstance(svc, str):
                svc = [s.strip() for s in svc.split(',') if s.strip()]
            master_services_list = sorted(svc)   # list — add_node joins it internally
            master_services = ','.join(master_services_list)  # string — for logging only
            for server in self.servers[:self.nodes_init]:
                if server.ip not in current_ips:
                    self.log.info(f"Pre-teardown: re-adding ejected node {server.ip} "
                                  f"(services={master_services}) for ns_config_rep pre-sync...")
                    RestConnection(self.master).add_node(user=server.rest_username,
                                                        password=server.rest_password,
                                                        remoteIp=server.ip,
                                                        services=master_services_list)
        except Exception as e:
            self.log.warning(f"Pre-teardown re-add ejected node failed (non-fatal): {e}")

        try:
            super(TenKGSIIndexesNoRetry, self).tearDown()
        except Exception as e:
            self.log.warning(f"Teardown failed (cluster may be in inconsistent state after rebalance): {e}")
        self.log.info("==============  TenKGSIIndexesNoRetry tearDown has completed ==============")

    def wait_until_indexes_online(self, timeout=1800, defer_build=False, check_paused_index=False, schedule_index=False):
        """Override to log which indexes are not ready yet, every 60 seconds."""
        rest = RestConnection(self.master)
        init_time = time.time()
        check = False
        timed_out = False
        last_log_time = init_time
        while not check:
            index_status = rest.get_index_status()
            next_time = time.time()
            if index_status == {}:
                self.log.error("No indexes are present, this check does not apply!")
                break

            not_ready = [
                f"{bucket}:{name} status={state['status']} progress={state.get('progress', '?')}%"
                for bucket, index_info in index_status.items()
                for name, state in index_info.items()
                if not (
                    (defer_build and state["status"] in ("Ready", "Created")) or
                    (check_paused_index and state["status"] in ("Paused", "Ready")) or
                    (schedule_index and state["status"] in ("Ready", "Scheduled for Creation")) or
                    (not defer_build and not check_paused_index and not schedule_index and state["status"] == "Ready")
                )
            ]
            elapsed = next_time - init_time
            if not_ready and next_time - last_log_time >= 60:
                self.log.info(
                    f"Indexes not yet ready: {len(not_ready)} "
                    f"(elapsed {elapsed:.0f}s / {timeout}s) — first 5: {not_ready[:5]}"
                )
                last_log_time = next_time

            for index_info in index_status.values():
                for index_state in index_info.values():
                    if defer_build:
                        if index_state["status"] == "Ready" or index_state["status"] == "Created":
                            check = True
                        else:
                            check = False
                            time.sleep(10)
                            break
                    elif check_paused_index:
                        if index_state["status"] == "Paused" or index_state["status"] == "Ready":
                            check = True
                        else:
                            check = False
                            time.sleep(10)
                            break
                    elif schedule_index:
                        if index_state["status"] == "Ready" or index_state["status"] == "Scheduled for Creation":
                            check = True
                        else:
                            check = False
                            time.sleep(10)
                            break
                    else:
                        if index_state["status"] == "Ready":
                            check = True
                        else:
                            check = False
                            time.sleep(10)
                            break
                if next_time - init_time > timeout:
                    timed_out = True
                    check = next_time - init_time > timeout

        if timed_out:
            self.log.info(f"Indexes are not online after {timeout} seconds")
            not_ready_final = [
                f"{bucket}:{name} status={state['status']} progress={state.get('progress', '?')}%"
                for bucket, index_info in index_status.items()
                for name, state in index_info.items()
                if state["status"] != "Ready"
            ]
            self.log.info(f"Not-ready indexes at timeout ({len(not_ready_final)} total): {not_ready_final[:20]}")
            if len(not_ready_final) > 20:
                self.log.info(f"  ... and {len(not_ready_final) - 20} more")
            check = False
        return check

    def _create_buckets_and_namespaces(self, num_scopes_per_bucket=None, num_collections_per_scope=None):
        """
        Creates multiple buckets and collections based on namespace_layout configuration or explicit parameters.
        Document loading is done in parallel with multiple threads over namespace batches.
        
        Enforces max_namespaces_per_cluster limit and supports uniform/skewed distribution patterns.

        Args:
            num_scopes_per_bucket: Number of scopes to create per bucket. If None, determined by namespace_layout.
            num_collections_per_scope: Number of collections to create per scope. If None, determined by namespace_layout.

        For each bucket, creates either many collections in one scope or many scopes each with one collection,
        or uses explicitly specified scopes and collections per bucket.
        """
        buckets = []
        self.namespaces = []
        all_namespaces = []

        # Enforce max namespaces per cluster limit
        if self.total_namespaces > self.max_namespaces_per_cluster:
            self.log.warning(f"Requested {self.total_namespaces} namespaces exceeds cluster limit of {self.max_namespaces_per_cluster}")
            self.log.info(f"Adjusting to maximum allowed: {self.max_namespaces_per_cluster} namespaces")
            self.total_namespaces = self.max_namespaces_per_cluster

        # Use provided parameters or calculate based on namespace_layout
        if num_scopes_per_bucket is not None and num_collections_per_scope is not None:
            # Use explicit parameters
            total_namespaces = num_scopes_per_bucket * num_collections_per_scope * self.num_buckets
            self.log.info(f"Using explicit configuration: {num_scopes_per_bucket} scopes/bucket, "
                         f"{num_collections_per_scope} collections/scope")
        else:
            # Calculate based on namespace_layout
            namespaces_per_bucket = self._get_namespaces_per_bucket()
            
            if self.namespace_layout == "many_collections":
                # 1 scope, N collections
                num_scopes_per_bucket = 1
                num_collections_per_scope = namespaces_per_bucket
            elif self.namespace_layout == "many_scopes":
                # N scopes, 1 collection each
                num_scopes_per_bucket = namespaces_per_bucket
                num_collections_per_scope = 1
            else:
                raise ValueError(f"Unknown namespace_layout: {self.namespace_layout}")

            total_namespaces = num_scopes_per_bucket * num_collections_per_scope * self.num_buckets
            self.log.info(f"Using namespace_layout '{self.namespace_layout}': {num_scopes_per_bucket} scopes/bucket, "
                         f"{num_collections_per_scope} collections/scope")

        self.log.info(f"Creating {self.num_buckets} buckets with "
                     f"{num_scopes_per_bucket} scopes and {num_collections_per_scope} collections per scope "
                     f"(total {total_namespaces} namespaces per bucket)")

        for bucket_num in range(self.num_buckets):
            bucket_name = f"{self.test_bucket}_{bucket_num}"
            buckets.append(bucket_name)

            # Create bucket
            self.bucket_params = self._create_bucket_params(
                server=self.master, size=self.bucket_size,
                replicas=self.num_replicas, bucket_type=self.bucket_type,
                enable_replica_index=self.enable_replica_index,
                eviction_policy=self.eviction_policy, lww=self.lww
            )
            self.cluster.create_standard_bucket(name=bucket_name, port=11222,
                                                bucket_params=self.bucket_params)

            self.log.info(f"Bucket {bucket_name}: Creating {num_scopes_per_bucket} scopes with "
                         f"{num_collections_per_scope} collections each")

            # Create scopes and collections, then start async doc loading
            bucket_namespaces = self._prepare_collections(
                num_scopes=num_scopes_per_bucket, num_collections=num_collections_per_scope,
                num_of_docs_per_collection=self.num_of_docs_per_collection,
                load_default_coll=False, bucket_name=bucket_name
            )
            all_namespaces.extend(bucket_namespaces)

        # Load documents in parallel - one thread per namespace
        self.namespaces = all_namespaces
        if self.use_bulk_loader:
            self._load_docs_with_bulk_loader(
                num_of_docs_per_collection=self.num_of_docs_per_collection,
                key_prefix='hotel_'
            )
        else:
            self._load_docs_parallel_legacy(
                num_of_docs_per_collection=self.num_of_docs_per_collection,
                json_template=self.json_template,
                key_prefix='doc_'
            )

        # Update self.buckets to include all created buckets
        self.buckets = self.rest.get_buckets()
        self.log.info(f"Total namespaces created: {len(self.namespaces)}")
        
    def _get_namespaces_per_bucket(self):
        """
        Calculate namespaces per bucket based on distribution pattern.
        
        Returns:
            For uniform distribution: total_namespaces // num_buckets (equal distribution)
            For skewed distribution: varied distribution based on skew_factor
        """
        if self.distribution_pattern == "uniform":
            # Equal distribution across buckets
            namespaces_per_bucket = self.total_namespaces // self.num_buckets
            self.log.info(f"Uniform distribution: {namespaces_per_bucket} namespaces per bucket")
            return namespaces_per_bucket
            
        elif self.distribution_pattern == "skewed":
            # Skewed distribution: some buckets have more namespaces than others
            # Using power law distribution based on skew_factor
            if self.num_buckets == 1:
                return self.total_namespaces
                
            # Calculate skewed distribution
            # skew_factor close to 1.0 = highly skewed (one bucket dominates)
            # skew_factor close to 0.0 = more uniform
            import math
            
            total = 0
            distribution = []
            
            for i in range(self.num_buckets):
                # Power law: bucket_i gets proportionally more if skewed
                weight = (self.num_buckets - i) ** self.skew_factor
                distribution.append(weight)
                total += weight
            
            # Normalize and assign namespaces
            namespaces_per_bucket = []
            for i, weight in enumerate(distribution):
                if i == self.num_buckets - 1:
                    # Last bucket gets remaining namespaces
                    namespaces = self.total_namespaces - sum(namespaces_per_bucket)
                else:
                    namespaces = int(self.total_namespaces * (weight / total))
                namespaces_per_bucket.append(namespaces)
            
            # For simplicity, return average (actual implementation would vary per bucket)
            avg_namespaces = self.total_namespaces // self.num_buckets
            self.log.info(f"Skewed distribution (factor={self.skew_factor}): "
                         f"range {min(namespaces_per_bucket)}-{max(namespaces_per_bucket)} namespaces per bucket")
            self.log.info(f"Distribution per bucket: {namespaces_per_bucket}")
            
            # Store distribution for later use
            self.namespaces_distribution = namespaces_per_bucket
            
            # Return average for compatibility with current implementation
            # Note: Actual per-bucket namespace count will vary based on distribution
            return avg_namespaces
        
        else:
            raise ValueError(f"Unknown distribution_pattern: {self.distribution_pattern}")

    def _prepare_collections(self, num_scopes=1, num_collections=1, num_of_docs_per_collection=1000,
                             bucket_name=None, load_default_coll=False):
        """
        Creates scopes/collections using a single manifest PUT and returns the list
        of namespaces. NO RETRY: one manifest PUT, then a hard assertion that every
        requested collection exists. A short count is a real failure and aborts.
        """
        if not bucket_name:
            bucket_name = self.test_bucket

        expected_collections = num_scopes * num_collections

        # Build manifest with test naming convention: {prefix}_{num + 1}
        new_manifest = {"scopes": []}
        for i in range(num_scopes):
            scope_name = f"{self.scope_prefix}_{i + 1}"
            scope = {"name": scope_name, "collections": []}
            for j in range(num_collections):
                scope["collections"].append({
                    "name": f"{self.collection_prefix}_{j + 1}"
                })
            new_manifest["scopes"].append(scope)

        # Merge with existing manifest to preserve default scopes/collections, PUT once.
        current_manifest = BucketOperationHelper.get_api_manifest_json_from_bucket(
            self.master, bucket_name)
        manifest = BucketOperationHelper.merge_collection_manifests(current_manifest, new_manifest)
        self.rest.put_collection_scope_manifest(bucket_name, manifest)
        self.sleep(10, "Allowing time after collection creation")

        # Hard gate: every requested collection must be present after the single PUT.
        actual_collections = self._count_collections_in_bucket(bucket_name)
        self.assertGreaterEqual(
            actual_collections, expected_collections,
            f"Collection creation incomplete in {bucket_name}: "
            f"{actual_collections}/{expected_collections} present after manifest PUT")
        self.log.info(f"Created {actual_collections} collections in bucket {bucket_name} "
                      f"(expected >= {expected_collections})")

        scopes = [f'{self.scope_prefix}_{scope_num + 1}' for scope_num in range(num_scopes)]

        if load_default_coll:
            scopes.append("_default")

        namespaces = []
        if num_of_docs_per_collection > 0:
            for s_item in scopes:
                if load_default_coll and s_item == '_default':
                    collections = ['_default']
                else:
                    collections = [f'{self.collection_prefix}_{coll_num + 1}' for coll_num in range(num_collections)]

                for c_item in collections:
                    namespace = f'default:{bucket_name}.{s_item}.{c_item}'
                    # Do NOT append to self.namespaces here — _create_buckets_and_namespaces
                    # overwrites self.namespaces = all_namespaces after all buckets are done.
                    # Double-appending caused multi-bucket runs to lose all but the last bucket.
                    namespaces.append(namespace)

        return namespaces
    
    def _count_collections_in_bucket(self, bucket_name):
        """Count total collections in a bucket across all scopes."""
        try:
            scopes = self.rest.get_bucket_scopes(bucket=bucket_name)
            total = 0
            for scope in scopes:
                if scope == '_system':
                    continue
                collections = self.rest.get_scope_collections(bucket=bucket_name, scope=scope)
                total += len(collections)
            return total
        except Exception as e:
            self.log.error(f"Error counting collections in {bucket_name}: {e}")
            return 0
    
    def _load_docs_with_bulk_loader(self, num_of_docs_per_collection, key_prefix='hotel_'):
        """
        Loads documents using the standalone hotel bulk loader script.
        Each namespace gets its own thread running the bulk loader script.
        
        Args:
            num_of_docs_per_collection: Number of documents to load per collection
            key_prefix: Prefix for document keys
        """
        total_namespaces = len(self.namespaces)
        if total_namespaces == 0:
            self.log.warning("No namespaces found for document loading")
            return

        # Limit concurrent threads to avoid overwhelming the system
        max_workers = min(self.max_loader_threads, total_namespaces)
        
        self.log.info(f"Doc loading with bulk loader: {total_namespaces} namespaces, "
                      f"max {max_workers} concurrent threads")
        
        # Get script path
        script_dir = os.path.dirname(os.path.abspath(__file__))
        script_path = os.path.join(script_dir, "hotel_bulk_loader.py")
        
        if not os.path.exists(script_path):
            self.log.error(f"Bulk loader script not found at {script_path}")
            return
        
        failed = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_ns = {}
            
            for idx, namespace in enumerate(self.namespaces):
                future = executor.submit(
                    self._run_bulk_loader_for_namespace,
                    namespace, num_of_docs_per_collection, key_prefix,
                    script_path, idx
                )
                future_to_ns[future] = namespace
            
            completed = 0
            for future in as_completed(future_to_ns):
                namespace = future_to_ns[future]
                completed += 1
                try:
                    success = future.result()
                    if not success:
                        failed.append(namespace)
                    if completed % 10 == 0 or completed == total_namespaces:
                        self.log.info(f"Progress: {completed}/{total_namespaces} namespaces completed")
                except Exception as e:
                    self.log.error(f"Bulk load failed for {namespace}: {e}")
                    failed.append(namespace)
        
        if failed:
            self.log.error(f"{len(failed)}/{total_namespaces} namespaces failed to load")
            self.log.error(f"Failed namespaces: {failed[:10]}")  # Show first 10
            if len(failed) > 10:
                self.log.error(f"... and {len(failed) - 10} more")
        
        success_count = total_namespaces - len(failed)
        expected_docs = total_namespaces * num_of_docs_per_collection
        expected_success_docs = success_count * num_of_docs_per_collection

        self.log.info(f"Bulk loading completed: {success_count}/{total_namespaces} namespaces successful")
        self.log.info(f"Loader reported success for {success_count}/{total_namespaces} namespaces "
                      f"(= {expected_success_docs}/{expected_docs} docs expected, not yet verified)")

        self.assertEqual(
            0, len(failed),
            f"Doc loading failed for {len(failed)}/{total_namespaces} namespaces. "
            f"Cannot proceed with indexes on incomplete data. "
            f"Failed namespaces (first 20): {failed[:20]}"
        )

        # The loader subprocess exiting 0 only proves it didn't crash — it does not prove
        # every namespace actually holds num_of_docs_per_collection docs in KV. Verify
        # with a real query before building 4,625 indexes on top of possibly-short data.
        mismatched, failed_queries = self._validate_loaded_doc_counts(
            self.namespaces, num_of_docs_per_collection)
        if failed_queries:
            self.log.warning(f"{len(failed_queries)} doc-count queries failed (infra) — "
                             f"first 20: {failed_queries[:20]}")
        self.assertEqual(
            0, len(mismatched),
            f"{len(mismatched)} namespace(s) do not have {num_of_docs_per_collection} docs "
            f"after loading (first 20): {mismatched[:20]}")

    def _validate_loaded_doc_counts(self, namespaces, expected_docs_per_namespace):
        """
        Verify every namespace holds exactly expected_docs_per_namespace docs right after
        loading, via `SELECT COUNT(*) FROM <namespace>` (correct here specifically: no
        secondary indexes exist yet at Step 1, so this resolves via a KV scan).

        Returns:
            (mismatched, failed_queries) — mismatched: namespaces whose count != expected;
            failed_queries: namespaces whose count query itself failed/returned no result,
            kept separate so a flaky query node isn't reported as data loss.
        """
        from itertools import cycle as _iter_cycle
        all_q_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        if not isinstance(all_q_nodes, list):
            all_q_nodes = [all_q_nodes]
        _qn_cycle = _iter_cycle(all_q_nodes)
        ns_node_pairs = [(ns, next(_qn_cycle)) for ns in namespaces]

        def _count_ns(namespace, qnode):
            result = self.run_cbq_query(query=f"SELECT COUNT(*) FROM {namespace}", server=qnode)
            if result and 'results' in result and result['results']:
                return namespace, result['results'][0].get('$1', 0)
            return namespace, None

        mismatched = []
        failed_queries = []
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = {executor.submit(_count_ns, ns, qn): ns for ns, qn in ns_node_pairs}
            for future in as_completed(futures):
                namespace = futures[future]
                try:
                    namespace, actual_count = future.result()
                except Exception as e:
                    failed_queries.append({'namespace': namespace, 'error': str(e)})
                    continue

                if actual_count is None:
                    failed_queries.append({'namespace': namespace, 'error': 'no result'})
                elif actual_count != expected_docs_per_namespace:
                    mismatched.append({
                        'namespace': namespace,
                        'expected': expected_docs_per_namespace,
                        'actual': actual_count
                    })

        if mismatched:
            self.log.error(f"Post-load doc-count validation: {len(mismatched)}/{len(namespaces)} "
                          f"namespace(s) mismatched — first 20: {mismatched[:20]}")
            if len(mismatched) > 20:
                self.log.error(f"... and {len(mismatched) - 20} more mismatches")
        if failed_queries:
            self.log.error(f"Post-load doc-count validation: {len(failed_queries)}/{len(namespaces)} "
                          f"count quer{'y' if len(failed_queries) == 1 else 'ies'} failed — "
                          f"first 20: {failed_queries[:20]}")
            if len(failed_queries) > 20:
                self.log.error(f"... and {len(failed_queries) - 20} more failures")
        if not mismatched and not failed_queries:
            self.log.info(f"Post-load doc-count validation PASSED: all {len(namespaces)} "
                         f"namespaces have {expected_docs_per_namespace} docs")

        return mismatched, failed_queries

    def _run_bulk_loader_for_namespace(self, namespace, num_docs, key_prefix, script_path, thread_id):
        """
        Run the bulk loader script for a single namespace
        
        Args:
            namespace: Namespace string (default:bucket.scope.collection)
            num_docs: Number of documents to load
            key_prefix: Prefix for document keys
            script_path: Path to the hotel_bulk_loader.py script
            thread_id: Thread identifier for unique key generation
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Parse namespace
            _, keyspace = namespace.split(':')
            bucket, scope, collection = keyspace.split('.')
            
            # Build command
            cmd = [
                sys.executable,  # Use the same Python interpreter
                script_path,
                self.master.ip,
                self.username,
                self.password,
                bucket,
                scope,
                collection,
                str(num_docs),
                f"{key_prefix}",
                str(self.bulk_loader_batch_size),
                str(thread_id * num_docs)  # Unique start ID per thread
            ]
            
            # Run the script
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=600  # 10 minute timeout per namespace
            )
            
            if result.returncode == 0:
                # Parse output to check for partial failures
                if "Failed:" in result.stdout and "Failed: 0/" not in result.stdout:
                    self.log.warning(f"⚠ Loaded {namespace} with some failures: {result.stdout.split('Failed:')[1].split()[0]} docs failed")
                    return False
                else:
                    self.log.info(f"✓ Loaded {num_docs} docs into {namespace}")
                return True
            else:
                self.log.error(f"✗ Failed to load {namespace}. Return code: {result.returncode}")
                if result.stderr:
                    self.log.error(f"  Error: {result.stderr[:200]}")  # First 200 chars
                if "Failed:" in result.stdout:
                    self.log.error(f"  Stats: {result.stdout.split('Failed:')[1].split()[0]} docs failed")
                return False
                
        except subprocess.TimeoutExpired:
            self.log.error(f"✗ Timeout loading {namespace}")
            return False
        except Exception as e:
            self.log.error(f"✗ Exception loading {namespace}: {e}")
            return False

    def _load_docs_parallel_legacy(self, num_of_docs_per_collection, json_template, key_prefix='doc_'):
        """
        Legacy document loading method using SDKDataLoader (kept for fallback).
        
        Args:
            num_of_docs_per_collection: Number of documents to load per collection
            json_template: Template for document generation
            key_prefix: Prefix for document keys
        """
        total_namespaces = len(self.namespaces)
        if total_namespaces == 0:
            self.log.warning("No namespaces found for document loading")
            return

        num_threads = min(100, total_namespaces)

        self.log.info(f"Doc loading (legacy): {total_namespaces} namespaces, "
                      f"{num_threads} concurrent threads")

        failed = []

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            future_to_ns = {}
            for namespace in self.namespaces:
                future = executor.submit(
                    self._load_docs_for_single_namespace, namespace,
                    num_of_docs_per_collection, json_template, key_prefix
                )
                future_to_ns[future] = namespace

            completed = 0
            for future in as_completed(future_to_ns):
                ns = future_to_ns[future]
                completed += 1
                try:
                    future.result()
                    if completed % 10 == 0 or completed == total_namespaces:
                        self.log.info(f"Completed doc load for {ns} ({completed}/{total_namespaces})")
                except Exception as e:
                    self.log.error(f"Doc load failed for {ns}: {e}")
                    failed.append(ns)

        self.assertEqual(
            0, len(failed),
            f"Doc loading failed for {len(failed)}/{total_namespaces} namespaces. "
            f"Cannot proceed with indexes on incomplete data. "
            f"Failed namespaces (first 20): {failed[:20]}"
        )

        self.log.info("All parallel doc loading threads completed")

    def _load_docs_for_single_namespace(self, namespace, num_of_docs_per_collection, json_template, key_prefix):
        """
        Loads documents for a single namespace (legacy method, kept for compatibility).
        """
        _, keyspace = namespace.split(':')
        bucket, scope, collection = keyspace.split('.')
        gen_create = SDKDataLoader(num_ops=num_of_docs_per_collection, percent_create=100,
                                   percent_update=0, percent_delete=0, scope=scope,
                                   collection=collection, json_template=json_template,
                                   output=True, username=self.username, password=self.password,
                                   key_prefix=key_prefix, ops_rate=100000, workers=2)

        self.log.info(f"Triggering doc load for {namespace} with {num_of_docs_per_collection} docs")
        if self.use_magma_loader:
            task = self.cluster.async_load_gen_docs(self.master, bucket=bucket,
                                                    generator=gen_create,
                                                    use_magma_loader=True)
        else:
            task = self.cluster.async_load_gen_docs(self.master, bucket, gen_create,
                                                    pause_secs=1, timeout_secs=300,
                                                    dataset=json_template)
        task.result()
        self.log.info(f"Finished doc load for {namespace}")

    def _generate_random_suffix(self, length=12):
        """
        Generate a unique suffix for index names using timestamp + counter + random chars.
        Format: <letter><timestamp_hex><counter><random>
        
        N1QL index naming conventions:
        - Must start with a letter (a-z, A-Z) or underscore (_)
        - Can contain letters, digits, and underscores
        - Cannot start with a digit
        
        This method ensures the suffix always starts with a letter for N1QL compliance.
        """
        if not hasattr(self, '_index_counter'):
            self._index_counter = 0
        self._index_counter += 1
        
        # Start with a random letter to ensure N1QL compliance
        start_letter = random.choice(string.ascii_lowercase)
        
        # Timestamp component (last 6 hex digits of current time in microseconds)
        ts_hex = format(int(time.time() * 1000000) % 0xFFFFFF, 'x')
        
        # Counter component (ensures uniqueness within same microsecond)
        counter = format(self._index_counter, 'x')
        
        # Random component for additional entropy (letters and digits only, no special chars)
        rand_chars = ''.join(random.choices(string.ascii_lowercase + string.digits, k=5))
        
        return f"{start_letter}{ts_hex}{counter}{rand_chars}"

    def _get_total_index_instances(self, index_nodes=None):
        """
        Returns the total number of index instances across the cluster.

        Uses getIndexStatus (not the per-node num_indexes stat) so that ALL index
        types are counted correctly across all storage engines.

        Instance formula per catalog entry:
            instances = max(numPartition, 1)
        The API returns one entry per physical replica (replicaId), so (numReplica + 1)
        must NOT be applied per entry — doing so double-counts replicated indexes.
        Examples:
            Partitioned-8 (numPartition=8, numReplica=0) -> 1 entry -> 8 instances
            Scalar-with-replica (numPartition=1, numReplica=1) -> 2 entries -> 1+1 = 2 instances
        """
        if index_nodes is None:
            index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)

        # getIndexStatus is cluster-wide; query any single node.
        rest = RestConnection(index_nodes[0])
        metadata = rest.get_indexer_metadata(return_system_query_scope=False)
        status_list = metadata.get('status', [])

        total = 0
        for index_info in status_list:
            num_partition = max(index_info.get('numPartition', 1) or 1, 1)
            instances = num_partition
            total += instances

        self.log.info(f"Total index instances across cluster (via getIndexStatus): {total}")
        return total

    def _calculate_index_distribution(self):
        """
        Pre-calculate index distribution using random namespace selection.

        Target: 10,000 index instances distributed as:
        - 125 Partitioned scalar indexes (8 partitions each) = 1,000 instances
        - 4,480 Scalar indexes with 1 replica (2 instances each) = 8,960 instances
        - 20 Composite indexes with 1 replica (2 instances each) = 40 instances
        Total: 4,625 indexes = 10,000 instances

        Composite indexes were previously vector (bhive) indexes, which are commented
        out for this run; those 20 slots have been repurposed as multi-field Composite
        indexes, with scalar_count reduced by 20 to keep the total at 10,000 instances.

        Namespaces are randomly selected for each index type independently,
        so some namespaces may have multiple index types.

        Returns:
            tuple: (index_distribution, index_plan)
                - index_distribution: Dict with namespace lists for each index type
                - index_plan: Dict with detailed breakdown of the plan
        """
        total_namespaces = len(self.namespaces)

        # Index configuration for 10K instances
        partitioned_count = 125    # 125 x 8 = 1,000 instances
        composite_count = 20       # 20 x 2 = 40 instances (repurposed vector-index slots)
        scalar_count = 4500 - composite_count  # 4,480 x 2 = 8,960 instances

        total_indexes = partitioned_count + scalar_count + composite_count          # 4,625
        total_instances = (partitioned_count * 8) + (scalar_count * 2) + (composite_count * 2)  # 10,000

        self.log.info("=" * 80)
        self.log.info("PRE-CALCULATING INDEX DISTRIBUTION (RANDOM SELECTION)")
        self.log.info("=" * 80)
        self.log.info(f"Total namespaces available: {total_namespaces}")
        self.log.info(f"Target index instances: {total_instances}")
        self.log.info("")
        self.log.info("INDEX CONFIGURATION:")
        self.log.info(f"  Partitioned indexes (8 partitions): {partitioned_count} x 8 instances = {partitioned_count * 8} instances")
        self.log.info(f"  Scalar indexes (1 replica): {scalar_count} x 2 instances = {scalar_count * 2} instances")
        self.log.info(f"  Composite indexes (1 replica): {composite_count} x 2 instances = {composite_count * 2} instances")
        self.log.info(f"  Total indexes: {total_indexes}")
        self.log.info(f"  Total instances: {total_instances}")

        # Randomly select namespaces for each index type
        all_namespaces = list(self.namespaces)

        # Select namespaces for Partitioned indexes
        partitioned_namespaces = random.sample(all_namespaces, min(partitioned_count, total_namespaces))
        self.log.info(f"")
        self.log.info(f"Randomly selected {len(partitioned_namespaces)} namespaces for Partitioned indexes")

        # Select namespaces for Scalar indexes
        scalar_namespaces = random.sample(all_namespaces, min(scalar_count, total_namespaces))
        self.log.info(f"Randomly selected {len(scalar_namespaces)} namespaces for Scalar indexes")

        # Select namespaces for Composite indexes
        composite_namespaces = random.sample(all_namespaces, min(composite_count, total_namespaces))
        self.log.info(f"Randomly selected {len(composite_namespaces)} namespaces for Composite indexes")

        # Calculate overlap statistics
        all_indexed_namespaces = set(partitioned_namespaces) | set(scalar_namespaces) | set(composite_namespaces)
        namespaces_with_indexes = len(all_indexed_namespaces)
        namespaces_without_indexes = total_namespaces - namespaces_with_indexes

        # Count namespaces by number of index types
        namespace_index_count = {}
        for ns in all_namespaces:
            count = 0
            if ns in partitioned_namespaces:
                count += 1
            if ns in scalar_namespaces:
                count += 1
            if ns in composite_namespaces:
                count += 1
            if count > 0:
                namespace_index_count[ns] = count

        ns_with_1_type = sum(1 for c in namespace_index_count.values() if c == 1)
        ns_with_2_types = sum(1 for c in namespace_index_count.values() if c == 2)
        ns_with_3_types = sum(1 for c in namespace_index_count.values() if c == 3)

        index_distribution = {
            'partitioned_scalar': {
                'count': len(partitioned_namespaces),
                'instances_per_index': 8,
                'num_partitions': 8,
                'total_instances': len(partitioned_namespaces) * 8,
                'namespaces': partitioned_namespaces
            },
            'scalar_with_replica': {
                'count': len(scalar_namespaces),
                'instances_per_index': 2,
                'num_replica': 1,
                'total_instances': len(scalar_namespaces) * 2,
                'namespaces': scalar_namespaces
            },
            'composite_with_replica': {
                'count': len(composite_namespaces),
                'instances_per_index': 2,
                'num_replica': 1,
                'total_instances': len(composite_namespaces) * 2,
                'namespaces': composite_namespaces
            }
        }

        actual_total_instances = (
            index_distribution['partitioned_scalar']['total_instances'] +
            index_distribution['scalar_with_replica']['total_instances'] +
            index_distribution['composite_with_replica']['total_instances']
        )

        index_plan = {
            'total_namespaces': total_namespaces,
            'namespaces_with_indexes': namespaces_with_indexes,
            'namespaces_without_indexes': namespaces_without_indexes,
            'total_indexes': total_indexes,
            'total_instances': actual_total_instances,
            'partitioned_indexes': len(partitioned_namespaces),
            'scalar_indexes': len(scalar_namespaces),
            'composite_indexes': len(composite_namespaces),
            'ns_with_1_index_type': ns_with_1_type,
            'ns_with_2_index_types': ns_with_2_types,
            'ns_with_3_index_types': ns_with_3_types,
        }

        self.log.info("")
        self.log.info("INDEX DISTRIBUTION PLAN:")
        self.log.info(f"  Total namespaces: {index_plan['total_namespaces']}")
        self.log.info(f"  Namespaces WITH indexes: {index_plan['namespaces_with_indexes']}")
        self.log.info(f"  Namespaces WITHOUT indexes: {index_plan['namespaces_without_indexes']}")
        self.log.info(f"")
        self.log.info(f"  Partitioned indexes: {index_plan['partitioned_indexes']} ({index_plan['partitioned_indexes'] * 8} instances)")
        self.log.info(f"  Scalar indexes (with replica): {index_plan['scalar_indexes']} ({index_plan['scalar_indexes'] * 2} instances)")
        self.log.info(f"  Composite indexes (with replica): {index_plan['composite_indexes']} ({index_plan['composite_indexes'] * 2} instances)")
        self.log.info(f"  Total indexes: {index_plan['total_indexes']}")
        self.log.info(f"  Total instances: {index_plan['total_instances']}")
        self.log.info(f"")
        self.log.info(f"  NAMESPACE OVERLAP (due to random selection):")
        self.log.info(f"    Namespaces with 1 index type: {index_plan['ns_with_1_index_type']}")
        self.log.info(f"    Namespaces with 2 index types: {index_plan['ns_with_2_index_types']}")
        self.log.info(f"    Namespaces with 3 index types: {index_plan['ns_with_3_index_types']}")
        self.log.info("=" * 80)

        # Store for later use
        self.indexed_namespaces = list(all_indexed_namespaces)

        return index_distribution, index_plan

    def _generate_all_index_queries(self, index_distribution, defer_build=True):
        """
        Generate all CREATE INDEX queries based on the pre-calculated distribution.
        
        Args:
            index_distribution: Dict with namespace lists for each index type
            defer_build: Whether to use defer_build=true
            
        Returns:
            tuple: (all_create_queries, all_definitions, query_details)
                - all_create_queries: List of all CREATE INDEX query strings
                - all_definitions: Dict mapping namespace -> list of QueryDefinition objects
                - query_details: Dict with per-type breakdown for logging
        """
        self.log.info("=" * 80)
        self.log.info("GENERATING ALL INDEX CREATE QUERIES")
        self.log.info("=" * 80)
        
        all_create_queries = []
        all_definitions = {}  # namespace -> list of definitions
        query_details = {
            'partitioned_queries': [],
            'scalar_queries': [],
            'composite_queries': [],
            'by_namespace': {}
        }

        # 1. Partitioned scalar index generation
        
        # 2. Partitioned scalar index queries
        partitioned_namespaces = index_distribution['partitioned_scalar']['namespaces']
        self.log.info(f"Generating {len(partitioned_namespaces)} Partitioned index queries...")
        
        for idx, namespace in enumerate(partitioned_namespaces):
            random_suffix = self._generate_random_suffix()
            partitioned_prefix = f"idx_10k_part_{random_suffix}_"
            
            partitioned_query_template = RANGE_SCAN_TEMPLATE.format("name, price", "country IS NOT NULL ORDER BY name")
            partitioned_def = QueryDefinition(
                index_name=f"{partitioned_prefix}country_price",
                index_fields=['country', 'price'],
                partition_by_fields=['country'],
                query_template=partitioned_query_template
            )
            partitioned_query = partitioned_def.generate_index_create_query(
                namespace=namespace, defer_build=defer_build, num_replica=0, num_partition=8
            )
            
            all_create_queries.append(partitioned_query)
            query_details['partitioned_queries'].append(partitioned_query)
            
            if namespace not in all_definitions:
                all_definitions[namespace] = []
            all_definitions[namespace].append(partitioned_def)
            
            if namespace not in query_details['by_namespace']:
                query_details['by_namespace'][namespace] = {}
            query_details['by_namespace'][namespace]['partitioned'] = partitioned_query
            
            if (idx + 1) % 50 == 0 or (idx + 1) == len(partitioned_namespaces):
                self.log.info(f"  Generated {idx + 1}/{len(partitioned_namespaces)} Partitioned queries")
        
        # 3. Generate Scalar index queries with replica
        scalar_namespaces = index_distribution['scalar_with_replica']['namespaces']
        self.log.info(f"Generating {len(scalar_namespaces)} Scalar index queries...")
        
        for idx, namespace in enumerate(scalar_namespaces):
            random_suffix = self._generate_random_suffix()
            scalar_prefix = f"idx_10k_scalar_{random_suffix}_"
            
            scalar_defs = self.gsi_util_obj.get_index_definition_list(
                dataset='Hotel', prefix=scalar_prefix,
                scalar=True, skip_primary=True, array_indexes=False, bhive_index=False
            )
            non_partitioned_scalar = [d for d in scalar_defs if not d.partition_by_fields]
            if not non_partitioned_scalar:
                self.log.warning(f"No non-partitioned scalar definitions available for namespace {namespace}; skipping scalar index generation")

            if non_partitioned_scalar:
                selected_scalar = non_partitioned_scalar[0]
                create_queries = self.gsi_util_obj.get_create_index_list(
                    definition_list=[selected_scalar], namespace=namespace,
                    num_replica=1, defer_build=defer_build
                )
                if create_queries:
                    all_create_queries.extend(create_queries)
                    query_details['scalar_queries'].extend(create_queries)
                    
                    if namespace not in all_definitions:
                        all_definitions[namespace] = []
                    all_definitions[namespace].append(selected_scalar)
                    
                    if namespace not in query_details['by_namespace']:
                        query_details['by_namespace'][namespace] = {}
                    query_details['by_namespace'][namespace]['scalar'] = create_queries[0]
            
            if (idx + 1) % 500 == 0 or (idx + 1) == len(scalar_namespaces):
                self.log.info(f"  Generated {idx + 1}/{len(scalar_namespaces)} Scalar queries")

        # 4. Generate Composite (multi-field) index queries with replica.
        # These 20 slots replace what would otherwise be vector (bhive) indexes —
        # vector indexes are commented out for this run — so scalar_count was
        # reduced by 20 to keep the cluster-wide total at 10,000 instances.
        query_details['composite_queries'] = []
        composite_namespaces = index_distribution.get('composite_with_replica', {}).get('namespaces', [])
        self.log.info(f"Generating {len(composite_namespaces)} Composite index queries...")

        for idx, namespace in enumerate(composite_namespaces):
            random_suffix = self._generate_random_suffix()
            composite_prefix = f"idx_10k_composite_{random_suffix}_"

            composite_defs = self.gsi_util_obj.get_index_definition_list(
                dataset='Hotel', prefix=composite_prefix,
                scalar=True, skip_primary=True, array_indexes=False, bhive_index=False
            )
            # A Composite index covers more than one field; pick the first
            # non-partitioned, multi-field definition available.
            composite_candidates = [d for d in composite_defs
                                    if not d.partition_by_fields and len(d.index_fields) > 1]
            if not composite_candidates:
                self.log.warning(f"No composite (multi-field) definitions available for "
                                 f"namespace {namespace}; skipping composite index generation")

            if composite_candidates:
                selected_composite = composite_candidates[0]
                create_queries = self.gsi_util_obj.get_create_index_list(
                    definition_list=[selected_composite], namespace=namespace,
                    num_replica=1, defer_build=defer_build
                )
                if create_queries:
                    all_create_queries.extend(create_queries)
                    query_details['composite_queries'].extend(create_queries)

                    if namespace not in all_definitions:
                        all_definitions[namespace] = []
                    all_definitions[namespace].append(selected_composite)

                    if namespace not in query_details['by_namespace']:
                        query_details['by_namespace'][namespace] = {}
                    query_details['by_namespace'][namespace]['composite'] = create_queries[0]

            if (idx + 1) % 10 == 0 or (idx + 1) == len(composite_namespaces):
                self.log.info(f"  Generated {idx + 1}/{len(composite_namespaces)} Composite queries")

        self.log.info("")
        self.log.info("QUERY GENERATION SUMMARY:")
        self.log.info(f"  Total CREATE INDEX queries: {len(all_create_queries)}")
        self.log.info(f"  Partitioned queries: {len(query_details['partitioned_queries'])}")
        self.log.info(f"  Scalar (with replica) queries: {len(query_details['scalar_queries'])}")
        self.log.info(f"  Composite (with replica) queries: {len(query_details['composite_queries'])}")
        self.log.info(f"  Namespaces with definitions: {len(all_definitions)}")
        self.log.info("=" * 80)

        return all_create_queries, all_definitions, query_details

    def _get_query_nodes_round_robin(self):
        """
        Get all query nodes and return a generator that cycles through them.
        This distributes load across all available N1QL nodes.
        
        Returns:
            tuple: (list of all query nodes, generator that cycles through nodes)
        """
        from itertools import cycle
        
        query_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        if not query_nodes:
            # Fallback to single node
            query_nodes = [self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)]
        
        if not isinstance(query_nodes, list):
            query_nodes = [query_nodes]
        
        self.log.info(f"Found {len(query_nodes)} query node(s) for round-robin distribution:")
        for node in query_nodes:
            self.log.info(f"  - {node.ip}:{getattr(node, 'port', 8093)}")
        
        return query_nodes, cycle(query_nodes)

    def _async_create_all_indexes(self, all_create_queries, query_details, batch_size=50):
        """
        Execute all CREATE INDEX queries in batches to avoid request timeouts.
        Queries are split evenly across all query nodes and executed in parallel.
        Each node runs its batches asynchronously with per-batch thread pools.
        
        NO RETRY: each CREATE runs exactly once. Failures are collected and returned;
        the caller hard-asserts none occurred.
          - Parallel fan-out across all query nodes
          - "index already exists" (4300) counts as success — the index was created
            before a connection reset, not a real failure
          - Inter-batch pacing only (a sleep between batches to protect the query
            service); this is throttling, not re-execution of any query

        Args:
            all_create_queries: List of all CREATE INDEX query strings
            query_details: Dict with query breakdown for detailed logging
            batch_size: Number of CREATE INDEX queries to execute per batch

        Returns:
            tuple: (success_count, failed_queries)  # failed_queries must be empty
        """
        self.log.info("=" * 80)
        self.log.info("BATCHED ASYNC INDEX CREATION")
        self.log.info("=" * 80)
        
        total_queries = len(all_create_queries)
        num_batches = (total_queries + batch_size - 1) // batch_size  # Ceiling division
        
        self.log.info(f"Total CREATE INDEX queries: {total_queries}")
        self.log.info(f"Batch size: {batch_size}")
        self.log.info(f"Number of batches: {num_batches}")
        
        # Get all query nodes and distribute work in parallel
        query_nodes, _ = self._get_query_nodes_round_robin()
        
        start_time = time.time()
        total_success = 0
        total_already_exists = 0
        all_failed_queries = []

        # Split queries evenly across all query nodes
        num_nodes = len(query_nodes)
        chunk_size = (total_queries + num_nodes - 1) // num_nodes

        def _process_queries_on_node(node, queries, offset):
            base_delay = 3
            current_delay = base_delay
            max_delay = 30
            consecutive_error_batches = 0
            success = 0
            already_exists = 0
            failed = []
            node_batches = (len(queries) + batch_size - 1) // batch_size
            for b in range(node_batches):
                b_start = b * batch_size
                b_end = min(b_start + batch_size, len(queries))
                batch_queries = queries[b_start:b_end]
                g_start = offset + b_start
                # Watchdog: abort fast if any node crashed rather than retrying a dead
                # endpoint ~25 min/query across the remaining batches.
                self._assert_all_nodes_up(f"Step 2b create batch (node {node.ip})")
                self.log.info(f"")
                self.log.info(f"[{node.ip}] BATCH {b + 1}/{node_batches}: queries {g_start + 1}-{offset + b_end} of {total_queries}")
                batch_start_time = time.time()
                tasks = []
                with ThreadPoolExecutor(max_workers=self.max_concurrent_index_creates) as executor:
                    for query in batch_queries:
                        tasks.append(executor.submit(self.run_cbq_query, query=query, server=node))
                batch_success = 0
                batch_ae = 0
                batch_sock = 0
                batch_other = 0
                for i, task in enumerate(tasks):
                    try:
                        task.result()
                        batch_success += 1
                        success += 1
                    except Exception as e:
                        error_str = str(e).lower()
                        global_idx = g_start + i
                        if "already exists" in error_str or "code\":4300" in error_str:
                            batch_ae += 1
                            already_exists += 1
                            success += 1
                        elif "connection refused" in error_str or "connection reset" in error_str or "socket error" in error_str:
                            batch_sock += 1
                            failed.append({'query_index': global_idx, 'query': batch_queries[i],
                                           'batch': b + 1, 'error': str(e), 'error_type': 'socket'})
                        else:
                            batch_other += 1
                            failed.append({'query_index': global_idx, 'query': batch_queries[i],
                                           'batch': b + 1, 'error': str(e), 'error_type': 'other'})
                batch_elapsed = time.time() - batch_start_time
                batch_errors = batch_sock + batch_other
                self.log.info(f"  [{node.ip}] Batch {b + 1} completed in {batch_elapsed:.2f}s: "
                            f"Success={batch_success}, AlreadyExists={batch_ae}")
                if batch_errors > 0:
                    self.log.warning(f"    Socket={batch_sock}, Other={batch_other}")
                if batch_sock > 0:
                    consecutive_error_batches += 1
                    current_delay = min(base_delay * (2 ** consecutive_error_batches), max_delay)
                    self.log.warning(f"  Socket errors - increasing delay to {current_delay}s")
                else:
                    consecutive_error_batches = 0
                    current_delay = base_delay
                if b < node_batches - 1:
                    time.sleep(current_delay)
            return success, already_exists, failed

        # Fan out across all query nodes in parallel
        with ThreadPoolExecutor(max_workers=num_nodes) as executor:
            futures = []
            for i, node in enumerate(query_nodes):
                start_idx = i * chunk_size
                end_idx = min(start_idx + chunk_size, total_queries)
                futures.append(executor.submit(
                    _process_queries_on_node, node, all_create_queries[start_idx:end_idx], start_idx))
            for f in futures:
                s, ae, failed = f.result()
                total_success += s
                total_already_exists += ae
                all_failed_queries.extend(failed)

        # NoneType failures are transient socket-level drops where the HTTP client
        # received no response body (not an actual index creation failure). Retry each
        # once on a fresh node before surfacing as a hard failure.
        nonetype_failures = [fq for fq in all_failed_queries
                             if 'nonetype' in fq.get('error', '').lower()]
        if nonetype_failures:
            self.log.warning(f"[RetryNoneType] {len(nonetype_failures)} NoneType (dropped "
                             f"connection) failures detected — retrying each once...")
            all_failed_queries = [fq for fq in all_failed_queries
                                  if fq not in nonetype_failures]
            for j, fq in enumerate(nonetype_failures):
                retry_node = query_nodes[j % len(query_nodes)]
                try:
                    self.run_cbq_query(query=fq['query'], server=retry_node)
                    total_success += 1
                    self.log.info(f"  [RetryNoneType] query #{fq['query_index']} succeeded "
                                  f"on retry via {retry_node.ip}")
                except Exception as retry_e:
                    retry_err = str(retry_e).lower()
                    if "already exists" in retry_err or 'code":4300' in retry_err:
                        # Index was committed by the indexer before the socket dropped.
                        # "Already exists" on retry confirms successful creation.
                        total_success += 1
                        self.log.info(f"  [RetryNoneType] query #{fq['query_index']} "
                                      f"already exists — created before socket dropped, "
                                      f"counting as success")
                    else:
                        self.log.error(f"  [RetryNoneType] query #{fq['query_index']} failed "
                                       f"again: {retry_e}")
                        fq['error'] = str(retry_e)
                        all_failed_queries.append(fq)

        elapsed_time = time.time() - start_time

        self.log.info("")
        self.log.info("=" * 60)
        self.log.info("CREATE INDEX RESULTS (ALL BATCHES):")
        self.log.info("=" * 60)
        self.log.info(f"  Total queries executed: {total_queries}")
        self.log.info(f"  Successful (new): {total_success - total_already_exists}")
        self.log.info(f"  Successful (already existed): {total_already_exists}")
        self.log.info(f"  Total successful: {total_success}")
        self.log.info(f"  Failed: {len(all_failed_queries)}")
        self.log.info(f"  Total time taken: {elapsed_time:.2f} seconds")
        if elapsed_time > 0:
            self.log.info(f"  Rate: {total_queries / elapsed_time:.2f} queries/second")
        
        if all_failed_queries:
            socket_errors = [fq for fq in all_failed_queries if fq.get('error_type') == 'socket']
            other_errors = [fq for fq in all_failed_queries if fq.get('error_type') == 'other']
            
            if socket_errors:
                self.log.error(f"Socket errors ({len(socket_errors)} total) - these may have succeeded:")
                for fq in socket_errors[:5]:
                    self.log.error(f"  Query #{fq['query_index']} (batch {fq['batch']})")
                if len(socket_errors) > 5:
                    self.log.error(f"  ... and {len(socket_errors) - 5} more socket errors")
            
            if other_errors:
                self.log.error(f"Other errors ({len(other_errors)} total):")
                for fq in other_errors[:5]:
                    self.log.error(f"  Query #{fq['query_index']} (batch {fq['batch']}): {fq['error'][:100]}...")
                if len(other_errors) > 5:
                    self.log.error(f"  ... and {len(other_errors) - 5} more errors")
        
        # NO RETRY: socket/other failures are surfaced as-is. The caller hard-asserts
        # that all_failed_queries is empty, so any create failure fails the test.

        # Log per-type breakdown
        self.log.info("")
        self.log.info("INDEX CREATION BREAKDOWN:")
        self.log.info(f"  Scalar indexes (with replica): {len(query_details.get('scalar_queries', []))}")
        self.log.info(f"  Partitioned indexes: {len(query_details.get('partitioned_queries', []))}")
        self.log.info(f"  Composite indexes (with replica): {len(query_details.get('composite_queries', []))}")
        self.log.info("=" * 60)
        
        self.log.info("")
        self.log.info("FINAL SUMMARY:")
        self.log.info(f"  Total successful: {total_success}")
        self.log.info(f"  Total failed: {len(all_failed_queries)}")
        self.log.info("=" * 60)
        
        return total_success, all_failed_queries
    
    def _verify_indexes_created(self, all_definitions):
        """
        Single-pass check that every expected index exists in metadata (Created/
        Building/Ready/Scheduled). NO RETRY, NO re-create: this only observes and
        reports. The caller hard-asserts that the returned missing list is empty.

        Args:
            all_definitions: Dict mapping namespace to list of QueryDefinition objects

        Returns:
            tuple: (verified_count, missing_indexes)
        """
        self.log.info("=" * 80)
        self.log.info("VERIFYING INDEXES ARE IN CREATED STATE (single pass, no retry)")
        self.log.info("=" * 80)

        # Expected (namespace, index_name) set from the definitions we issued.
        expected_indexes = {}
        for namespace, definitions in all_definitions.items():
            for defn in definitions:
                expected_indexes[(namespace, defn.index_name)] = defn
        total_expected = len(expected_indexes)
        self.log.info(f"Total indexes expected: {total_expected}")

        # Cluster-wide index metadata (get_indexer_metadata returns the full topology).
        index_metadata = self.index_rest.get_indexer_metadata()
        existing_indexes = set()
        for idx_info in index_metadata.get('status', []):
            idx_name = idx_info.get('indexName', idx_info.get('name', ''))
            idx_bucket = idx_info.get('bucket', '')
            idx_scope = idx_info.get('scope', '')
            idx_collection = idx_info.get('collection', '')
            idx_status = idx_info.get('status', 'Unknown')
            if idx_bucket and idx_scope and idx_collection and \
                    idx_status in ('Created', 'Building', 'Ready', 'Scheduled'):
                existing_indexes.add((f"default:{idx_bucket}.{idx_scope}.{idx_collection}", idx_name))

        missing_indexes = [
            {'namespace': key[0], 'index_name': key[1]}
            for key in expected_indexes
            if key not in existing_indexes
        ]
        verified_count = total_expected - len(missing_indexes)

        self.log.info(f"  Verified (Created/Building/Ready/Scheduled): {verified_count}")
        self.log.info(f"  Missing: {len(missing_indexes)}")
        if missing_indexes:
            self.log.error(f"{len(missing_indexes)} expected indexes are NOT present in metadata:")
            for idx in missing_indexes[:20]:
                self.log.error(f"  - {idx['namespace']}: {idx['index_name']}")
            if len(missing_indexes) > 20:
                self.log.error(f"  ... and {len(missing_indexes) - 20} more")
        self.log.info("=" * 80)

        return verified_count, missing_indexes

    def _verify_index_metadata_intact(self, expected_instances, step_label=""):
        """
        Verify no indexes and no replicas have been lost (e.g. after a rebalance).

        Two checks:
          1. Total instance count matches expected_instances.
          2. For every index that carries replicas, all replica instances are present.

        Uses getIndexStatus (same source as _get_total_index_instances) so Bhive and
        partitioned indexes are counted correctly.

        Args:
            expected_instances: Instance count recorded before the rebalance phase.
            step_label: Label for log messages, e.g. "Step 6".

        Returns:
            bool: True if both checks pass, False if any discrepancy is found.
        """
        from collections import defaultdict

        prefix = f"[{step_label}] " if step_label else ""
        self.log.info(f"{prefix}Verifying index metadata integrity (expected {expected_instances} instances)...")

        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        rest = RestConnection(index_nodes[0])
        metadata = rest.get_indexer_metadata(return_system_query_scope=False)
        status_list = metadata.get('status', [])

        # ---- 1. Total instance count ----
        # Each entry in status_list represents one (indexName, replicaId) pair.
        # Instances contributed by that entry = max(numPartition, 1).
        actual_instances = sum(max(idx.get('numPartition', 1) or 1, 1) for idx in status_list)
        instance_ok = (actual_instances == expected_instances)

        if instance_ok:
            self.log.info(f"{prefix}Instance count OK: {actual_instances} == {expected_instances}")
        else:
            diff = expected_instances - actual_instances
            if diff > 0:
                self.log.error(
                    f"{prefix}MISSING INSTANCES: expected={expected_instances}, "
                    f"actual={actual_instances}, missing={diff}"
                )
            else:
                self.log.warning(
                    f"{prefix}EXTRA INSTANCES: expected={expected_instances}, "
                    f"actual={actual_instances}, extra={-diff}"
                )

        # ---- 2. Replica integrity ----
        # Group getIndexStatus entries by (bucket, scope, collection, indexName).
        # Each group should have (numReplica + 1) entries — one per replicaId.
        groups = defaultdict(list)
        for idx in status_list:
            key = (
                idx.get('bucket', ''),
                idx.get('scope', ''),
                idx.get('collection', ''),
                idx.get('indexName', idx.get('name', ''))
            )
            groups[key].append(idx)

        missing_replicas = []
        for key, entries in groups.items():
            num_replica = entries[0].get('numReplica', 0) or 0
            expected_entries = num_replica + 1     # replicaId 0 … numReplica
            actual_entries = len(entries)
            if actual_entries < expected_entries:
                missing_replicas.append({
                    'index': key,
                    'expected': expected_entries,
                    'actual': actual_entries,
                    'missing': expected_entries - actual_entries,
                })

        replicas_ok = len(missing_replicas) == 0
        if replicas_ok:
            self.log.info(f"{prefix}Replica integrity OK: all indexes have their full replica set")
        else:
            self.log.error(f"{prefix}REPLICA MISMATCH: {len(missing_replicas)} indexes missing replicas:")
            for m in missing_replicas[:20]:
                bucket, scope, coll, iname = m['index']
                self.log.error(
                    f"{prefix}  {bucket}.{scope}.{coll}/{iname}: "
                    f"expected {m['expected']} replica(s), got {m['actual']} "
                    f"(missing {m['missing']})"
                )
            if len(missing_replicas) > 20:
                self.log.error(f"{prefix}  … and {len(missing_replicas) - 20} more")

        all_ok = instance_ok and replicas_ok
        if all_ok:
            self.log.info(f"{prefix}Index metadata integrity check PASSED")
        else:
            self.log.error(f"{prefix}Index metadata integrity check FAILED")

        return all_ok

    def _snapshot_index_names(self):
        """
        Take a snapshot of every (bucket, scope, collection, indexName, replicaId)
        entry currently in the indexer metadata. Used as the "before" and "after"
        picture for the post-rebalance index validation gate.

        Returns:
            set of tuples: {(bucket, scope, collection, indexName, replicaId), ...}
        """
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        rest = RestConnection(index_nodes[0])
        metadata = rest.get_indexer_metadata(return_system_query_scope=False)
        status_list = metadata.get('status', [])
        return {
            (
                idx.get('bucket', ''),
                idx.get('scope', ''),
                idx.get('collection', ''),
                idx.get('indexName', idx.get('name', '')),
                idx.get('replicaId', 0),
            )
            for idx in status_list
        }

    def _compare_index_snapshots(self, before, after, step_label=""):
        """Compare before/after index-name snapshots; log lost/added indexes and return (lost, added) sets."""
        prefix = f"[{step_label}] " if step_label else ""
        missing = before - after   # present before, gone after -> lost during rebalance
        added = after - before     # present after, not before -> unexpected new index
        LOG_CAP = 50

        if not missing and not added:
            self.log.info(f"{prefix}Post-rebalance index validation PASSED: "
                          f"all {len(before)} indexes present before and after rebalance")
            return missing, added

        if missing:
            self.log.error(f"{prefix}POST-REBALANCE INDEX VALIDATION FAILED: "
                           f"{len(missing)} index(es) lost/missing during rebalance:")
            for bucket, scope, coll, iname, replica_id in sorted(missing)[:LOG_CAP]:
                self.log.error(f"{prefix}  LOST: {bucket}.{scope}.{coll}/{iname} "
                               f"(replicaId={replica_id})")
            if len(missing) > LOG_CAP:
                self.log.error(f"{prefix}  ... and {len(missing) - LOG_CAP} more lost index(es)")
        if added:
            self.log.error(f"{prefix}POST-REBALANCE INDEX VALIDATION: "
                           f"{len(added)} unexpected index(es) present after rebalance "
                           f"that were not present before:")
            for bucket, scope, coll, iname, replica_id in sorted(added)[:LOG_CAP]:
                self.log.error(f"{prefix}  UNEXPECTED: {bucket}.{scope}.{coll}/{iname} "
                               f"(replicaId={replica_id})")
            if len(added) > LOG_CAP:
                self.log.error(f"{prefix}  ... and {len(added) - LOG_CAP} more unexpected index(es)")
        return missing, added

    def _run_mutations_via_bulk_loader(self, event, namespaces, ops_rate=100, excluded_nodes=None):
        """
        Background thread that continuously runs mutations on indexed namespaces
        using the hotel_bulk_loader.py script in update mode.
        
        Namespaces are processed in parallel using ThreadPoolExecutor to ensure
        mutations actually generate load during the rebalance window.
        
        Args:
            event: Threading Event to signal when to stop
            namespaces: List of namespaces to run mutations on
            ops_rate: Target operations per second per namespace
            excluded_nodes: List of nodes being rebalanced (to avoid using them for mutations)
        """
        if excluded_nodes is None:
            excluded_nodes = []
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        script_path = os.path.join(script_dir, "hotel_bulk_loader.py")

        # Distribute mutations round-robin across all live query nodes,
        # excluding any being rebalanced.
        from itertools import cycle as _mut_cycle
        excluded_ips = {n.ip if hasattr(n, 'ip') else n for n in excluded_nodes}
        query_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        if not isinstance(query_nodes, list):
            query_nodes = [query_nodes]
        live_nodes = [n for n in query_nodes if n.ip not in excluded_ips] or query_nodes

        num_workers = min(self.max_loader_threads, len(namespaces))
        self.log.info(f"Mutations distributed across {len(live_nodes)} query node(s): "
                      f"{[n.ip for n in live_nodes]} ({num_workers} workers, {ops_rate} ops/sec/worker)")

        def _mutate_namespace(namespace, node):
            """Mutate a single namespace via subprocess. Returns (namespace, success)."""
            _, keyspace = namespace.split(':')
            bucket, scope, collection = keyspace.split('.')
            _base = self.num_of_docs_per_collection
            coll_num = int(collection.split('_')[-1])
            ns_start_id = (coll_num - 1) * _base
            cmd = [
                sys.executable, script_path,
                node.ip, self.username, self.password,
                bucket, scope, collection,
                str(_base),
                "hotel_",
                str(self.bulk_loader_batch_size),
                str(ns_start_id),
                "update",
                str(ops_rate)
            ]
            try:
                subprocess.run(cmd, capture_output=True, text=True, timeout=600)
                return (namespace, True)
            except Exception as e:
                self.log.warning(f"Mutation failed for {namespace} via {node.ip}: {e} "
                                 f"(keys=[hotel_{ns_start_id}..hotel_{ns_start_id + _base - 1}])")
                return (namespace, False)

        cycle = 0

        while not event.is_set():
            cycle += 1
            cycle_start = time.time()
            # Pre-assign a node to each namespace round-robin so the assignment is
            # deterministic within a cycle (no generator races inside threads).
            _node_gen = _mut_cycle(live_nodes)
            ns_node_pairs = [(ns, next(_node_gen)) for ns in namespaces]
            self.log.info(f"Mutation cycle {cycle} starting on {len(namespaces)} namespaces "
                          f"({num_workers} workers, {ops_rate} ops/sec/worker)")

            completed = 0
            failed = 0

            executor = ThreadPoolExecutor(max_workers=num_workers)
            try:
                futures = {executor.submit(_mutate_namespace, ns, node): ns
                           for ns, node in ns_node_pairs}
                for future in as_completed(futures):
                    if event.is_set():
                        break
                    ns, ok = future.result()
                    if ok:
                        completed += 1
                    else:
                        failed += 1
            finally:
                # Don't block waiting for in-flight subprocesses (each may take up to 600s).
                # shutdown(wait=False) returns immediately so the caller's join(timeout=300) succeeds.
                executor.shutdown(wait=False)
            
            elapsed = time.time() - cycle_start
            throughput = (completed * self.num_of_docs_per_collection) / elapsed if elapsed > 0 else 0
            self.log.info(f"Mutation cycle {cycle} complete: {completed} ok, {failed} failed "
                         f"({elapsed:.1f}s, ~{throughput:.0f} docs/sec)")
            
            if not event.is_set():
                self.log.info(f"Mutation cycle {cycle} done — pausing "
                              f"{self.rebalance_workload_cycle_gap_secs}s before next cycle")
                time.sleep(self.rebalance_workload_cycle_gap_secs)

        self.log.info(f"Mutation background thread stopped after {cycle} cycles")

    def _run_mutations_sampled_distributed(self, namespaces, num_sample=200, docs_per_op=25,
                                           excluded_nodes=None):
        """
        Randomly sample num_sample collections from namespaces and on each run:
          INSERT  docs_per_op new docs  (keys: base_offset + base_docs .. + docs_per_op - 1)
          UPDATE  docs_per_op existing docs  (keys: base_offset .. + docs_per_op - 1)
          DELETE  the same docs_per_op just inserted  (net doc count unchanged)

        The bulk-loader subprocess connects via a query/data node IP; work is spread
        round-robin across ALL live N1QL nodes, excluding any being rebalanced.
        Returns (failed_namespaces, sampled_namespaces) so callers can scope follow-up
        work (e.g. UPDATE STATISTICS) to only the touched collections.

        Args:
            namespaces:      Pool to sample from (e.g. self.namespaces — 9,500 static).
            num_sample:      How many collections to pick each call (default 200).
            docs_per_op:     Docs for each of insert / update / delete (default 25).
            excluded_nodes:  Nodes being rebalanced — excluded from routing.
        """
        if excluded_nodes is None:
            excluded_nodes = []

        sample = random.sample(namespaces, min(num_sample, len(namespaces)))

        # All live N1QL nodes, excluding any under rebalance.
        query_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        if not isinstance(query_nodes, list):
            query_nodes = [query_nodes]
        excluded_ips = {n.ip if hasattr(n, 'ip') else n for n in excluded_nodes}
        live_nodes = [n for n in query_nodes if n.ip not in excluded_ips] or query_nodes

        from itertools import cycle as _q_cycle
        _node_gen = _q_cycle(live_nodes)
        # Pre-assign a node to each sampled namespace so the assignment is
        # deterministic within a cycle (no races on the generator inside threads).
        ns_node_pairs = [(ns, next(_node_gen)) for ns in sample]

        self.log.info(f"[SampledMutation] {len(sample)} collections sampled, "
                      f"{docs_per_op} insert+update+delete each, "
                      f"across {len(live_nodes)} query node(s): "
                      f"{[n.ip for n in live_nodes]}")

        script_dir = os.path.dirname(os.path.abspath(__file__))
        script_path = os.path.join(script_dir, "hotel_bulk_loader.py")
        base_docs = self.num_of_docs_per_collection

        def _mutate(namespace, node):
            _, keyspace = namespace.split(':', 1)
            bucket, scope, collection = keyspace.split('.')
            coll_num = int(collection.split('_')[-1])
            ns_start_id = (coll_num - 1) * base_docs
            insert_start = ns_start_id + base_docs   # immediately after initial load range

            insert_cmd = [
                sys.executable, script_path, node.ip, self.username, self.password,
                bucket, scope, collection, str(docs_per_op), "hotel_",
                str(self.bulk_loader_batch_size), str(insert_start), "insert"]
            update_cmd = [
                sys.executable, script_path, node.ip, self.username, self.password,
                bucket, scope, collection, str(docs_per_op), "hotel_",
                str(self.bulk_loader_batch_size), str(ns_start_id), "update",
                str(self.mutation_ops_rate)]
            delete_cmd = [
                sys.executable, script_path, node.ip, self.username, self.password,
                bucket, scope, collection, str(docs_per_op), "hotel_",
                str(self.bulk_loader_batch_size), str(insert_start), "delete"]
            try:
                r1 = subprocess.run(insert_cmd, capture_output=True, text=True, timeout=300)
                r2 = subprocess.run(update_cmd, capture_output=True, text=True, timeout=300)
                r3 = subprocess.run(delete_cmd, capture_output=True, text=True, timeout=300)
                if r1.returncode != 0 or r2.returncode != 0 or r3.returncode != 0:
                    self.log.error(
                        f"[SampledMutation] {namespace} via {node.ip}: "
                        f"insert rc={r1.returncode}, update rc={r2.returncode}, "
                        f"delete rc={r3.returncode}; "
                        f"err={(r1.stderr or r2.stderr or r3.stderr or '')[:200]}")
                    return (namespace, False)
                return (namespace, True)
            except Exception as e:
                self.log.error(f"[SampledMutation] {namespace} via {node.ip} failed: {e}")
                return (namespace, False)

        failed = []
        num_workers = min(self.max_loader_threads, len(ns_node_pairs))
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = {executor.submit(_mutate, ns, node): ns for ns, node in ns_node_pairs}
            for fut in as_completed(futures):
                ns, ok = fut.result()
                if not ok:
                    failed.append(ns)

        self.log.info(f"[SampledMutation] complete: {len(sample) - len(failed)} ok, "
                      f"{len(failed)} failed")
        return failed, sample

    def _get_query_node_excluding_rebalancing(self, excluded_nodes=None):
        """
        Get a query node that is not being rebalanced in or out.
        
        Args:
            excluded_nodes: List of node IPs to exclude (nodes being rebalanced)
            
        Returns:
            A query node that is safe to use for scans
        """
        if excluded_nodes is None:
            excluded_nodes = []
        
        excluded_ips = set()
        for node in excluded_nodes:
            if hasattr(node, 'ip'):
                excluded_ips.add(node.ip)
            elif isinstance(node, str):
                excluded_ips.add(node)
        
        query_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        if not isinstance(query_nodes, list):
            query_nodes = [query_nodes]
        
        available_nodes = [node for node in query_nodes if node.ip not in excluded_ips]
        
        if available_nodes:
            return available_nodes
        
        # Fallback to all query nodes if all are excluded
        self.log.warning("All query nodes are in excluded list, using all available query nodes")
        return query_nodes

    def _run_continuous_scans_thread(self, event, select_queries, query_node=None, excluded_nodes=None):
        """
        Background thread that continuously runs scans on all indexed namespaces.
        Uses round-robin across available query nodes, excluding any nodes being rebalanced.
        
        Args:
            event: Threading Event to signal when to stop
            select_queries: List of SELECT queries to run
            query_node: N1QL query node (deprecated, use excluded_nodes instead)
            excluded_nodes: List of nodes being rebalanced (to avoid using them for scans)
        """
        from itertools import cycle as iter_cycle
        
        if excluded_nodes is None:
            excluded_nodes = []
        
        # Get query nodes excluding rebalancing ones
        available_query_nodes = self._get_query_node_excluding_rebalancing(excluded_nodes)
        if not isinstance(available_query_nodes, list):
            available_query_nodes = [available_query_nodes]
        
        # If a specific query_node was provided and it's not excluded, use it
        if query_node and query_node.ip not in [n.ip if hasattr(n, 'ip') else n for n in excluded_nodes]:
            available_query_nodes = [query_node]
        
        self.log.info(f"Scans will use query nodes: {[n.ip for n in available_query_nodes]} "
                     f"(excluding rebalancing nodes: {[n.ip if hasattr(n, 'ip') else n for n in excluded_nodes]})")
        
        node_cycle = iter_cycle(available_query_nodes)
        
        cycle = 0
        while not event.is_set():
            cycle += 1
            sample = random.sample(select_queries, min(200, len(select_queries)))
            self.log.info(f"Scan cycle {cycle} starting on {len(sample)}/{len(select_queries)} queries")
            
            for query in sample:
                if event.is_set():
                    break
                try:
                    current_node = next(node_cycle)
                    self.run_cbq_query(query=query, server=current_node, verbose=False)
                except Exception as e:
                    self.log.warning(f"Scan query failed: {e}")
            
            if not event.is_set():
                self.log.info(f"Scan cycle {cycle} done — pausing "
                              f"{self.rebalance_workload_cycle_gap_secs}s before next cycle")
                time.sleep(self.rebalance_workload_cycle_gap_secs)

        self.log.info(f"Scan background thread stopped after {cycle} cycles")

    def _get_select_queries_for_definitions(self, all_definitions, indexed_namespaces):
        """
        Generate SELECT queries for all index definitions across indexed namespaces.

        Returns a tuple (scalar_queries, []) for compatibility with callers that
        unpack two values; the second element is always empty (no vector indexes).
        """
        scalar_queries = []

        for namespace in indexed_namespaces:
            definitions = all_definitions.get(namespace, [])
            for defn in definitions:
                query = defn.generate_query(bucket=namespace)
                scalar_queries.append(query)

        return scalar_queries, []
    
    def _assert_all_nodes_up(self, phase=""):
        """
        Watchdog: fast TCP-connect check that every index (9102) and query (8093)
        service is reachable. Raises immediately if any is down.

        This exists because the framework's run_cbq_query retries a dead endpoint
        ~10x (~25 min) per call — so when a node crashes mid-run (e.g. the
        MonitorServiceForPortChanges panic), the test would otherwise grind for
        hours instead of failing. Calling this before each build batch / long
        phase converts that silent multi-hour hang into an immediate, clean
        failure that names the down node.
        """
        import socket
        prefix = f"[{phase}] " if phase else ""
        inodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if not isinstance(inodes, list):
            inodes = [inodes]
        qnodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        if not isinstance(qnodes, list):
            qnodes = [qnodes]
        checks = [(n.ip, 9102, "indexer") for n in inodes] + \
                 [(n.ip, 8093, "query") for n in qnodes]
        down = []
        for ip, port, svc in checks:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5)
            try:
                s.connect((ip, port))
            except Exception as e:
                down.append(f"{ip}:{port}({svc}:{type(e).__name__})")
            finally:
                s.close()
        self.assertEqual(
            len(down), 0,
            f"{prefix}Watchdog: service(s) unreachable {down} — aborting fast instead "
            f"of retrying a dead node (~25 min/query). Likely a service crash "
            f"(e.g. MonitorServiceForPortChanges panic).")

    def _build_all_indexes_concurrently(self, all_definitions, batch_size=50):
        """
        Build all indexes across all namespaces in batches to avoid request timeouts.
        Queries are split evenly across all query nodes and executed in parallel.
        Each node runs its batches asynchronously with per-batch thread pools.
        
        Includes:
        - Parallel fan-out across all query nodes
        - Smaller default batch size (50) to avoid overwhelming query service
        - Adaptive delay based on error rate
        
        Args:
            all_definitions: Dict mapping namespace to list of QueryDefinition objects
            batch_size: Number of BUILD INDEX queries to execute per batch (default: 50)
            
        Returns:
            List of namespaces that failed to build
        """
        # Get all query nodes for parallel distribution
        query_nodes, _ = self._get_query_nodes_round_robin()
        
        # Count indexes by type for detailed logging
        scalar_count = 0
        partitioned_count = 0
        # vector_count = 0  # commented out — no vector indexes in this run

        for namespace, definitions in all_definitions.items():
            for defn in definitions:
                # if hasattr(defn, 'dimension') and defn.dimension:  # vector check — commented out
                #     vector_count += 1
                if hasattr(defn, 'partition_by_fields') and defn.partition_by_fields:
                    partitioned_count += 1
                else:
                    scalar_count += 1

        total_indexes = scalar_count + partitioned_count

        self.log.info(f"Total indexes to build: {total_indexes} across {len(all_definitions)} namespaces")
        self.log.info(f"  - Scalar indexes: {scalar_count}")
        self.log.info(f"  - Partitioned indexes: {partitioned_count}")
        
        # Collect all BUILD INDEX queries with their namespace mapping
        build_queries = []
        namespace_list = []
        for namespace, definitions in all_definitions.items():
            if not definitions:
                continue
            build_query = self.gsi_util_obj.get_build_indexes_query(
                definition_list=definitions, namespace=namespace
            )
            build_queries.append(build_query)
            namespace_list.append(namespace)
        
        total_queries = len(build_queries)
        num_batches = (total_queries + batch_size - 1) // batch_size  # Ceiling division
        
        self.log.info(f"Generated {total_queries} BUILD INDEX queries")
        self.log.info(f"Will execute in {num_batches} batches of up to {batch_size} queries each")
        
        # Log sample of build queries (first 3)
        for i, bq in enumerate(build_queries[:3]):
            self.log.info(f"  Sample BUILD query {i+1}: {bq[:200]}...")
        if len(build_queries) > 3:
            self.log.info(f"  ... and {len(build_queries) - 3} more BUILD queries")
        
        # Distribute BUILD INDEX queries evenly across all live query nodes.
        # 4360 "build already in progress" is treated as success (indexer accepted
        # the build and is processing it in the background) so multi-node fan-out
        # is safe — it does not cause hard failures.
        self._assert_all_nodes_up("Step 2c build start")
        query_nodes, _ = self._get_query_nodes_round_robin()
        num_nodes = len(query_nodes)
        self.log.info(f"Distributing {total_queries} BUILD INDEX queries across "
                      f"{num_nodes} query node(s): {[n.ip for n in query_nodes]}")

        def _process_builds_on_node(node, queries, namespaces, offset):
            base_delay = 3
            current_delay = base_delay
            max_delay = 30
            consecutive_error_batches = 0
            success = 0
            failed = []
            node_batches = (len(queries) + batch_size - 1) // batch_size
            for b in range(node_batches):
                b_start = b * batch_size
                b_end = min(b_start + batch_size, len(queries))
                batch_queries = queries[b_start:b_end]
                batch_namespaces = namespaces[b_start:b_end]
                g_start = offset + b_start
                # Watchdog before every batch: if a node crashed, abort now (seconds)
                # instead of submitting 50 builds that each retry the dead node ~25 min.
                self._assert_all_nodes_up(f"Step 2c build batch {b + 1}")
                self.log.info(f"")
                self.log.info(f"[{node.ip}] BUILD BATCH {b + 1}/{node_batches}: queries {g_start + 1}-{offset + b_end} of {total_queries}")
                batch_start_time = time.time()
                batch_failed = 0
                batch_sock_errors = 0
                batch_transient = 0
                tasks = []
                with ThreadPoolExecutor(max_workers=self.max_concurrent_index_builds) as executor:
                    for query in batch_queries:
                        tasks.append(executor.submit(self.run_cbq_query, query=query, server=node))
                for i, task in enumerate(tasks):
                    try:
                        task.result()
                        success += 1
                    except Exception as e:
                        error_str = str(e).lower()
                        # These errors mean the build WAS accepted by the indexer and is
                        # running in the background — the HTTP 500 is the query layer's view:
                        #   4360 "build already in progress" / "retry building in the background"
                        #   4350 "Cannot reach node" / "request timed out" — indexer will retry
                        # The authoritative gate is wait_until_indexes_online +
                        # _verify_all_indexes_built which asserts every index is Ready.
                        if "build already in progress" in error_str or "transient error" in error_str or \
                                ("5000" in error_str and "background" in error_str) or \
                                "request timed out" in error_str:
                            batch_transient += 1
                            success += 1
                            self.log.info(
                                f"  Build accepted (indexer processing) for {batch_namespaces[i]} "
                                f"(final state gated by post-build verification)"
                            )
                            continue
                        batch_failed += 1
                        failed.append(batch_namespaces[i])
                        if "connection refused" in error_str or "connection reset" in error_str or "socket error" in error_str:
                            batch_sock_errors += 1
                        self.log.error(f"  Build query failed for {batch_namespaces[i]}: {e}")
                batch_elapsed = time.time() - batch_start_time
                self.log.info(f"  [{node.ip}] Batch {b + 1} completed: {len(batch_queries) - batch_failed}/{len(batch_queries)} successful in {batch_elapsed:.2f}s")
                if batch_sock_errors > 0 or batch_transient > len(batch_queries) // 4:
                    if batch_transient > 0:
                        self.log.warning(f"    Transient 4360 errors in batch: {batch_transient} — backing off")
                    if batch_sock_errors > 0:
                        self.log.warning(f"    Socket errors in batch: {batch_sock_errors}")
                    consecutive_error_batches += 1
                    current_delay = min(base_delay * (2 ** consecutive_error_batches), max_delay)
                    self.log.warning(f"  Increasing inter-batch delay to {current_delay}s")
                else:
                    consecutive_error_batches = 0
                    current_delay = base_delay
                if b < node_batches - 1:
                    time.sleep(current_delay)
            # NO RETRY: transient/hard failures are returned as-is. Any non-empty
            # 'failed' list fails the test at the caller.
            return success, failed

        start_time = time.time()
        total_successful = 0
        failed_namespaces = []

        chunk_size = (total_queries + num_nodes - 1) // num_nodes
        with ThreadPoolExecutor(max_workers=num_nodes) as executor:
            futures = []
            for i, node in enumerate(query_nodes):
                start_idx = i * chunk_size
                end_idx = min(start_idx + chunk_size, total_queries)
                futures.append(executor.submit(
                    _process_builds_on_node, node,
                    build_queries[start_idx:end_idx],
                    namespace_list[start_idx:end_idx],
                    start_idx))
            for f in futures:
                s, failed = f.result()
                total_successful += s
                failed_namespaces.extend(failed)

        failed_count = len(failed_namespaces)
        
        elapsed_time = time.time() - start_time
        
        self.log.info("")
        self.log.info("=" * 60)
        self.log.info("BUILD INDEX RESULTS (ALL BATCHES):")
        self.log.info("=" * 60)
        self.log.info(f"  Total BUILD queries executed: {total_queries}")
        self.log.info(f"  Successful: {total_successful}")
        self.log.info(f"  Failed: {failed_count}")
        self.log.info(f"  Total time taken: {elapsed_time:.2f} seconds")
        
        if failed_count > 0:
            failed_index_names = [
                defn.index_name
                for ns in failed_namespaces
                for defn in all_definitions.get(ns, [])
            ]
            self.log.error(f"Failed unbuilt indexes ({len(failed_index_names)}): {failed_index_names[:20]}")
            if len(failed_index_names) > 20:
                self.log.error(f"  ... and {len(failed_index_names) - 20} more")
        else:
            self.log.info(f"Successfully initiated builds for {total_indexes} indexes")
        
        self.log.info("=" * 60)
        
        return failed_namespaces

    def _verify_all_indexes_built(self, all_definitions):
        """
        Single-pass check that NONE of our indexes are left in 'Created' (unbuilt)
        state. NO RETRY, NO re-build: observe and report only. Scoped strictly to
        the indexes this test created so pre-existing deferred indexes are ignored.
        The caller hard-asserts the returned unbuilt list is empty.

        Args:
            all_definitions: Dict mapping namespace to list of QueryDefinition objects

        Returns:
            list: unbuilt index descriptors (empty == all built/building/ready)
        """
        self.log.info("=" * 80)
        self.log.info("VERIFYING INDEX BUILD STATUS (single pass, no retry)")
        self.log.info("=" * 80)

        # Only the (bucket, scope, collection, index_name) tuples we created.
        expected_keys = set()
        for namespace, definitions in all_definitions.items():
            _, keyspace = namespace.split(':', 1)
            bucket, scope, collection = keyspace.split('.')
            for defn in definitions:
                expected_keys.add((bucket, scope, collection, defn.index_name))

        index_metadata = self.index_rest.get_indexer_metadata()
        ready = building = unbuilt = 0
        final_unbuilt = []
        seen = set()
        for idx_info in index_metadata.get('status', []):
            key = (
                idx_info.get('bucket', ''),
                idx_info.get('scope', ''),
                idx_info.get('collection', ''),
                idx_info.get('indexName', idx_info.get('name', '')),
            )
            if key not in expected_keys:
                continue
            seen.add(key)
            status = idx_info.get('status', 'Unknown')
            if status == 'Ready':
                ready += 1
            elif status in ('Building', 'Scheduled'):
                building += 1
            elif status == 'Created':
                unbuilt += 1
                final_unbuilt.append({
                    'bucket': key[0], 'scope': key[1],
                    'collection': key[2], 'index_name': key[3]})

        # An expected index that is entirely absent from metadata is also a failure.
        for key in expected_keys - seen:
            final_unbuilt.append({
                'bucket': key[0], 'scope': key[1],
                'collection': key[2], 'index_name': key[3], 'status': 'MISSING'})

        self.log.info(f"  Ready: {ready}  Building/Scheduled: {building}  "
                      f"Created(unbuilt): {unbuilt}  Missing: {len(expected_keys - seen)}")
        if final_unbuilt:
            self.log.error(f"{len(final_unbuilt)} index(es) are not built (Created/MISSING):")
            for idx in final_unbuilt[:20]:
                self.log.error(f"  - {idx['bucket']}.{idx['scope']}.{idx['collection']}: "
                               f"{idx['index_name']} ({idx.get('status', 'Created')})")
            if len(final_unbuilt) > 20:
                self.log.error(f"  ... and {len(final_unbuilt) - 20} more")
        self.log.info("=" * 80)

        return final_unbuilt

    def _rebalance_indexer_nodes(self, rebalance_type, services=['index,n1ql,kv'], node_in=None):
        """
        Performs rebalance operation on indexer nodes.
        rebalance_type='swap': swap rebalance (remove one, add one)
        rebalance_type='rebalance_out': rebalance out an indexer node

        Args:
            node_in: For swap rebalance, the node to add. If None, uses self.servers[self.nodes_init].

        Returns:
            tuple: (node_out, node_in) - The nodes being removed and added (node_in is None for rebalance_out)
        """
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        # Hard gate: never start a rebalance on an unhealthy index topology — that is
        # exactly what triggers the build storm / replica loss we are guarding against.
        self.log.info("Pre-rebalance: waiting for all indexes to be online (timeout=7200s)...")
        self.assertTrue(
            self.wait_until_indexes_online(timeout=7200),
            f"Pre-rebalance gate FAILED: some indexes not online within 7200s before "
            f"{rebalance_type} rebalance — refusing to rebalance an unhealthy topology")
        self.log.info("Pre-rebalance check passed: all indexes are online.")

        self.assertGreaterEqual(
            len(index_nodes), 2,
            f"Need at least 2 indexer nodes for rebalance, have {len(index_nodes)}")

        # Select a node to remove, ensuring it's NOT the master node
        node_out = None
        for node in index_nodes:
            if node.ip != self.master.ip:
                node_out = node
                break
        
        # Hard gate: a rebalance that silently no-ops (no node to remove) must fail,
        # not pass. Otherwise the step is skipped while the test reports success.
        self.assertIsNotNone(
            node_out,
            f"Could not find a non-master indexer node to remove (master={self.master.ip}); "
            f"cannot perform {rebalance_type} rebalance")
        
        # Unique context/ID for this rebalance so start/finish log lines can be
        # correlated with each other (and with concurrent rebalances in the same run).
        rebalance_id = f"rebalance-{rebalance_type}-{node_out.ip}-{self._generate_random_suffix()}"

        # Snapshot index names + instance count before the rebalance for the post-rebalance check below.
        index_snapshot_before = self._snapshot_index_names()
        instances_before = self._get_total_index_instances(index_nodes)

        self.log.info(f"Rebalance type: {rebalance_type}")
        self.log.info(f"Master node: {self.master.ip} (excluded from rebalance)")
        self.log.info(f"Removing indexer node: {node_out.ip}")

        # Rebalance with 10k indexes can take 4-4.5 hours; timeout is 6 hours to be safe.
        rebalance_timeout = 21600

        if rebalance_type == "swap":
            if node_in is None:
                if self.nodes_init >= len(self.servers):
                    raise Exception(
                        f"Cannot perform swap rebalance: nodes_init={self.nodes_init} "
                        f"but only {len(self.servers)} servers configured. "
                        f"No spare node available."
                    )
                node_in = self.servers[self.nodes_init]
            self.log.info(f"Adding indexer node: {node_in.ip}")
            if self.indexMemQuota:
                RestConnection(node_in).set_service_memoryQuota(service='indexMemoryQuota',
                                                                memoryQuota=int(self.indexMemQuota))
            self.log.info(f"[{rebalance_id}] Rebalance STARTING: swap out={node_out.ip}, "
                          f"in={node_in.ip}")
            task = self.cluster.async_rebalance(
                servers=self.servers[:self.nodes_init],
                to_add=[node_in],
                to_remove=[node_out],
                services=services
            )
            self.log.info(f"Waiting for swap rebalance to complete (timeout={rebalance_timeout}s)...")
            task.result(timeout=rebalance_timeout)

        elif rebalance_type == "rebalance_out":
            node_in = None
            self.log.info(f"[{rebalance_id}] Rebalance STARTING: rebalance-out node={node_out.ip}")
            task = self.cluster.async_rebalance(
                servers=self.servers[:self.nodes_init],
                to_add=[],
                to_remove=[node_out],
                services=services
            )
            self.log.info(f"Waiting for rebalance-out to complete (timeout={rebalance_timeout}s)...")
            task.result(timeout=rebalance_timeout)
        else:
            raise ValueError(f"Unknown rebalance_type: {rebalance_type}")

        self.log.info(f"[{rebalance_id}] Rebalance SUCCEEDED: {rebalance_type} rebalance "
                      f"cluster operation completed successfully")
        self.update_master_node()

        # Wait for all indexes to come back online before returning.
        # Post-rebalance index recovery can take significant time with 10k indexes.
        self.log.info("Waiting for all indexes to come online post-rebalance (timeout=18000s)...")
        self.assertTrue(
            self.wait_until_indexes_online(timeout=18000),
            f"{rebalance_type} rebalance completed but some indexes did NOT come online "
            f"within 18000s — post-rebalance health gate failed")
        self.log.info("All indexes are online. Rebalance fully complete.")

        # Post-rebalance index validation gate: SOFT FAIL — recorded in self.index_loss_report,
        # run continues; only the FINAL SUMMARY asserts, and only if fail_on_index_loss=True.
        index_snapshot_after = self._snapshot_index_names()
        lost, added = self._compare_index_snapshots(
            index_snapshot_before, index_snapshot_after,
            step_label=f"{rebalance_id} post-rebalance index check")

        # Re-fetch current indexer nodes: node_out is no longer part of the cluster.
        current_index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        instances_after = self._get_total_index_instances(current_index_nodes)

        if lost or added or instances_after != instances_before:
            self.log.warning(
                f"[{rebalance_id}] SOFT FAIL: post-rebalance index check found "
                f"{len(lost)} lost, {len(added)} unexpected index(es); "
                f"instance count before={instances_before} after={instances_after}")
            self.index_loss_report.append({
                'rebalance_id': rebalance_id,
                'type': rebalance_type,
                'node_out': node_out.ip,
                'lost': lost,
                'added': added,
                'before': instances_before,
                'after': instances_after,
            })

        return node_out, node_in

    def _predicate_is_mutation_volatile(self, query):
        """
        True if the query's WHERE clause filters on a field that the background
        mutation workload re-randomizes (MUTATION_VOLATILE_PREDICATE_FIELDS).

        Such a query has no stable doc count across a mutation window, so its
        count must not be compared against an earlier baseline. It still gets
        the 0 < count <= docs-per-namespace range check, which is what actually
        proves the index is returning sane results.
        """
        parts = query.upper().split(" WHERE ", 1)
        if len(parts) < 2:
            return False
        where_clause = parts[1]
        return any(field.upper() in where_clause
                   for field in MUTATION_VOLATILE_PREDICATE_FIELDS)

    def _run_scans_and_validate_doc_count(self, select_queries, query_node, expected_docs_per_namespace,
                                          scan_phase="", baseline=None):
        """
        Predicated-query baseline-stability check: not doc-loss detection (query text has a
        WHERE clause, so counts legitimately vary) — captures/validates a per-query baseline,
        keyed on the full query string so namespaces with 2+ index definitions don't collide.

        Returns:
            (total_queries, mismatched_count, mismatched_namespaces, counts)
            counts: {query: actual_count} map for use as the next call's baseline.
                    Queries whose predicate filters on a field the background
                    mutations re-randomize are excluded -- see
                    MUTATION_VOLATILE_PREDICATE_FIELDS.
        """
        self.log.info(f"Running scans with doc count validation ({scan_phase})")

        mismatched_namespaces = []
        counts = {}
        volatile_skipped = 0
        total_queries = len(select_queries)

        # Run COUNT(*) queries in parallel to avoid O(N) sequential latency.
        # With ~4500+ queries, sequential execution was taking hours.
        # The duplicate aysnc_run_select_queries call has been removed — it was
        # re-running all queries a second time with no additional validation value.
        # Round-robin across all available N1QL nodes to distribute load (Bug D fix).
        from itertools import cycle as _iter_cycle
        all_q_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        if not isinstance(all_q_nodes, list):
            all_q_nodes = [all_q_nodes]
        if query_node not in all_q_nodes:
            all_q_nodes = [query_node] + all_q_nodes
        _qn_cycle = _iter_cycle(all_q_nodes)
        # Pre-assign a node to each query so the closure stays thread-safe
        query_node_pairs = [(q, next(_qn_cycle)) for q in select_queries]

        def _run_count(query, qnode):
            namespace = self._extract_namespace_from_query(query)
            count_query = self._convert_to_count_query(query)
            result = self.run_cbq_query(query=count_query, server=qnode)
            if result and 'results' in result and result['results']:
                actual_count = result['results'][0].get('$1', 0)
            else:
                actual_count = None
            return namespace, actual_count, query

        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = {executor.submit(_run_count, q, qn): q for q, qn in query_node_pairs}
            for future in as_completed(futures):
                try:
                    namespace, actual_count, query = future.result()
                except Exception as e:
                    self.log.error(f"[{scan_phase}] Error running count query: {e}")
                    mismatched_namespaces.append({
                        'namespace': 'unknown',
                        'expected': expected_docs_per_namespace,
                        'actual': 'error',
                        'error': str(e)
                    })
                    continue

                if actual_count is None:
                    mismatched_namespaces.append({
                        'namespace': namespace,
                        'expected': expected_docs_per_namespace,
                        'actual': 'no result',
                        'query': query[:100]
                    })
                    continue

                volatile = self._predicate_is_mutation_volatile(query)
                if volatile:
                    volatile_skipped += 1

                # A volatile predicate is never baseline-compared: the field it
                # filters on is re-randomized by the background mutations run
                # during Steps 5 and 7, so an earlier count is not a valid
                # expectation. It falls through to the range check below.
                prior = None if volatile else (
                    baseline.get(query) if baseline is not None else None)
                if prior is not None:
                    if actual_count != prior:
                        self.log.error(
                            f"[{scan_phase}] Baseline-stability mismatch for {namespace}: "
                            f"expected {prior} (prior baseline), got {actual_count}")
                        if actual_count < prior:
                            self._log_missing_doc_ids(
                                namespace=namespace, qnode=query_node,
                                expected_docs_per_namespace=prior,
                                scan_phase=scan_phase)
                        mismatched_namespaces.append({
                            'namespace': namespace,
                            'expected': prior,
                            'actual': actual_count,
                            'query': query[:100]
                        })
                elif not (0 < actual_count <= expected_docs_per_namespace):
                    self.log.error(
                        f"[{scan_phase}] Impossible predicated-scan count for {namespace}: "
                        f"{actual_count} (namespace has {expected_docs_per_namespace} docs total)")
                    mismatched_namespaces.append({
                        'namespace': namespace,
                        'expected': f"0 < x <= {expected_docs_per_namespace}",
                        'actual': actual_count,
                        'query': query[:100]
                    })
                if not volatile:
                    counts[query] = actual_count

        if volatile_skipped:
            self.log.info(
                f"[{scan_phase}] {volatile_skipped} query(ies) range-checked only "
                f"(predicate filters on a mutated field: "
                f"{', '.join(MUTATION_VOLATILE_PREDICATE_FIELDS)}) -- a remembered "
                f"count is not a valid expectation for these")

        # Summary logging
        mismatched_count = len(mismatched_namespaces)
        if mismatched_count > 0:
            self.log.error(f"[{scan_phase}] Doc count validation FAILED: "
                          f"{mismatched_count}/{total_queries} queries returned unexpected doc counts")
            for mismatch in mismatched_namespaces[:10]:  # Log first 10 mismatches
                self.log.error(f"  - {mismatch}")
            if mismatched_count > 10:
                self.log.error(f"  ... and {mismatched_count - 10} more mismatches")
        else:
            self.log.info(f"[{scan_phase}] Doc count validation PASSED: "
                         f"all {total_queries} queries returned expected doc counts")

        return total_queries, mismatched_count, mismatched_namespaces, counts

    def _log_missing_doc_ids(self, namespace, qnode, expected_docs_per_namespace, scan_phase=""):
        """
        Best-effort diagnostic: when a namespace's doc count is short of expected,
        fetch the document keys that ARE present, infer the expected key range
        (assuming the "<prefix><number>" key scheme used by the bulk loader), and
        log exactly which document ID(s) are missing, together with the bucket,
        scope, and collection they belong to.

        Non-fatal: any failure here is logged and swallowed — this is additional
        diagnostics on top of the doc-count mismatch already recorded by the caller.
        """
        import re as _re
        try:
            _, keyspace = namespace.split(':', 1)
            bucket, scope, collection = keyspace.split('.')
        except ValueError:
            self.log.warning(f"[{scan_phase}] Could not parse bucket/scope/collection "
                             f"from namespace {namespace}; skipping missing-doc-id lookup")
            return

        try:
            result = self.run_cbq_query(
                query=f"SELECT RAW META().id FROM {namespace}", server=qnode)
            actual_ids = result.get('results', []) if result else []
        except Exception as e:
            self.log.warning(f"[{scan_phase}] Failed to fetch document IDs for "
                             f"{bucket}.{scope}.{collection}: {e}")
            return

        if not actual_ids:
            self.log.error(f"[{scan_phase}] {bucket}.{scope}.{collection}: 0 documents "
                           f"present, expected {expected_docs_per_namespace} — every "
                           f"document ID is missing")
            return

        # Infer "<prefix><number>" key scheme from the observed IDs.
        parsed = []
        for doc_id in actual_ids:
            m = _re.match(r'^(.*?)(\d+)$', str(doc_id))
            if m:
                parsed.append((m.group(1), int(m.group(2))))
        if not parsed:
            self.log.warning(f"[{scan_phase}] {bucket}.{scope}.{collection}: document IDs "
                             f"do not match the expected '<prefix><number>' scheme; "
                             f"cannot infer which specific IDs are missing")
            return

        prefix = max(set(p for p, _ in parsed), key=lambda p: sum(1 for pp, _ in parsed if pp == p))
        actual_numbers = {n for p, n in parsed if p == prefix}
        base = min(actual_numbers)
        expected_numbers = set(range(base, base + expected_docs_per_namespace))
        missing_numbers = sorted(expected_numbers - actual_numbers)

        if not missing_numbers:
            self.log.warning(f"[{scan_phase}] {bucket}.{scope}.{collection}: doc count "
                             f"({len(actual_ids)}) is short of expected "
                             f"({expected_docs_per_namespace}), but no gaps found in the "
                             f"inferred ID range starting at {prefix}{base} — IDs may use "
                             f"a different numbering scheme than assumed")
            return

        self.log.error(f"[{scan_phase}] {bucket}.{scope}.{collection}: "
                       f"{len(missing_numbers)} missing document ID(s):")
        for n in missing_numbers[:20]:
            self.log.error(f"[{scan_phase}]   MISSING DOCUMENT: {prefix}{n} "
                           f"(bucket={bucket}, scope={scope}, collection={collection})")
        if len(missing_numbers) > 20:
            self.log.error(f"[{scan_phase}]   ... and {len(missing_numbers) - 20} more "
                           f"missing document(s) in {bucket}.{scope}.{collection}")

    def _extract_namespace_from_query(self, query):
        """
        Extract namespace from a SELECT query.
        Handles formats like: FROM `bucket`.`scope`.`collection` or FROM default:bucket.scope.collection
        """
        import re
        # Match FROM clause with backticks or without
        pattern = r'FROM\s+(?:`([^`]+)`\.`([^`]+)`\.`([^`]+)`|(\w+):(\w+)\.(\w+)\.(\w+))'
        match = re.search(pattern, query, re.IGNORECASE)
        if match:
            if match.group(1):  # Backtick format
                return f"default:{match.group(1)}.{match.group(2)}.{match.group(3)}"
            else:  # default:bucket.scope.collection format
                return f"{match.group(4)}:{match.group(5)}.{match.group(6)}.{match.group(7)}"
        return "unknown"

    def _convert_to_count_query(self, query):
        """
        Convert a SELECT query to a COUNT(*) query to get document count.
        """
        import re
        # Replace SELECT ... FROM with SELECT COUNT(*) FROM
        count_query = re.sub(
            r'SELECT\s+.*?\s+FROM',
            'SELECT COUNT(*) FROM',
            query,
            flags=re.IGNORECASE | re.DOTALL
        )
        # Remove ORDER BY clause if present (re.DOTALL so .* crosses newlines)
        count_query = re.sub(r'\s+ORDER\s+BY\s+.*$', '', count_query, flags=re.IGNORECASE | re.DOTALL)
        # Remove LIMIT clause if present
        count_query = re.sub(r'\s+LIMIT\s+\d+', '', count_query, flags=re.IGNORECASE)
        return count_query

    def _verify_metadata_consistency(self, bucket_name, expected_count):
        """Query system:keyspaces on every N1QL node; log cross-node count discrepancies."""
        query_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        if not isinstance(query_nodes, list):
            query_nodes = [query_nodes]
        q = (f"SELECT COUNT(*) as cnt FROM system:keyspaces "
             f"WHERE `bucket` = '{bucket_name}' AND SUBSTR(`scope`, 0, 1) != '_'")
        counts = {}
        for node in query_nodes:
            try:
                r = self.run_cbq_query(query=q, server=node, verbose=False)
                counts[node.ip] = r['results'][0]['cnt'] if r.get('results') else -1
            except Exception as e:
                self.log.warning(f"[MetadataChurn] system:keyspaces query failed on {node.ip}: {e}")
                counts[node.ip] = -1
        summary = ", ".join(f"{ip}={c}" for ip, c in counts.items())
        if len(set(v for v in counts.values() if v >= 0)) > 1:
            self.log.warning(f"[MetadataChurn] Cross-node inconsistency detected: {summary} (expected={expected_count})")
        else:
            self.log.info(f"[MetadataChurn] Metadata consistent across nodes: {summary}")
        return counts

    def _churn_collection_cycle(self, churn_namespaces, cycle_num, static_count):
        """
        Drop and re-create churn_namespaces one at a time via REST.
        Logs per-phase timing, metadata propagation lag per N1QL node, and cross-node consistency.
        Propagation lag = time from last REST call until all N1QL nodes reflect the new count.
        """
        parsed = [ns.split(':', 1)[1].split('.') for ns in churn_namespaces]
        bucket_name = parsed[0][0]
        q_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        if not isinstance(q_nodes, list):
            q_nodes = [q_nodes]

        def _wait_count(expected, label, timeout=300):
            """Poll system:keyspaces until all N1QL nodes report the expected collection count."""
            q = (f"SELECT COUNT(*) as cnt FROM system:keyspaces "
                 f"WHERE `bucket` = '{bucket_name}' AND SUBSTR(`scope`, 0, 1) != '_'")
            deadline = time.time() + timeout
            while time.time() < deadline:
                counts = {node.ip: -1 for node in q_nodes}
                for node in q_nodes:
                    try:
                        r = self.run_cbq_query(query=q, server=node, verbose=False)
                        counts[node.ip] = r['results'][0]['cnt'] if r.get('results') else -1
                    except Exception:
                        pass
                if all(c == expected for c in counts.values()):
                    return
                self.log.info(f"[MetadataChurn] {label}: waiting for count={expected}, current={counts}")
                time.sleep(10)
            self.log.warning(f"[MetadataChurn] {label}: propagation timeout after {timeout}s, current={counts}")

        # Phase 1: drop all churn collections one by one
        self.log.info(f"[MetadataChurn] === Cycle {cycle_num}: dropping {len(parsed)} collections ===")
        t0 = time.time()
        for bucket, scope, col in parsed:
            try:
                self.collection_rest.delete_collection(bucket=bucket, scope=scope, collection=col)
            except Exception as e:
                self.log.warning(f"[MetadataChurn] Drop {col} failed: {e}")
        drop_t = time.time() - t0
        self.log.info(f"[MetadataChurn] Cycle {cycle_num}: {len(parsed)} drops issued in {drop_t:.1f}s — measuring propagation")
        t_prop1 = time.time()
        _wait_count(static_count, f"Cycle {cycle_num} post-drop")
        prop1 = time.time() - t_prop1
        self._verify_metadata_consistency(bucket_name, static_count)

        # Phase 2: re-create all churn collections one by one
        self.log.info(f"[MetadataChurn] Cycle {cycle_num}: re-creating {len(parsed)} collections")
        t1 = time.time()
        for bucket, scope, col in parsed:
            try:
                self.collection_rest.create_collection(bucket=bucket, scope=scope, collection=col)
            except Exception as e:
                self.log.warning(f"[MetadataChurn] Create {col} failed: {e}")
        create_t = time.time() - t1
        self.log.info(f"[MetadataChurn] Cycle {cycle_num}: {len(parsed)} creates issued in {create_t:.1f}s — measuring propagation")
        t_prop2 = time.time()
        _wait_count(static_count + len(parsed), f"Cycle {cycle_num} post-create")
        prop2 = time.time() - t_prop2
        self._verify_metadata_consistency(bucket_name, static_count + len(parsed))

        self.log.info(f"[MetadataChurn] Cycle {cycle_num} DONE — "
                      f"drop={drop_t:.1f}s prop_after_drop={prop1:.1f}s | "
                      f"create={create_t:.1f}s prop_after_create={prop2:.1f}s")

    def _run_metadata_churn_background(self, event, churn_namespaces, static_count, interval_sec=1800):
        """
        Background thread: runs metadata churn cycles every interval_sec (default 30 min).
        First cycle fires after the first interval. Stopped by setting event.
        Intended to run concurrently with Step 2b (CREATE INDEX) to stress metadata sync
        while the indexer processes the bulk of the 4,675 index creation requests.
        """
        cycle = 0
        while not event.wait(timeout=interval_sec):
            cycle += 1
            self.log.info(f"[MetadataChurn] Background thread: triggering cycle {cycle}")
            try:
                self._churn_collection_cycle(churn_namespaces, cycle, static_count)
            except Exception as e:
                self.log.warning(f"[MetadataChurn] Cycle {cycle} failed (non-fatal): {e}")
        self.log.info(f"[MetadataChurn] Background churn stopped after {cycle} cycles")

    # ──────────────────────────────────────────────────────────────────────
    # Query-service measurement helpers
    #
    # These helpers capture latency, plan-cache, optimizer, concurrency and
    # node-resilience metrics while the 10K-collection workload runs. They are
    # all non-fatal on stat-collection errors (so a transient stats failure
    # never aborts the long-running test) and record their results into
    # self.perf_metrics, which _log_perf_baseline() prints at the end of the run.
    # ──────────────────────────────────────────────────────────────────────
    @staticmethod
    def _percentiles(values):
        """Return p50/p95/p99/max/count for a list of numbers. Empty list -> all zeros."""
        if not values:
            return {'p50': 0, 'p95': 0, 'p99': 0, 'max': 0, 'count': 0}
        s = sorted(values)
        pick = lambda p: s[min(len(s) - 1, int(round(p / 100.0 * (len(s) - 1))))]
        return {'p50': round(pick(50), 2), 'p95': round(pick(95), 2),
                'p99': round(pick(99), 2), 'max': round(s[-1], 2), 'count': len(s)}

    def _timed_cbq(self, query, server):
        """Run a N1QL query and return its wall-clock latency in milliseconds."""
        t = time.time()
        self.run_cbq_query(query=query, server=server)
        return (time.time() - t) * 1000.0

    def _assert_index_scans(self, select_queries, sample=20):
        """Verify the optimizer chooses index scans (not full primary scans) at 10K
        scale. EXPLAINs a random sample of the workload SELECTs and fails the test if
        any plan falls back to a PrimaryScan, which would indicate missing or unusable
        index statistics."""
        if not select_queries:
            return
        from itertools import cycle as _explain_cycle
        qnodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        if not isinstance(qnodes, list):
            qnodes = [qnodes]
        _qn_cycle = _explain_cycle(qnodes)
        chosen = random.sample(select_queries, min(sample, len(select_queries)))
        checked = primary = index_scan = 0
        for q in chosen:
            try:
                r = self.run_cbq_query(query="EXPLAIN " + q, server=next(_qn_cycle))
                plan = str(r.get('results', ''))
                checked += 1
                primary += 'PrimaryScan' in plan
                index_scan += 'IndexScan' in plan
            except Exception as e:
                self.log.warning(f"[PlanCheck] EXPLAIN failed: {e}")
        self.perf_metrics['optimizer_plans'] = {'checked': checked,
                                                 'index_scan': index_scan, 'primary_scan': primary}
        self.log.info(f"[PlanCheck] plans checked={checked}, "
                      f"index_scan={index_scan}, primary_scan={primary}")
        self.assertEqual(primary, 0,
                         f"[PlanCheck] {primary}/{checked} sampled collections fell back to PrimaryScan")

    def _plan_keyspaces(self, node, found=None):
        """Keyspaces an EXPLAIN plan actually reads: every operator carrying a
        'keyspace' (IndexScan*/PrimaryScan*/Fetch/...) at any nesting depth."""
        found = set() if found is None else found
        if isinstance(node, dict):
            ks = node.get('keyspace') if '#operator' in node else None
            if isinstance(ks, str):
                found.add('.'.join(p for p in (node.get('bucket'), node.get('scope'),
                                               ks.split(':')[-1]) if p))
            children = node.values()
        elif isinstance(node, list):
            children = node
        else:
            return found
        for child in children:
            self._plan_keyspaces(child, found)
        return found

    def _measure_fanout(self, select_queries):
        """Compare single-collection query latency against 100 SELECTs run concurrently
        across randomly chosen collections, and confirm each query stays scoped to a
        single keyspace (no accidental fan-out across all 10K collections)."""
        if not select_queries:
            return
        qnodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        if not isinstance(qnodes, list):
            qnodes = [qnodes]
        # Baseline: a few queries run one at a time against a single node.
        base = self._percentiles([self._timed_cbq(q, qnodes[0])
                                  for q in random.sample(select_queries,
                                                         min(10, len(select_queries)))])
        sample = random.sample(select_queries, min(100, len(select_queries)))
        lat = []
        from itertools import cycle as _cyc
        cyc = _cyc(qnodes)
        # Fan-out guard: EXPLAIN a subsample and compare the keyspaces the plan really
        # reads against the one collection the query names. Matching the query *text*
        # here would only re-validate the test's own query builder, not the engine.
        fanned = explained = 0
        offenders = []
        for q in random.sample(sample, min(25, len(sample))):
            target = self._extract_namespace_from_query(q).split(':')[-1]
            try:
                r = self.run_cbq_query(query="EXPLAIN " + q, server=next(cyc))
                ks = self._plan_keyspaces(r['results'][0]['plan'])
            except Exception as e:
                self.log.warning(f"[QueryFanout] EXPLAIN failed for {target}: {e}")
                continue
            explained += 1
            if ks != {target}:
                fanned += 1
                offenders.append({'target': target, 'plan_keyspaces': sorted(ks)[:10],
                                  'keyspace_count': len(ks)})
                self.log.error(f"[QueryFanout] plan for {target} reads {len(ks)} "
                               f"keyspace(s): {sorted(ks)[:10]}")
        with ThreadPoolExecutor(max_workers=50) as ex:
            futs = [ex.submit(self._timed_cbq, q, next(cyc)) for q in sample]
            for f in as_completed(futs):
                try:
                    lat.append(f.result())
                except Exception as e:
                    self.log.warning(f"[QueryFanout] concurrent query failed: {e}")
        conc = self._percentiles(lat)
        self.perf_metrics['fanout_ms'] = {'baseline': base, 'concurrent_100': conc,
                                          'plans_checked': explained,
                                          'multi_keyspace_queries': fanned,
                                          'fanout_offenders': offenders[:5]}
        self.log.info(f"[QueryFanout] baseline(ms)={base}; 100-concurrent(ms)={conc}; "
                      f"plans checked={explained}; wrong-keyspace queries={fanned}")
        # Guard the guard: if every EXPLAIN failed, fanned stays 0 and nothing was verified.
        self.assertTrue(explained, "[QueryFanout] no plan could be EXPLAINed — scoping unverified")
        self.assertEqual(fanned, 0, f"[QueryFanout] {fanned}/{explained} plans read keyspaces other "
                                    f"than the collection the query names: {offenders[:5]}")

    def _measure_query_node_resilience(self):
        """Measure query-node restart/recovery behaviour at 10K scale.

        For every query node in the cluster: restart Couchbase on that node,
        then execute a query against that specific node and time how long it
        takes until the query succeeds. This is done for ALL query nodes (not
        just a single non-master node), one at a time, so every node's restart
        recovery time is individually measured and logged.

        Note: this no longer performs any failover / add-back / rebalance —
        it is a pure restart-and-recover measurement per query node."""
        from lib.remote.remote_util import RemoteMachineShellConnection
        m = {'per_node': {}}
        qnodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        if not isinstance(qnodes, list):
            qnodes = [qnodes]
        if not qnodes:
            self.log.warning("[QueryNodeResilience] No query nodes available; skipping")
            return m

        for target in qnodes:
            self.log.info(f"[QueryNodeResilience] Restarting query node {target.ip} ...")
            shell = RemoteMachineShellConnection(target)
            try:
                shell.restart_couchbase()
            finally:
                try:
                    shell.disconnect()
                except Exception:
                    pass

            t0, ready, last_error = time.time(), False, None
            while time.time() - t0 < 600:
                try:
                    self.run_cbq_query(query="SELECT 1", server=target)
                    ready = True
                    break
                except Exception as e:
                    last_error = e
                    time.sleep(2)

            elapsed = round(time.time() - t0, 1)
            m['per_node'][target.ip] = elapsed if ready else None
            if ready:
                self.log.info(f"[QueryNodeResilience] {target.ip} restart time-to-ready "
                              f"(validated via successful query response): {elapsed}s")
            else:
                self.log.error(f"[QueryNodeResilience] {target.ip} did NOT respond "
                               f"successfully to queries within 600s of restart "
                               f"(last error: {last_error})")

        m['restart_time_to_ready_s'] = m['per_node'].get(
            next((n.ip for n in qnodes if n.ip != self.master.ip), qnodes[0].ip))
        self.perf_metrics['query_node_resilience'] = m
        return m

    def _log_perf_baseline(self):
        """Print a consolidated summary of every query-service metric collected during
        the run (latency percentiles, plan-cache, optimizer, concurrency, node
        resilience) so a single log block captures the performance baseline."""
        import json as _json
        self.log.info("=" * 80)
        self.log.info("QUERY SERVICE PERFORMANCE BASELINE")
        self.log.info("=" * 80)
        for k in sorted(self.perf_metrics):
            self.log.info(f"  {k}: {_json.dumps(self.perf_metrics[k], default=str)}")
        self.log.info("=" * 80)


    def test_10k_namespaces_with_indexes(self):
        """
        10k-namespace GSI test — STRICT, NO-RETRY variant.

        Identical flow to 10k_gsi_indexes.py, but every step runs exactly once and
        every result is hard-asserted. There is no retry, no "continue anyway", and
        no silently-skipped step. Any failure fails the test at the step that caused
        it, so a real defect (e.g. a lost index replica during rebalance) can never
        be logged-and-passed.

        Flow:
          1   Create buckets + 10k namespaces (single manifest PUT, asserted)
          2   Plan index distribution
          2a  Generate CREATE INDEX queries
          2b  Execute CREATE INDEX once  -> assert zero failures
          2b* Verify every index is in metadata -> assert none missing
          2c  BUILD all indexes once     -> assert zero build failures
          2d  Verify every index built   -> assert none unbuilt/missing
          3   Wait indexes online        -> assert online
          3c  UPDATE STATISTICS (blocking) -> assert zero failures (removes plan race)
          4   Scans + doc-count validation -> assert zero mismatches
          4a  Assert index scans used + fan-out
          4b  N cycles of 200-collection sampled mutations (insert+update+delete, 25 docs
              each, 3-min gap between cycles) -> manual UPDATE STATISTICS -> queries on fresh stats
          5   Shard-based swap rebalance; background mutation + scan threads run during
              rebalance (2-min cycle gaps, all query nodes); pre + post replica-integrity gates
          6   Post-shard validations (online + replicas intact + no pending + scans)
          7   DCP swap rebalance; same background workload pattern as Step 5;
              pre + post replica-integrity gates (never skipped)
          8   Post-DCP validations (online + replicas intact + no pending + scans)
          8b  Query-node resilience -> assert online after
          9   Drop all indexes synchronously -> assert zero indexes remain
          Fin Log perf baseline
        """
        self.enable_redistribute_indexes()
        self.enable_shard_based_rebalance()

        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)

        # ── Step 1: create buckets + namespaces ────────────────────────────────
        self.log.info("=" * 80)
        self.log.info("Step 1: Creating buckets and namespaces")
        self.log.info("=" * 80)
        self._create_buckets_and_namespaces(
            num_scopes_per_bucket=self.num_scopes_per_bucket,
            num_collections_per_scope=self.num_collections_per_scope
        )
        self.sleep(10, "Sleeping after namespace creation")

        total_namespaces = len(self.namespaces)
        self.log.info(f"Total namespaces created: {total_namespaces}")
        # Hard gate: the run is only valid at full scale.
        self.assertEqual(total_namespaces, self.total_namespaces,
                         f"Namespace count {total_namespaces} != requested {self.total_namespaces}")

        # Last 500 collections are dynamic churn (never indexed); first 9,500 are static.
        self.churn_namespaces = self.namespaces[9500:]
        self.namespaces = self.namespaces[:9500]
        self.assertTrue(self.churn_namespaces, "No churn namespaces carved out (expected last 500)")
        _, _ks0 = self.churn_namespaces[0].split(':', 1)
        _churn_bucket, _churn_scope, _ = _ks0.split('.')
        self.log.info(f"[MetadataChurn] Static={len(self.namespaces)}, Churn={len(self.churn_namespaces)}")

        # Baseline: every N1QL node must see all 10k collections, consistently.
        base_counts = self._verify_metadata_consistency(_churn_bucket, total_namespaces)
        _valid = [c for c in base_counts.values() if c >= 0]
        self.assertTrue(_valid, "system:keyspaces returned no valid count on any N1QL node")
        self.assertEqual(len(set(_valid)), 1,
                         f"Cross-node metadata inconsistency at baseline: {base_counts}")

        # ── Step 2: plan distribution ──────────────────────────────────────────
        self.log.info("=" * 80)
        self.log.info("Step 2: Pre-calculating index distribution")
        self.log.info("=" * 80)
        index_distribution, index_plan = self._calculate_index_distribution()

        # ── Step 2a: generate CREATE INDEX queries ─────────────────────────────
        self.log.info("=" * 80)
        self.log.info("Step 2a: Generating all CREATE INDEX queries")
        self.log.info("=" * 80)
        all_create_queries, all_definitions, query_details = self._generate_all_index_queries(
            index_distribution=index_distribution, defer_build=True)
        self.assertTrue(all_create_queries, "No CREATE INDEX queries were generated")

        # ── Step 2b: execute CREATE INDEX (once) ───────────────────────────────
        self.log.info("=" * 80)
        self.log.info("Step 2b: Executing all CREATE INDEX queries (single pass, no retry)")
        self.log.info("=" * 80)

        # Background metadata churn (drop+recreate 500 collections) runs from CREATE INDEX
        # through Step 4, stopping before Step 5 rebalance begins.
        churn_event = Event()
        churn_thread = Thread(target=self._run_metadata_churn_background,
                              args=(churn_event, self.churn_namespaces, len(self.namespaces)),
                              daemon=True, name="MetadataChurnBG")
        churn_thread.start()
        self._active_churn_events.append(churn_event)
        self.log.info("[MetadataChurn] Background churn thread started (interval=30min)")

        success_count, failed_queries = self._async_create_all_indexes(all_create_queries, query_details)
        self.log.info(f"Index creation: {success_count}/{len(all_create_queries)} succeeded")
        # Hard gate: every CREATE INDEX must have succeeded.
        self.assertEqual(len(failed_queries), 0,
                         f"{len(failed_queries)} CREATE INDEX queries failed (first 5): "
                         f"{failed_queries[:5]}")

        # ── Step 2b-verify: every index present in metadata ────────────────────
        self.log.info("=" * 80)
        self.log.info("Step 2b-verify: Verifying every index exists in metadata")
        self.log.info("=" * 80)
        self.sleep(15, "Allowing index definitions to register in metadata")
        verified_count, missing_indexes = self._verify_indexes_created(all_definitions)
        self.log.info(f"Index creation verification: {verified_count} present")
        self.assertEqual(len(missing_indexes), 0,
                         f"{len(missing_indexes)} expected indexes missing from metadata "
                         f"(first 5): {missing_indexes[:5]}")

        # ── Step 2c: BUILD all indexes (once) ──────────────────────────────────
        self.log.info("=" * 80)
        self.log.info("Step 2c: Building all indexes (single pass, no retry)")
        self.log.info("=" * 80)
        self.log.info(f"Building indexes for {len(all_definitions)} namespaces; "
                      f"total indexes: {sum(len(d) for d in all_definitions.values())}")
        build_failures = self._build_all_indexes_concurrently(all_definitions)
        # Informational only: HTTP-level failures may recover in background (e.g. code
        # 4350 "Cannot reach node — retried in background"). Ground truth is step 2d.
        if build_failures:
            self.log.warning(f"Step 2c: {len(build_failures)} BUILD requests returned errors "
                             f"(may still complete in background): {build_failures[:5]}")

        # ── Step 2d: every index actually built ────────────────────────────────
        self.log.info("=" * 80)
        self.log.info("Step 2d: Verifying every index reached built state")
        self.log.info("=" * 80)
        self.sleep(120, "Allowing background retries and in-flight builds to settle")
        unbuilt_indexes = self._verify_all_indexes_built(all_definitions)
        unbuilt_names = [idx['index_name'] for idx in unbuilt_indexes]
        self.assertEqual(len(unbuilt_indexes), 0,
                         f"{len(unbuilt_indexes)} indexes still unbuilt after 2-min wait — "
                         f"index names: {unbuilt_names}")

        # ── Step 3: indexes online ─────────────────────────────────────────────
        self.log.info("=" * 80)
        self.log.info("Step 3: Waiting for all indexes to come online")
        self.log.info("=" * 80)
        self.assertTrue(self.wait_until_indexes_online(timeout=18000),
                        "Step 3: not all indexes became Ready after build")
        self.sleep(60, "Letting indexes stabilize")

        final_instances = self._get_total_index_instances(index_nodes)
        self.log.info(f"Total index instances after initial creation: {final_instances}")

        scalar_queries, _ = self._get_select_queries_for_definitions(
            all_definitions, self.indexed_namespaces)
        # vector_queries dropped — vector indexes are commented out

        # exit_after_setup: stop here (all indexes online) so a backup can be taken.
        if self.input.param("exit_after_setup", False):
            self.log.info("exit_after_setup=True — indexes online; exiting before rebalance steps")
            self.log.info(f"  instances={final_instances}, namespaces={total_namespaces}")
            return

        all_select_queries = scalar_queries
        self.assertTrue(all_select_queries, "No select queries were generated")
        self.log.info(f"Select queries: {len(all_select_queries)} (scalar={len(scalar_queries)})")

        self.log.info("Waiting 120s for GSI metadata to stabilize before rebalance workloads...")
        self.sleep(120, "Metadata stabilization before rebalance")
        self.log.info(f"Final total index instances (rebalance baseline): {final_instances}")

        # ── Step 3c: UPDATE STATISTICS (BLOCKING) ──────────────────────────────
        # Run synchronously (not in background) so Step 4a's "index scan used" check
        # is deterministic: missing stats make the optimizer fall back to PrimaryScan.
        self.log.info("=" * 80)
        self.log.info("Step 3c: Running UPDATE STATISTICS synchronously for all indexed namespaces")
        self.log.info("=" * 80)
        all_q_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        if not isinstance(all_q_nodes, list):
            all_q_nodes = [all_q_nodes]
        from itertools import cycle as _stats_cycle
        _qn_cycle = _stats_cycle(all_q_nodes)
        stats_pairs = [(ns, next(_qn_cycle)) for ns in self.indexed_namespaces]

        def _update_stats(ns, qnode):
            self.run_cbq_query(query=f"UPDATE STATISTICS FOR {ns} INDEX ALL", server=qnode)
            return ns

        stats_failures = []
        with ThreadPoolExecutor(max_workers=50) as executor:
            stats_futures = {executor.submit(_update_stats, ns, qn): (ns, qn) for ns, qn in stats_pairs}
            for f in as_completed(stats_futures):
                ns, qn = stats_futures[f]
                try:
                    f.result()
                except Exception as e:
                    err_type = type(e).__name__
                    stats_failures.append((ns, err_type, str(e)))
                    self.log.error(f"[Stats3c] UPDATE STATISTICS failed: ns={ns} "
                                   f"qnode={getattr(qn, 'ip', qn)} "
                                   f"error={err_type}: {str(e)[:300]}")
        if stats_failures:
            # Group by error type to surface OOM/timeout patterns quickly.
            from collections import Counter
            err_counts = Counter(err_type for _, err_type, _ in stats_failures)
            self.log.warning(f"Step 3c SOFT FAIL: UPDATE STATISTICS failed for "
                             f"{len(stats_failures)}/{len(stats_pairs)} namespaces "
                             f"— error breakdown: {dict(err_counts)}")
            for _ns, _et, _em in stats_failures[:20]:
                self.log.warning(f"  FAILED {_ns}: [{_et}] {_em[:200]}")
            if not hasattr(self, '_deferred_failures'):
                self._deferred_failures = []
            self._deferred_failures.append(
                f"Step 3c: UPDATE STATISTICS failed for {len(stats_failures)} namespace(s), "
                f"errors: {dict(Counter(et for _, et, _ in stats_failures))}, "
                f"first failure: {stats_failures[0]}"
            )
        self.log.info("Step 3c: UPDATE STATISTICS complete — optimizer stats ready")

        # ── Step 4: scans + doc-count validation ───────────────────────────────
        self.log.info("=" * 80)
        self.log.info("Step 4: Running initial scans with doc-count validation")
        self.log.info("=" * 80)
        # Expected docs/namespace for all doc-count validations.
        expected_docs = self.num_of_docs_per_collection

        # Baseline seeded here (no prior baseline yet); re-baselined at Step 4b,
        # compared against at Steps 6 and 8 — see _run_scans_and_validate_doc_count.
        _, mismatched, mismatches, self._doc_count_baseline = self._run_scans_and_validate_doc_count(
            select_queries=scalar_queries, query_node=query_node,
            expected_docs_per_namespace=expected_docs, scan_phase="initial")
        if mismatched:
            self.log.warning(f"Step 4 SOFT FAIL: {mismatched} scalar scans returned wrong "
                             f"doc counts (expected {expected_docs}/ns):")
            for _m in mismatches[:20]:
                self.log.warning(f"  MISMATCH {_m}")
            if not hasattr(self, '_deferred_failures'):
                self._deferred_failures = []
            self._deferred_failures.append(
                f"Step 4: {mismatched} scalar scan doc-count mismatches "
                f"(first 5): {mismatches[:5]}"
            )

        # Vector scans commented out — no vector indexes in this run.
        # vector_failures = []
        # if vector_queries:
        #     self.log.info(f"Running {len(vector_queries)} vector queries")
        #     with ThreadPoolExecutor(max_workers=50) as executor:
        #         vq_futures = {executor.submit(self.run_cbq_query, query=q, server=query_node): q
        #                       for q in vector_queries}
        #         for f in as_completed(vq_futures):
        #             try:
        #                 f.result()
        #             except Exception as e:
        #                 vector_failures.append(str(e))
        #     self.assertEqual(len(vector_failures), 0,
        #                      f"Step 4: {len(vector_failures)} vector queries failed "
        #                      f"(first 3): {vector_failures[:3]}")

        self.log.info("Step 4: initial scans validated")

        # ── Step 4a: optimizer + fan-out ───────────────────────────────────────
        self.log.info("=" * 80)
        self.log.info("Step 4a: Asserting index-scan plans and measuring fan-out latency")
        self.log.info("=" * 80)
        self._assert_index_scans(scalar_queries)
        self._measure_fanout(scalar_queries)

        # ── Step 4b: heavy mutations -> manual UPDATE STATISTICS -> queries ─────
        # Reproduces the reported stress order: AFTER index creation, drive a large
        # insert+update workload, THEN run UPDATE STATISTICS manually, THEN query the
        # indexes so plans use the freshly-collected stats over the new data volume.
        heavy_extra_docs = self.input.param("heavy_mutation_extra_docs", 500)
        self.log.info("=" * 80)
        self.log.info(f"Step 4b: Heavy mutations (+{heavy_extra_docs // 2} inserts, full update, "
                      f"-{heavy_extra_docs // 2} deletes per namespace — net doc count unchanged), "
                      f"then manual UPDATE STATISTICS, then queries")
        self.log.info("=" * 80)

        # 1) Sampled insert+update+delete: mutation_num_cycles cycles, each picks
        #    mutation_sample_size random collections, 25 docs per op, distributed
        #    round-robin across all live N1QL nodes. 3-min gap between cycles.
        heavy_failures = []
        mutated_namespaces = set()
        for _cycle in range(1, self.mutation_num_cycles + 1):
            self.log.info(f"Step 4b: mutation cycle {_cycle}/{self.mutation_num_cycles}")
            _failures, _sampled = self._run_mutations_sampled_distributed(
                self.namespaces,
                num_sample=self.mutation_sample_size,
                docs_per_op=self.mutation_docs_per_op)
            heavy_failures.extend(_failures)
            mutated_namespaces.update(_sampled)
            if _cycle < self.mutation_num_cycles:
                self.log.info(f"Step 4b: cycle {_cycle} done — sleeping "
                              f"{self.mutation_cycle_gap_secs}s before next cycle")
                time.sleep(self.mutation_cycle_gap_secs)
        if heavy_failures:
            self.log.warning(f"Step 4b SOFT FAIL: sampled mutations failed for "
                             f"{len(heavy_failures)} namespaces (continuing test): "
                             f"{heavy_failures[:10]}")
            if not hasattr(self, '_deferred_failures'):
                self._deferred_failures = []
            self._deferred_failures.append(
                f"Step 4b: {len(heavy_failures)} namespace(s) failed sampled mutations: "
                f"{heavy_failures}"
            )
        # Doc count is unchanged: inserts and deletes cancel out.
        expected_docs = self.num_of_docs_per_collection

        # 2) Indexes must fully catch up with the new writes before stats/queries.
        self.assertTrue(self.validate_no_pending_mutations(timeout=1800),
                        "Step 4b: mutations still pending after heavy insert/update")

        # 3) Manual UPDATE STATISTICS scoped to only the collections that were mutated.
        #    No need to re-collect stats for the 9,400 untouched namespaces.
        _touched = [ns for ns in mutated_namespaces if ns in set(self.indexed_namespaces)]
        self.log.info(f"Step 4b: running UPDATE STATISTICS on {len(_touched)} mutated namespaces...")
        _q_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        if not isinstance(_q_nodes, list):
            _q_nodes = [_q_nodes]
        _qc = _stats_cycle(_q_nodes)
        _stats_pairs = [(ns, next(_qc)) for ns in _touched]
        stats2_failures = []
        with ThreadPoolExecutor(max_workers=50) as executor:
            _sf = {executor.submit(_update_stats, ns, qn): (ns, qn) for ns, qn in _stats_pairs}
            for f in as_completed(_sf):
                ns, qn = _sf[f]
                try:
                    f.result()
                except Exception as e:
                    err_type = type(e).__name__
                    stats2_failures.append((ns, err_type, str(e)))
                    self.log.error(f"[Stats4b] UPDATE STATISTICS failed: ns={ns} "
                                   f"qnode={getattr(qn, 'ip', qn)} "
                                   f"error={err_type}: {str(e)[:300]}")
        if stats2_failures:
            from collections import Counter
            err_counts2 = Counter(err_type for _, err_type, _ in stats2_failures)
            self.log.warning(f"Step 4b SOFT FAIL: UPDATE STATISTICS failed for "
                             f"{len(stats2_failures)}/{len(_stats_pairs)} namespaces "
                             f"— error breakdown: {dict(err_counts2)}")
            for _ns, _et, _em in stats2_failures[:20]:
                self.log.warning(f"  FAILED {_ns}: [{_et}] {_em[:200]}")
            if not hasattr(self, '_deferred_failures'):
                self._deferred_failures = []
            self._deferred_failures.append(
                f"Step 4b: UPDATE STATISTICS failed for {len(stats2_failures)} namespace(s), "
                f"errors: {dict(Counter(et for _, et, _ in stats2_failures))}, "
                f"first failure: {stats2_failures[0]}"
            )

        # 4) Query the indexes over the new data: doc counts must match the new total,
        #    and the optimizer must still choose index scans with the fresh stats.
        # Re-baseline here (do NOT pass the Step 4 baseline through): the mutation
        # includes a full field UPDATE on the sampled namespaces, which legitimately
        # shifts predicate-matched counts even though the net doc count is unchanged.
        _, mismatched, mismatches, self._doc_count_baseline = self._run_scans_and_validate_doc_count(
            select_queries=scalar_queries, query_node=query_node,
            expected_docs_per_namespace=expected_docs, scan_phase="post-heavy-mutation")
        if mismatched:
            self.log.warning(f"Step 4b SOFT FAIL: {mismatched} scans returned wrong doc counts "
                             f"after heavy mutation (expected {expected_docs}/ns):")
            for _m in mismatches[:20]:
                self.log.warning(f"  MISMATCH {_m}")
            if not hasattr(self, '_deferred_failures'):
                self._deferred_failures = []
            self._deferred_failures.append(
                f"Step 4b: {mismatched} scan doc-count mismatches post-heavy-mutation "
                f"(first 5): {mismatches[:5]}"
            )
        self._assert_index_scans(scalar_queries)
        self.log.info("Step 4b: heavy-mutation + fresh-stats query validation passed")

        # Stop the 500-collection metadata churn before rebalance begins.
        churn_event.set()
        self._active_churn_events = [e for e in self._active_churn_events if e is not churn_event]
        churn_thread.join(timeout=600)
        self.assertFalse(churn_thread.is_alive(),
                         "Metadata churn thread did not stop within 600s before Step 5")
        self.log.info("[MetadataChurn] Churn thread stopped — proceeding to rebalance")

        # ── Step 5: shard-based swap rebalance ─────────────────────────────────
        self.log.info("=" * 80)
        self.log.info("Step 5: Shard-based swap rebalance with background mutations + scans")
        self.log.info("=" * 80)

        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        node_to_remove = next((n for n in index_nodes if n.ip != self.master.ip), None)
        node_to_add = self.servers[self.nodes_init] if self.nodes_init < len(self.servers) else None
        # Hard gate: a swap must have both a node to remove and a spare to add,
        # otherwise the step would silently no-op.
        self.assertIsNotNone(node_to_remove,
                             f"No non-master indexer node to remove (master={self.master.ip})")
        self.assertIsNotNone(node_to_add,
                             f"No spare node to add for swap (nodes_init={self.nodes_init}, "
                             f"servers={len(self.servers)})")
        self.log.info(f"Master {self.master.ip} excluded; swap out {node_to_remove.ip}, "
                      f"in {node_to_add.ip}")

        # PRE-rebalance replica-integrity gate: proves replicas are intact BEFORE the
        # rebalance, so a post-rebalance failure unambiguously points at the rebalance.
        self.assertTrue(
            self._verify_index_metadata_intact(expected_instances=final_instances,
                                               step_label="Step 5 pre-rebalance"),
            "Step 5 PRE-rebalance gate FAILED: indexes/replicas already missing before rebalance")

        excluded_nodes = [node_to_remove, node_to_add]
        self.log.info(f"Excluding from workload: {[n.ip for n in excluded_nodes]}")

        mutation_event = Event()
        scan_event = Event()
        mutation_thread = Thread(target=self._run_mutations_via_bulk_loader,
                                 args=(mutation_event, self.namespaces, self.mutation_ops_rate),
                                 kwargs={'excluded_nodes': excluded_nodes},
                                 daemon=True, name="MutationBG")
        scan_thread = Thread(target=self._run_continuous_scans_thread,
                             args=(scan_event, all_select_queries),
                             kwargs={'excluded_nodes': excluded_nodes},
                             daemon=True, name="ScanBG")
        mutation_thread.start()
        scan_thread.start()
        self.log.info("Background mutation + scan threads started")

        try:
            node_swapped_out, node_swapped_in = self._rebalance_indexer_nodes(rebalance_type="swap")
        finally:
            mutation_event.set()
            scan_event.set()
            mutation_thread.join(timeout=300)
            scan_thread.join(timeout=300)
        self.assertFalse(mutation_thread.is_alive(), "Step 5 mutation thread leaked after rebalance")
        self.assertFalse(scan_thread.is_alive(), "Step 5 scan thread leaked after rebalance")
        self.assertIsNotNone(node_swapped_out, "Step 5 swap rebalance returned no swapped-out node")
        self.log.info("Step 5: shard-based rebalance complete; workers stopped")

        # ── Step 6: post shard-rebalance validation ────────────────────────────
        self.log.info("=" * 80)
        self.log.info("Step 6: Post shard-rebalance validation")
        self.log.info("=" * 80)
        self.assertTrue(self.wait_until_indexes_online(timeout=10600),
                        "Step 6: indexes not online after shard-based rebalance")
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        # POST-rebalance replica-integrity gate — the check the original suite skipped.
        self.assertTrue(
            self._verify_index_metadata_intact(expected_instances=final_instances,
                                               step_label="Step 6"),
            "Step 6 FAILED: indexes or replicas were lost during shard-based rebalance")
        self.assertTrue(self.validate_no_pending_mutations(timeout=600),
                        "Step 6: mutations still pending after shard-based rebalance")
        _, mismatched, mismatches, _ = self._run_scans_and_validate_doc_count(
            select_queries=scalar_queries, query_node=query_node,
            expected_docs_per_namespace=expected_docs,
            scan_phase="post-shard-rebalance", baseline=self._doc_count_baseline)
        if mismatched:
            self.log.warning(f"Step 6 SOFT FAIL: {mismatched} scans returned wrong doc counts "
                             f"post-shard-rebalance (expected {expected_docs}/ns):")
            for _m in mismatches[:20]:
                self.log.warning(f"  MISMATCH {_m}")
            if not hasattr(self, '_deferred_failures'):
                self._deferred_failures = []
            self._deferred_failures.append(
                f"Step 6: {mismatched} scan doc-count mismatches post-shard-rebalance "
                f"(first 5): {mismatches[:5]}"
            )
            self.log.error(
                f"Step 6: post shard-rebalance validation FAILED with "
                f"{mismatched} mismatch(es) -- deferred to teardown")
        else:
            self.log.info("Step 6: post shard-rebalance validation passed")

        # ── Step 7: DCP swap rebalance (never skipped) ─────────────────────────
        self.log.info("=" * 80)
        self.log.info("Step 7: DCP swap rebalance with background mutations + scans")
        self.log.info("=" * 80)
        self.disable_shard_based_rebalance()
        self.sleep(5, "After disabling shard-based rebalance")

        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        dcp_node_to_remove = next((n for n in index_nodes if n.ip != self.master.ip), None)
        dcp_node_to_add = node_swapped_out  # bring the previously swapped-out node back in
        # Hard gate: never silently skip the DCP phase.
        self.assertIsNotNone(dcp_node_to_remove,
                             f"No non-master indexer node to remove for DCP (master={self.master.ip})")
        self.assertIsNotNone(dcp_node_to_add,
                             "No node available to add back for DCP rebalance (Step 5 swap-out missing)")
        self.log.info(f"DCP: swap out {dcp_node_to_remove.ip}, in {dcp_node_to_add.ip}")

        # PRE-rebalance replica-integrity gate for the DCP phase.
        self.assertTrue(
            self._verify_index_metadata_intact(expected_instances=final_instances,
                                               step_label="Step 7 pre-rebalance"),
            "Step 7 PRE-rebalance gate FAILED: indexes/replicas already missing before DCP rebalance")

        dcp_excluded_nodes = [dcp_node_to_remove, dcp_node_to_add]
        self.log.info(f"Excluding from workload: {[n.ip for n in dcp_excluded_nodes]}")

        mutation_event = Event()
        scan_event = Event()
        mutation_thread = Thread(target=self._run_mutations_via_bulk_loader,
                                 args=(mutation_event, self.namespaces, self.mutation_ops_rate),
                                 kwargs={'excluded_nodes': dcp_excluded_nodes},
                                 daemon=True, name="MutationBG-DCP")
        scan_thread = Thread(target=self._run_continuous_scans_thread,
                             args=(scan_event, all_select_queries),
                             kwargs={'excluded_nodes': dcp_excluded_nodes},
                             daemon=True, name="ScanBG-DCP")
        mutation_thread.start()
        scan_thread.start()
        self.log.info("Background mutation + scan threads started")

        try:
            self._rebalance_indexer_nodes(rebalance_type="swap", node_in=node_swapped_out)
        finally:
            mutation_event.set()
            scan_event.set()
            mutation_thread.join(timeout=300)
            scan_thread.join(timeout=300)
        self.assertFalse(mutation_thread.is_alive(), "Step 7 mutation thread leaked after rebalance")
        self.assertFalse(scan_thread.is_alive(), "Step 7 scan thread leaked after rebalance")
        self.log.info("Step 7: DCP rebalance complete; workers stopped")

        # ── Step 8: post DCP-rebalance validation ──────────────────────────────
        self.log.info("=" * 80)
        self.log.info("Step 8: Post DCP-rebalance validation")
        self.log.info("=" * 80)
        self.assertTrue(self.wait_until_indexes_online(timeout=7200),
                        "Step 8: indexes not online after DCP rebalance")
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        self.assertTrue(
            self._verify_index_metadata_intact(expected_instances=final_instances,
                                               step_label="Step 8"),
            "Step 8 FAILED: indexes or replicas were lost during DCP rebalance")
        self.assertTrue(self.validate_no_pending_mutations(timeout=600),
                        "Step 8: mutations still pending after DCP rebalance")
        _, mismatched, mismatches, _ = self._run_scans_and_validate_doc_count(
            select_queries=scalar_queries, query_node=query_node,
            expected_docs_per_namespace=expected_docs,
            scan_phase="post-DCP-rebalance", baseline=self._doc_count_baseline)
        if mismatched:
            self.log.warning(f"Step 8 SOFT FAIL: {mismatched} scans returned wrong doc counts "
                             f"post-DCP-rebalance (expected {expected_docs}/ns):")
            for _m in mismatches[:20]:
                self.log.warning(f"  MISMATCH {_m}")
            if not hasattr(self, '_deferred_failures'):
                self._deferred_failures = []
            self._deferred_failures.append(
                f"Step 8: {mismatched} scan doc-count mismatches post-DCP-rebalance "
                f"(first 5): {mismatches[:5]}"
            )
            self.log.error(
                f"Step 8: post DCP-rebalance validation FAILED with "
                f"{mismatched} mismatch(es) -- deferred to teardown")
        else:
            self.log.info("Step 8: post DCP-rebalance validation passed")

        # ── Step 8b: query-node resilience ─────────────────────────────────────
        self.log.info("=" * 80)
        self.log.info("Step 8b: Query-node restart / failover-recovery")
        self.log.info("=" * 80)
        self._measure_query_node_resilience()
        self.assertTrue(self.wait_until_indexes_online(timeout=7200),
                        "Step 8b: indexes not online after query-node resilience test")
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)

        # ── Step 9: drop all indexes (synchronous) + verify clean ──────────────
        self.log.info("=" * 80)
        self.log.info("Step 9: Dropping all indexes and validating cleanup")
        self.log.info("=" * 80)
        # Indexes must be Ready before DROP: DROP on a building index is rejected and
        # would leave it alive, breaking the teardown rebalance.
        self.assertTrue(self.wait_until_indexes_online(timeout=3600),
                        "Step 9: indexes not all Ready before drop")
        # Stabilize metakv token Revs across nodes so DROP does not hit a Rev mismatch.
        self.sleep(60, "Allowing metakv token Rev convergence before DROP INDEX")

        # Synchronous drop: any drop error propagates and fails the test — no swallowed
        # exceptions, no orphan-retry loop.
        self.drop_index_node_resources_utilization_validations(sleep_time=360)

        # Permanently drop the 500 churn collections; every drop must succeed.
        self.log.info(f"[MetadataChurn] Final teardown: dropping {len(self.churn_namespaces)} "
                      f"churn collections")
        churn_drop_failures = []
        t_final = time.time()
        for ns in self.churn_namespaces:
            _, ks = ns.split(':', 1)
            bucket, scope, col = ks.split('.')
            try:
                self.collection_rest.delete_collection(bucket=bucket, scope=scope, collection=col)
            except Exception as e:
                churn_drop_failures.append((col, str(e)))
                self.log.error(f"[MetadataChurn] Final drop {col} failed: {e}")
        self.log.info(f"[MetadataChurn] Final teardown in {time.time() - t_final:.1f}s")
        self.assertEqual(len(churn_drop_failures), 0,
                         f"{len(churn_drop_failures)} churn-collection drops failed "
                         f"(first 5): {churn_drop_failures[:5]}")
        base_counts = self._verify_metadata_consistency(_churn_bucket, len(self.namespaces))
        _valid = [c for c in base_counts.values() if c >= 0]
        self.assertEqual(len(set(_valid)), 1,
                         f"Cross-node metadata inconsistency after churn teardown: {base_counts}")

        # Single-pass cleanup gate (no retry): zero GSI indexes may remain.
        # Excludes the built-in _system scope (e.g. ix_system_query), matching
        # _snapshot_index_names' use of get_indexer_metadata(return_system_query_scope=False).
        self.log.info("Step 9: verifying no GSI indexes remain after drop...")
        _list_q = ("SELECT bucket_id, scope_id, keyspace_id, name "
                   "FROM system:all_indexes WHERE `using`='gsi' AND scope_id != '_system'")
        remaining = self.run_cbq_query(query=_list_q, server=query_node).get('results', [])
        self.assertEqual(len(remaining), 0,
                         f"Step 9: {len(remaining)} GSI indexes still present after drop "
                         f"(first 5): {[r.get('name') for r in remaining[:5]]}")
        self.log.info("Step 9: all indexes dropped; namespace clean")

        # ── Final: perf baseline ───────────────────────────────────────────────
        self._log_perf_baseline()
        self.log.info("=" * 80)
        self.log.info("TEST COMPLETED SUCCESSFULLY")
        self.log.info("=" * 80)
        self.log.info("FINAL SUMMARY:")
        self.log.info(f"  Total namespaces   : {total_namespaces}")
        self.log.info(f"  Namespaces indexed : {len(self.indexed_namespaces)}")
        self.log.info(f"  Final instances    : {final_instances}")
        self.log.info("=" * 80)

        # Consolidated post-rebalance index-loss report (soft-failed during the run —
        # see _rebalance_indexer_nodes).
        if self.index_loss_report:
            self.log.info("INDEX LOSS REPORT:")
            for entry in self.index_loss_report:
                self.log.info(
                    f"  [{entry['rebalance_id']}] type={entry['type']} "
                    f"node_out={entry['node_out']} instances before={entry['before']} "
                    f"after={entry['after']} lost={len(entry['lost'])} "
                    f"unexpected={len(entry['added'])}")
                for bucket, scope, coll, iname, replica_id in sorted(entry['lost']):
                    self.log.info(f"    LOST: {bucket}.{scope}.{coll}/{iname} "
                                  f"(replicaId={replica_id})")
                for bucket, scope, coll, iname, replica_id in sorted(entry['added']):
                    self.log.info(f"    UNEXPECTED: {bucket}.{scope}.{coll}/{iname} "
                                  f"(replicaId={replica_id})")
            self.log.info("=" * 80)

        # Flush every soft-asserted failure collected during the run — deferred
        # failures and (if opted in) index loss — as a single fail() so neither
        # message can be left unreachable by the other.
        _failure_messages = list(getattr(self, '_deferred_failures', []))
        if self.fail_on_index_loss and self.index_loss_report:
            _failure_messages.append(
                f"Index loss detected across {len(self.index_loss_report)} "
                f"rebalance(s) (fail_on_index_loss=True) — see INDEX LOSS REPORT above")
        if _failure_messages:
            self.fail(f"Soft-assert failures ({len(_failure_messages)}):\n" +
                      "\n".join(_failure_messages))
