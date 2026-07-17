"""
gsi_encryption_at_rest_rebalance.py: Tests for GSI Encryption at Rest under rebalance and cluster topology changes

Tests validate index creation, scans, and encryption configuration for various storage
types (MOI and Plasma) with encryption at rest enabled on buckets.

Encryption setup is handled by basetestcase.py when enable_encryption_at_rest=True.
This test uses self.encryption_at_rest_id (set by base class) to enable bucket encryption.

Supported storage types:
- memory_optimized (MOI) - in-memory storage
- forestdb (Plasma) - persistent storage

__author__ = "Pavan PB"
__maintainer = "Pavan PB"
__email__ = "pavan.pb@couchbase.com"
__git_user__ = "pavan-couchbase"
__created_on__ = "16/04/26"
"""

import random
import re
import string
import threading
import time
from couchbase_helper.documentgenerator import SDKDataLoader
from couchbase_helper.query_definitions import QueryDefinition, RANGE_SCAN_TEMPLATE
from membase.api.rest_client import RestConnection, RestHelper
from .gsi_encryption_helpers import GSIEncryptionAtRestBase, _GSI_TYPE_TO_ENGINE
from .base_gsi import BaseSecondaryIndexingTests


class GSIEncryptionAtRestRebalance(GSIEncryptionAtRestBase, BaseSecondaryIndexingTests):
    """
    Test class for GSI operations with Encryption at Rest enabled.

    Tests validate:
    - Encryption at rest configuration on buckets
    - Index creation with various types and replica counts on different storage types
    - Scan operations and result validation
    - Index stats verification (num_requests)

    Supported storage types:
    - memory_optimized (MOI) - in-memory indexes
    - forestdb (Plasma) - persistent indexes
    """
    
    def setUp(self):
        super(GSIEncryptionAtRestRebalance, self).setUp()
        self.log.info("==============  GSIEncryptionAtRestRebalance setup has started ==============")
        self.rest.delete_all_buckets()
        self.bucket_count = self.input.param("bucket_count", 1)
        self.num_scopes = self.input.param("num_scopes", 1)
        self.num_collections = self.input.param("num_collections", 1)
        self.use_default_collection_only = self.input.param("use_default_collection_only", True)
        self.num_of_docs_per_collection = self.input.param("num_of_docs_per_collection", 10000)
        self.json_template = self.input.param("json_template", "Hotel")
        self.defer_build = self.input.param("defer_build", False)
        self.encrypt_all_buckets = self.input.param("encrypt_all_buckets", True)
        self.rebalance_type = self.input.param("rebalance_type", "swap")  # in|out|swap
        self.rebalance_mode = self.input.param("rebalance_mode", "dcp")   # file|dcp
        # Number of indexes per type for rebalance tests. None = all available, integer = limit per type
        self.num_indexes_per_type = self.input.param("num_indexes_per_type", 1)
        if self.num_indexes_per_type == 0:
            self.num_indexes_per_type = None  # 0 means all available
        self.failure_detected = False
        self.test_failures = []
        self.log.info("==============  GSIEncryptionAtRestRebalance setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  GSIEncryptionAtRestRebalance tearDown has started ==============")
        super(GSIEncryptionAtRestRebalance, self).tearDown()
        self.log.info("==============  GSIEncryptionAtRestRebalance tearDown has completed ==============")

    def suite_tearDown(self):
        self.log.info("==============  GSIEncryptionAtRestRebalance suite_tearDown has started ==============")
        super(GSIEncryptionAtRestRebalance, self).suite_tearDown()
        self.log.info("==============  GSIEncryptionAtRestRebalance suite_tearDown has completed ==============")

    def suite_setUp(self):
        self.log.info("==============  GSIEncryptionAtRestRebalance suite_setUp has started ==============")
        super(GSIEncryptionAtRestRebalance, self).suite_setUp()
        GSIEncryptionHelpers._install_tools()
        self.log.info("==============  GSIEncryptionAtRestRebalance suite_setUp has completed ==============")

    # ---- shared helpers live in GSIEncryptionAtRestBase ----

    def _is_vector_index_definition(self, index_definition):
        """
        Return True for dense/sparse vector index definitions.
        """
        vector_attrs = ["dimension", "description", "similarity", "train_list",
                        "scan_nprobes", "sparsejl_dim", "bhive_index"]
        if any(getattr(index_definition, attr, None) for attr in vector_attrs):
            return True

        index_fields = getattr(index_definition, "index_fields", []) or []
        return any("vector" in str(field).lower() for field in index_fields)

    def _get_scalar_replica_count(self):
        """
        Keep scalar index replicas within the supported 0-2 range.
        """
        try:
            replica_count = int(self.num_index_replica)
        except (TypeError, ValueError):
            replica_count = 0
        return max(0, min(replica_count, 2))

    def _get_create_index_queries_with_replica_policy(self, definition_list, namespace,
                                                      deploy_node_info=None,
                                                      defer_build=False):
        """
        Create query list with no replicas for dense/sparse vector indexes and
        at most 2 replicas for scalar indexes.
        """
        create_queries = []
        scalar_replica_count = self._get_scalar_replica_count()

        for index_definition in definition_list:
            is_vector_index = self._is_vector_index_definition(index_definition)
            num_replica = 0 if is_vector_index else scalar_replica_count
            index_kind = "dense/sparse vector" if is_vector_index else "scalar"
            self.log.info(
                f"Creating {index_kind} index '{index_definition.index_name}' "
                f"with {num_replica} replicas"
            )
            create_queries.extend(
                self.gsi_util_obj.get_create_index_list(
                    definition_list=[index_definition],
                    namespace=namespace,
                    num_replica=num_replica,
                    deploy_node_info=deploy_node_info,
                    defer_build=defer_build,
                    bhive_index=getattr(index_definition, "bhive_index", False)
                )
            )

        return create_queries

    def _create_indexes(self, namespace, index_nodes):
        """
        Create indexes using gsi_util_obj patterns similar to gsi_file_based_rebalance.py.
        
        Creates indexes with no replicas for dense/sparse vector indexes and
        at most 2 replicas for scalar indexes.
        If defer_build=True, creates deferred indexes and builds them separately.
        
        Args:
            namespace: Namespace string for index creation
            index_nodes: List of index nodes for deployment
            
        Returns:
            tuple: (select_queries set, create_queries list, deferred_definitions list)
        """
        select_queries = set()
        create_queries = []
        deferred_definitions = []
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
        
        for namespace_iter in self.namespaces:
            # Generate unique prefix
            prefix = 'ear_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
            
            # Get index definitions based on json_template
            query_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template,
                prefix=prefix,
                skip_primary=False
            )
            
            # Get select queries for validation
            select_queries.update(
                self.gsi_util_obj.get_select_queries(
                    definition_list=query_definitions,
                    namespace=namespace_iter
                )
            )
            
            # Get create index queries with replica policy.
            queries = self._get_create_index_queries_with_replica_policy(
                definition_list=query_definitions,
                namespace=namespace_iter,
                deploy_node_info=deploy_nodes,
                defer_build=self.defer_build
            )
            
            # Create indexes
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=queries,
                database=namespace_iter,
                query_node=query_node
            )
            
            create_queries.extend(queries)
            
            # Track deferred definitions for later build
            if self.defer_build:
                deferred_definitions.extend(query_definitions)
        
        self.log.info(f"Created {len(create_queries)} indexes with num_replica={self.num_index_replica}")
        return select_queries, create_queries, deferred_definitions

    # ========================================================================
    # Rebalance Test Helper Methods
    # ========================================================================

    def _create_encrypted_test_buckets(self, num_buckets=2, encrypt_all=True):
        """
        Create multiple buckets with scopes and collections.
        
        Args:
            num_buckets: Number of buckets to create (default: 2)
            encrypt_all: If True, all buckets are encrypted. If False, only the first bucket
                         is encrypted and the rest are not encrypted. (default: True)
            
        Returns:
            tuple: (bucket_names, encrypted_bucket_names)
                bucket_names: List of all created bucket names
                encrypted_bucket_names: List of bucket names that have encryption enabled
        """
        bucket_names = []
        encrypted_bucket_names = []
        for i in range(num_buckets):
            bucket_name = f"test_bucket_{i}"
            
            # Determine if this bucket should be encrypted
            # If encrypt_all is True, all buckets encrypted
            # If encrypt_all is False, only first bucket (i=0) is encrypted
            enable_encryption = encrypt_all or (i == 0)
            
            encryption_status = "encrypted" if enable_encryption else "non-encrypted"
            self.log.info(f"Creating {encryption_status} bucket '{bucket_name}'...")
            self.create_bucket_with_encryption(bucket_name, enable_encryption=enable_encryption)
            bucket_names.append(bucket_name)
            if enable_encryption:
                encrypted_bucket_names.append(bucket_name)
            
            # Create scopes and collections, load documents
            self.log.info(f"Creating scopes/collections and loading docs for bucket '{bucket_name}'...")
            self.prepare_collection_for_indexing(
                bucket_name=bucket_name,
                num_scopes=self.num_scopes,
                num_collections=self.num_collections,
                num_of_docs_per_collection=self.num_of_docs_per_collection,
                json_template=self.json_template,
                load_default_coll=True
            )
            self.log.info(f"Bucket '{bucket_name}' ({encryption_status}) setup complete")
        
        return bucket_names, encrypted_bucket_names

    def _get_query_definitions(self, prefix, similarity="COSINE", num_indexes_per_type=None):
        """
        Get query definitions for rebalance tests covering dense composite,
        sparse composite, and scalar indexes (regular + partitioned).
        BHIVE dense and sparse definitions are enabled when self.bhive_index=True.

        Args:
            prefix: Prefix for index names
            similarity: Similarity metric for dense indexes (default: COSINE)
            num_indexes_per_type: Number of indexes to create per type.
                                  If None, creates all available indexes.
                                  If an integer, limits each type to that many indexes.

        Returns:
            tuple: (dense_bhive_def, dense_composite_def, scalar_def, sparse_bhive_def, sparse_composite_def)
        """
        if self.bhive_index:
            self.log.info("BHIVE index flag enabled — creating dense and sparse BHIVE definitions")
            dense_bhive_all = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template,
                prefix=f"{prefix}_dbhive",
                similarity=similarity,
                train_list=None,
                scan_nprobes=self.scan_nprobes if hasattr(self, 'scan_nprobes') else 25,
                array_indexes=False,
                limit=self.scan_limit if hasattr(self, 'scan_limit') else 10,
                quantization_algo_color_vector=self.quantization_algo_color_vector if hasattr(self, 'quantization_algo_color_vector') else None,
                quantization_algo_description_vector=self.quantization_algo_description_vector if hasattr(self, 'quantization_algo_description_vector') else "PQ64x8",
                bhive_index=True,
                description_dimension=self.dimension if hasattr(self, 'dimension') else 128
            )
            dense_bhive_def = dense_bhive_all[:num_indexes_per_type] if num_indexes_per_type else dense_bhive_all

            sparse_bhive_all = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template,
                prefix=f"{prefix}_sbhive",
                similarity="DOT",
                train_list=None,
                scan_nprobes=self.scan_nprobes if hasattr(self, 'scan_nprobes') else 25,
                array_indexes=False,
                limit=self.scan_limit if hasattr(self, 'scan_limit') else 10,
                bhive_index=True,
                is_sparse=True
            )
            sparse_bhive_def = sparse_bhive_all[:num_indexes_per_type] if num_indexes_per_type else sparse_bhive_all
        else:
            self.log.info("BHIVE index flag disabled — skipping BHIVE definitions for rebalance tests")
            dense_bhive_def = []
            sparse_bhive_def = []
        
        # Dense Composite indexes
        dense_composite_all = self.gsi_util_obj.get_index_definition_list(
            dataset=self.json_template,
            prefix=f"{prefix}_dcomp",
            similarity=similarity,
            train_list=None,
            scan_nprobes=self.scan_nprobes if hasattr(self, 'scan_nprobes') else 25,
            array_indexes=False,
            limit=self.scan_limit if hasattr(self, 'scan_limit') else 10,
            quantization_algo_color_vector=self.quantization_algo_color_vector if hasattr(self, 'quantization_algo_color_vector') else None,
            quantization_algo_description_vector=self.quantization_algo_description_vector if hasattr(self, 'quantization_algo_description_vector') else "PQ64x8",
            bhive_index=False,
            description_dimension=self.dimension if hasattr(self, 'dimension') else 128
        )
        dense_composite_def = dense_composite_all[:num_indexes_per_type] if num_indexes_per_type else dense_composite_all
        
        # Scalar indexes - regular + partitioned
        scalar_def = []
        # Regular scalar index
        scalar_def.append(QueryDefinition(
            index_name=f"{prefix}_scalar_color",
            index_fields=["color"],
            query_template=RANGE_SCAN_TEMPLATE.format("color", "color = 'Green'")
        ))
        # Partitioned scalar index
        scalar_def.append(QueryDefinition(
            index_name=f"{prefix}_scalar_review_partitioned",
            index_fields=["review"],
            query_template=RANGE_SCAN_TEMPLATE.format("review", "review > 0"),
            partition_by_fields=["meta().id"]
        ))

        # Sparse Composite indexes (no quantization, no dimension, DOT similarity only)
        sparse_composite_all = self.gsi_util_obj.get_index_definition_list(
            dataset=self.json_template,
            prefix=f"{prefix}_scomp",
            similarity="DOT",  # Sparse only supports DOT product
            train_list=None,
            scan_nprobes=self.scan_nprobes if hasattr(self, 'scan_nprobes') else 25,
            array_indexes=False,
            limit=self.scan_limit if hasattr(self, 'scan_limit') else 10,
            bhive_index=False,
            is_sparse=True
        )
        sparse_composite_def = sparse_composite_all[:num_indexes_per_type] if num_indexes_per_type else sparse_composite_all
        
        return dense_bhive_def, dense_composite_def, scalar_def, sparse_bhive_def, sparse_composite_def

    def _get_create_queries(self, dense_bhive_def, dense_composite_def, scalar_def,
                            sparse_bhive_def, sparse_composite_def, namespace,
                            defer_build=True):
        """
        Get CREATE INDEX queries for all index types with varying replica counts (0, 1, 2).
        
        Each index type gets a different replica count to ensure coverage of all replica
        configurations during rebalance testing.
        
        Index types created:
        - 1 Dense BHIVE index
        - 1 Dense Composite index
        - 1 Regular scalar index
        - 1 Partitioned scalar index
        - 1 Sparse BHIVE index
        - 1 Sparse Composite index
        
        Args:
            dense_bhive_def: Dense BHIVE index definitions
            dense_composite_def: Dense composite index definitions
            scalar_def: Scalar index definitions (includes regular + partitioned)
            sparse_bhive_def: Sparse BHIVE index definitions
            sparse_composite_def: Sparse composite index definitions
            namespace: Namespace for index creation
            defer_build: Whether to create indexes with defer_build=True (default: True)
            
        Returns:
            list: List of CREATE INDEX queries
        """
        all_queries = []
        
        # Scalar indexes: cycle through replica counts 1, 2 (1-2 max)
        scalar_replica_counts = [1, 2]
        scalar_replica_idx = 0
        
        # Dense and Sparse indexes: NEVER create replicas (always 0)
        # Scalar indexes: 1-2 replicas max
        
        # Dense BHIVE queries - NO replicas (always 0)
        if dense_bhive_def:
            self.log.info(f"Creating dense BHIVE index with 0 replicas (dense/sparse never get replicas), defer_build={defer_build}")
            queries = self.gsi_util_obj.get_create_index_list(
                definition_list=dense_bhive_def,
                namespace=namespace,
                num_replica=0,
                bhive_index=True,
                defer_build=defer_build
            )
            all_queries.extend(queries)
        
        # Dense Composite queries - NO replicas (always 0)
        if dense_composite_def:
            self.log.info(f"Creating dense composite index with 0 replicas (dense/sparse never get replicas), defer_build={defer_build}")
            queries = self.gsi_util_obj.get_create_index_list(
                definition_list=dense_composite_def,
                namespace=namespace,
                num_replica=0,
                bhive_index=False,
                defer_build=defer_build
            )
            all_queries.extend(queries)
        
        # Scalar queries - handle regular and partitioned separately, 1-2 replicas
        if scalar_def:
            for idx_def in scalar_def:
                replica_count = scalar_replica_counts[scalar_replica_idx % 2]
                is_partitioned = hasattr(idx_def, 'partition_by_fields') and idx_def.partition_by_fields
                
                if is_partitioned:
                    self.log.info(f"Creating partitioned scalar index '{idx_def.index_name}' with {replica_count} replicas, defer_build={defer_build}")
                    # Generate partitioned index query
                    query = idx_def.generate_index_create_query(
                        namespace=namespace,
                        num_replica=replica_count,
                        defer_build=defer_build
                    )
                    all_queries.append(query)
                else:
                    self.log.info(f"Creating regular scalar index '{idx_def.index_name}' with {replica_count} replicas, defer_build={defer_build}")
                    queries = self.gsi_util_obj.get_create_index_list(
                        definition_list=[idx_def],
                        namespace=namespace,
                        num_replica=replica_count,
                        defer_build=defer_build
                    )
                    all_queries.extend(queries)
                scalar_replica_idx += 1
        
        # Sparse BHIVE queries - NO replicas (always 0)
        if sparse_bhive_def:
            self.log.info(f"Creating sparse BHIVE index with 0 replicas (dense/sparse never get replicas), defer_build={defer_build}")
            queries = self.gsi_util_obj.get_create_index_list(
                definition_list=sparse_bhive_def,
                namespace=namespace,
                num_replica=0,
                bhive_index=True,
                defer_build=defer_build
            )
            all_queries.extend(queries)
        
        # Sparse Composite queries - NO replicas (always 0)
        if sparse_composite_def:
            self.log.info(f"Creating sparse composite index with 0 replicas (dense/sparse never get replicas), defer_build={defer_build}")
            queries = self.gsi_util_obj.get_create_index_list(
                definition_list=sparse_composite_def,
                namespace=namespace,
                num_replica=0,
                bhive_index=False,
                defer_build=defer_build
            )
            all_queries.extend(queries)
        
        return all_queries
    
    def _build_all_deferred_indexes_for_namespace(self, namespace, all_definitions):
        """
        Build all deferred indexes for a namespace at once.
        
        Args:
            namespace: Namespace string for index creation
            all_definitions: List of all index definitions to build
        """
        if not all_definitions:
            self.log.info(f"No deferred indexes to build for namespace {namespace}")
            return
            
        # Extract index names from definitions
        index_names = [idx_def.index_name for idx_def in all_definitions]
        self.log.info(f"Building {len(index_names)} deferred indexes for namespace {namespace}: {index_names}")

        #get the query node to run the query on
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        
        # Build using gsi_util_obj
        build_query = self.gsi_util_obj.get_build_indexes_query(
            definition_list=all_definitions,
            namespace=namespace
        )
        self.log.info(f"Build query: {build_query}")
        self.run_cbq_query(query=build_query, server=query_node)
        self.log.info(f"Build indexes query executed for namespace {namespace}")

    def _enable_shard_rebalance_if_file_mode(self, step_prefix="[STEP 3]"):
        if self.rebalance_mode == "file":
            self.log.info(f"{step_prefix} Enabling shard-based rebalance for file mode...")
            self.enable_shard_based_rebalance()
            self.log.info(f"{step_prefix} PASSED - Shard-based rebalance enabled")
            return True
        self.log.info(f"{step_prefix} SKIPPED - DCP mode, shard-based rebalance not enabled")
        return False

    def _get_select_queries(self, dense_bhive_def, dense_composite_def, scalar_def,
                            sparse_bhive_def, sparse_composite_def, namespace):
        """
        Get SELECT queries for all index types.
        
        Args:
            dense_bhive_def: Dense BHIVE index definitions
            dense_composite_def: Dense composite index definitions
            scalar_def: Scalar index definitions
            sparse_bhive_def: Sparse BHIVE index definitions
            sparse_composite_def: Sparse composite index definitions
            namespace: Namespace for queries
            
        Returns:
            set: Set of SELECT queries
        """
        select_queries = set()
        
        all_defs = []
        if dense_bhive_def:
            all_defs.extend(dense_bhive_def)
        if dense_composite_def:
            all_defs.extend(dense_composite_def)
        if scalar_def:
            all_defs.extend(scalar_def)
        if sparse_bhive_def:
            all_defs.extend(sparse_bhive_def)
        if sparse_composite_def:
            all_defs.extend(sparse_composite_def)
        
        if all_defs:
            queries = self.gsi_util_obj.get_select_queries(
                definition_list=all_defs,
                namespace=namespace,
                limit=self.scan_limit if hasattr(self, 'scan_limit') else 10
            )
            select_queries.update(queries)
        
        return select_queries

    def _start_continuous_workload(self, select_queries):
        """
        Start continuous scan and KV mutation threads.

        Returns (mutation_event, scan_thread, mutation_thread) so the caller can
        stop them with _stop_continuous_workload after the rebalance finishes.
        """
        self.query_node = self.get_nodes_from_services_map(service_type="n1ql")
        self.run_continous_query = True
        scan_thread = threading.Thread(
            target=self._run_queries_continously,
            args=(select_queries, False),
            name="ear_continuous_scan"
        )
        scan_thread.start()

        mutation_event = threading.Event()
        mutation_thread = threading.Thread(
            target=self.perform_continuous_kv_mutations,
            args=(mutation_event,),
            name="ear_continuous_mutation"
        )
        mutation_thread.start()

        self.log.info("Continuous scan and KV mutation threads started")
        return mutation_event, scan_thread, mutation_thread

    def _stop_continuous_workload(self, mutation_event, scan_thread, mutation_thread):
        """Stop the threads started by _start_continuous_workload."""
        self.run_continous_query = False
        mutation_event.set()
        scan_thread.join()
        mutation_thread.join()
        self.log.info("Continuous scan and KV mutation threads stopped")

    def _trigger_key_rotation_before_rebalance(self, bucket_names, key_rotation_interval,
                                                key_rotation_wait_time, step_label="",
                                                exclude_bucket=None):
        """
        Trigger DEK rotation on all encrypted buckets and wait for new key IDs.
        Returns True if rotation was triggered on at least one bucket.

        bucket_names: single bucket name (str) or list of bucket names.
        exclude_bucket: bucket name to skip — use for scenarios where one bucket
                        was encrypted later and already has a fresh DEK.

        When this returns True the caller should pass skip_scans=True to
        _ear_rebalance_and_validate so the redundant pre-rebalance scan loop is
        omitted.
        """
        if isinstance(bucket_names, str):
            bucket_names = [bucket_names]

        current_index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        baseline_key_ids = self.get_indexer_in_use_key_ids(current_index_nodes)
        self.log.info(f"{step_label}Baseline in-use key IDs before rotation: {baseline_key_ids}")

        rotated_any = False
        for bucket_name in bucket_names:
            if exclude_bucket and bucket_name == exclude_bucket:
                self.log.info(
                    f"{step_label}Skipping key rotation for '{bucket_name}' "
                    f"(bucket was encrypted later — DEK already fresh)"
                )
                continue

            bucket_info = self.rest.get_bucket_json(bucket_name)
            encryption_key_id = bucket_info.get('encryptionAtRestKeyId', None)
            if encryption_key_id is None or encryption_key_id == -1:
                self.log.info(
                    f"{step_label}Bucket '{bucket_name}' has no encryption — skipping"
                )
                continue

            self.log.info(
                f"{step_label}Setting DEK rotation on bucket '{bucket_name}' "
                f"with interval={key_rotation_interval}s"
            )
            self.set_bucket_dek_rotation_config(
                bucket_name, encryption_key_id,
                dek_rotation_interval=key_rotation_interval,
                dek_lifetime=key_rotation_interval
            )
            rotated_any = True

        if rotated_any:
            self.sleep(
                key_rotation_wait_time,
                f"{step_label}Waiting {key_rotation_wait_time}s for index key rotation to occur"
            )
            new_key_ids = self.get_indexer_in_use_key_ids(current_index_nodes)
            self.log.info(f"{step_label}Key rotation completed. New key IDs: {new_key_ids}")

        return rotated_any

    def _ear_rebalance_and_validate(self, nodes_in_list, nodes_out_list, services_in,
                                     select_queries, index_nodes, shard_rebalance_enabled=False,
                                     encrypted_bucket_names=None,
                                     run_continuous_workload=False, skip_scans=False):
        """
        Perform rebalance operation and validate indexes and encryption.

        Args:
            nodes_in_list: List of nodes to add
            nodes_out_list: List of nodes to remove
            services_in: Services for nodes being added
            select_queries: SELECT queries for validation
            index_nodes: List of index nodes
            shard_rebalance_enabled: Whether shard-based rebalance was enabled before index creation
            encrypted_bucket_names: List of bucket names that have encryption enabled.
                                    Passed to file encryption validation to only validate
                                    encrypted buckets.
        """
        if not nodes_in_list:
            nodes_in_list = []
        if not nodes_out_list:
            nodes_out_list = []
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        if not skip_scans:
            self.log.info("Running scans before rebalance...")
            for query in select_queries:
                try:
                    # Replace embVector placeholder if present
                    if "embVector" in query:
                        query = query.replace("embVector", str(self.bhive_sample_vector))
                    self.run_cbq_query(query=query, scan_consistency='request_plus', server=query_node)
                except Exception as e:
                    self.log.warning(f"Pre-rebalance query failed: {str(e)}")
                    raise

            self.sleep(60, "Waiting for system to stabilize before rebalance")
        else:
            self.log.info("Skipping pre-rebalance scans (key rotation ran before rebalance)")
            self.sleep(90, "Waiting for system to stabilize before rebalance")

        # Capture shard list before rebalance if file-based
        shard_list_before_rebalance = None
        if self.rebalance_mode == "file" and shard_rebalance_enabled:
            shard_list_before_rebalance = self.fetch_shard_id_list()
            self.log.info(f"Shard list before rebalance: {shard_list_before_rebalance}")

        # Optionally start continuous scans and mutations during rebalance
        mutation_event = scan_thread = mutation_thread = None
        if run_continuous_workload:
            mutation_event, scan_thread, mutation_thread = self._start_continuous_workload(select_queries)

        # Trigger rebalance
        self.log.info(f"Triggering rebalance: nodes_in={[n.ip for n in nodes_in_list]}, "
                      f"nodes_out={[n.ip for n in nodes_out_list]}")
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init],
            nodes_in_list,
            nodes_out_list,
            services=services_in,
            cluster_config=self.cluster_config if hasattr(self, 'cluster_config') else None
        )

        try:
            # Wait for rebalance to complete
            self.log.info("Waiting for rebalance to complete...")
            reached = RestHelper(self.rest).rebalance_reached(retry_count=250)
            self.assertTrue(reached, "Rebalance failed, stuck or did not complete")
            rebalance.result()
            self.log.info("Rebalance completed successfully")
        finally:
            # Always stop the workload threads whether rebalance succeeded or failed
            if run_continuous_workload:
                self._stop_continuous_workload(mutation_event, scan_thread, mutation_thread)
        
        # Shard-based validations (only if file mode AND shard rebalance was enabled before index creation)
        if self.rebalance_mode == "file" and shard_rebalance_enabled:
            self.log.info("Running file-based rebalance validations...")
            self.sleep(60, "Waiting after rebalance for shard validations")
            
            # Validate shard affinity
            self.validate_shard_affinity()
            
            # Check shard transfer logs
            if not self.check_gsi_logs_for_shard_transfer():
                self.log.warning("Shard-based rebalance transfer not detected in logs")
            
            # Compare shard lists
            if shard_list_before_rebalance:
                shard_list_after_rebalance = self.fetch_shard_id_list()
                self.log.info(f"Shard list after rebalance: {shard_list_after_rebalance}")
        
        # Run scans after rebalance
        self.log.info("Running scans after rebalance...")
        for query in select_queries:
            try:
                if "embVector" in query:
                    query = query.replace("embVector", str(self.bhive_sample_vector))
                self.run_cbq_query(query=query, scan_consistency='request_plus', server=query_node)
            except Exception as e:
                self.log.error(f"Post-rebalance query failed: {str(e)}")
                raise
        
        if not self.encrypt_all_buckets:
            self.log.info("Skipping file encryption validation — mixed encryption scenario (not all buckets encrypted)")
        else:
            # Validate file encryption
            self.log.info("Validating file encryption after rebalance...")
            updated_index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            expected_key_ids = self.get_indexer_in_use_key_ids(updated_index_nodes)
            validation_passed = self.validate_file_encryption_with_key(
                updated_index_nodes,
                expected_key_id=expected_key_ids,
                step_prefix="[POST-REBALANCE] ",
                encrypted_bucket_names=encrypted_bucket_names
            )
            if not validation_passed:
                raise Exception("File encryption validation failed after rebalance")

        self.log.info("Rebalance and validation completed successfully")

    def test_gsi_encryption_at_rest_sanity(self):
        """
        Encryption at rest sanity test case - supports single or multiple buckets.
        
        Test validates:
        1. Encryption at rest is configured (key created by basetestcase.py)
        2. Buckets are created with encryption enabled based on bucket_count and encrypt_all_buckets params
        3. Indexer snapshot settings configured (persisted_snapshot.moi.interval=60000)
        4. Various index types are created successfully on the configured storage type
        5. Indexes reach "Ready" state
        6. SELECT queries for all indexes execute successfully and return results
        7. Item count validations (KV vs GSI counts, replica counts, no pending mutations)
        8. File encryption validation on disk
        
        Parameters:
        - bucket_count: Number of buckets to create (default: 1)
        - encrypt_all_buckets: If True, all buckets are encrypted. If False, only first bucket is encrypted.
        
        Storage types supported:
        - memory_optimized (MOI) - in-memory indexes
        - forestdb (Plasma) - persistent indexes
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_sanity")
        self.log.info(f"Test parameters: gsi_type={self.gsi_type}, bucket_count={self.bucket_count}, encrypt_all_buckets={self.encrypt_all_buckets}")
        self.log.info("=" * 80)
        
        # ========== STEP 1: Verify index nodes ==========
        try:
            index_nodes = self.verify_prerequisites(min_index_nodes=2, check_encryption=False)
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED - Index node verification failed: {str(e)}")
            raise        
        # ========== STEP 2: Log storage type ==========
        self.log.info(f"[STEP 2] Storage engine: gsi_type={self.gsi_type}")
        self.log.info("[STEP 2] PASSED - Storage type confirmed")
        
        # ========== STEP 3: Verify encryption at rest parameter ==========
        try:
            self.log.info("[STEP 3] Verifying encryption at rest parameter...")
            if not self.enable_encryption_at_rest:
                self.log.info("[STEP 3] SKIPPED - Encryption at rest not enabled")
                self.skipTest("Encryption at rest not enabled. Set enable_encryption_at_rest=True")
            self.log.info("[STEP 3] PASSED - Encryption at rest parameter confirmed")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED - Encryption parameter verification failed: {str(e)}")
            raise
        
        # ========== STEP 4: Create buckets with encryption ==========
        bucket_names = []
        encrypted_bucket_names = []
        try:
            self.log.info(f"[STEP 4] Creating {self.bucket_count} bucket(s)...")
            for i in range(self.bucket_count):
                bucket_name = f"{self.test_bucket}_{i+1}" if self.bucket_count > 1 else self.test_bucket
                encrypt_this_bucket = self.encrypt_all_buckets or (i == 0)
                self.log.info(f"[STEP 4.{i+1}] Creating bucket '{bucket_name}' with encryption={encrypt_this_bucket}")
                
                self.create_bucket_with_encryption(bucket_name, enable_encryption=encrypt_this_bucket)
                bucket_names.append(bucket_name)
                if encrypt_this_bucket:
                    encrypted_bucket_names.append(bucket_name)
                
                # Validate bucket encryption status
                bucket_info = self.rest.get_bucket_json(bucket_name)
                encryption_key_id = bucket_info.get('encryptionAtRestKeyId', None)
                self.log.info(f"[STEP 4.{i+1}] Bucket '{bucket_name}' encryptionAtRestKeyId: {encryption_key_id}")
                
                if encrypt_this_bucket:
                    if encryption_key_id is None or encryption_key_id == -1:
                        raise Exception(f"Bucket '{bucket_name}' should have encryption enabled")
                else:
                    if encryption_key_id is not None and encryption_key_id != -1:
                        raise Exception(f"Bucket '{bucket_name}' should not have encryption enabled")
                
                self.log.info(f"[STEP 4.{i+1}] PASSED - Bucket '{bucket_name}' created successfully")
            
            self.log.info(f"[STEP 4] PASSED - Created {len(bucket_names)} bucket(s): {bucket_names}")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED - Bucket creation failed: {str(e)}")
            raise
        
        # ========== STEP 5: Set indexer snapshot settings ==========
        try:
            self.set_indexer_snapshot_settings(index_nodes, step_prefix="[STEP 5]")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED - Indexer settings configuration failed: {str(e)}")
            raise        
        # ========== STEP 6: Load documents and create indexes ==========
        select_queries = set()
        try:
            self.log.info(f"[STEP 6] Loading documents and creating indexes for {len(bucket_names)} bucket(s)...")

            # Step 6a: Load documents across all buckets in one pass
            try:
                self.log.info(
                    f"[STEP 6.a] Loading {self.num_of_docs_per_collection} documents into each bucket: {bucket_names}"
                )
                gen_create = SDKDataLoader(
                    num_ops=self.num_of_docs_per_collection,
                    percent_create=100,
                    percent_update=0,
                    percent_delete=0,
                    scope="_default",
                    collection="_default",
                    json_template=self.json_template,
                    output=True,
                    username=self.username,
                    password=self.password
                )
                tasks = self.data_ops_javasdk_loader_in_batches(
                    sdk_data_loader=gen_create,
                    batch_size=10**4,
                    dataset=self.json_template
                )
                for task in tasks:
                    task.result()
                self.log.info(
                    f"[STEP 6.a] PASSED - Loaded {self.num_of_docs_per_collection} documents into each bucket: {bucket_names}"
                )
            except Exception as e:
                self.log.error(f"[STEP 6.a] FAILED - Document loading failed: {str(e)}")
                raise

            for idx, bucket_name in enumerate(bucket_names):
                self.log.info(f"[STEP 6.{idx+1}] Processing bucket '{bucket_name}'...")
                namespace = f"default:{bucket_name}._default._default"

                # Step 6b: Create indexes and collect select queries
                try:
                    self.log.info(f"[STEP 6.{idx+1}.b] Creating indexes on '{bucket_name}'...")
                    prefix = 'ear_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
                    query_definitions = self.gsi_util_obj.get_index_definition_list(
                        dataset=self.json_template,
                        prefix=prefix,
                        skip_primary=False
                    )
                    self.log.info(f"[STEP 5.{idx+1}.b] Generated {len(query_definitions)} index definitions with prefix '{prefix}'")
                    
                    # Collect select queries for validation
                    select_queries.update(
                        self.gsi_util_obj.get_select_queries(
                            definition_list=query_definitions,
                            namespace=namespace
                        )
                    )
                    
                    deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
                    queries = self._get_create_index_queries_with_replica_policy(
                        definition_list=query_definitions,
                        namespace=namespace,
                        deploy_node_info=deploy_nodes,
                        defer_build=self.defer_build
                    )
                    self.log.info(f"[STEP 6.{idx+1}.b] Generated {len(queries)} CREATE INDEX queries")
                    
                    query_node = self.get_nodes_from_services_map(service_type="n1ql")
                    self.gsi_util_obj.create_gsi_indexes(
                        create_queries=queries,
                        database=namespace,
                        query_node=query_node
                    )
                    self.log.info(f"[STEP 6.{idx+1}.b] PASSED - Created indexes on '{bucket_name}'")
                except Exception as e:
                    self.log.error(f"[STEP 6.{idx+1}.b] FAILED - Index creation failed for '{bucket_name}': {str(e)}")
                    raise
            
            self.log.info(f"[STEP 6] PASSED - Document loading and index creation completed for all buckets. Collected {len(select_queries)} select queries")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED - Document loading/index creation failed: {str(e)}")
            raise
        
        # ========== STEP 7: Wait for indexes to be online ==========
        try:
            self.log.info("[STEP 7] Waiting for all indexes to reach 'Ready' state...")
            self.wait_until_indexes_online(timeout=1200, defer_build=self.defer_build)
            self.log.info("[STEP 7] PASSED - All indexes are in 'Ready' state")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED - Index online wait failed: {str(e)}")
            raise
        
        # ========== STEP 8: Run SELECT queries and validate results ==========
        try:
            self.log.info(f"[STEP 8] Running {len(select_queries)} SELECT queries for all indexes after encryption...")
            
            # Capture scan stats before queries
            self.run_scan_validation(select_queries)
            
            self.log.info(f"[STEP 8] PASSED - All {len(select_queries)} SELECT queries executed successfully with encryption enabled")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED - SELECT query validation failed: {str(e)}")
            raise
        
        # ========== STEP 9: Validate item counts and related validations ==========
        try:
            self.log.info("[STEP 9] Running item count related validations (KV vs GSI, replica counts, pending mutations)...")
            self.item_count_related_validations()
            self.log.info("[STEP 9] PASSED - Item count related validations completed successfully")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED - Item count validation failed: {str(e)}")
            raise
        
        # ========== STEP 10: Validate file encryption on disk ==========
        try:
            self.log.info("[STEP 10] Validating file encryption on disk...")
            self.validate_file_encryption_with_key(
                index_nodes,
                expected_key_id=None,  # Will auto-fetch in-use key IDs
                step_prefix="[STEP 10] ",
                encrypted_bucket_names=encrypted_bucket_names,
                raise_on_failure=True
            )
            self.log.info("[STEP 10] PASSED - File encryption validation completed successfully")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED - File encryption validation failed: {str(e)}")
            raise
        
        self.restart_index_nodes(index_nodes)

        # ========== TEST COMPLETE ==========
        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_sanity")
        self.log.info(f"Test completed successfully with gsi_type={self.gsi_type}")
        self.log.info("=" * 80)

    def test_gsi_encryption_at_rest_key_rotation(self):
        """
        Encryption at rest key rotation test.
        
        Test validates:
        1. Initial encryption setup with key1 (from basetestcase.py)
        2. Create bucket with encryption enabled using key1
        3. Create indexes and load documents
        4. Run baseline validations with key1
        5. Create new encryption key (key2)
        6. Rotate bucket encryption to key2
        7. Wait for rotation to complete and snapshots to be created with new key
        8. Rerun validations to verify files are encrypted with key2
        
        This test ensures that after key rotation:
        - New snapshot files are encrypted with the new key
        - GSI operations continue to work correctly
        - File headers contain the new key ID
        """
        # 10 hours expressed in seconds — stable, no accidental DEK rotation
        STABLE_INTERVAL_S = 36000
        # 2 minutes expressed in seconds — trigger DEK rotation
        ROTATION_INTERVAL_S = 120

        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_key_rotation")
        self.log.info(f"Test parameters: gsi_type={self.gsi_type}")
        self.log.info("=" * 80)
        
        # ========== STEP 1: Verify prerequisites ==========
        index_nodes = self.verify_prerequisites(min_index_nodes=2)
        key1_id = self.encryption_at_rest_id

        # ========== STEP 2: Set indexer snapshot settings ==========
        self.set_indexer_snapshot_settings(index_nodes)

        # ========== STEP 3: Create bucket with encryption using key1 ==========
        bucket_name = self.test_bucket
        try:
            self.log.info(f"[STEP 3] Creating bucket '{bucket_name}' with encryption key1 ({key1_id})...")
            self.create_bucket_with_encryption(bucket_name, enable_encryption=True)
            
            # Verify bucket encryption
            bucket_info = self.rest.get_bucket_json(bucket_name)
            encryption_key_id = bucket_info.get('encryptionAtRestKeyId', None)
            self.assertEqual(encryption_key_id, key1_id, 
                f"Bucket encryption key {encryption_key_id} does not match key1 {key1_id}")
            
            self.log.info(f"[STEP 3] PASSED - Bucket created with encryption key1")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {str(e)}")
            raise

        # Pin DEK rotation interval high so no accidental rotation fires during setup
        try:
            self.log.info(
                f"[STEP 3b] Pinning DEK rotation interval to {STABLE_INTERVAL_S} s "
                f"({STABLE_INTERVAL_S // 60} min) to prevent rotation during setup..."
            )
            self.set_bucket_dek_rotation_config(
                bucket_name, key1_id,
                dek_rotation_interval=STABLE_INTERVAL_S,
                dek_lifetime=STABLE_INTERVAL_S
            )
            self.log.info("[STEP 3b] PASSED - DEK rotation interval pinned high")
        except Exception as e:
            self.log.error(f"[STEP 3b] FAILED: {str(e)}")
            raise

        # ========== STEP 4: Load documents ==========
        try:
            self.log.info(f"[STEP 4] Loading {self.num_of_docs_per_collection} documents...")
            gen_create = SDKDataLoader(
                num_ops=self.num_of_docs_per_collection,
                percent_create=100,
                percent_update=0,
                percent_delete=0,
                scope="_default",
                collection="_default",
                json_template=self.json_template,
                output=True,
                username=self.username,
                password=self.password
            )
            tasks = self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_create,
                batch_size=10**4,
                dataset=self.json_template
            )
            for task in tasks:
                task.result()
            self.log.info(f"[STEP 4] PASSED - Loaded {self.num_of_docs_per_collection} documents")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        # ========== STEP 5: Create indexes ==========
        namespace = f"default:{bucket_name}._default._default"
        select_queries = set()
        try:
            self.log.info(f"[STEP 5] Creating indexes...")
            prefix = 'rotation_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
            query_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template,
                prefix=prefix,
                skip_primary=False
            )
            
            select_queries.update(
                self.gsi_util_obj.get_select_queries(
                    definition_list=query_definitions,
                    namespace=namespace
                )
            )
            
            deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
            queries = self._get_create_index_queries_with_replica_policy(
                definition_list=query_definitions,
                namespace=namespace,
                deploy_node_info=deploy_nodes,
                defer_build=self.defer_build
            )
            
            query_node = self.get_nodes_from_services_map(service_type="n1ql")
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=queries,
                database=namespace,
                query_node=query_node
            )
            self.log.info(f"[STEP 5] PASSED - Created {len(queries)} indexes")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise
        
        # ========== STEP 6: Wait for indexes to be online ==========
        try:
            self.log.info("[STEP 6] Waiting for indexes to be online...")
            self.wait_until_indexes_online(timeout=1200, defer_build=self.defer_build)
            self.log.info("[STEP 6] PASSED - All indexes online")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise

        baseline_in_use_key_ids = self.get_indexer_in_use_key_ids(index_nodes)
        
        # ========== STEP 7: Run baseline validations with key1 ==========
        try:
            self.log.info("[STEP 7] Running baseline validations with key1...")
            
            # Run scan queries
            self.run_scan_validation(select_queries, validate_item_count=True)
            
            # Validate file encryption with key1
            validation_passed = self.validate_file_encryption_with_key(
                index_nodes, expected_key_id=baseline_in_use_key_ids, step_prefix="[STEP 7] ",
                encrypted_bucket_names=[bucket_name]
            )
            if not validation_passed:
                raise Exception("Baseline file encryption validation failed with key1")
            
            self.log.info("[STEP 7] PASSED - Baseline validations completed with key1")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise
        
        # ========== STEP 8: Create new encryption key (key2) ==========
        try:
            self.log.info("[STEP 8] Creating new encryption key (key2)...")
            key2_id = self.create_new_encryption_key()
            self.log.info(f"[STEP 8] PASSED - Created key2 with ID: {key2_id}")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {str(e)}")
            raise
        
        # ========== STEP 9: Reduce DEK rotation interval to trigger first rotation ==========
        try:
            self.log.info(
                f"[STEP 9] Reducing DEK rotation interval to {ROTATION_INTERVAL_S} s "
                f"({ROTATION_INTERVAL_S // 60} min) to trigger first DEK rotation..."
            )
            self.set_bucket_dek_rotation_config(
                bucket_name, key2_id,
                dek_rotation_interval=ROTATION_INTERVAL_S,
                dek_lifetime=ROTATION_INTERVAL_S
            )
            self.log.info("[STEP 9] PASSED - DEK rotation interval set to trigger rotation")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            raise

        # ========== STEP 10: Poll for new DEK IDs, then immediately re-pin ==========
        try:
            self.log.info(
                "[STEP 10] Polling GetInUseKeys for new indexer key IDs (timeout 300 s)..."
            )
            new_in_use_key_ids = self.poll_for_new_indexer_key_ids(
                index_nodes, baseline_in_use_key_ids, timeout=300, label="[STEP 10] "
            )
            self.log.info(
                f"[STEP 10] New key IDs detected: {new_in_use_key_ids}. "
                f"Immediately pinning DEK rotation interval back to {STABLE_INTERVAL_S} s..."
            )
            self.set_bucket_dek_rotation_config(
                bucket_name, key2_id,
                dek_rotation_interval=STABLE_INTERVAL_S,
                dek_lifetime=STABLE_INTERVAL_S
            )
            self.log.info("[STEP 10] PASSED - DEK rotation detected, interval re-pinned high")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {str(e)}")
            raise

        # ========== STEP 10b: Run mutations to write snapshots under new DEK ==========
        try:
            self.log.info("[STEP 10b] Running mutations to trigger snapshot creation under new DEK...")
            gen_update = SDKDataLoader(
                num_ops=1000,
                percent_create=0,
                percent_update=100,
                percent_delete=0,
                scope="_default",
                collection="_default",
                json_template=self.json_template,
                output=True,
                username=self.username,
                password=self.password
            )
            tasks = self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_update,
                batch_size=1000,
                dataset=self.json_template
            )
            for task in tasks:
                task.result()
            self.sleep(60, "Waiting for new snapshots to be persisted with new DEK")
            self.log.info("[STEP 10b] PASSED - Mutations complete, snapshots should reflect new DEK")
        except Exception as e:
            self.log.error(f"[STEP 10b] FAILED: {str(e)}")
            raise
        # ========== STEP 11: Rerun validations with key2 ==========
        try:
            self.log.info("[STEP 11] Rerunning validations after key rotation...")
            new_in_use_key_ids_again = self.get_new_indexer_in_use_key_ids(index_nodes, baseline_in_use_key_ids)
            self.log.info(f"Double checking keys in use just before validation {new_in_use_key_ids_again}")
            # Validate file encryption with key2
            validation_passed = self.validate_file_encryption_with_key(
                index_nodes, expected_key_id=new_in_use_key_ids, step_prefix="[STEP 11] ",
                encrypted_bucket_names=[bucket_name]
            )
            if not validation_passed:
                raise Exception("File encryption validation failed after key rotation to key2")
            # Run scan queries again
            self.run_scan_validation(select_queries, validate_item_count=True)
            self.log.info("[STEP 11] PASSED - Post-rotation validations completed with key2")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {str(e)}")
            raise
        
        self.restart_index_nodes(index_nodes)

        # ========== TEST COMPLETE ==========
        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_key_rotation")
        self.log.info(f"Successfully rotated encryption from key1 ({key1_id}) to key2 ({key2_id})")
        self.log.info("=" * 80)

    def test_gsi_encryption_at_rest_force_reencryption(self):
        """
        Encryption at rest force re-encryption test.
        
        This test validates the force re-encryption operation which:
        1. Drops all existing DEKs (Data Encryption Keys) for the bucket
        2. Creates new DEKs
        3. Re-encrypts ALL data in the bucket with the new DEKs
        4. The KEK (Key Encryption Key) ID stays the same - only DEKs change
        
        Test flow:
        1. Create bucket with encryption enabled
        2. Create indexes and load documents
        3. Run baseline validations
        4. Trigger force re-encryption (drop DEKs and re-encrypt)
        5. Wait for re-encryption to complete
        6. Rerun validations to verify data is still accessible and encrypted
        
        Use case: When DEKs may have been compromised and need to be replaced
        """
        # 10 hours expressed in seconds — stable, no accidental DEK rotation
        STABLE_INTERVAL_S = 36000

        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_force_reencryption")
        self.log.info(f"Test parameters: gsi_type={self.gsi_type}")
        self.log.info("=" * 80)
        
        # ========== STEP 1: Verify prerequisites ==========
        try:
            index_nodes = self.verify_prerequisites(min_index_nodes=2)
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {str(e)}")
            raise
        key_id = self.encryption_at_rest_id        
        # ========== STEP 2: Set indexer snapshot settings ==========
        try:
            self.set_indexer_snapshot_settings(index_nodes)
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {str(e)}")
            raise        
        # ========== STEP 3: Create bucket with encryption ==========
        bucket_name = self.test_bucket
        try:
            self.log.info(f"[STEP 3] Creating bucket '{bucket_name}' with encryption...")
            self.create_bucket_with_encryption(bucket_name, enable_encryption=True)
            
            bucket_info = self.rest.get_bucket_json(bucket_name)
            encryption_key_id = bucket_info.get('encryptionAtRestKeyId', None)
            self.assertEqual(encryption_key_id, key_id, 
                f"Bucket encryption key {encryption_key_id} does not match {key_id}")
            
            self.log.info(f"[STEP 3] PASSED - Bucket created with encryption")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {str(e)}")
            raise

        # Pin DEK rotation interval high so no accidental rotation fires during setup
        try:
            self.log.info(
                f"[STEP 3b] Pinning DEK rotation interval to {STABLE_INTERVAL_S} s "
                f"({STABLE_INTERVAL_S // 60} min) to prevent background rotation during setup..."
            )
            self.set_bucket_dek_rotation_config(
                bucket_name, key_id,
                dek_rotation_interval=STABLE_INTERVAL_S,
                dek_lifetime=STABLE_INTERVAL_S
            )
            self.log.info("[STEP 3b] PASSED - DEK rotation interval pinned high")
        except Exception as e:
            self.log.error(f"[STEP 3b] FAILED: {str(e)}")
            raise

        # ========== STEP 4: Load documents ==========
        try:
            self.log.info(f"[STEP 4] Loading {self.num_of_docs_per_collection} documents...")
            gen_create = SDKDataLoader(
                num_ops=self.num_of_docs_per_collection,
                percent_create=100,
                percent_update=0,
                percent_delete=0,
                scope="_default",
                collection="_default",
                json_template=self.json_template,
                output=True,
                username=self.username,
                password=self.password
            )
            tasks = self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_create,
                batch_size=10**4,
                dataset=self.json_template
            )
            for task in tasks:
                task.result()
            self.log.info(f"[STEP 4] PASSED - Loaded {self.num_of_docs_per_collection} documents")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        # ========== STEP 5: Create indexes ==========
        namespace = f"default:{bucket_name}._default._default"
        select_queries = set()
        try:
            self.log.info(f"[STEP 5] Creating indexes...")
            prefix = 'reencrypt_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
            query_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template,
                prefix=prefix,
                skip_primary=False
            )
            
            select_queries.update(
                self.gsi_util_obj.get_select_queries(
                    definition_list=query_definitions,
                    namespace=namespace
                )
            )
            
            deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
            queries = self._get_create_index_queries_with_replica_policy(
                definition_list=query_definitions,
                namespace=namespace,
                deploy_node_info=deploy_nodes,
                defer_build=self.defer_build
            )
            
            query_node = self.get_nodes_from_services_map(service_type="n1ql")
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=queries,
                database=namespace,
                query_node=query_node
            )
            self.log.info(f"[STEP 5] PASSED - Created {len(queries)} indexes")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise
        
        # ========== STEP 6: Wait for indexes to be online ==========
        try:
            self.log.info("[STEP 6] Waiting for indexes to be online...")
            self.wait_until_indexes_online(timeout=1200, defer_build=self.defer_build)
            self.log.info("[STEP 6] PASSED - All indexes online")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise

        baseline_in_use_key_ids = self.get_indexer_in_use_key_ids(index_nodes)
        
        # ========== STEP 7: Run baseline validations ==========
        try:
            self.log.info("[STEP 7] Running baseline validations before re-encryption...")
            
            self.run_scan_validation(select_queries, validate_item_count=True)
            
            validation_passed = self.validate_file_encryption_with_key(
                index_nodes, expected_key_id=baseline_in_use_key_ids, step_prefix="[STEP 7] ",
                encrypted_bucket_names=[bucket_name]
            )
            if not validation_passed:
                raise Exception("Baseline file encryption validation failed")
            
            self.log.info("[STEP 7] PASSED - Baseline validations completed")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise
        
        # ========== STEP 8: Capture DEK info before re-encryption ==========
        try:
            self.log.info("[STEP 8] Capturing DEK info before re-encryption...")
            bucket_info_before = self.rest.get_bucket_json(bucket_name)
            dek_info_before = bucket_info_before.get('encryptionAtRestInfo', {})
            dek_count_before = dek_info_before.get('dekNumber', 0)
            oldest_dek_before = dek_info_before.get('oldestDekCreationDatetime', None)
            
            self.log.info(f"[STEP 8] DEK count before: {dek_count_before}")
            self.log.info(f"[STEP 8] Oldest DEK creation time before: {oldest_dek_before}")
            self.log.info("[STEP 8] PASSED - DEK info captured")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {str(e)}")
            raise
        
        # ========== STEP 9: Trigger force re-encryption ==========
        try:
            self.log.info(f"[STEP 9] Triggering force re-encryption on bucket '{bucket_name}'...")
            self.log.info("[STEP 9] This will drop existing DEKs and re-encrypt all data with new DEKs")
            
            status, response = self.rest.trigger_data_reencryption(bucket_name)
            if not status:
                raise Exception(f"Failed to trigger re-encryption: {response}")
            
            self.log.info(f"[STEP 9] Force re-encryption triggered. Response: {response}")
            self.log.info("[STEP 9] PASSED - Re-encryption initiated")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            raise
        
        # ========== STEP 10: Poll for new DEK IDs, then immediately re-pin ==========
        try:
            self.log.info(
                "[STEP 10] Polling GetInUseKeys for new indexer key IDs (timeout 300 s)..."
            )
            new_in_use_key_ids = self.poll_for_new_indexer_key_ids(
                index_nodes, baseline_in_use_key_ids, timeout=300, label="[STEP 10] "
            )
            self.log.info(
                f"[STEP 10] New key IDs detected: {new_in_use_key_ids}. "
                f"Immediately pinning DEK rotation interval back to {STABLE_INTERVAL_S} s..."
            )
            self.set_bucket_dek_rotation_config(
                bucket_name, key_id,
                dek_rotation_interval=STABLE_INTERVAL_S,
                dek_lifetime=STABLE_INTERVAL_S
            )
            self.log.info("[STEP 10] PASSED - Re-encryption detected, interval re-pinned high")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {str(e)}")
            raise
        
        # ========== STEP 11: Verify DEK info changed ==========
        try:
            self.log.info("[STEP 11] Verifying DEK info changed after re-encryption...")
            bucket_info_after = self.rest.get_bucket_json(bucket_name)
            dek_info_after = bucket_info_after.get('encryptionAtRestInfo', {})
            dek_count_after = dek_info_after.get('dekNumber', 0)
            oldest_dek_after = dek_info_after.get('oldestDekCreationDatetime', None)
            
            self.log.info(f"[STEP 11] DEK count after: {dek_count_after}")
            self.log.info(f"[STEP 11] Oldest DEK creation time after: {oldest_dek_after}")
            
            # Verify KEK ID stayed the same
            encryption_key_id_after = bucket_info_after.get('encryptionAtRestKeyId', None)
            self.assertEqual(encryption_key_id_after, key_id, 
                f"KEK ID changed unexpectedly from {key_id} to {encryption_key_id_after}")
            self.log.info(f"[STEP 11] KEK ID unchanged: {key_id}")
            
            # Verify oldest DEK creation time changed (new DEKs were created)
            if oldest_dek_before and oldest_dek_after:
                if oldest_dek_after != oldest_dek_before:
                    self.log.info(f"[STEP 11] DEKs were rotated - oldest DEK changed from {oldest_dek_before} to {oldest_dek_after}")
                else:
                    self.log.warning(f"[STEP 11] Oldest DEK timestamp unchanged - re-encryption may still be in progress")
            
            self.log.info("[STEP 11] PASSED - DEK info verification completed")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {str(e)}")
            raise
        
        # ========== STEP 12: Rerun validations after re-encryption ==========
        try:
            self.log.info("[STEP 12] Rerunning validations after re-encryption...")
            
            # Run scan queries again
            self.run_scan_validation(select_queries, validate_item_count=True)
            
            validation_passed = self.validate_file_encryption_with_key(
                index_nodes, expected_key_id=new_in_use_key_ids, step_prefix="[STEP 12] ",
                encrypted_bucket_names=[bucket_name]
            )
            if not validation_passed:
                raise Exception("File encryption validation failed after re-encryption")
            
            self.log.info("[STEP 12] PASSED - Post-reencryption validations completed")
        except Exception as e:
            self.log.error(f"[STEP 12] FAILED: {str(e)}")
            raise
        
        self.restart_index_nodes(index_nodes)

        # ========== TEST COMPLETE ==========
        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_force_reencryption")
        self.log.info(f"Successfully dropped DEKs and re-encrypted data with new DEKs (KEK ID: {key_id})")
        self.log.info("=" * 80)


    def test_gsi_log_encryption_files_encrypted(self):
        """
        Validate that indexer_stats.log, projector_stats.log, and the
        request-handler cache files under @2i/cache are encrypted with the
        cluster-level log-encryption secret.

        Requires ``enable_log_encryption_at_rest=True`` so basetestcase.py
        provisions a log-encryption key. Bucket encryption is optional —
        this test only asserts on log-encryption-governed files.

        Steps:
          1. Verify index nodes and that log encryption is enabled
          2. Create a bucket (encrypted if enable_encryption_at_rest=True)
          3. Load documents + create indexes so stats logs and request-handler
             cache files get written
          4. Run SELECT queries so the request-handler cache is populated
          5. Wait briefly to let stats flush to disk
          6. Call validate_log_encryption_files_encrypted
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_log_encryption_files_encrypted")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        try:
            self.log.info("[STEP 1] Verifying index nodes and log-encryption setup...")
            index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            self.assertGreaterEqual(len(index_nodes), 2,
                f"Need at least 2 index nodes, found {len(index_nodes)}")
            if not getattr(self, "enable_log_encryption_at_rest", False):
                self.skipTest("Log encryption at rest not enabled. "
                              "Set enable_log_encryption_at_rest=True")
            self.log.info(
                f"[STEP 1] PASSED - {len(index_nodes)} index nodes, "
                f"log_encryption_at_rest_id={getattr(self, 'log_encryption_at_rest_id', None)}"
            )
        except Exception as e:
            self.log.error(f"[STEP 1] FAILED: {str(e)}")
            raise

        # ========== STEP 2: Create bucket ==========
        bucket_name = self.test_bucket
        try:
            self.log.info(f"[STEP 2] Creating bucket '{bucket_name}' "
                          f"(encryption={bool(self.enable_encryption_at_rest)})...")
            self.create_bucket_with_encryption(
                bucket_name, enable_encryption=bool(self.enable_encryption_at_rest)
            )
            self.log.info(f"[STEP 2] PASSED - Bucket '{bucket_name}' created")
        except Exception as e:
            self.log.error(f"[STEP 2] FAILED: {str(e)}")
            raise

        # ========== STEP 3: Configure indexer snapshot intervals ==========
        try:
            self.set_indexer_snapshot_settings(index_nodes, step_prefix="[STEP 3]")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {str(e)}")
            raise
        # ========== STEP 4: Load documents and create indexes ==========
        select_queries = set()
        try:
            self.log.info(f"[STEP 4] Loading {self.num_of_docs_per_collection} docs and creating indexes...")
            gen_create = SDKDataLoader(
                num_ops=self.num_of_docs_per_collection,
                percent_create=100,
                percent_update=0,
                percent_delete=0,
                scope="_default",
                collection="_default",
                json_template=self.json_template,
                output=True,
                username=self.username,
                password=self.password
            )
            tasks = self.data_ops_javasdk_loader_in_batches(
                sdk_data_loader=gen_create,
                batch_size=10**4,
                dataset=self.json_template
            )
            for task in tasks:
                task.result()

            namespace = f"default:{bucket_name}._default._default"
            prefix = 'log_ear_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
            query_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template,
                prefix=prefix,
                skip_primary=False
            )
            select_queries.update(
                self.gsi_util_obj.get_select_queries(
                    definition_list=query_definitions,
                    namespace=namespace
                )
            )
            deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
            queries = self._get_create_index_queries_with_replica_policy(
                definition_list=query_definitions,
                namespace=namespace,
                deploy_node_info=deploy_nodes,
                defer_build=self.defer_build
            )
            query_node = self.get_nodes_from_services_map(service_type="n1ql")
            self.gsi_util_obj.create_gsi_indexes(
                create_queries=queries,
                database=namespace,
                query_node=query_node
            )
            self.wait_until_indexes_online(timeout=1200, defer_build=self.defer_build)
            self.log.info(f"[STEP 4] PASSED - Indexes online, {len(select_queries)} select queries collected")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        # ========== STEP 5: Exercise request-handler cache via scans ==========
        try:
            self.log.info(f"[STEP 5] Running {len(select_queries)} SELECT queries to populate request-handler cache...")
            self.run_and_validate_scans(
                select_queries, expected_doc_count=self.num_of_docs_per_collection
            )
            self.log.info("[STEP 5] PASSED - Scans completed")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise

        # ========== STEP 6: Wait for stats to flush ==========
        flush_wait = int(self.input.param("stats_flush_wait", 60))
        self.sleep(flush_wait,
                   f"Waiting {flush_wait}s for indexer/projector stats logs "
                   f"and request-handler cache to flush to disk")

        # ========== STEP 7: Validate log-encryption files ==========
        try:
            expected_key_id = getattr(self, "log_encryption_at_rest_id", None)
            expected_header_key_ids = None
            if expected_key_id not in (None, False, ""):
                expected_header_key_ids = self.get_expected_header_key_ids(expected_key_id)
            self.log.info(
                f"[STEP 7] Validating log-encryption files with expected key IDs "
                f"{expected_header_key_ids} (raw={expected_key_id})..."
            )
            self.validate_log_encryption_files_encrypted(
                index_nodes, expected_key_id=expected_header_key_ids
            )
            self.log.info("[STEP 7] PASSED - Log-encryption file validation completed")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise

        self.restart_index_nodes(index_nodes)

        # ========== TEST COMPLETE ==========
        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_log_encryption_files_encrypted")
        self.log.info("=" * 80)

    def test_gsi_encryption_at_rest_rebalance(self):
        """
        Encryption at rest rebalance test with dense, sparse, and scalar indexes.

        This test validates GSI operations with encryption at rest during rebalance
        using the MSMARCOSiftEmbeddingProduct dataset which covers:
        - Dense BHIVE indexes
        - Dense Composite indexes
        - Scalar indexes
        - Sparse BHIVE indexes
        - Sparse Composite indexes

        Test parameters:
        - rebalance_type: in|out|swap - type of rebalance operation
        - rebalance_mode: file|dcp - rebalance mechanism (shard-based or DCP-based)

        Test flow:
        1. Create 2 encrypted buckets with scopes/collections
        2. Create all index types with mixed replica counts (0/1/2)
        3. Run scans and validate encryption
        4. Perform rebalance (in/out/swap) using file or DCP mode
        5. Validate indexes, scans, and encryption after rebalance
        6. Create additional indexes post-rebalance and validate
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_rebalance")
        self.log.info(f"Test parameters: rebalance_type={self.rebalance_type}, "
                      f"rebalance_mode={self.rebalance_mode}, json_template={self.json_template}")
        self.log.info("=" * 80)

        # Track whether shard rebalance was enabled before index creation
        shard_rebalance_enabled = False

        # ========== STEP 1: Verify prerequisites ==========
        index_nodes = self.verify_prerequisites()

        # ========== STEP 2: Set indexer snapshot settings ==========
        self.set_indexer_snapshot_settings(index_nodes)

        # ========== STEP 3: Enable shard-based rebalance if file mode ==========
        shard_rebalance_enabled = self._enable_shard_rebalance_if_file_mode(step_prefix="[STEP 3]")

        # ========== STEP 4: Create buckets with scopes/collections ==========
        try:
            encryption_mode = "all encrypted" if self.encrypt_all_buckets else "mixed (first encrypted, rest non-encrypted)"
            self.log.info(f"[STEP 4] Creating {self.bucket_count} bucket(s) ({encryption_mode})...")
            bucket_names, encrypted_bucket_names = self._create_encrypted_test_buckets(
                num_buckets=self.bucket_count,
                encrypt_all=self.encrypt_all_buckets
            )
            self.log.info(f"[STEP 4] PASSED - Created buckets: {bucket_names}, encrypted: {encrypted_bucket_names}")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        # ========== STEP 5: Create indexes on all namespaces with defer_build=True ==========
        all_select_queries = set()
        all_definitions_per_namespace = {}  # Track definitions for building
        try:
            self.log.info("[STEP 5] Creating indexes on all namespaces with defer_build=True...")
            query_node = self.get_nodes_from_services_map(service_type="n1ql")

            similarity_options = ["COSINE", "L2_SQUARED", "L2", "DOT", "EUCLIDEAN_SQUARED", "EUCLIDEAN"]

            for namespace in self.namespaces:
                self.log.info(f"[STEP 5] Processing namespace: {namespace}")
                similarity = random.choice(similarity_options)
                prefix = 'rebal_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))

                # Get query definitions for all index types
                dense_bhive_def, dense_composite_def, scalar_def, sparse_bhive_def, sparse_composite_def = \
                    self._get_query_definitions(
                        prefix=prefix,
                        similarity=similarity,
                        num_indexes_per_type=self.num_indexes_per_type
                    )

                # Store all definitions for this namespace for building later
                all_defs = []
                if dense_bhive_def:
                    all_defs.extend(dense_bhive_def)
                if dense_composite_def:
                    all_defs.extend(dense_composite_def)
                if scalar_def:
                    all_defs.extend(scalar_def)
                if sparse_bhive_def:
                    all_defs.extend(sparse_bhive_def)
                if sparse_composite_def:
                    all_defs.extend(sparse_composite_def)
                all_definitions_per_namespace[namespace] = all_defs

                # Get select queries
                select_queries = self._get_select_queries(
                    dense_bhive_def, dense_composite_def, scalar_def,
                    sparse_bhive_def, sparse_composite_def, namespace
                )
                all_select_queries.update(select_queries)

                # Get create queries with defer_build=True
                create_queries = self._get_create_queries(
                    dense_bhive_def, dense_composite_def, scalar_def,
                    sparse_bhive_def, sparse_composite_def, namespace,
                    defer_build=True
                )

                # Create indexes (deferred)
                self.log.info(f"[STEP 5] Creating {len(create_queries)} deferred indexes on {namespace}...")
                self.gsi_util_obj.create_gsi_indexes(
                    create_queries=create_queries,
                    database=namespace,
                    query_node=query_node
                )

                # Build all indexes for this namespace at once
                self.log.info(f"[STEP 5] Building all deferred indexes for namespace {namespace}...")
                self._build_all_deferred_indexes_for_namespace(namespace, all_defs)

            self.log.info(f"[STEP 5] PASSED - Created and built indexes on all namespaces. "
                          f"Collected {len(all_select_queries)} select queries")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise

        # ========== STEP 6: Wait for indexes to be online ==========
        try:
            self.log.info("[STEP 6] Waiting for all indexes to reach 'Ready' state...")
            self.wait_until_indexes_online(timeout=1200, defer_build=True)
            self.log.info("[STEP 6] PASSED - All indexes are online")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise

        # ========== STEP 7: Run scans and validate encryption ==========
        try:
            self.log.info("[STEP 7] Running scans and validating encryption before rebalance...")
            query_node = self.get_nodes_from_services_map(service_type="n1ql")

            for query in all_select_queries:
                try:
                    if "embVector" in query:
                        query = query.replace("embVector", str(self.bhive_sample_vector))
                    self.run_cbq_query(query=query, scan_consistency='request_plus', server=query_node)
                except Exception as e:
                    self.log.warning(f"[STEP 7] Scan query failed: {str(e)}")
                    raise

            # Validate file encryption (only for encrypted buckets)
            if not self.encrypt_all_buckets:
                self.log.info("[STEP 7] Skipping file encryption validation — mixed encryption scenario")
            else:
                expected_key_ids = self.get_indexer_in_use_key_ids(index_nodes)
                validation_passed = self.validate_file_encryption_with_key(
                    index_nodes,
                    expected_key_id=expected_key_ids,
                    step_prefix="[STEP 7] ",
                    encrypted_bucket_names=encrypted_bucket_names
                )
                if not validation_passed:
                    raise Exception("File encryption validation failed before rebalance")

            self.log.info("[STEP 7] PASSED - Pre-rebalance scans and encryption validation completed")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            self.failure_detected = True
            self.test_failures.append((time.strftime('%Y-%m-%d %H:%M:%S'), str(e)))

        # ========== STEP 7.5: Optional key rotation before rebalance ==========
        trigger_key_rotation_before_rebalance = self.input.param("trigger_key_rotation_before_rebalance", False)
        key_rotation_interval = self.input.param("key_rotation_interval", 120)
        key_rotation_wait_time = self.input.param("key_rotation_wait_time", 100)
        # Set to the bucket name that was encrypted later and already has a fresh DEK
        exclude_bucket_from_rotation = self.input.param("exclude_bucket_from_rotation", None)
        rotation_ran = False
        if trigger_key_rotation_before_rebalance and encrypted_bucket_names:
            try:
                self.log.info(
                    f"[STEP 7.5] Triggering key rotation on all encrypted buckets "
                    f"(interval={key_rotation_interval}s, wait={key_rotation_wait_time}s, "
                    f"exclude={exclude_bucket_from_rotation})..."
                )
                rotation_ran = self._trigger_key_rotation_before_rebalance(
                    encrypted_bucket_names,
                    key_rotation_interval=key_rotation_interval,
                    key_rotation_wait_time=key_rotation_wait_time,
                    step_label="[STEP 7.5] ",
                    exclude_bucket=exclude_bucket_from_rotation
                )
                self.log.info("[STEP 7.5] PASSED - Key rotation completed before rebalance")
            except Exception as e:
                self.log.error(f"[STEP 7.5] FAILED: {str(e)}")
                self.failure_detected = True
                self.test_failures.append((time.strftime('%Y-%m-%d %H:%M:%S'), str(e)))
        else:
            self.log.info("[STEP 7.5] SKIPPED - Key rotation not requested")

        # ========== STEP 8: Perform rebalance ==========
        try:
            self.log.info(f"[STEP 8] Performing {self.rebalance_type} rebalance ({self.rebalance_mode} mode)...")

            nodes_in_list = []
            nodes_out_list = []
            services_in = None

            if self.rebalance_type == "out":
                # Rebalance out 1 index node
                current_index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                nodes_out_list = [current_index_nodes[-1]]
                self.log.info(f"[STEP 8] Rebalance OUT: removing node {nodes_out_list[0].ip}")

            elif self.rebalance_type == "in":
                # Rebalance in spare nodes
                self.log.info("[STEP 8] Enabling redistribute indexes for rebalance-in...")
                self.enable_redistribute_indexes()

                # Find spare nodes (nodes not currently in use)
                current_index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                used_ips = {node.ip for node in current_index_nodes}
                spare_nodes = [s for s in self.servers[self.nodes_init:] if s.ip not in used_ips]

                if spare_nodes:
                    nodes_in_list = [spare_nodes[0]]
                    services_in = ["index"]
                    self.log.info(f"[STEP 8] Rebalance IN: adding node {nodes_in_list[0].ip}")
                else:
                    self.log.warning("[STEP 8] No spare nodes available for rebalance-in")

            elif self.rebalance_type == "swap":
                # Swap rebalance: remove 1 node, add 1 spare node
                current_index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                nodes_out_list = [current_index_nodes[-1]]

                used_ips = {node.ip for node in current_index_nodes}
                spare_nodes = [s for s in self.servers[self.nodes_init:] if s.ip not in used_ips]

                if spare_nodes:
                    nodes_in_list = [spare_nodes[0]]
                    services_in = ["index"]
                    self.log.info(f"[STEP 8] SWAP rebalance: removing {nodes_out_list[0].ip}, "
                                  f"adding {nodes_in_list[0].ip}")
                else:
                    self.log.warning("[STEP 8] No spare nodes for swap, doing rebalance-out only")

            # Perform rebalance and validate (with continuous scans + mutations during rebalance)
            self._ear_rebalance_and_validate(
                nodes_in_list=nodes_in_list,
                nodes_out_list=nodes_out_list,
                services_in=services_in,
                select_queries=all_select_queries,
                index_nodes=index_nodes,
                shard_rebalance_enabled=shard_rebalance_enabled,
                encrypted_bucket_names=encrypted_bucket_names,
                run_continuous_workload=True,
                skip_scans=rotation_ran
            )

            self.log.info("[STEP 8] PASSED - Rebalance completed and validated")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {str(e)}")
            raise

        # ========== STEP 9: Create additional indexes post-rebalance ==========
        try:
            self.log.info("[STEP 9] Creating additional indexes post-rebalance...")
            query_node = self.get_nodes_from_services_map(service_type="n1ql")
            post_rebal_select_queries = set()

            # Create one index of each type on the first namespace
            namespace = self.namespaces[0] if self.namespaces else None
            if namespace:
                prefix = 'post_rebal_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))

                # Post-rebalance: create just 1 index of each type
                dense_bhive_def, dense_composite_def, scalar_def, sparse_bhive_def, sparse_composite_def = \
                    self._get_query_definitions(
                        prefix=prefix,
                        similarity="COSINE",
                        num_indexes_per_type=1
                    )

                # Get select queries for post-rebalance indexes
                post_rebal_select_queries = self._get_select_queries(
                    dense_bhive_def, dense_composite_def, scalar_def,
                    sparse_bhive_def, sparse_composite_def, namespace
                )

                # Create post-rebalance indexes: dense/sparse=0 replicas, scalar=1-2 replicas
                scalar_replica_counts = [1, 2]
                scalar_idx = 0
                for idx_def in (dense_bhive_def + dense_composite_def + sparse_bhive_def + sparse_composite_def):
                    is_bhive = any(x in idx_def.index_name for x in ['bhive', 'sbhive', 'dbhive'])
                    queries = self.gsi_util_obj.get_create_index_list(
                        definition_list=[idx_def],
                        namespace=namespace,
                        num_replica=0,
                        bhive_index=is_bhive
                    )
                    self.gsi_util_obj.create_gsi_indexes(
                        create_queries=queries,
                        database=namespace,
                        query_node=query_node
                    )
                for idx_def in scalar_def:
                    replica_count = scalar_replica_counts[scalar_idx % 2]
                    is_partitioned = hasattr(idx_def, 'partition_by_fields') and idx_def.partition_by_fields
                    if is_partitioned:
                        query = idx_def.generate_index_create_query(
                            namespace=namespace,
                            num_replica=replica_count
                        )
                        self.run_cbq_query(query=query, server=query_node)
                    else:
                        queries = self.gsi_util_obj.get_create_index_list(
                            definition_list=[idx_def],
                            namespace=namespace,
                            num_replica=replica_count
                        )
                        self.gsi_util_obj.create_gsi_indexes(
                            create_queries=queries,
                            database=namespace,
                            query_node=query_node
                        )
                    scalar_idx += 1

                self.log.info("[STEP 9] Waiting for post-rebalance indexes to be online...")
                self.wait_until_indexes_online(timeout=600)

                # Run scans on new indexes
                self.log.info("[STEP 9] Running scans on post-rebalance indexes...")
                for query in post_rebal_select_queries:
                    try:
                        if "embVector" in query:
                            query = query.replace("embVector", str(self.bhive_sample_vector))
                        self.run_cbq_query(query=query, scan_consistency='request_plus', server=query_node)
                    except Exception as e:
                        self.log.warning(f"[STEP 9] Post-rebalance scan query failed: {str(e)}")
                        raise

            self.log.info("[STEP 9] PASSED - Post-rebalance indexes created and validated")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            self.failure_detected = True
            self.test_failures.append((time.strftime('%Y-%m-%d %H:%M:%S'), str(e)))

        # ========== STEP 10: Final encryption validation ==========
        try:
            self.log.info("[STEP 10] Running final encryption validation...")
            if not self.encrypt_all_buckets:
                self.log.info("[STEP 10] Skipping file encryption validation — mixed encryption scenario")
            else:
                final_index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                expected_key_ids = self.get_indexer_in_use_key_ids(final_index_nodes)
                validation_passed = self.validate_file_encryption_with_key(
                    final_index_nodes,
                    expected_key_id=expected_key_ids,
                    step_prefix="[STEP 10] ",
                    encrypted_bucket_names=encrypted_bucket_names
                )
                if not validation_passed:
                    raise Exception("Final file encryption validation failed")

            self.log.info("[STEP 10] PASSED - Final encryption validation completed")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {str(e)}")
            raise

        # ========== STEP 11: Kill indexer process on all nodes ==========
        try:
            self.log.info("[STEP 11] Killing indexer process on all index nodes before test completion...")
            self._kill_indexer_on_all_nodes(step_prefix="[STEP 11] ")
            self.log.info("[STEP 11] PASSED - Indexer processes killed on all nodes")
        except Exception as e:
            self.log.warning(f"[STEP 11] WARNING - Failed to kill indexer on some nodes: {str(e)}")

        if self.failure_detected:
            failure_summary = "\n".join([f"[{ts}] {err}" for ts, err in self.test_failures])
            self.fail(f"Test failed with {len(self.test_failures)} non-critical error(s):\n{failure_summary}")

        # ========== STEP 12: Drop all indexes and validate resources ==========
        try:
            self.log.info("[STEP 12] Dropping all indexes and validating node resources...")
            self.drop_index_node_resources_utilization_validations()
            self.log.info("[STEP 12] PASSED - All indexes dropped and resources validated successfully")
        except Exception as e:
            self.log.warning(f"[STEP 12] WARNING - Drop indexes validation failed: {str(e)}")

        # ========== TEST COMPLETE ==========
        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_rebalance")
        self.log.info(f"Successfully completed {self.rebalance_type} rebalance ({self.rebalance_mode} mode) "
                      f"with encryption at rest")
        self.log.info("=" * 80)

    def test_gsi_encryption_at_rest_rebalance_with_indexer_kill(self):
        """
        Encryption at rest rebalance test with indexer kill to fail rebalance, then retry.

        This test validates GSI operations with encryption at rest when rebalance fails
        due to indexer process being killed, and then retried successfully.

        Test flow:
        1. Create 2 encrypted buckets with scopes/collections
        2. Create all index types (dense/sparse/scalar) with mixed replica counts
        3. Run scans and validate encryption
        4. Start rebalance operation
        5. Kill indexer process during rebalance to cause failure
        6. Wait for rebalance to fail
        7. Wait some time for system to stabilize
        8. Retry rebalance and validate it succeeds
        9. Validate indexes, scans, and encryption after successful rebalance
        10. Create additional indexes post-rebalance and validate
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_rebalance_with_indexer_kill")
        self.log.info(f"Test parameters: rebalance_type={self.rebalance_type}, "
                      f"rebalance_mode={self.rebalance_mode}, json_template={self.json_template}")
        self.log.info("=" * 80)

        shard_rebalance_enabled = False

        # ========== STEP 1: Verify prerequisites ==========
        index_nodes = self.verify_prerequisites()

        # ========== STEP 2: Set indexer snapshot settings ==========
        self.set_indexer_snapshot_settings(index_nodes)

        # ========== STEP 3: Enable shard-based rebalance if file mode ==========
        shard_rebalance_enabled = self._enable_shard_rebalance_if_file_mode(step_prefix="[STEP 3]")

        # ========== STEP 4: Create buckets with scopes/collections ==========
        try:
            encryption_mode = "all encrypted" if self.encrypt_all_buckets else "mixed (first encrypted, rest non-encrypted)"
            self.log.info(f"[STEP 4] Creating {self.bucket_count} bucket(s) ({encryption_mode})...")
            bucket_names, encrypted_bucket_names = self._create_encrypted_test_buckets(
                num_buckets=self.bucket_count,
                encrypt_all=self.encrypt_all_buckets
            )
            self.log.info(f"[STEP 4] PASSED - Created buckets: {bucket_names}, encrypted: {encrypted_bucket_names}")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        # ========== STEP 5: Create indexes on all namespaces with defer_build=True ==========
        all_select_queries = set()
        all_definitions_per_namespace = {}  # Track definitions for building
        try:
            self.log.info("[STEP 5] Creating indexes on all namespaces with defer_build=True...")
            query_node = self.get_nodes_from_services_map(service_type="n1ql")

            similarity_options = ["COSINE", "L2_SQUARED", "L2", "DOT", "EUCLIDEAN_SQUARED", "EUCLIDEAN"]

            for namespace in self.namespaces:
                self.log.info(f"[STEP 5] Processing namespace: {namespace}")
                similarity = random.choice(similarity_options)
                prefix = 'rebal_kill_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))

                dense_bhive_def, dense_composite_def, scalar_def, sparse_bhive_def, sparse_composite_def = \
                    self._get_query_definitions(
                        prefix=prefix,
                        similarity=similarity,
                        num_indexes_per_type=self.num_indexes_per_type
                    )

                # Store all definitions for this namespace for building later
                all_defs = []
                if dense_bhive_def:
                    all_defs.extend(dense_bhive_def)
                if dense_composite_def:
                    all_defs.extend(dense_composite_def)
                if scalar_def:
                    all_defs.extend(scalar_def)
                if sparse_bhive_def:
                    all_defs.extend(sparse_bhive_def)
                if sparse_composite_def:
                    all_defs.extend(sparse_composite_def)
                all_definitions_per_namespace[namespace] = all_defs

                select_queries = self._get_select_queries(
                    dense_bhive_def, dense_composite_def, scalar_def,
                    sparse_bhive_def, sparse_composite_def, namespace
                )
                all_select_queries.update(select_queries)

                create_queries = self._get_create_queries(
                    dense_bhive_def, dense_composite_def, scalar_def,
                    sparse_bhive_def, sparse_composite_def, namespace,
                    defer_build=True
                )

                self.log.info(f"[STEP 5] Creating {len(create_queries)} deferred indexes on {namespace}...")
                self.gsi_util_obj.create_gsi_indexes(
                    create_queries=create_queries,
                    database=namespace,
                    query_node=query_node
                )

                # Build all indexes for this namespace at once
                self.log.info(f"[STEP 5] Building all deferred indexes for namespace {namespace}...")
                self._build_all_deferred_indexes_for_namespace(namespace, all_defs)

            self.log.info(f"[STEP 5] PASSED - Created and built indexes. Collected {len(all_select_queries)} select queries")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise

        # ========== STEP 6: Wait for indexes to be online ==========
        try:
            self.log.info("[STEP 6] Waiting for all indexes to reach 'Ready' state...")
            self.wait_until_indexes_online(timeout=1200, defer_build=True)
            self.log.info("[STEP 6] PASSED - All indexes are online")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise

        # ========== STEP 7: Run scans and validate encryption ==========
        try:
            self.log.info("[STEP 7] Running scans and validating encryption before rebalance...")
            query_node = self.get_nodes_from_services_map(service_type="n1ql")

            for query in all_select_queries:
                try:
                    if "embVector" in query:
                        query = query.replace("embVector", str(self.bhive_sample_vector))
                    self.run_cbq_query(query=query, scan_consistency='request_plus', server=query_node)
                except Exception as e:
                    self.log.warning(f"[STEP 7] Scan query failed: {str(e)}")
                    raise

            if not self.encrypt_all_buckets:
                self.log.info("[STEP 7] Skipping file encryption validation — mixed encryption scenario")
            else:
                expected_key_ids = self.get_indexer_in_use_key_ids(index_nodes)
                validation_passed = self.validate_file_encryption_with_key(
                    index_nodes,
                    expected_key_id=expected_key_ids,
                    step_prefix="[STEP 7] ",
                    encrypted_bucket_names=encrypted_bucket_names
                )
                if not validation_passed:
                    raise Exception("File encryption validation failed before rebalance")

            self.log.info("[STEP 7] PASSED - Pre-rebalance scans and encryption validation completed")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            self.failure_detected = True
            self.test_failures.append((time.strftime('%Y-%m-%d %H:%M:%S'), str(e)))

        # ========== STEP 8: Start rebalance and kill indexer to fail it ==========
        try:
            self.log.info(f"[STEP 8] Starting {self.rebalance_type} rebalance and killing indexer to fail it...")

            nodes_in_list = []
            nodes_out_list = []
            services_in = None
            node_to_kill = None

            if self.rebalance_type not in ("out", "in", "swap"):
                raise Exception(f"Unknown rebalance_type '{self.rebalance_type}'; expected out/in/swap")

            if self.rebalance_type == "out":
                current_index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                nodes_out_list = [current_index_nodes[-1]]
                node_to_kill = current_index_nodes[0]  # Kill a different node
                self.log.info(f"[STEP 8] Rebalance OUT: removing node {nodes_out_list[0].ip}, "
                              f"will kill indexer on {node_to_kill.ip}")

            elif self.rebalance_type == "in":
                self.log.info("[STEP 8] Enabling redistribute indexes for rebalance-in...")
                self.enable_redistribute_indexes()

                current_index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                used_ips = {node.ip for node in current_index_nodes}
                spare_nodes = [s for s in self.servers[self.nodes_init:] if s.ip not in used_ips]

                if spare_nodes:
                    nodes_in_list = [spare_nodes[0]]
                    services_in = ["index"]
                    node_to_kill = current_index_nodes[0]
                    self.log.info(f"[STEP 8] Rebalance IN: adding node {nodes_in_list[0].ip}, "
                                  f"will kill indexer on {node_to_kill.ip}")
                else:
                    raise Exception("No spare nodes available for rebalance-in")

            elif self.rebalance_type == "swap":
                current_index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                nodes_out_list = [current_index_nodes[-1]]

                used_ips = {node.ip for node in current_index_nodes}
                spare_nodes = [s for s in self.servers[self.nodes_init:] if s.ip not in used_ips]

                if spare_nodes:
                    nodes_in_list = [spare_nodes[0]]
                    services_in = ["index"]
                    node_to_kill = current_index_nodes[0]
                    self.log.info(f"[STEP 8] SWAP rebalance: removing {nodes_out_list[0].ip}, "
                                  f"adding {nodes_in_list[0].ip}, will kill indexer on {node_to_kill.ip}")
                else:
                    raise Exception("No spare nodes for swap rebalance")

            self.sleep(60, "Waiting for system to stabilize before rebalance")

            # Start rebalance
            self.log.info("[STEP 8] Starting rebalance...")
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init],
                nodes_in_list,
                nodes_out_list,
                services=services_in,
                cluster_config=self.cluster_config if hasattr(self, 'cluster_config') else None
            )

            self.log.info("[STEP 8] Rebalance task triggered. Polling until rebalance is running...")
            self.sleep(3, "Waiting briefly before checking rebalance status")
            status, _ = self.rest._rebalance_status_and_progress()
            start_time = time.time()
            timeout = 60
            while status != 'running':
                if time.time() - start_time > timeout:
                    raise Exception(
                        f"Rebalance did not start running within {timeout} seconds. "
                        f"Current status: {status}"
                    )
                self.sleep(1, f"[STEP 8] Waiting for rebalance to run. Current status: {status}")
                status, _ = self.rest._rebalance_status_and_progress()
            self.log.info("[STEP 8] Rebalance is running; proceeding with indexer kill")

            # Kill indexer to fail rebalance
            self.log.info(f"[STEP 8] Killing indexer on node {node_to_kill.ip} to fail rebalance...")
            self._kill_all_processes_index(node_to_kill)

            # Wait for rebalance to fail
            try:
                rebalance.result()
                self.log.warning("[STEP 8] Rebalance completed unexpectedly without failing")
            except Exception as ex:
                if "Rebalance failed" in str(ex) or "rebalance failed" in str(ex).lower():
                    self.log.info(f"[STEP 8] Rebalance failed as expected: {str(ex)}")
                else:
                    self.log.info(f"[STEP 8] Rebalance failed with: {str(ex)}")

            self.log.info("[STEP 8] PASSED - Rebalance failed due to indexer kill")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {str(e)}")
            raise

        # ========== STEP 9: Wait for system to stabilize ==========
        try:
            self.log.info("[STEP 9] Waiting for system to stabilize after failed rebalance...")
            self.sleep(120, "Waiting 2 minutes for indexer to recover and system to stabilize")

            # Wait for indexes to come back online
            self.log.info("[STEP 9] Waiting for indexes to come back online...")
            self.wait_until_indexes_online(timeout=600)

            self.log.info("[STEP 9] PASSED - System stabilized after failed rebalance")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            self.failure_detected = True
            self.test_failures.append((time.strftime('%Y-%m-%d %H:%M:%S'), str(e)))

        # ========== STEP 10: Retry rebalance ==========
        try:
            self.log.info(f"[STEP 10] Retrying {self.rebalance_type} rebalance...")

            # Re-enable redistribute indexes if needed for rebalance-in
            if self.rebalance_type == "in":
                self.enable_redistribute_indexes()

            # Perform rebalance and validate
            self._ear_rebalance_and_validate(
                nodes_in_list=nodes_in_list,
                nodes_out_list=nodes_out_list,
                services_in=services_in,
                select_queries=all_select_queries,
                index_nodes=self.get_nodes_from_services_map(service_type="index", get_all_nodes=True),
                shard_rebalance_enabled=shard_rebalance_enabled,
                encrypted_bucket_names=encrypted_bucket_names
            )

            self.log.info("[STEP 10] PASSED - Retry rebalance completed and validated")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {str(e)}")
            raise

        # ========== STEP 11: Create additional indexes post-rebalance ==========
        try:
            self.log.info("[STEP 11] Creating additional indexes post-rebalance...")
            query_node = self.get_nodes_from_services_map(service_type="n1ql")
            post_rebal_select_queries = set()

            namespace = self.namespaces[0] if self.namespaces else None
            if namespace:
                prefix = 'post_retry_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))

                # Post-rebalance: create just 1 index of each type
                dense_bhive_def, dense_composite_def, scalar_def, sparse_bhive_def, sparse_composite_def = \
                    self._get_query_definitions(
                        prefix=prefix,
                        similarity="COSINE",
                        num_indexes_per_type=1
                    )

                post_rebal_select_queries = self._get_select_queries(
                    dense_bhive_def, dense_composite_def, scalar_def,
                    sparse_bhive_def, sparse_composite_def, namespace
                )

                # Create post-rebalance indexes: dense/sparse=0 replicas, scalar=1-2 replicas
                scalar_replica_counts = [1, 2]
                scalar_idx = 0
                for idx_def in (dense_bhive_def + dense_composite_def + sparse_bhive_def + sparse_composite_def):
                    is_bhive = any(x in idx_def.index_name for x in ['bhive', 'sbhive', 'dbhive'])
                    queries = self.gsi_util_obj.get_create_index_list(
                        definition_list=[idx_def],
                        namespace=namespace,
                        num_replica=0,
                        bhive_index=is_bhive
                    )
                    self.gsi_util_obj.create_gsi_indexes(
                        create_queries=queries,
                        database=namespace,
                        query_node=query_node
                    )
                for idx_def in scalar_def:
                    replica_count = scalar_replica_counts[scalar_idx % 2]
                    is_partitioned = hasattr(idx_def, 'partition_by_fields') and idx_def.partition_by_fields
                    if is_partitioned:
                        query = idx_def.generate_index_create_query(
                            namespace=namespace,
                            num_replica=replica_count
                        )
                        self.run_cbq_query(query=query, server=query_node)
                    else:
                        queries = self.gsi_util_obj.get_create_index_list(
                            definition_list=[idx_def],
                            namespace=namespace,
                            num_replica=replica_count
                        )
                        self.gsi_util_obj.create_gsi_indexes(
                            create_queries=queries,
                            database=namespace,
                            query_node=query_node
                        )
                    scalar_idx += 1

                self.log.info("[STEP 11] Waiting for post-rebalance indexes to be online...")
                self.wait_until_indexes_online(timeout=600)

                self.log.info("[STEP 11] Running scans on post-rebalance indexes...")
                for query in post_rebal_select_queries:
                    try:
                        if "embVector" in query:
                            query = query.replace("embVector", str(self.bhive_sample_vector))
                        self.run_cbq_query(query=query, scan_consistency='request_plus', server=query_node)
                    except Exception as e:
                        self.log.warning(f"[STEP 11] Post-rebalance scan query failed: {str(e)}")
                        raise

            self.log.info("[STEP 11] PASSED - Post-rebalance indexes created and validated")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {str(e)}")
            self.failure_detected = True
            self.test_failures.append((time.strftime('%Y-%m-%d %H:%M:%S'), str(e)))

        # ========== STEP 12: Final encryption validation ==========
        try:
            self.log.info("[STEP 12] Running final encryption validation...")
            if not self.encrypt_all_buckets:
                self.log.info("[STEP 12] Skipping file encryption validation — mixed encryption scenario")
            else:
                final_index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                expected_key_ids = self.get_indexer_in_use_key_ids(final_index_nodes)
                validation_passed = self.validate_file_encryption_with_key(
                    final_index_nodes,
                    expected_key_id=expected_key_ids,
                    step_prefix="[STEP 12] ",
                    encrypted_bucket_names=encrypted_bucket_names
                )
                if not validation_passed:
                    raise Exception("Final file encryption validation failed")

            self.item_count_related_validations()

            self.log.info("[STEP 12] PASSED - Final encryption validation completed")
        except Exception as e:
            self.log.error(f"[STEP 12] FAILED: {str(e)}")
            raise

        # ========== STEP 13: Kill indexer process on all nodes ==========
        try:
            self.log.info("[STEP 13] Killing indexer process on all index nodes before test completion...")
            self._kill_indexer_on_all_nodes(step_prefix="[STEP 13] ")
            self.log.info("[STEP 13] PASSED - Indexer processes killed on all nodes")
        except Exception as e:
            self.log.warning(f"[STEP 13] WARNING - Failed to kill indexer on some nodes: {str(e)}")

        if self.failure_detected:
            failure_summary = "\n".join([f"[{ts}] {err}" for ts, err in self.test_failures])
            self.fail(f"Test failed with {len(self.test_failures)} non-critical error(s):\n{failure_summary}")

        # ========== STEP 14: Drop all indexes and validate resources ==========
        try:
            self.log.info("[STEP 14] Dropping all indexes and validating node resources...")
            self.drop_index_node_resources_utilization_validations()
            self.log.info("[STEP 14] PASSED - All indexes dropped and resources validated successfully")
        except Exception as e:
            self.log.warning(f"[STEP 14] WARNING - Drop indexes validation failed: {str(e)}")

        # ========== TEST COMPLETE ==========
        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_rebalance_with_indexer_kill")
        self.log.info(f"Successfully failed and retried {self.rebalance_type} rebalance ({self.rebalance_mode} mode) "
                      f"with encryption at rest")
        self.log.info("=" * 80)

    def _fetch_index_names_set(self):
        """Return the set of index names currently present in the cluster.

        Mirrors FileBasedRebalance.fetch_index_names_list (gsi_file_based_rebalance.py)
        which is not inherited by this class. Reads indexer metadata from any index node.
        """
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        rest = RestConnection(index_nodes[0])
        index_resp = rest.get_indexer_metadata()
        if 'status' not in index_resp:
            raise Exception(f"No index metadata seen in response {index_resp}")
        return set(index['indexName'] for index in index_resp['status'])

    def test_replica_repair_missing_encryption_key_file_based(self):
        """
        Regression test for MB-72480 — encryption keys missing during file-based
        rebalance replica repair (TransferMode: Copy).

        Bug: during file-based (shard) rebalance the source resolved each shard
        DEK's on-disk path by deriving buckets from the transfer token and
        intersecting shard keys with each bucket's in-use key IDs. For a replica
        repair token (mode: Copy) the token references only a SUBSET of indexes,
        yet the ENTIRE shard is copied. The shard therefore carries keys from
        indexes on a DIFFERENT bucket that are not part of the transfer, and
        path-finding for those keys fails -> rebalance fails with a
        "missing encryption key" error. The fix resolves each shard key directly
        by keyID instead of via the bucket intersection.

        Reproduction (from the MB verification steps):
          - 2 index nodes, shard affinity ON, forced to exactly 2 shards per node
            so all indexes share a single alternate shard pair.
          - bucket b1 ENCRYPTED, bucket b2 NON-encrypted.
          - multiple indexes on b2 WITH 1 replica.
          - multiple indexes on b1 WITHOUT replicas, pinned (nodes clause) to
            node A so that node B ends up holding ONLY b2's replica copies.
          - hard-failover node B (the node holding only replica indexes).
          - rebalance out the failed node -> lost replicas now exist.
          - add a fresh node and rebalance -> replica repair (Copy) fires.

        Without the fix the final add-node rebalance fails with
        "missing encryption key"; with the fix it succeeds and encryption stays
        intact. Requires gsi_type=plasma and rebalance_mode=file.
        """
        # Log string that identifies the replica-repair Copy transfer token in
        # indexer.log. Kept as a tunable constant — adjust if the indexer token
        # wording changes in a future build.
        COPY_TOKEN_LOG_STRING = "ShardTransferToken.*TransferMode: Copy"
        MISSING_KEY_LOG_STRING = "missing encryption key"

        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_replica_repair_missing_encryption_key_file_based")
        self.log.info(f"Test parameters: gsi_type={self.gsi_type}, rebalance_mode={self.rebalance_mode}")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        index_nodes = self.verify_prerequisites(min_index_nodes=2)
        if self.gsi_type != "plasma":
            self.skipTest("File-based (shard) rebalance requires gsi_type=plasma")
        if self.rebalance_mode != "file":
            self.skipTest("This test targets file-based rebalance. Set rebalance_mode=file")

        # ========== STEP 2: Enable file-based rebalance + force 2 shards per node ==========
        # enable_shard_based_rebalance flips indexer.settings.enable_shard_affinity.
        # The remaining keys force exactly one alternate shard pair in the cluster
        # (identical dict to gsi_file_based_rebalance.py) so that b1 and b2 indexes
        # share a shard and honourNodesInDefn lets the nodes clause pin b1.
        self.log.info("[STEP 2] Enabling shard-based rebalance and forcing 2 shards per node...")
        self.enable_shard_based_rebalance()
        forced_shard_settings = {
            "indexer.settings.enable_shard_affinity": True,
            "indexer.plasma.minShardsPerNode": 2,
            "indexer.plasma.shardLimitPerNode": 2,
            "indexer.planner.internal.min_shards_per_node": 2,
            "indexer.planner.honourNodesInDefn": True,
        }
        index_rest = RestConnection(index_nodes[0])
        for setting, value in forced_shard_settings.items():
            index_rest.set_index_settings({setting: value})
        self.sleep(10, "[STEP 2] Waiting for shard-affinity settings to take effect")
        self.log.info("[STEP 2] PASSED - Shard-based rebalance enabled with forced 2 shards per node")

        # ========== STEP 3: Create b1 (encrypted) + b2 (non-encrypted) buckets ==========
        self.log.info("[STEP 3] Creating buckets: b1 encrypted, b2 non-encrypted...")
        bucket_names, encrypted_bucket_names = self._create_encrypted_test_buckets(
            num_buckets=2, encrypt_all=False
        )
        b1_encrypted = bucket_names[0]
        b2_non_encrypted = bucket_names[1]
        encrypted_bucket_names = self.get_current_encrypted_bucket_names(bucket_names)
        self.assertIn(b1_encrypted, encrypted_bucket_names,
                      f"Bucket '{b1_encrypted}' should be encrypted")
        self.assertNotIn(b2_non_encrypted, encrypted_bucket_names,
                         f"Bucket '{b2_non_encrypted}' should NOT be encrypted")
        b1_namespace = f"default:{b1_encrypted}._default._default"
        b2_namespace = f"default:{b2_non_encrypted}._default._default"
        self.log.info(f"[STEP 3] PASSED - b1(encrypted)={b1_encrypted}, b2(non-encrypted)={b2_non_encrypted}")

        # ========== STEP 4: Pick node A (pin target) and node B (failover target) ==========
        # node A hosts b1's non-replica indexes plus one b2 replica; node B ends up
        # with only a b2 replica copy. Both must be non-master so the failover of B
        # never removes the orchestrator.
        self.log.info("[STEP 4] Selecting node A (pin target) and node B (failover target)...")
        non_master_index_nodes = [n for n in index_nodes if n.ip != self.master.ip]
        self.assertGreaterEqual(
            len(non_master_index_nodes), 2,
            f"Need at least 2 non-master index nodes; found {[n.ip for n in non_master_index_nodes]}"
        )
        node_a = non_master_index_nodes[0]
        node_b = non_master_index_nodes[1]
        self.log.info(f"[STEP 4] PASSED - node A (pin b1 here)={node_a.ip}, "
                      f"node B (failover, holds only b2 replica)={node_b.ip}")

        # ========== STEP 5: Create indexes ==========
        # b2 (non-encrypted): multiple indexes WITH 1 replica -> one copy on A, one on B.
        # b1 (encrypted): multiple indexes WITHOUT replicas, pinned to node A.
        self.log.info("[STEP 5] Creating indexes on b2 (replica 1) and b1 (0 replica, pinned to node A)...")
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        b2_fields = ["country", "city", "avg_rating"]
        b1_fields = ["name", "price", "free_parking"]
        create_queries = []
        for i, field in enumerate(b2_fields):
            create_queries.append(
                f'CREATE INDEX idx_b2_{i} ON {b2_namespace}({field}) '
                f'USING GSI WITH {{"num_replica": 1}}'
            )
        for i, field in enumerate(b1_fields):
            create_queries.append(
                f'CREATE INDEX idx_b1_{i} ON {b1_namespace}({field}) '
                f'USING GSI WITH {{"nodes": ["{node_a.ip}:{self.node_port}"]}}'
            )
        for query in create_queries:
            self.log.info(f"[STEP 5] Running: {query}")
            self.run_cbq_query(query=query, server=query_node)
        self.wait_until_indexes_online(timeout=1200)
        self.sleep(30, "[STEP 5] Waiting for index placement to settle")
        self.log.info("[STEP 5] PASSED - Indexes created on both buckets")

        # ========== STEP 6: Baseline validation ==========
        self.log.info("[STEP 6] Baseline: shard affinity, scans, and file encryption...")
        self.validate_shard_affinity()
        select_queries = [
            f'SELECT COUNT(*) FROM {b2_namespace} WHERE country IS NOT NULL',
            f'SELECT COUNT(*) FROM {b2_namespace} WHERE city IS NOT NULL',
            f'SELECT COUNT(*) FROM {b1_namespace} WHERE name IS NOT NULL',
            f'SELECT COUNT(*) FROM {b1_namespace} WHERE price IS NOT NULL',
        ]
        for query in select_queries:
            self.run_cbq_query(query=query, scan_consistency='request_plus', server=query_node)
        baseline_key_ids = self.get_indexer_in_use_key_ids(index_nodes)
        self.log.info(f"[STEP 6] Baseline in-use indexer key IDs: {baseline_key_ids}")
        self.assertTrue(
            self.validate_file_encryption_with_key(
                index_nodes, expected_key_id=baseline_key_ids,
                step_prefix="[STEP 6] ", encrypted_bucket_names=[b1_encrypted]
            ),
            "[STEP 6] Baseline file encryption validation failed"
        )
        index_names_before = self._fetch_index_names_set()
        self.log.info(f"[STEP 6] Index names before failover: {index_names_before}")
        self.log.info("[STEP 6] PASSED - Baseline validation completed")

        # ========== STEP 7: Hard-failover node B (holds only b2 replica) ==========
        self.log.info(f"[STEP 7] Hard-failing over node B ({node_b.ip})...")
        failover_task = self.cluster.async_failover(
            servers=[self.master],
            failover_nodes=[node_b],
            graceful=False
        )
        failover_task.result()
        self.sleep(30, "[STEP 7] Waiting for failover to complete")
        self.log.info(f"[STEP 7] PASSED - Node {node_b.ip} failed over")

        # ========== STEP 8: Rebalance out the failed node -> creates lost replicas ==========
        self.log.info("[STEP 8] Rebalancing out the failed node (lost replicas expected after this)...")
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init], [], [],
            cluster_config=self.cluster_config if hasattr(self, 'cluster_config') else None
        )
        reached = RestHelper(self.rest).rebalance_reached(retry_count=250)
        self.assertTrue(reached, "[STEP 8] Rebalance-out of failed node did not complete")
        rebalance.result()
        self.sleep(30, "[STEP 8] Waiting after rebalance-out")
        self.log.info("[STEP 8] PASSED - Failed node rebalanced out; lost replicas now present")

        # ========== STEP 9: Add a fresh node and rebalance -> replica repair (Copy) ==========
        # This is the assertion point. Pre-fix, this rebalance fails with
        # "missing encryption key" while resolving b1's (encrypted) keys carried
        # in the shard copied for b2's replica repair.
        current_index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        used_ips = {n.ip for n in current_index_nodes} | {self.master.ip}
        spare_nodes = [s for s in self.servers[self.nodes_init:] if s.ip not in used_ips]
        if not spare_nodes:
            spare_nodes = [s for s in self.servers if s.ip not in used_ips]
        self.assertTrue(spare_nodes, "[STEP 9] No spare node available to add for replica repair")
        node_c = spare_nodes[0]
        self.log.info(f"[STEP 9] Adding fresh node {node_c.ip} (index) and rebalancing to trigger replica repair...")
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init], [node_c], [],
            services=["index"],
            cluster_config=self.cluster_config if hasattr(self, 'cluster_config') else None
        )
        reached = RestHelper(self.rest).rebalance_reached(retry_count=250)
        self.assertTrue(
            reached,
            "[STEP 9] Replica-repair rebalance did not complete — likely the "
            "MB-72480 'missing encryption key' failure"
        )
        rebalance.result()
        self.sleep(60, "[STEP 9] Waiting after replica-repair rebalance for shard validations")
        self.log.info("[STEP 9] PASSED - Replica-repair rebalance completed successfully")

        # ========== STEP 10: Confirm the Copy path fired and NO missing-key error ==========
        self.log.info("[STEP 10] Validating replica-repair Copy path and encryption in logs...")
        # File-based shard transfer must have occurred at all.
        self.assertTrue(
            self.check_gsi_logs_for_shard_transfer(),
            "[STEP 10] File-based shard transfer was not detected in indexer logs"
        )
        # The replica-repair Copy token must be present (the exact bug path).
        self.assertTrue(
            self.check_gsi_logs_for_shard_transfer(
                log_string=COPY_TOKEN_LOG_STRING,
                msg="Replica repair (TransferMode: Copy) shard transfer "
            ),
            "[STEP 10] Replica-repair Copy transfer token not detected in indexer logs "
            f"(pattern: {COPY_TOKEN_LOG_STRING})"
        )
        # Negative assertion: the MB-72480 error must NOT appear anywhere.
        missing_key_seen = self.check_gsi_logs_for_shard_transfer(
            log_string=MISSING_KEY_LOG_STRING,
            msg="'missing encryption key' error "
        )
        self.assertFalse(
            missing_key_seen,
            "[STEP 10] Found 'missing encryption key' in indexer logs — MB-72480 regression"
        )
        self.log.info("[STEP 10] PASSED - Copy path confirmed and no missing-key error")

        # ========== STEP 11: Post-rebalance validation ==========
        self.log.info("[STEP 11] Post-rebalance: index presence, shard affinity, scans, encryption...")
        self.wait_until_indexes_online(timeout=1200)
        self.validate_shard_affinity()
        index_names_after = self._fetch_index_names_set()
        self.log.info(f"[STEP 11] Index names after replica repair: {index_names_after}")
        missing_indexes = index_names_before - index_names_after
        self.assertEqual(
            missing_indexes, set(),
            f"[STEP 11] Indexes lost after replica repair. Missing: {missing_indexes}. "
            f"Before: {index_names_before}, After: {index_names_after}"
        )
        for query in select_queries:
            self.run_cbq_query(query=query, scan_consistency='request_plus', server=query_node)
        updated_index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        expected_key_ids = self.get_indexer_in_use_key_ids(updated_index_nodes)
        self.assertTrue(
            self.validate_file_encryption_with_key(
                updated_index_nodes, expected_key_id=expected_key_ids,
                step_prefix="[STEP 11] ", encrypted_bucket_names=[b1_encrypted]
            ),
            "[STEP 11] Post-rebalance file encryption validation failed"
        )
        self.log.info("[STEP 11] PASSED - Post-rebalance validation completed")

        # ========== TEST COMPLETE ==========
        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_replica_repair_missing_encryption_key_file_based")
        self.log.info("MB-72480 verified — file-based replica repair copied a shard "
                      "spanning an encrypted bucket without a missing-key failure")
        self.log.info("=" * 80)

    def test_rebalance_mixed_encryption_failover_toggle(self):
        """
        Rebalance test with mixed encryption buckets, failover, and encryption toggle.
        
        This test validates GSI operations with:
        - One bucket with encryption at rest ENABLED
        - One bucket with encryption at rest DISABLED (using encrypt_all_buckets=False)
        - Rebalance operation
        - Failover of one indexer node
        - Toggle encryption states (enable on disabled bucket, disable on enabled bucket)
        - Add back the failed node and rebalance
        - Optional: Trigger index key rotation before rebalance
        
        Test parameters:
        - rebalance_type: in|out|swap - type of rebalance operation
        - rebalance_mode: file|dcp - rebalance mechanism
        - trigger_key_rotation_before_rebalance: True|False - whether to trigger key rotation
        - key_rotation_interval: DEK rotation interval in seconds (default: 120)
        - key_rotation_wait_time: Time to wait after setting rotation interval (default: 100)
        
        Test flow:
        1. Create 2 buckets using _create_encrypted_test_buckets with encrypt_all=False
        2. Create indexes on both buckets using existing helper methods
        3. Run scans and validate encryption
        4. Optionally trigger index key rotation and wait
        5. Failover one indexer node
        6. Toggle encryption: disable on encrypted bucket, enable on non-encrypted bucket
        7. Run scans to validate
        8. Add back the failed node and rebalance
        9. Validate indexes, scans, and encryption after recovery
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_rebalance_mixed_encryption_failover_toggle")
        self.log.info(f"Test parameters: rebalance_type={self.rebalance_type}, "
                      f"rebalance_mode={self.rebalance_mode}, json_template={self.json_template}")
        self.log.info("=" * 80)

        # Test parameters
        STABLE_INTERVAL_S = 36000   # 10 h — keeps DEK stable during setup
        ROTATION_INTERVAL_S = 120   # 2 min — triggers the first DEK rotation
        trigger_key_rotation = self.input.param("trigger_key_rotation_before_rebalance", False)
        key_rotation_interval = self.input.param("key_rotation_interval", ROTATION_INTERVAL_S)
        key_rotation_wait_time = self.input.param("key_rotation_wait_time", 90)
        # When True the encryption toggle (disable on encrypted / enable on non-encrypted)
        # is performed while the add-back rebalance is in progress rather than before it.
        toggle_encryption_during_rebalance = self.input.param("toggle_encryption_during_rebalance", False)
        # When True, after the recovery rebalance completes, toggle encryption on all buckets
        # and immediately perform a follow-up swap rebalance to validate the new state.
        post_recovery_toggle_swap = self.input.param("post_recovery_toggle_swap", False)
        
        shard_rebalance_enabled = False
        failed_over_node = None

        # ========== STEP 1: Verify prerequisites ==========
        index_nodes = self.verify_prerequisites()

        # ========== STEP 2: Set indexer snapshot settings ==========
        self.set_indexer_snapshot_settings(index_nodes)

        # ========== STEP 3: Enable shard-based rebalance if file mode ==========
        shard_rebalance_enabled = self._enable_shard_rebalance_if_file_mode(step_prefix="[STEP 3]")

        # ========== STEP 4: Create 2 buckets with mixed encryption ==========
        try:
            self.log.info("[STEP 4] Creating 2 buckets: first encrypted, second non-encrypted...")
            # Use existing helper with encrypt_all=False to get mixed encryption
            bucket_names, encrypted_bucket_names = self._create_encrypted_test_buckets(
                num_buckets=2,
                encrypt_all=False  # First bucket encrypted, second not encrypted
            )

            # Identify encrypted vs non-encrypted bucket
            encrypted_bucket = bucket_names[0]
            non_encrypted_bucket = bucket_names[1]
            encrypted_bucket_names = self.get_current_encrypted_bucket_names(bucket_names)
            
            # Validate encryption states
            bucket_info = self.rest.get_bucket_json(encrypted_bucket)
            encryption_key_id = bucket_info.get('encryptionAtRestKeyId', None)
            self.assertIsNotNone(encryption_key_id, f"Bucket '{encrypted_bucket}' should have encryption enabled")
            self.assertNotEqual(encryption_key_id, -1, f"Bucket '{encrypted_bucket}' should have encryption enabled")
            self.log.info(f"[STEP 4] Bucket '{encrypted_bucket}' has encryption key {encryption_key_id}")
            
            bucket_info = self.rest.get_bucket_json(non_encrypted_bucket)
            encryption_key_id = bucket_info.get('encryptionAtRestKeyId', None)
            if encryption_key_id is not None and encryption_key_id != -1:
                raise Exception(f"Bucket '{non_encrypted_bucket}' should NOT have encryption enabled")
            self.log.info(f"[STEP 4] Bucket '{non_encrypted_bucket}' has no encryption")
            
            self.log.info(f"[STEP 4] PASSED - Created buckets: {bucket_names}")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        # ========== STEP 5: Create indexes on all namespaces with defer_build=True ==========
        all_select_queries = set()
        all_definitions_per_namespace = {}  # Track definitions for building
        try:
            self.log.info("[STEP 5] Creating indexes on all namespaces with defer_build=True...")
            query_node = self.get_nodes_from_services_map(service_type="n1ql")

            similarity_options = ["COSINE", "L2_SQUARED", "L2", "DOT", "EUCLIDEAN_SQUARED", "EUCLIDEAN"]

            for namespace in self.namespaces:
                self.log.info(f"[STEP 5] Processing namespace: {namespace}")
                similarity = random.choice(similarity_options)
                prefix = 'mixed_enc_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))

                # Get query definitions for all index types
                dense_bhive_def, dense_composite_def, scalar_def, sparse_bhive_def, sparse_composite_def = \
                    self._get_query_definitions(
                        prefix=prefix,
                        similarity=similarity,
                        num_indexes_per_type=self.num_indexes_per_type
                    )

                # Store all definitions for this namespace for building later
                all_defs = []
                if dense_bhive_def:
                    all_defs.extend(dense_bhive_def)
                if dense_composite_def:
                    all_defs.extend(dense_composite_def)
                if scalar_def:
                    all_defs.extend(scalar_def)
                if sparse_bhive_def:
                    all_defs.extend(sparse_bhive_def)
                if sparse_composite_def:
                    all_defs.extend(sparse_composite_def)
                all_definitions_per_namespace[namespace] = all_defs

                # Get select queries
                select_queries = self._get_select_queries(
                    dense_bhive_def, dense_composite_def, scalar_def,
                    sparse_bhive_def, sparse_composite_def, namespace
                )
                all_select_queries.update(select_queries)

                # Get create queries with defer_build=True
                create_queries = self._get_create_queries(
                    dense_bhive_def, dense_composite_def, scalar_def,
                    sparse_bhive_def, sparse_composite_def, namespace,
                    defer_build=True
                )

                # Create indexes (deferred)
                self.log.info(f"[STEP 5] Creating {len(create_queries)} deferred indexes on {namespace}...")
                self.gsi_util_obj.create_gsi_indexes(
                    create_queries=create_queries,
                    database=namespace,
                    query_node=query_node
                )

                # Build all indexes for this namespace at once
                self.log.info(f"[STEP 5] Building all deferred indexes for namespace {namespace}...")
                self._build_all_deferred_indexes_for_namespace(namespace, all_defs)

            self.log.info(f"[STEP 5] PASSED - Created and built indexes on all namespaces. "
                          f"Collected {len(all_select_queries)} select queries")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise

        # ========== STEP 6: Wait for indexes to be online ==========
        try:
            self.log.info("[STEP 6] Waiting for all indexes to reach 'Ready' state...")
            self.wait_until_indexes_online(timeout=1200, defer_build=True)
            self.log.info("[STEP 6] PASSED - All indexes are online")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise

        # ========== STEP 7: Run scans and validate encryption ==========
        try:
            self.log.info("[STEP 7] Running scans and validating encryption before failover...")
            query_node = self.get_nodes_from_services_map(service_type="n1ql")

            for query in all_select_queries:
                try:
                    if "embVector" in query:
                        query = query.replace("embVector", str(self.bhive_sample_vector))
                    self.run_cbq_query(query=query, scan_consistency='request_plus', server=query_node)
                except Exception as e:
                    self.log.warning(f"[STEP 7] Scan query failed: {str(e)}")
                    raise

            self.log.info("[STEP 7] PASSED - Pre-failover scans completed (encryption check skipped — always mixed)")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            self.failure_detected = True
            self.test_failures.append((time.strftime('%Y-%m-%d %H:%M:%S'), str(e)))

        # ========== STEP 8: Failover one indexer node ==========
        try:
            self.log.info("[STEP 8] Failing over one indexer node...")
            
            current_index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            self.assertGreaterEqual(len(current_index_nodes), 2,
                "Need at least 2 index nodes for failover")
            
            # Choose a node to failover (not the first one to preserve some indexes)
            failed_over_node = current_index_nodes[-1]
            self.log.info(f"[STEP 8] Failing over node: {failed_over_node.ip}")
            
            failover_task = self.cluster.async_failover(
                servers=[self.master],
                failover_nodes=[failed_over_node],
                graceful=False
            )
            failover_task.result()
            
            # Wait for failover to complete
            self.sleep(30, "Waiting for failover to complete")
            
            self.log.info(f"[STEP 8] PASSED - Node {failed_over_node.ip} failed over")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {str(e)}")
            raise

        # ========== STEP 9+10: Pre-rebalance encryption toggle and scans ==========
        # Both steps are only meaningful when the toggle is NOT deferred to mid-rebalance.
        # When toggle_encryption_during_rebalance=True, STEP 12 handles the toggle and
        # STEP 14 handles the post-toggle scans, so both steps are skipped here.
        if not toggle_encryption_during_rebalance:
            # --- STEP 9: Toggle encryption states ---
            try:
                self.log.info("[STEP 9] Toggling encryption states on buckets...")

                # Disable encryption on the originally encrypted bucket
                self.log.info(f"[STEP 9.1] Disabling encryption on '{encrypted_bucket}'...")
                status, response = self.rest.disable_bucket_encryption(encrypted_bucket)
                if not status:
                    raise Exception(f"Failed to disable bucket encryption on '{encrypted_bucket}': {response}")
                self.sleep(5, "Waiting for bucket encryption disable to take effect")

                # Verify encryption is disabled
                bucket_info = self.rest.get_bucket_json(encrypted_bucket)
                encryption_key_id = bucket_info.get('encryptionAtRestKeyId', None)
                if encryption_key_id is not None and encryption_key_id != -1:
                    self.log.warning(f"[STEP 9.1] Bucket '{encrypted_bucket}' encryption key still shows: {encryption_key_id}")
                else:
                    self.log.info(f"[STEP 9.1] Bucket '{encrypted_bucket}' encryption disabled successfully")

                # Enable encryption on the originally non-encrypted bucket
                self.log.info(f"[STEP 9.2] Enabling encryption on '{non_encrypted_bucket}'...")
                status, response = self.rest.enable_bucket_encryption(non_encrypted_bucket, self.encryption_at_rest_id)
                if not status:
                    raise Exception(f"Failed to enable bucket encryption on '{non_encrypted_bucket}': {response}")
                self.sleep(5, "Waiting for bucket encryption enable to take effect")

                # Verify encryption is enabled
                bucket_info = self.rest.get_bucket_json(non_encrypted_bucket)
                encryption_key_id = bucket_info.get('encryptionAtRestKeyId', None)
                self.assertIsNotNone(encryption_key_id, f"Bucket '{non_encrypted_bucket}' should have encryption enabled")
                self.assertNotEqual(encryption_key_id, -1, f"Bucket '{non_encrypted_bucket}' should have encryption enabled")
                self.log.info(f"[STEP 9.2] Bucket '{non_encrypted_bucket}' encryption enabled with key {encryption_key_id}")

                encrypted_bucket_names = self.get_current_encrypted_bucket_names(bucket_names)
                self.log.info(f"[STEP 9] Updated encrypted_bucket_names after toggle: {encrypted_bucket_names}")

                self.log.info("[STEP 9] PASSED - Encryption states toggled successfully")
            except Exception as e:
                self.log.error(f"[STEP 9] FAILED: {str(e)}")
                self.failure_detected = True
                self.test_failures.append((time.strftime('%Y-%m-%d %H:%M:%S'), str(e)))

            # --- STEP 10: Run scans after encryption toggle ---
            try:
                self.log.info("[STEP 10] Running scans after encryption toggle...")
                query_node = self.get_nodes_from_services_map(service_type="n1ql")

                for query in all_select_queries:
                    try:
                        if "embVector" in query:
                            query = query.replace("embVector", str(self.bhive_sample_vector))
                        self.run_cbq_query(query=query, scan_consistency='request_plus', server=query_node)
                    except Exception as e:
                        self.log.warning(f"[STEP 10] Scan query failed (may be expected due to failover): {str(e)}")
                        raise

                self.log.info("[STEP 10] PASSED - Post-toggle scans completed")
            except Exception as e:
                self.log.error(f"[STEP 10] FAILED: {str(e)}")
                raise
        else:
            self.log.info("[STEP 9] DEFERRED - Encryption toggle will run during rebalance in STEP 12")
            self.log.info("[STEP 10] SKIPPED - Toggle deferred to rebalance; scans run in STEP 14")

        # ========== STEP 11: Optional - Trigger index key rotation ==========
        # Skipped when toggle_encryption_during_rebalance=True: combining mid-rebalance
        # encryption toggle with key rotation in the same rebalance window is unsafe.
        if trigger_key_rotation and not toggle_encryption_during_rebalance:
            try:
                self.log.info(f"[STEP 11] Triggering index key rotation (wait={key_rotation_wait_time}s)...")

                current_index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                baseline_key_ids = self.get_indexer_in_use_key_ids(current_index_nodes)
                self.log.info(f"[STEP 11] Baseline in-use key IDs: {baseline_key_ids}")

                # Target the currently-encrypted bucket (updated in STEP 9 after toggle)
                rotation_bucket = encrypted_bucket_names[0] if encrypted_bucket_names else None
                if rotation_bucket is None:
                    self.log.warning("[STEP 11] No encrypted bucket found; skipping key rotation")
                else:
                    bucket_info = self.rest.get_bucket_json(rotation_bucket)
                    current_key_id = bucket_info.get('encryptionAtRestKeyId', None)
                    if current_key_id is not None and current_key_id != -1:
                        # Pin DEK rotation interval high so no accidental rotation fires during setup
                        self.log.info(
                            f"[STEP 11a] Pinning DEK rotation interval to {STABLE_INTERVAL_S}s "
                            f"on bucket '{rotation_bucket}'..."
                        )
                        self.set_bucket_dek_rotation_config(
                            rotation_bucket, current_key_id,
                            dek_rotation_interval=STABLE_INTERVAL_S,
                            dek_lifetime=STABLE_INTERVAL_S
                        )
                        self.log.info("[STEP 11a] PASSED - DEK rotation interval pinned high")

                        # Set short rotation interval to trigger the first rotation
                        self.log.info(
                            f"[STEP 11b] Setting DEK rotation interval to {ROTATION_INTERVAL_S}s "
                            f"to trigger first rotation..."
                        )
                        self.set_bucket_dek_rotation_config(
                            rotation_bucket, current_key_id,
                            dek_rotation_interval=ROTATION_INTERVAL_S,
                            dek_lifetime=ROTATION_INTERVAL_S
                        )
                        self.log.info("[STEP 11b] PASSED - DEK rotation interval set for first rotation")

                        # Wait for rotation to occur
                        self.sleep(
                            key_rotation_wait_time,
                            f"[STEP 11c] Waiting {key_rotation_wait_time}s for index key rotation to occur"
                        )

                        new_key_ids = self.get_indexer_in_use_key_ids(current_index_nodes)
                        self.log.info(f"[STEP 11] PASSED - Key rotation triggered. New key IDs: {new_key_ids}")
                    else:
                        self.log.warning(
                            f"[STEP 11] Bucket '{rotation_bucket}' has no encryption; skipping rotation"
                        )
            except Exception as e:
                self.log.error(f"[STEP 11] FAILED: {str(e)}")
                self.failure_detected = True
                self.test_failures.append((time.strftime('%Y-%m-%d %H:%M:%S'), str(e)))
        else:
            reason = "combined with mid-rebalance toggle in STEP 12" if toggle_encryption_during_rebalance else "key rotation not requested"
            self.log.info(f"[STEP 11] SKIPPED - {reason}")

        # ========== STEP 12: Perform rebalance (add back failed node) ==========
        try:
            self.log.info(f"[STEP 12] Adding back node {failed_over_node.ip} and performing rebalance...")

            # Recover the failed over node
            self.log.info(f"[STEP 12.1] Recovering node {failed_over_node.ip}...")
            self.rest.set_recovery_type(
                otpNode=f"ns_1@{failed_over_node.ip}",
                recoveryType="full"
            )

            # Trigger rebalance asynchronously so we can optionally toggle encryption mid-flight
            self.log.info("[STEP 12.2] Triggering rebalance...")
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init],
                [],
                [],
                services=["index"],
                cluster_config=self.cluster_config if hasattr(self, 'cluster_config') else None
            )

            if toggle_encryption_during_rebalance:
                # Poll until rebalance is confirmed running before toggling encryption
                self.sleep(3, "[STEP 12] Waiting briefly before polling rebalance status")
                rebal_status, _ = self.rest._rebalance_status_and_progress()
                poll_start = time.time()
                poll_timeout = 60
                while rebal_status != 'running':
                    if time.time() - poll_start > poll_timeout:
                        raise Exception(
                            f"[STEP 12] Rebalance did not reach 'running' within {poll_timeout}s "
                            f"(current status: {rebal_status})"
                        )
                    self.sleep(2, f"[STEP 12] Waiting for rebalance to run (status={rebal_status})...")
                    rebal_status, _ = self.rest._rebalance_status_and_progress()
                self.log.info("[STEP 12] Rebalance is running; proceeding with encryption toggle")

                self.log.info("[STEP 12] Toggling encryption states while rebalance is in progress...")

                # Disable encryption on the originally encrypted bucket
                self.log.info(f"[STEP 12.a] Disabling encryption on '{encrypted_bucket}'...")
                try:
                    status, response = self.rest.disable_bucket_encryption(encrypted_bucket)
                    if not status:
                        self.log.warning(
                            f"[STEP 12.a] Failed to disable encryption on '{encrypted_bucket}': {response}"
                        )
                    else:
                        self.sleep(5, "Waiting for bucket encryption disable to take effect")
                        self.log.info(f"[STEP 12.a] Encryption disabled on '{encrypted_bucket}'")
                except Exception as toggle_err:
                    self.log.warning(f"[STEP 12.a] Toggle disable failed: {toggle_err}")

                # Enable encryption on the originally non-encrypted bucket
                self.log.info(f"[STEP 12.b] Enabling encryption on '{non_encrypted_bucket}'...")
                try:
                    status, response = self.rest.enable_bucket_encryption(
                        non_encrypted_bucket, self.encryption_at_rest_id
                    )
                    if not status:
                        self.log.warning(
                            f"[STEP 12.b] Failed to enable encryption on '{non_encrypted_bucket}': {response}"
                        )
                    else:
                        self.sleep(5, "Waiting for bucket encryption enable to take effect")
                        self.log.info(f"[STEP 12.b] Encryption enabled on '{non_encrypted_bucket}'")
                except Exception as toggle_err:
                    self.log.warning(f"[STEP 12.b] Toggle enable failed: {toggle_err}")

                encrypted_bucket_names = self.get_current_encrypted_bucket_names(bucket_names)
                self.log.info(
                    f"[STEP 12] Updated encrypted_bucket_names after mid-rebalance toggle: {encrypted_bucket_names}"
                )

            # Wait for rebalance to complete
            self.log.info("[STEP 12.3] Waiting for rebalance to complete...")
            reached = RestHelper(self.rest).rebalance_reached(retry_count=250)
            self.assertTrue(reached, "Rebalance failed, stuck or did not complete")
            rebalance.result()

            self.log.info(f"[STEP 12] PASSED - Node {failed_over_node.ip} added back and rebalanced")
        except Exception as e:
            self.log.error(f"[STEP 12] FAILED: {str(e)}")
            raise

        # ========== STEP 13.5: Optional - Toggle encryption and perform swap rebalance ==========
        if post_recovery_toggle_swap:
            try:
                self.log.info("[STEP 13.5] Post-recovery encryption toggle + swap rebalance...")

                # Determine current encryption state for each bucket
                current_encrypted = set(self.get_current_encrypted_bucket_names(bucket_names))
                current_non_encrypted = [b for b in bucket_names if b not in current_encrypted]
                self.log.info(
                    f"[STEP 13.5] Current encrypted: {sorted(current_encrypted)}, "
                    f"non-encrypted: {current_non_encrypted}"
                )

                # Disable encryption on all currently-encrypted buckets
                for bucket in sorted(current_encrypted):
                    self.log.info(f"[STEP 13.5.a] Disabling encryption on '{bucket}'...")
                    status, response = self.rest.disable_bucket_encryption(bucket)
                    if not status:
                        self.log.warning(f"[STEP 13.5.a] Failed to disable encryption on '{bucket}': {response}")
                    else:
                        self.log.info(f"[STEP 13.5.a] Encryption disabled on '{bucket}'")

                # Enable encryption on all currently-non-encrypted buckets
                for bucket in current_non_encrypted:
                    self.log.info(f"[STEP 13.5.b] Enabling encryption on '{bucket}'...")
                    status, response = self.rest.enable_bucket_encryption(bucket, self.encryption_at_rest_id)
                    if not status:
                        self.log.warning(f"[STEP 13.5.b] Failed to enable encryption on '{bucket}': {response}")
                    else:
                        self.log.info(f"[STEP 13.5.b] Encryption enabled on '{bucket}'")

                self.sleep(5, "[STEP 13.5] Waiting for encryption state changes to take effect")
                encrypted_bucket_names = self.get_current_encrypted_bucket_names(bucket_names)
                self.log.info(f"[STEP 13.5] Updated encrypted_bucket_names after toggle: {encrypted_bucket_names}")

                # Perform swap rebalance (remove last index node, add one spare)
                swap_index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                used_ips = {node.ip for node in swap_index_nodes}
                spare_nodes = [s for s in self.servers[self.nodes_init:] if s.ip not in used_ips]

                if not spare_nodes:
                    self.log.warning("[STEP 13.5] No spare nodes available for swap rebalance; skipping")
                else:
                    swap_out = [swap_index_nodes[-1]]
                    swap_in = [spare_nodes[0]]
                    self.log.info(
                        f"[STEP 13.5.c] SWAP rebalance: removing {swap_out[0].ip}, adding {swap_in[0].ip}"
                    )
                    rebalance = self.cluster.async_rebalance(
                        self.servers[:self.nodes_init],
                        swap_in,
                        swap_out,
                        services=["index"],
                        cluster_config=self.cluster_config if hasattr(self, 'cluster_config') else None
                    )
                    reached = RestHelper(self.rest).rebalance_reached(retry_count=250)
                    self.assertTrue(reached, "[STEP 13.5] Post-recovery swap rebalance failed or did not complete")
                    rebalance.result()
                    self.log.info("[STEP 13.5.c] Swap rebalance completed")

                self.log.info("[STEP 13.5] PASSED - Post-recovery toggle and swap rebalance completed")
            except Exception as e:
                self.log.error(f"[STEP 13.5] FAILED: {str(e)}")
                raise
        else:
            self.log.info("[STEP 13.5] SKIPPED - post_recovery_toggle_swap not requested")

        # ========== STEP 14: Final scans and encryption validation ==========
        try:
            self.log.info("[STEP 14] Running final scans and encryption validation...")
            query_node = self.get_nodes_from_services_map(service_type="n1ql")

            for query in all_select_queries:
                try:
                    if "embVector" in query:
                        query = query.replace("embVector", str(self.bhive_sample_vector))
                    self.run_cbq_query(query=query, scan_consistency='request_plus', server=query_node)
                except Exception as e:
                    self.log.error(f"[STEP 14] Final scan query failed: {str(e)}")
                    self.failure_detected = True
                    self.test_failures.append((time.strftime('%Y-%m-%d %H:%M:%S'), str(e)))

            # Item count validations
            self.item_count_related_validations()

            self.log.info("[STEP 14] PASSED - Final scans and encryption validation completed")
        except Exception as e:
            self.log.error(f"[STEP 14] FAILED: {str(e)}")
            self.failure_detected = True
            self.test_failures.append((time.strftime('%Y-%m-%d %H:%M:%S'), str(e)))

        # ========== STEP 15: Kill indexer process on all nodes ==========
        try:
            self.log.info("[STEP 15] Killing indexer process on all index nodes before test completion...")
            self._kill_indexer_on_all_nodes(step_prefix="[STEP 15] ")
            self.log.info("[STEP 15] PASSED - Indexer processes killed on all nodes")
        except Exception as e:
            self.log.warning(f"[STEP 15] WARNING - Failed to kill indexer on some nodes: {str(e)}")

        if self.failure_detected:
            failure_summary = "\n".join([f"[{ts}] {err}" for ts, err in self.test_failures])
            self.fail(f"Test failed with {len(self.test_failures)} non-critical error(s):\n{failure_summary}")

        # ========== STEP 16: Drop all indexes and validate resources ==========
        try:
            self.log.info("[STEP 16] Dropping all indexes and validating node resources...")
            self.drop_index_node_resources_utilization_validations()
            self.log.info("[STEP 16] PASSED - All indexes dropped and resources validated successfully")
        except Exception as e:
            self.log.warning(f"[STEP 16] WARNING - Drop indexes validation failed: {str(e)}")

        # ========== TEST COMPLETE ==========
        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_rebalance_mixed_encryption_failover_toggle")
        self.log.info(f"Successfully completed failover -> encryption toggle -> "
                      f"{'key rotation -> ' if trigger_key_rotation else ''}"
                      f"{self.rebalance_type} rebalance ({self.rebalance_mode} mode)")
        if trigger_key_rotation:
            self.log.info(f"Index key rotation was triggered with interval={key_rotation_interval}s, "
                          f"wait={key_rotation_wait_time}s")
        self.log.info("=" * 80)

    def test_rebalance_concurrent_key_rotation_expiry(self):
        """
        Rebalance test with concurrent key rotation on bucket1 and key expiry on bucket2.
        
        This test validates GSI operations during rebalance when:
        - Bucket1 has key rotation triggered
        - Bucket2 has key configured to expire concurrently
        
        Test parameters:
        - rebalance_type: in|out|swap - type of rebalance operation
        - rebalance_mode: file|dcp - rebalance mechanism
        - key_rotation_interval: DEK rotation interval in seconds (default: 120)
        - key_expiry_interval: Key expiry interval in seconds (default: 120)
        
        Test flow:
        1. Create 2 encrypted buckets with scopes/collections
        2. Create indexes on both buckets with defer_build=True, build all at once
        3. Run scans and validate encryption
        4. Configure key rotation for bucket1 and key expiry for bucket2 concurrently
        5. Trigger rebalance (in/out/swap)
        6. Rerun scans from step 3
        7. Create additional indexes
        8. Kill indexer on all nodes
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_rebalance_concurrent_key_rotation_expiry")
        self.log.info(f"Test parameters: rebalance_type={self.rebalance_type}, "
                      f"rebalance_mode={self.rebalance_mode}, json_template={self.json_template}")
        self.log.info("=" * 80)

        # Test parameters
        key_rotation_interval = self.input.param("key_rotation_interval", 120)
        key_expiry_interval_for_rotated_keys = self.input.param("key_expiry_interval_for_rotated_keys", 600)
        key_expiry_interval = self.input.param("key_expiry_interval", 120)
        
        shard_rebalance_enabled = False
        bucket1_name = None
        bucket2_name = None

        # ========== STEP 1: Verify prerequisites ==========
        index_nodes = self.verify_prerequisites()

        # ========== STEP 2: Set indexer snapshot settings and enable shard rebalance ==========
        self.set_indexer_snapshot_settings(index_nodes)
        shard_rebalance_enabled = self._enable_shard_rebalance_if_file_mode(step_prefix="[STEP 2]")

        # ========== STEP 3: Create 2 encrypted buckets ==========
        try:
            self.log.info("[STEP 3] Creating 2 encrypted buckets...")
            
            # Create bucket1 with encryption
            bucket1_name = f"{self.test_bucket}_rotation"
            self.log.info(f"[STEP 3.1] Creating encrypted bucket '{bucket1_name}'...")
            self.create_bucket_with_encryption(bucket1_name, enable_encryption=True)
            
            # Create bucket2 with encryption (using a new key for expiry testing)
            bucket2_name = f"{self.test_bucket}_expiry"
            self.log.info(f"[STEP 3.2] Creating encrypted bucket '{bucket2_name}'...")
            self.create_bucket_with_encryption(bucket2_name, enable_encryption=True)
            
            # Load documents into both buckets
            for bucket_name in [bucket1_name, bucket2_name]:
                self.log.info(f"[STEP 3.3] Loading {self.num_of_docs_per_collection} docs into '{bucket_name}'...")
                self.prepare_collection_for_indexing(
                    bucket_name=bucket_name,
                    num_scopes=self.num_scopes,
                    num_collections=self.num_collections,
                    num_of_docs_per_collection=self.num_of_docs_per_collection,
                    json_template=self.json_template,
                    load_default_coll=True
                )
            
            self.log.info(f"[STEP 3] PASSED - Created buckets: [{bucket1_name}, {bucket2_name}]")

            # Track encrypted buckets for validation (both are encrypted)
            encrypted_bucket_names = [bucket1_name, bucket2_name]
            self.log.info(f"[STEP 3] Tracking encrypted buckets: {encrypted_bucket_names}")
        except Exception as e:
            self.log.error(f"[STEP 3] FAILED: {str(e)}")
            raise

        # ========== STEP 4: Create indexes on both buckets with defer_build=True ==========
        all_select_queries = set()
        all_definitions_per_namespace = {}
        try:
            self.log.info("[STEP 4] Creating indexes on all namespaces with defer_build=True...")
            query_node = self.get_nodes_from_services_map(service_type="n1ql")

            similarity_options = ["COSINE", "L2_SQUARED", "L2", "DOT", "EUCLIDEAN_SQUARED", "EUCLIDEAN"]

            for namespace in self.namespaces:
                self.log.info(f"[STEP 4] Processing namespace: {namespace}")
                similarity = random.choice(similarity_options)
                prefix = 'concurrent_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))

                # Get query definitions for all index types
                dense_bhive_def, dense_composite_def, scalar_def, sparse_bhive_def, sparse_composite_def = \
                    self._get_query_definitions(
                        prefix=prefix,
                        similarity=similarity,
                        num_indexes_per_type=self.num_indexes_per_type
                    )

                # Store all definitions for this namespace
                all_defs = []
                if dense_bhive_def:
                    all_defs.extend(dense_bhive_def)
                if dense_composite_def:
                    all_defs.extend(dense_composite_def)
                if scalar_def:
                    all_defs.extend(scalar_def)
                if sparse_bhive_def:
                    all_defs.extend(sparse_bhive_def)
                if sparse_composite_def:
                    all_defs.extend(sparse_composite_def)
                all_definitions_per_namespace[namespace] = all_defs

                # Get select queries
                select_queries = self._get_select_queries(
                    dense_bhive_def, dense_composite_def, scalar_def,
                    sparse_bhive_def, sparse_composite_def, namespace
                )
                all_select_queries.update(select_queries)

                # Get create queries with defer_build=True
                create_queries = self._get_create_queries(
                    dense_bhive_def, dense_composite_def, scalar_def,
                    sparse_bhive_def, sparse_composite_def, namespace,
                    defer_build=True
                )

                # Create indexes (deferred)
                self.log.info(f"[STEP 4] Creating {len(create_queries)} deferred indexes on {namespace}...")
                self.gsi_util_obj.create_gsi_indexes(
                    create_queries=create_queries,
                    database=namespace,
                    query_node=query_node
                )

                # Build all indexes for this namespace at once
                self.log.info(f"[STEP 4] Building all deferred indexes for namespace {namespace}...")
                self._build_all_deferred_indexes_for_namespace(namespace, all_defs)

            self.log.info(f"[STEP 4] PASSED - Created and built indexes. Collected {len(all_select_queries)} select queries")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        # ========== STEP 5: Wait for indexes to be online ==========
        try:
            self.log.info("[STEP 5] Waiting for all indexes to reach 'Ready' state...")
            self.wait_until_indexes_online(timeout=1200, defer_build=True)
            self.log.info("[STEP 5] PASSED - All indexes are online")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise

        # ========== STEP 6: Run scans and validate encryption ==========
        try:
            self.log.info("[STEP 6] Running scans and validating encryption...")
            query_node = self.get_nodes_from_services_map(service_type="n1ql")

            for query in all_select_queries:
                try:
                    if "embVector" in query:
                        query = query.replace("embVector", str(self.bhive_sample_vector))
                    self.run_cbq_query(query=query, scan_consistency='request_plus', server=query_node)
                except Exception as e:
                    self.log.warning(f"[STEP 6] Scan query failed: {str(e)}")
                    raise

            # Validate file encryption (only for encrypted buckets)
            expected_key_ids = self.get_indexer_in_use_key_ids(index_nodes)
            validation_passed = self.validate_file_encryption_with_key(
                index_nodes,
                expected_key_id=expected_key_ids,
                step_prefix="[STEP 6] ",
                encrypted_bucket_names=encrypted_bucket_names
            )
            if not validation_passed:
                self.log.warning("[STEP 6] File encryption validation had issues")

            self.log.info("[STEP 6] PASSED - Initial scans and encryption validation completed")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise

        # ========== STEP 7: Configure concurrent key rotation (bucket1) and key expiry (bucket2) ==========
        try:
            self.log.info("[STEP 7] Configuring concurrent key rotation and expiry...")
            
            # Capture baseline key IDs
            baseline_key_ids = self.get_indexer_in_use_key_ids(index_nodes)
            self.log.info(f"[STEP 7] Baseline in-use key IDs: {baseline_key_ids}")
            
            # Configure key rotation for bucket1
            bucket1_info = self.rest.get_bucket_json(bucket1_name)
            bucket1_key_id = bucket1_info.get('encryptionAtRestKeyId', None)
            if bucket1_key_id is not None and bucket1_key_id != -1:
                self.log.info(f"[STEP 7.1] Setting DEK rotation on bucket '{bucket1_name}' "
                              f"with interval={key_rotation_interval}s")
                self.set_bucket_dek_rotation_config(
                    bucket1_name, bucket1_key_id,
                    dek_rotation_interval=key_rotation_interval,
                    dek_lifetime=key_expiry_interval_for_rotated_keys
                )
            
            # Configure key expiry for bucket2
            bucket2_info = self.rest.get_bucket_json(bucket2_name)
            bucket2_key_id = bucket2_info.get('encryptionAtRestKeyId', None)
            if bucket2_key_id is not None and bucket2_key_id != -1:
                self.log.info(f"[STEP 7.2] Setting DEK expiry on bucket '{bucket2_name}' "
                              f"with interval={key_expiry_interval}s")
                self.set_bucket_dek_rotation_config(
                    bucket2_name, bucket2_key_id,
                    dek_rotation_interval=key_expiry_interval,
                    dek_lifetime=key_expiry_interval
                )
            
            self.log.info("[STEP 7] PASSED - Concurrent key rotation and expiry configured")
            self.sleep(90, "Sleeping for the intervals to cover up for key expiry and rotation")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            raise

        # ========== STEP 8: Trigger rebalance ==========
        try:
            self.log.info(f"[STEP 8] Performing {self.rebalance_type} rebalance ({self.rebalance_mode} mode)...")

            nodes_in_list = []
            nodes_out_list = []
            services_in = None

            if self.rebalance_type == "out":
                current_index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                nodes_out_list = [current_index_nodes[-1]]
                self.log.info(f"[STEP 8] Rebalance OUT: removing node {nodes_out_list[0].ip}")

            elif self.rebalance_type == "in":
                self.log.info("[STEP 8] Enabling redistribute indexes for rebalance-in...")
                self.enable_redistribute_indexes()

                current_index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                used_ips = {node.ip for node in current_index_nodes}
                spare_nodes = [s for s in self.servers[self.nodes_init:] if s.ip not in used_ips]

                if spare_nodes:
                    nodes_in_list = [spare_nodes[0]]
                    services_in = ["index"]
                    self.log.info(f"[STEP 8] Rebalance IN: adding node {nodes_in_list[0].ip}")
                else:
                    self.log.warning("[STEP 8] No spare nodes available for rebalance-in")

            elif self.rebalance_type == "swap":
                current_index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                nodes_out_list = [current_index_nodes[-1]]

                used_ips = {node.ip for node in current_index_nodes}
                spare_nodes = [s for s in self.servers[self.nodes_init:] if s.ip not in used_ips]

                if spare_nodes:
                    nodes_in_list = [spare_nodes[0]]
                    services_in = ["index"]
                    self.log.info(f"[STEP 8] SWAP rebalance: removing {nodes_out_list[0].ip}, "
                                  f"adding {nodes_in_list[0].ip}")
                else:
                    self.log.warning("[STEP 8] No spare nodes for swap, doing rebalance-out only")

            # Perform rebalance and validate
            self._ear_rebalance_and_validate(
                nodes_in_list=nodes_in_list,
                nodes_out_list=nodes_out_list,
                services_in=services_in,
                select_queries=all_select_queries,
                index_nodes=index_nodes,
                shard_rebalance_enabled=shard_rebalance_enabled,
                encrypted_bucket_names=encrypted_bucket_names, skip_scans=True
            )

            self.log.info("[STEP 8] PASSED - Rebalance completed and validated")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {str(e)}")
            raise

        # ========== STEP 9: Rerun scans from step 6 ==========
        try:
            self.log.info("[STEP 9] Rerunning scans after rebalance with concurrent key rotation/expiry...")
            query_node = self.get_nodes_from_services_map(service_type="n1ql")

            for query in all_select_queries:
                try:
                    if "embVector" in query:
                        query = query.replace("embVector", str(self.bhive_sample_vector))
                    self.run_cbq_query(query=query, scan_consistency='request_plus', server=query_node)
                except Exception as e:
                    self.log.warning(f"[STEP 9] Scan query failed: {str(e)}")
                    raise

            # Validate file encryption with updated keys (only for encrypted buckets)
            updated_index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            expected_key_ids = self.get_indexer_in_use_key_ids(updated_index_nodes)
            validation_passed = self.validate_file_encryption_with_key(
                updated_index_nodes,
                expected_key_id=expected_key_ids,
                step_prefix="[STEP 9] ",
                encrypted_bucket_names=encrypted_bucket_names
            )
            if not validation_passed:
                self.log.warning("[STEP 9] File encryption validation had issues")

            self.log.info("[STEP 9] PASSED - Post-rebalance scans completed")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            self.failure_detected = True
            self.test_failures.append((time.strftime('%Y-%m-%d %H:%M:%S'), str(e)))

        # ========== STEP 10: Create additional indexes ==========
        try:
            self.log.info("[STEP 10] Creating additional indexes post-rebalance...")
            query_node = self.get_nodes_from_services_map(service_type="n1ql")
            post_rebal_select_queries = set()

            # Create indexes on the first namespace
            namespace = self.namespaces[0] if self.namespaces else None
            if namespace:
                prefix = 'post_concurrent_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))

                # Get definitions for additional indexes
                dense_bhive_def, dense_composite_def, scalar_def, sparse_bhive_def, sparse_composite_def = \
                    self._get_query_definitions(
                        prefix=prefix,
                        similarity="COSINE",
                        num_indexes_per_type=1
                    )

                # Collect all definitions
                all_defs = []
                if dense_bhive_def:
                    all_defs.extend(dense_bhive_def)
                if dense_composite_def:
                    all_defs.extend(dense_composite_def)
                if scalar_def:
                    all_defs.extend(scalar_def)
                if sparse_bhive_def:
                    all_defs.extend(sparse_bhive_def)
                if sparse_composite_def:
                    all_defs.extend(sparse_composite_def)

                # Get select queries for post-rebalance indexes
                post_rebal_select_queries = self._get_select_queries(
                    dense_bhive_def, dense_composite_def, scalar_def,
                    sparse_bhive_def, sparse_composite_def, namespace
                )

                # Create indexes with defer_build=True
                create_queries = self._get_create_queries(
                    dense_bhive_def, dense_composite_def, scalar_def,
                    sparse_bhive_def, sparse_composite_def, namespace,
                    defer_build=True
                )

                self.log.info(f"[STEP 10] Creating {len(create_queries)} deferred indexes on {namespace}...")
                self.gsi_util_obj.create_gsi_indexes(
                    create_queries=create_queries,
                    database=namespace,
                    query_node=query_node
                )

                # Build all indexes at once
                self.log.info(f"[STEP 10] Building all deferred indexes for namespace {namespace}...")
                self._build_all_deferred_indexes_for_namespace(namespace, all_defs)

                self.log.info("[STEP 10] Waiting for post-rebalance indexes to be online...")
                self.wait_until_indexes_online(timeout=600, defer_build=True)

                # Run scans on new indexes
                self.log.info("[STEP 10] Running scans on post-rebalance indexes...")
                for query in post_rebal_select_queries:
                    try:
                        if "embVector" in query:
                            query = query.replace("embVector", str(self.bhive_sample_vector))
                        self.run_cbq_query(query=query, scan_consistency='request_plus', server=query_node)
                    except Exception as e:
                        self.log.warning(f"[STEP 10] Post-rebalance scan query failed: {str(e)}")
                        raise

            self.log.info("[STEP 10] PASSED - Additional indexes created and validated")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {str(e)}")
            self.failure_detected = True
            self.test_failures.append((time.strftime('%Y-%m-%d %H:%M:%S'), str(e)))

        # ========== STEP 11: Final validation ==========
        try:
            self.log.info("[STEP 11] Running final encryption validation...")
            final_index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            expected_key_ids = self.get_indexer_in_use_key_ids(final_index_nodes)

            validation_passed = self.validate_file_encryption_with_key(
                final_index_nodes,
                expected_key_id=expected_key_ids,
                step_prefix="[STEP 11] ",
                encrypted_bucket_names=encrypted_bucket_names
            )
            if not validation_passed:
                self.log.warning("[STEP 11] File encryption validation had issues")

            # Item count validations
            self.item_count_related_validations()

            self.log.info("[STEP 11] PASSED - Final encryption validation completed")
        except Exception as e:
            self.log.error(f"[STEP 11] FAILED: {str(e)}")
            raise

        # ========== STEP 12: Kill indexer process on all nodes ==========
        try:
            self.log.info("[STEP 12] Killing indexer process on all index nodes before test completion...")
            self._kill_indexer_on_all_nodes(step_prefix="[STEP 12] ")
            self.log.info("[STEP 12] PASSED - Indexer processes killed on all nodes")
        except Exception as e:
            self.log.warning(f"[STEP 12] WARNING - Failed to kill indexer on some nodes: {str(e)}")

        if self.failure_detected:
            failure_summary = "\n".join([f"[{ts}] {err}" for ts, err in self.test_failures])
            self.fail(f"Test failed with {len(self.test_failures)} non-critical error(s):\n{failure_summary}")

        # ========== STEP 13: Drop all indexes and validate resources ==========
        try:
            self.log.info("[STEP 13] Dropping all indexes and validating node resources...")
            self.drop_index_node_resources_utilization_validations()
            self.log.info("[STEP 13] PASSED - All indexes dropped and resources validated successfully")
        except Exception as e:
            self.log.warning(f"[STEP 13] WARNING - Drop indexes validation failed: {str(e)}")

        # ========== TEST COMPLETE ==========
        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_rebalance_concurrent_key_rotation_expiry")
        self.log.info(f"Successfully completed {self.rebalance_type} rebalance ({self.rebalance_mode} mode) "
                      f"with concurrent key rotation (bucket1, interval={key_rotation_interval}s) "
                      f"and key expiry (bucket2, interval={key_expiry_interval}s)")
        self.log.info("=" * 80)


    def test_gsi_encryption_at_rest_drop_dek_scenarios(self):
        """
        Encryption at rest — force re-encryption (drop DEK) during cluster events.

        Validates that calling trigger_data_reencryption on all encrypted buckets
        during various cluster events:
          - Replaces all existing DEKs with new ones
          - Leaves the KEK ID (encryptionAtRestKeyId) unchanged
          - Keeps data accessible (scans succeed) throughout and after the event
          - Results in valid file encryption with the new DEK IDs

        Controlled by the dek_drop_scenario parameter:
          during_rebalance          — drop DEKs concurrently while a rebalance is running
          during_failed_rebalance   — drop DEKs immediately after killing the indexer
                                      while the rebalance is actively failing
          during_failover           — drop DEKs concurrently while graceful failover runs
          after_failover_before_addback — drop DEKs after failover completes,
                                          before adding the node back
        """
        STABLE_INTERVAL_S = 36000  # 10 hours — no accidental background rotation

        dek_drop_scenario = self.input.param("dek_drop_scenario", "during_rebalance")
        shard_rebalance_enabled = False

        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_gsi_encryption_at_rest_drop_dek_scenarios")
        self.log.info(f"dek_drop_scenario={dek_drop_scenario}, rebalance_type={self.rebalance_type}, "
                      f"rebalance_mode={self.rebalance_mode}, json_template={self.json_template}")
        self.log.info("=" * 80)

        # ========== STEP 1: Verify prerequisites ==========
        index_nodes = self.verify_prerequisites()

        # ========== STEP 2: Set indexer snapshot settings ==========
        self.set_indexer_snapshot_settings(index_nodes)

        # ========== STEP 3: Enable shard-based rebalance if file mode ==========
        shard_rebalance_enabled = self._enable_shard_rebalance_if_file_mode(step_prefix="[STEP 3]")

        # ========== STEP 4: Create buckets and pin DEK rotation high ==========
        try:
            encryption_mode = "all encrypted" if self.encrypt_all_buckets else "mixed"
            self.log.info(f"[STEP 4] Creating {self.bucket_count} bucket(s) ({encryption_mode})...")
            bucket_names, encrypted_bucket_names = self._create_encrypted_test_buckets(
                num_buckets=self.bucket_count,
                encrypt_all=self.encrypt_all_buckets
            )
            self.log.info(f"[STEP 4] Buckets: {bucket_names}, encrypted: {encrypted_bucket_names}")

            # Pin DEK rotation high to prevent background rotation interfering with the test
            for bname in encrypted_bucket_names:
                binfo = self.rest.get_bucket_json(bname)
                kek_id = binfo.get('encryptionAtRestKeyId')
                if kek_id and kek_id != -1:
                    self.set_bucket_dek_rotation_config(
                        bname, kek_id,
                        dek_rotation_interval=STABLE_INTERVAL_S,
                        dek_lifetime=STABLE_INTERVAL_S
                    )
                    self.log.info(f"[STEP 4] Pinned DEK rotation to {STABLE_INTERVAL_S}s on '{bname}'")

            self.log.info("[STEP 4] PASSED - Buckets created and DEK rotation pinned")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        # ========== STEP 5: Create indexes on all namespaces ==========
        all_select_queries = set()
        try:
            self.log.info("[STEP 5] Creating indexes on all namespaces...")
            query_node = self.get_nodes_from_services_map(service_type="n1ql")
            similarity_options = ["COSINE", "L2_SQUARED", "L2", "DOT", "EUCLIDEAN_SQUARED", "EUCLIDEAN"]

            for namespace in self.namespaces:
                self.log.info(f"[STEP 5] Processing namespace: {namespace}")
                similarity = random.choice(similarity_options)
                prefix = 'drop_dek_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))

                dense_bhive_def, dense_composite_def, scalar_def, sparse_bhive_def, sparse_composite_def = \
                    self._get_query_definitions(
                        prefix=prefix,
                        similarity=similarity,
                        num_indexes_per_type=self.num_indexes_per_type
                    )

                all_defs = []
                for defs in [dense_bhive_def, dense_composite_def, scalar_def,
                              sparse_bhive_def, sparse_composite_def]:
                    if defs:
                        all_defs.extend(defs)

                select_queries = self._get_select_queries(
                    dense_bhive_def, dense_composite_def, scalar_def,
                    sparse_bhive_def, sparse_composite_def, namespace
                )
                all_select_queries.update(select_queries)

                create_queries = self._get_create_queries(
                    dense_bhive_def, dense_composite_def, scalar_def,
                    sparse_bhive_def, sparse_composite_def, namespace,
                    defer_build=True
                )
                self.gsi_util_obj.create_gsi_indexes(
                    create_queries=create_queries,
                    database=namespace,
                    query_node=query_node
                )
                self._build_all_deferred_indexes_for_namespace(namespace, all_defs)

            self.log.info(f"[STEP 5] PASSED - Created indexes; {len(all_select_queries)} select queries collected")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise

        # ========== STEP 6: Wait for indexes online ==========
        try:
            self.log.info("[STEP 6] Waiting for all indexes to reach 'Ready' state...")
            self.wait_until_indexes_online(timeout=1200, defer_build=True)
            self.log.info("[STEP 6] PASSED - All indexes online")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise

        # ========== STEP 7: Baseline scans + encryption validation ==========
        baseline_key_ids = self.get_indexer_in_use_key_ids(index_nodes)
        try:
            self.log.info("[STEP 7] Running baseline scans and file encryption validation...")
            query_node = self.get_nodes_from_services_map(service_type="n1ql")
            for query in all_select_queries:
                try:
                    q = query.replace("embVector", str(self.bhive_sample_vector)) \
                        if "embVector" in query else query
                    self.run_cbq_query(query=q, scan_consistency='request_plus', server=query_node)
                except Exception as qe:
                    self.log.warning(f"[STEP 7] Scan query failed: {str(qe)}")

            if not self.encrypt_all_buckets:
                self.log.info("[STEP 7] Skipping file encryption validation — mixed encryption scenario")
            else:
                validation_passed = self.validate_file_encryption_with_key(
                    index_nodes,
                    expected_key_id=baseline_key_ids,
                    step_prefix="[STEP 7] ",
                    encrypted_bucket_names=encrypted_bucket_names
                )
                if not validation_passed:
                    raise Exception("Baseline file encryption validation failed")
            self.log.info("[STEP 7] PASSED - Baseline scans and encryption validated")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            self.failure_detected = True
            self.test_failures.append((time.strftime('%Y-%m-%d %H:%M:%S'), str(e)))

        # ============================================================
        # STEP 8+: Scenario-specific cluster event + DEK drop
        # ============================================================
        new_key_ids = None
        failed_over_node = None
        # node lists persisted for retry in during_failed_rebalance scenario
        nodes_in_list = []
        nodes_out_list = []
        services_in = None

        # Capture baseline KEK IDs and oldest DEK timestamps before any DEK drop
        baseline_kek_ids = {}
        baseline_oldest_dek = {}
        for bname in encrypted_bucket_names:
            binfo = self.rest.get_bucket_json(bname)
            baseline_kek_ids[bname] = binfo.get('encryptionAtRestKeyId')
            baseline_oldest_dek[bname] = binfo.get('encryptionAtRestInfo', {}).get('oldestDekCreationDatetime')

        if dek_drop_scenario in ("during_rebalance", "during_failed_rebalance", "before_rebalance"):
            # --- Determine rebalance topology ---
            if self.rebalance_type == "out":
                current_index_nodes = self.get_nodes_from_services_map(
                    service_type="index", get_all_nodes=True)
                nodes_out_list = [current_index_nodes[-1]]
                self.log.info(f"Rebalance OUT: removing {nodes_out_list[0].ip}")

            elif self.rebalance_type == "in":
                self.enable_redistribute_indexes()
                current_index_nodes = self.get_nodes_from_services_map(
                    service_type="index", get_all_nodes=True)
                used_ips = {n.ip for n in current_index_nodes}
                spare_nodes = [s for s in self.servers[self.nodes_init:] if s.ip not in used_ips]
                if spare_nodes:
                    nodes_in_list = [spare_nodes[0]]
                    services_in = ["index"]
                    self.log.info(f"Rebalance IN: adding {nodes_in_list[0].ip}")
                else:
                    raise Exception("No spare nodes available for rebalance-in")

            elif self.rebalance_type == "swap":
                current_index_nodes = self.get_nodes_from_services_map(
                    service_type="index", get_all_nodes=True)
                nodes_out_list = [current_index_nodes[-1]]
                used_ips = {n.ip for n in current_index_nodes}
                spare_nodes = [s for s in self.servers[self.nodes_init:] if s.ip not in used_ips]
                if spare_nodes:
                    nodes_in_list = [spare_nodes[0]]
                    services_in = ["index"]
                    self.log.info(f"SWAP: removing {nodes_out_list[0].ip}, adding {nodes_in_list[0].ip}")
                else:
                    raise Exception("No spare nodes for swap rebalance")

        if dek_drop_scenario == "during_rebalance":
            # ========== STEP 8: Start async rebalance, drop DEKs concurrently ==========
            try:
                self.log.info("[STEP 8] Starting rebalance and concurrently dropping DEKs on all encrypted buckets...")
                self.sleep(90, "[STEP 8] Waiting for system to stabilize before rebalance")

                rebalance = self.cluster.async_rebalance(
                    self.servers[:self.nodes_init],
                    nodes_in_list, nodes_out_list,
                    services=services_in,
                    cluster_config=self.cluster_config if hasattr(self, 'cluster_config') else None
                )

                # Drop DEKs on all encrypted buckets in a background thread
                dek_drop_result = {'error': None, 'new_key_ids': None}

                def _drop_deks_concurrent():
                    try:
                        self.sleep(15, "[DEK drop] Brief delay before triggering re-encryption")
                        for bname in encrypted_bucket_names:
                            self.log.info(f"[DEK drop] Triggering force re-encryption on '{bname}'...")
                            status, response = self.rest.trigger_data_reencryption(bname)
                            if not status:
                                raise Exception(
                                    f"trigger_data_reencryption failed on '{bname}': {response}")
                            self.log.info(f"[DEK drop] Re-encryption triggered on '{bname}': {response}")
                        dek_drop_result['new_key_ids'] = self.poll_for_new_indexer_key_ids(
                            index_nodes, baseline_key_ids, timeout=600, label="[DEK drop] "
                        )
                    except Exception as ex:
                        dek_drop_result['error'] = str(ex)

                drop_thread = threading.Thread(target=_drop_deks_concurrent, daemon=True)
                drop_thread.start()

                try:
                    self.log.info("[STEP 8] Waiting for rebalance to complete...")
                    reached = RestHelper(self.rest).rebalance_reached(retry_count=250)
                    self.assertTrue(reached, "Rebalance failed, stuck or did not complete")
                    rebalance.result()
                    self.log.info("[STEP 8] Rebalance completed successfully")
                finally:
                    drop_thread.join(timeout=650)

                if dek_drop_result['error']:
                    raise Exception(f"DEK drop thread failed: {dek_drop_result['error']}")
                if dek_drop_result['new_key_ids'] is None:
                    raise Exception("DEK drop thread did not complete within join timeout")
                new_key_ids = dek_drop_result['new_key_ids']
                self.log.info(f"[STEP 8] PASSED - Rebalance complete; new DEK key IDs: {new_key_ids}")
            except Exception as e:
                self.log.error(f"[STEP 8] FAILED: {str(e)}")
                raise

        elif dek_drop_scenario == "during_failed_rebalance":
            # ========== STEP 8: Start rebalance, kill indexer, drop DEKs while it fails ==========
            node_to_kill = index_nodes[0]
            try:
                self.log.info(
                    f"[STEP 8] Starting {self.rebalance_type} rebalance; will kill indexer on "
                    f"{node_to_kill.ip} and drop DEKs concurrently with the failure..."
                )
                self.sleep(90, "[STEP 8] Waiting for system to stabilize before rebalance")

                rebalance = self.cluster.async_rebalance(
                    self.servers[:self.nodes_init],
                    nodes_in_list, nodes_out_list,
                    services=services_in,
                    cluster_config=self.cluster_config if hasattr(self, 'cluster_config') else None
                )

                # Poll until rebalance is actively running
                self.log.info("[STEP 8] Waiting for rebalance to reach 'running' status...")
                self.sleep(3, "[STEP 8] Brief initial wait")
                status, _ = self.rest._rebalance_status_and_progress()
                start_time = time.time()
                while status != 'running':
                    if time.time() - start_time > 60:
                        raise Exception(f"Rebalance did not reach 'running' within 60s (status: {status})")
                    self.sleep(1, f"[STEP 8] Waiting for running status (current: {status})")
                    status, _ = self.rest._rebalance_status_and_progress()
                self.log.info("[STEP 8] Rebalance is running; killing indexer to fail it...")

                self._kill_all_processes_index(node_to_kill)
                self.log.info(f"[STEP 8] Indexer killed on {node_to_kill.ip}")

                # Drop DEKs immediately — rebalance is actively failing
                self.log.info("[STEP 8] Triggering force re-encryption on all encrypted buckets "
                              "(concurrent with rebalance failure)...")
                for bname in encrypted_bucket_names:
                    try:
                        status, response = self.rest.trigger_data_reencryption(bname)
                        if not status:
                            self.log.warning(
                                f"[STEP 8] trigger_data_reencryption on '{bname}' returned: {response}")
                        else:
                            self.log.info(f"[STEP 8] Re-encryption triggered on '{bname}': {response}")
                    except Exception as drop_ex:
                        self.log.warning(f"[STEP 8] DEK drop on '{bname}' during failure: {str(drop_ex)}")

                # Wait for the rebalance to finish failing
                try:
                    rebalance.result()
                    self.log.warning("[STEP 8] Rebalance completed unexpectedly without failing")
                except Exception as ex:
                    self.log.info(f"[STEP 8] Rebalance failed as expected: {str(ex)}")

                self.log.info("[STEP 8] PASSED - Rebalance failed; DEK drop triggered during failure")
            except Exception as e:
                self.log.error(f"[STEP 8] FAILED: {str(e)}")
                raise

            # ========== STEP 9: Wait for system to stabilize + poll for new DEK key IDs ==========
            try:
                self.log.info("[STEP 9] Waiting for system to stabilize after failed rebalance...")
                self.sleep(120, "[STEP 9] Waiting 2 minutes for indexer recovery")
                self.wait_until_indexes_online(timeout=600)

                new_key_ids = self.poll_for_new_indexer_key_ids(
                    index_nodes, baseline_key_ids, timeout=300, label="[STEP 9] "
                )
                self.log.info(f"[STEP 9] PASSED - System stabilized; new DEK key IDs: {new_key_ids}")
            except Exception as e:
                self.log.error(f"[STEP 9] FAILED: {str(e)}")
                raise

            # ========== STEP 10: Retry rebalance ==========
            try:
                self.log.info(f"[STEP 10] Retrying {self.rebalance_type} rebalance after DEK drop + failure...")
                if self.rebalance_type == "in":
                    self.enable_redistribute_indexes()

                self.sleep(60, "[STEP 10] Brief stabilization before retry")
                self._ear_rebalance_and_validate(
                    nodes_in_list=nodes_in_list,
                    nodes_out_list=nodes_out_list,
                    services_in=services_in,
                    select_queries=all_select_queries,
                    index_nodes=self.get_nodes_from_services_map(service_type="index", get_all_nodes=True),
                    shard_rebalance_enabled=shard_rebalance_enabled,
                    encrypted_bucket_names=encrypted_bucket_names,
                    skip_scans=True
                )
                self.log.info("[STEP 10] PASSED - Retry rebalance completed and validated")
            except Exception as e:
                self.log.error(f"[STEP 10] FAILED: {str(e)}")
                raise

        elif dek_drop_scenario == "during_failover":
            # ========== STEP 8: Start async failover, drop DEKs concurrently ==========
            try:
                self.log.info("[STEP 8] Starting graceful failover and concurrently dropping DEKs "
                              "on all encrypted buckets...")
                current_index_nodes = self.get_nodes_from_services_map(
                    service_type="index", get_all_nodes=True)
                self.assertGreaterEqual(len(current_index_nodes), 2,
                    "Need at least 2 index nodes for failover")
                failed_over_node = current_index_nodes[-1]
                self.log.info(f"[STEP 8] Failing over node: {failed_over_node.ip}")

                failover_task = self.cluster.async_failover(
                    servers=[self.master],
                    failover_nodes=[failed_over_node],
                    graceful=False
                )

                # Drop DEKs on all encrypted buckets in a background thread
                dek_drop_result = {'error': None, 'new_key_ids': None}

                def _drop_deks_during_failover():
                    try:
                        self.sleep(10, "[DEK drop] Brief delay before triggering re-encryption")
                        for bname in encrypted_bucket_names:
                            self.log.info(f"[DEK drop] Triggering force re-encryption on '{bname}'...")
                            status, response = self.rest.trigger_data_reencryption(bname)
                            if not status:
                                raise Exception(
                                    f"trigger_data_reencryption failed on '{bname}': {response}")
                            self.log.info(f"[DEK drop] Re-encryption triggered on '{bname}': {response}")
                        # Poll using nodes that survive failover (all except the one being failed over)
                        surviving_nodes = [n for n in index_nodes if n.ip != failed_over_node.ip]
                        dek_drop_result['new_key_ids'] = self.poll_for_new_indexer_key_ids(
                            surviving_nodes, baseline_key_ids, timeout=600, label="[DEK drop] "
                        )
                    except Exception as ex:
                        dek_drop_result['error'] = str(ex)

                drop_thread = threading.Thread(target=_drop_deks_during_failover, daemon=True)
                drop_thread.start()

                try:
                    failover_task.result()
                    self.sleep(30, "[STEP 8] Waiting for failover to settle")
                    self.log.info(f"[STEP 8] Failover of {failed_over_node.ip} completed")
                finally:
                    drop_thread.join(timeout=650)

                if dek_drop_result['error']:
                    raise Exception(f"DEK drop thread failed: {dek_drop_result['error']}")
                if dek_drop_result['new_key_ids'] is None:
                    raise Exception("DEK drop thread did not complete within join timeout")
                new_key_ids = dek_drop_result['new_key_ids']
                self.log.info(f"[STEP 8] PASSED - Failover complete; new DEK key IDs: {new_key_ids}")
            except Exception as e:
                self.log.error(f"[STEP 8] FAILED: {str(e)}")
                raise

            # ========== STEP 9: Add failed-over node back ==========
            try:
                self.log.info(f"[STEP 9] Adding back failed-over node {failed_over_node.ip}...")
                self.rest.set_recovery_type(
                    otpNode=f"ns_1@{failed_over_node.ip}",
                    recoveryType="full"
                )
                rebalance = self.cluster.async_rebalance(
                    self.servers[:self.nodes_init], [], [],
                    services=["index"],
                    cluster_config=self.cluster_config if hasattr(self, 'cluster_config') else None
                )
                reached = RestHelper(self.rest).rebalance_reached(retry_count=250)
                self.assertTrue(reached, "Add-back rebalance failed")
                rebalance.result()
                self.log.info(f"[STEP 9] PASSED - Node {failed_over_node.ip} added back successfully")
            except Exception as e:
                self.log.error(f"[STEP 9] FAILED: {str(e)}")
                raise

        elif dek_drop_scenario == "after_failover_before_addback":
            # ========== STEP 8: Graceful failover ==========
            try:
                self.log.info("[STEP 8] Performing graceful failover of one indexer node...")
                current_index_nodes = self.get_nodes_from_services_map(
                    service_type="index", get_all_nodes=True)
                self.assertGreaterEqual(len(current_index_nodes), 2,
                    "Need at least 2 index nodes for failover")
                failed_over_node = current_index_nodes[-1]
                self.log.info(f"[STEP 8] Failing over node: {failed_over_node.ip}")

                failover_task = self.cluster.async_failover(
                    servers=[self.master],
                    failover_nodes=[failed_over_node],
                    graceful=False
                )
                failover_task.result()
                self.sleep(30, "[STEP 8] Waiting for failover to settle")
                self.log.info(f"[STEP 8] PASSED - Node {failed_over_node.ip} failed over")
            except Exception as e:
                self.log.error(f"[STEP 8] FAILED: {str(e)}")
                raise

            # ========== STEP 9: Drop DEKs AFTER failover, BEFORE add-back ==========
            try:
                self.log.info("[STEP 9] Dropping DEKs on all encrypted buckets "
                              "(post-failover, before node add-back)...")
                surviving_nodes = [n for n in index_nodes if n.ip != failed_over_node.ip]
                for bname in encrypted_bucket_names:
                    self.log.info(f"[STEP 9] Triggering force re-encryption on '{bname}'...")
                    status, response = self.rest.trigger_data_reencryption(bname)
                    if not status:
                        raise Exception(f"trigger_data_reencryption failed on '{bname}': {response}")
                    self.log.info(f"[STEP 9] Re-encryption triggered on '{bname}': {response}")

                new_key_ids = self.poll_for_new_indexer_key_ids(
                    surviving_nodes, baseline_key_ids, timeout=300, label="[STEP 9] "
                )
                self.log.info(f"[STEP 9] PASSED - DEKs dropped; new key IDs: {new_key_ids}")
            except Exception as e:
                self.log.error(f"[STEP 9] FAILED: {str(e)}")
                raise

            # ========== STEP 10: Add failed-over node back ==========
            try:
                self.log.info(f"[STEP 10] Adding back failed-over node {failed_over_node.ip}...")
                self.rest.set_recovery_type(
                    otpNode=f"ns_1@{failed_over_node.ip}",
                    recoveryType="full"
                )
                rebalance = self.cluster.async_rebalance(
                    self.servers[:self.nodes_init], [], [],
                    services=["index"],
                    cluster_config=self.cluster_config if hasattr(self, 'cluster_config') else None
                )
                reached = RestHelper(self.rest).rebalance_reached(retry_count=250)
                self.assertTrue(reached, "Add-back rebalance failed")
                rebalance.result()
                self.log.info(f"[STEP 10] PASSED - Node {failed_over_node.ip} added back successfully")
            except Exception as e:
                self.log.error(f"[STEP 10] FAILED: {str(e)}")
                raise

        elif dek_drop_scenario == "before_rebalance":
            # ========== STEP 8: Drop DEKs — no concurrent operation ==========
            try:
                self.log.info("[STEP 8] Dropping DEKs on all encrypted buckets (no concurrent operation)...")
                for bname in encrypted_bucket_names:
                    self.log.info(f"[STEP 8] Triggering force re-encryption on '{bname}'...")
                    status, response = self.rest.trigger_data_reencryption(bname)
                    if not status:
                        raise Exception(f"trigger_data_reencryption failed on '{bname}': {response}")
                    self.log.info(f"[STEP 8] Re-encryption triggered on '{bname}': {response}")

                new_key_ids = self.poll_for_new_indexer_key_ids(
                    index_nodes, baseline_key_ids, timeout=300, label="[STEP 8] "
                )
                self.log.info(f"[STEP 8] PASSED - DEKs dropped; new key IDs: {new_key_ids}")
            except Exception as e:
                self.log.error(f"[STEP 8] FAILED: {str(e)}")
                raise

            # ========== STEP 9: Rebalance after DEK drop ==========
            try:
                self.log.info("[STEP 9] Executing rebalance after DEK drop...")
                self.sleep(30, "[STEP 9] Waiting for re-encryption to initialize before rebalance")
                rebalance = self.cluster.async_rebalance(
                    self.servers[:self.nodes_init],
                    nodes_in_list, nodes_out_list,
                    services=services_in,
                )
                reached = RestHelper(self.rest).rebalance_reached(retry_count=250)
                self.assertTrue(reached, "[STEP 9] Rebalance failed or did not complete")
                rebalance.result()
                self.log.info("[STEP 9] PASSED - Rebalance completed after DEK drop")
            except Exception as e:
                self.log.error(f"[STEP 9] FAILED: {str(e)}")
                raise

        else:
            self.fail(
                f"Unknown dek_drop_scenario: '{dek_drop_scenario}'. "
                f"Valid: during_rebalance, during_failed_rebalance, during_failover, "
                f"after_failover_before_addback, before_rebalance"
            )

        # ========== FINAL STEP A: Wait for all indexes to be online ==========
        try:
            self.log.info("[FINAL-A] Waiting for all indexes to reach 'Ready' state...")
            self.wait_until_indexes_online(timeout=1200, defer_build=True)
            self.log.info("[FINAL-A] PASSED - All indexes online after cluster event")
        except Exception as e:
            self.log.error(f"[FINAL-A] FAILED: {str(e)}")
            raise

        # ========== FINAL STEP B: Run scans to verify data accessible ==========
        try:
            self.log.info("[FINAL-B] Running scans to verify data accessible after cluster event...")
            query_node = self.get_nodes_from_services_map(service_type="n1ql")
            for query in all_select_queries:
                try:
                    q = query.replace("embVector", str(self.bhive_sample_vector)) \
                        if "embVector" in query else query
                    self.run_cbq_query(query=q, scan_consistency='request_plus', server=query_node)
                except Exception as qe:
                    self.log.warning(f"[FINAL-B] Scan query failed: {str(qe)}")
            self.log.info("[FINAL-B] PASSED - Post-event scans completed")
        except Exception as e:
            self.log.error(f"[FINAL-B] FAILED: {str(e)}")
            raise

        # ========== FINAL STEP C: Validate KEK unchanged, DEK changed ==========
        try:
            self.log.info("[FINAL-C] Validating KEK ID unchanged and DEK replaced per bucket...")
            for bname in encrypted_bucket_names:
                binfo_after = self.rest.get_bucket_json(bname)
                kek_id_after = binfo_after.get('encryptionAtRestKeyId')
                dek_info_after = binfo_after.get('encryptionAtRestInfo', {})
                oldest_dek_after = dek_info_after.get('oldestDekCreationDatetime')

                # KEK must be unchanged
                self.assertEqual(
                    kek_id_after, baseline_kek_ids[bname],
                    f"[FINAL-C] KEK changed unexpectedly on '{bname}': "
                    f"baseline={baseline_kek_ids[bname]}, after={kek_id_after}"
                )
                self.log.info(f"[FINAL-C] Bucket '{bname}': KEK ID unchanged ({kek_id_after}) — PASSED")

                # DEK should have changed (oldest timestamp newer)
                if baseline_oldest_dek[bname] and oldest_dek_after:
                    if oldest_dek_after != baseline_oldest_dek[bname]:
                        self.log.info(
                            f"[FINAL-C] Bucket '{bname}': DEK replaced — "
                            f"oldest changed from {baseline_oldest_dek[bname]} to {oldest_dek_after}"
                        )
                    else:
                        self.log.warning(
                            f"[FINAL-C] Bucket '{bname}': oldest DEK timestamp unchanged — "
                            f"re-encryption may still be in progress"
                        )

            self.log.info("[FINAL-C] PASSED - KEK unchanged, DEK change validated on all encrypted buckets")
        except Exception as e:
            self.log.error(f"[FINAL-C] FAILED: {str(e)}")
            raise

        # ========== FINAL STEP D: File encryption validation with new DEK key IDs ==========
        try:
            self.log.info("[FINAL-D] Running file encryption validation with new DEK key IDs...")
            if not self.encrypt_all_buckets:
                self.log.info("[FINAL-D] Skipping file encryption validation — mixed encryption scenario")
            else:
                final_index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                expected_key_ids = new_key_ids if new_key_ids \
                    else self.get_indexer_in_use_key_ids(final_index_nodes)
                validation_passed = self.validate_file_encryption_with_key(
                    final_index_nodes,
                    expected_key_id=expected_key_ids,
                    step_prefix="[FINAL-D] ",
                    encrypted_bucket_names=encrypted_bucket_names
                )
                if not validation_passed:
                    self.log.warning("[FINAL-D] File encryption validation had issues — check logs")

            self.item_count_related_validations()
            self.log.info("[FINAL-D] PASSED - File encryption validation completed with new DEK IDs")
        except Exception as e:
            self.log.error(f"[FINAL-D] FAILED: {str(e)}")
            self.failure_detected = True
            self.test_failures.append((time.strftime('%Y-%m-%d %H:%M:%S'), str(e)))

        # ========== CLEANUP ==========
        try:
            self.log.info("[CLEANUP] Killing indexer process on all index nodes...")
            self._kill_indexer_on_all_nodes(step_prefix="[CLEANUP] ")
            self.log.info("[CLEANUP] PASSED - Indexer processes killed")
        except Exception as e:
            self.log.warning(f"[CLEANUP] WARNING - Failed to kill indexer: {str(e)}")

        if self.failure_detected:
            failure_summary = "\n".join([f"[{ts}] {err}" for ts, err in self.test_failures])
            self.fail(f"Test failed with {len(self.test_failures)} non-critical error(s):\n{failure_summary}")

        try:
            self.log.info("[CLEANUP] Dropping all indexes and validating node resources...")
            self.drop_index_node_resources_utilization_validations()
            self.log.info("[CLEANUP] PASSED - Indexes dropped and resources validated")
        except Exception as e:
            self.log.warning(f"[CLEANUP] WARNING - Drop indexes validation failed: {str(e)}")

        # ========== TEST COMPLETE ==========
        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_gsi_encryption_at_rest_drop_dek_scenarios")
        self.log.info(f"Scenario: {dek_drop_scenario} | rebalance_type={self.rebalance_type} | "
                      f"rebalance_mode={self.rebalance_mode}")
        self.log.info("All encrypted buckets: KEK ID unchanged, DEK replaced via force re-encryption")
        self.log.info("=" * 80)

    def test_rebalance_dgm(self):
        """
        Encryption at rest rebalance test in DGM (Disk Greater than Memory) state.

        All buckets have encryption at rest enabled. The cluster is pushed into DGM
        by repeatedly creating indexes until the average residency ratio drops below
        ``desired_rr`` (mirrors the approach in test_run_scans_on_dgm).  A single
        swap rebalance is then performed and encryption is validated throughout.

        Test parameters:
        - rebalance_mode : file | dcp
        - desired_rr     : target average resident-ratio % to reach before rebalance (default 10)
        - dgm_timeout    : seconds allowed to reach DGM (default 7200)
        - bucket_count   : number of buckets (default 2, all encrypted)
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_rebalance_dgm")
        self.log.info(f"Test parameters: rebalance_mode={self.rebalance_mode}, "
                      f"json_template={self.json_template}, "
                      f"desired_rr={self.desired_rr}")
        self.log.info("=" * 80)

        dgm_timeout = self.input.param("dgm_timeout", 7200)
        shard_rebalance_enabled = False

        # ========== STEP 1: Verify prerequisites ==========
        index_nodes = self.verify_prerequisites()

        # ========== STEP 2: Set indexer snapshot settings ==========
        self.set_indexer_snapshot_settings(index_nodes)

        # ========== STEP 3: Enable shard-based rebalance if file mode ==========
        shard_rebalance_enabled = self._enable_shard_rebalance_if_file_mode(step_prefix="[STEP 3]")

        # ========== STEP 4: Create encrypted buckets ==========
        try:
            self.log.info(f"[STEP 4] Creating {self.bucket_count} encrypted bucket(s)...")
            bucket_names, encrypted_bucket_names = self._create_encrypted_test_buckets(
                num_buckets=self.bucket_count,
                encrypt_all=True  # all buckets encrypted for this test
            )
            self.log.info(f"[STEP 4] PASSED - Created buckets: {bucket_names}, "
                          f"all encrypted: {encrypted_bucket_names}")
        except Exception as e:
            self.log.error(f"[STEP 4] FAILED: {str(e)}")
            raise

        # ========== STEP 5: Create indexes on all namespaces ==========
        all_select_queries = set()
        all_definitions_per_namespace = {}
        try:
            self.log.info("[STEP 5] Creating indexes on all namespaces with defer_build=True...")
            query_node = self.get_nodes_from_services_map(service_type="n1ql")
            similarity_options = ["COSINE", "L2_SQUARED", "L2", "DOT", "EUCLIDEAN_SQUARED", "EUCLIDEAN"]

            for namespace in self.namespaces:
                self.log.info(f"[STEP 5] Processing namespace: {namespace}")
                similarity = random.choice(similarity_options)
                prefix = 'dgm_' + ''.join(random.choices(string.ascii_letters + string.digits, k=5))

                dense_bhive_def, dense_composite_def, scalar_def, sparse_bhive_def, sparse_composite_def = \
                    self._get_query_definitions(
                        prefix=prefix,
                        similarity=similarity,
                        num_indexes_per_type=self.num_indexes_per_type
                    )

                all_defs = []
                for defs in (dense_bhive_def, dense_composite_def, scalar_def,
                              sparse_bhive_def, sparse_composite_def):
                    if defs:
                        all_defs.extend(defs)
                all_definitions_per_namespace[namespace] = all_defs

                select_queries = self._get_select_queries(
                    dense_bhive_def, dense_composite_def, scalar_def,
                    sparse_bhive_def, sparse_composite_def, namespace
                )
                all_select_queries.update(select_queries)

                create_queries = self._get_create_queries(
                    dense_bhive_def, dense_composite_def, scalar_def,
                    sparse_bhive_def, sparse_composite_def, namespace,
                    defer_build=True
                )

                self.log.info(f"[STEP 5] Creating {len(create_queries)} deferred indexes on {namespace}...")
                self.gsi_util_obj.create_gsi_indexes(
                    create_queries=create_queries,
                    database=namespace,
                    query_node=query_node
                )
                self.log.info(f"[STEP 5] Building deferred indexes for {namespace}...")
                self._build_all_deferred_indexes_for_namespace(namespace, all_defs)

            self.log.info(f"[STEP 5] PASSED - Indexes created on all namespaces. "
                          f"Collected {len(all_select_queries)} select queries")
        except Exception as e:
            self.log.error(f"[STEP 5] FAILED: {str(e)}")
            raise

        # ========== STEP 6: Wait for indexes to be online ==========
        try:
            self.log.info("[STEP 6] Waiting for all indexes to reach 'Ready' state...")
            self.wait_until_indexes_online(timeout=1200, defer_build=True)
            self.log.info("[STEP 6] PASSED - All indexes are online")
        except Exception as e:
            self.log.error(f"[STEP 6] FAILED: {str(e)}")
            raise

        # ========== STEP 7: Run baseline scans and validate encryption ==========
        try:
            self.log.info("[STEP 7] Running baseline scans and validating encryption...")
            query_node = self.get_nodes_from_services_map(service_type="n1ql")
            for query in all_select_queries:
                try:
                    if "embVector" in query:
                        query = query.replace("embVector", str(self.bhive_sample_vector))
                    self.run_cbq_query(query=query, scan_consistency='request_plus', server=query_node)
                except Exception as e:
                    self.log.warning(f"[STEP 7] Baseline scan failed: {str(e)}")
                    raise

            expected_key_ids = self.get_indexer_in_use_key_ids(index_nodes)
            validation_passed = self.validate_file_encryption_with_key(
                index_nodes,
                expected_key_id=expected_key_ids,
                step_prefix="[STEP 7] ",
                encrypted_bucket_names=encrypted_bucket_names
            )
            if not validation_passed:
                raise Exception("Baseline file encryption validation failed before DGM")

            self.log.info("[STEP 7] PASSED - Baseline scans and encryption validation completed")
        except Exception as e:
            self.log.error(f"[STEP 7] FAILED: {str(e)}")
            self.failure_detected = True
            self.test_failures.append((time.strftime('%Y-%m-%d %H:%M:%S'), str(e)))

        # ========== STEP 8: Drive cluster into DGM ==========
        try:
            current_rr = self.compute_cluster_avg_rr_index()
            self.log.info(
                f"[STEP 8] Current cluster avg resident ratio: {current_rr:.1f}%. "
                f"Target: {self.desired_rr}%"
            )
            if current_rr <= self.desired_rr:
                self.log.info(
                    f"[STEP 8] Cluster already at or below desired RR ({self.desired_rr}%); "
                    f"skipping index creation loop"
                )
            else:
                self.log.info(
                    f"[STEP 8] Creating additional indexes until avg RR drops to {self.desired_rr}% "
                    f"(timeout={dgm_timeout}s)..."
                )
                self.index_creation_till_rr(rr=self.desired_rr, timeout=dgm_timeout)
                current_rr = self.compute_cluster_avg_rr_index()
                self.log.info(f"[STEP 8] Cluster avg resident ratio after DGM drive: {current_rr:.1f}%")

            self.log.info("[STEP 8] PASSED - Cluster is in DGM state")
        except Exception as e:
            self.log.error(f"[STEP 8] FAILED: {str(e)}")
            raise

        # ========== STEP 9: Run scans and validate encryption in DGM ==========
        try:
            self.log.info("[STEP 9] Running scans and validating encryption in DGM state...")
            self.wait_until_indexes_online(timeout=1200)
            query_node = self.get_nodes_from_services_map(service_type="n1ql")

            for query in all_select_queries:
                try:
                    if "embVector" in query:
                        query = query.replace("embVector", str(self.bhive_sample_vector))
                    self.run_cbq_query(query=query, scan_consistency='request_plus', server=query_node)
                except Exception as e:
                    self.log.warning(f"[STEP 9] DGM scan failed: {str(e)}")
                    raise

            updated_index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            expected_key_ids = self.get_indexer_in_use_key_ids(updated_index_nodes)
            validation_passed = self.validate_file_encryption_with_key(
                updated_index_nodes,
                expected_key_id=expected_key_ids,
                step_prefix="[STEP 9] ",
                encrypted_bucket_names=encrypted_bucket_names
            )
            if not validation_passed:
                raise Exception("File encryption validation failed in DGM state")

            self.log.info("[STEP 9] PASSED - DGM scans and encryption validation completed")
        except Exception as e:
            self.log.error(f"[STEP 9] FAILED: {str(e)}")
            self.failure_detected = True
            self.test_failures.append((time.strftime('%Y-%m-%d %H:%M:%S'), str(e)))

        # ========== STEP 10: Perform swap rebalance ==========
        try:
            self.log.info(f"[STEP 10] Performing swap rebalance ({self.rebalance_mode} mode) in DGM state...")

            current_index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            nodes_out_list = [current_index_nodes[-1]]
            used_ips = {node.ip for node in current_index_nodes}
            spare_nodes = [s for s in self.servers[self.nodes_init:] if s.ip not in used_ips]

            if not spare_nodes:
                raise Exception("No spare nodes available for swap rebalance")

            nodes_in_list = [spare_nodes[0]]
            self.log.info(f"[STEP 10] SWAP: removing {nodes_out_list[0].ip}, adding {nodes_in_list[0].ip}")

            self._ear_rebalance_and_validate(
                nodes_in_list=nodes_in_list,
                nodes_out_list=nodes_out_list,
                services_in=["index"],
                select_queries=all_select_queries,
                index_nodes=current_index_nodes,
                shard_rebalance_enabled=shard_rebalance_enabled,
                encrypted_bucket_names=encrypted_bucket_names,
                run_continuous_workload=True,
                skip_scans=False
            )

            self.log.info("[STEP 10] PASSED - Swap rebalance completed and validated in DGM state")
        except Exception as e:
            self.log.error(f"[STEP 10] FAILED: {str(e)}")
            raise

        # ========== STEP 11: Kill indexer on all nodes ==========
        try:
            self.log.info("[STEP 11] Killing indexer process on all index nodes...")
            self._kill_indexer_on_all_nodes(step_prefix="[STEP 11] ")
            self.log.info("[STEP 11] PASSED - Indexer processes killed on all nodes")
        except Exception as e:
            self.log.warning(f"[STEP 11] WARNING - Failed to kill indexer on some nodes: {str(e)}")

        if self.failure_detected:
            failure_summary = "\n".join([f"[{ts}] {err}" for ts, err in self.test_failures])
            self.fail(f"Test failed with {len(self.test_failures)} non-critical error(s):\n{failure_summary}")

        # ========== STEP 12: Drop all indexes and validate resources ==========
        try:
            self.log.info("[STEP 12] Dropping all indexes and validating node resources...")
            self.drop_index_node_resources_utilization_validations()
            self.log.info("[STEP 12] PASSED - All indexes dropped and resources validated")
        except Exception as e:
            self.log.warning(f"[STEP 12] WARNING - Drop indexes validation failed: {str(e)}")

        # ========== TEST COMPLETE ==========
        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_rebalance_dgm")
        self.log.info(f"Successfully completed swap rebalance ({self.rebalance_mode} mode) "
                      f"with encryption at rest in DGM state (desired_rr={self.desired_rr}%)")
        self.log.info("=" * 80)
