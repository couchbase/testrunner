"""Indexer_meta_data_migration.py: Tests for indexer metadata store migration/toggling.

__author__ = "Yash N Dodderi"
"""


from gsi.base_gsi import BaseSecondaryIndexingTests
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from threading import Event

class IndexerMetadataMigration(BaseSecondaryIndexingTests):
    def setUp(self):
        super(IndexerMetadataMigration, self).setUp()
        self.log.info("==============  IndexerMetadataMigration setup has started ==============")
        self.log.info("==============  IndexerMetadataMigration setup has ended ==============")

    def tearDown(self):
        self.log.info("==============  IndexerMetadataMigration tearDown has started ==============")
        super(IndexerMetadataMigration, self).tearDown()
        self.log.info("==============  IndexerMetadataMigration tearDown has completed ==============")

    def _get_total_metastore_size(self, index_nodes=None):
        """
        Returns the total metadata disk size across all indexer nodes.
        Uses the :9102/stats/metadata?debug=1 endpoint.
        """
        if index_nodes is None:
            index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        total_size = 0
        node_sizes = {}
        for node in index_nodes:
            stats = RestConnection(node).get_indexer_metastore_stats()
            disk_size = stats.get("meta_store_disk_inuse", 0)
            node_sizes[node.ip] = disk_size
            total_size += disk_size
        self.log.info(f"Metastore sizes per node: {node_sizes}, total: {total_size}")
        return total_size, node_sizes

    def _get_total_index_instances(self, index_nodes):
        total = 0
        for node in index_nodes:
            rest = RestConnection(node)
            stats = rest.get_indexer_stats()
            count = stats.get("num_indexes", 0)
            self.log.info(f"Node {node.ip}: num_indexes={count}")
            total += count
        self.log.info(f"Total index instances across cluster: {total}")
        return total

    def test_toggling_migration_flag(self):
        """
        1) Create a bunch of indexes.
        2) Validate all indexers report meta_store_type=magma via :9102/stats/metadata?debug=1.
        3) Disable magma metadata store backend via indexer settings endpoint:
             :9102/settings
           by setting indexer.metadata.store_backend=fdb.
        4) Swap rebalance one of the indexer nodes.
        5) Rebalance in another node and ensure it uses forestdb as backend.
        6) Ensure the nodes that were originally indexer nodes continue to use magma as backend.
        """
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        original_index_nodes = index_nodes
        self.enable_redistribute_indexes()

        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template=self.json_template,
                                             load_default_coll=True)
        self.sleep(10)

        select_queries = []
        index_types = [
            {"scalar": True, "bhive": False, "label": "scalar"},
            {"scalar": False, "bhive": False, "label": "composite"},
            {"scalar": False, "bhive": True, "label": "bhive"},
        ]

        for idx_type in index_types:
            prefix = f"idx_toggle_{idx_type['label']}_"
            definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template, prefix=prefix, array_indexes=False,
                bhive_index=idx_type["bhive"], scalar=idx_type["scalar"])

            for namespace in self.namespaces:
                create_queries = self.gsi_util_obj.get_create_index_list(
                    definition_list=definitions, namespace=namespace,
                    num_replica=1, bhive_index=idx_type["bhive"])
                select_queries.extend(
                    self.gsi_util_obj.get_select_queries(definition_list=definitions, namespace=namespace))
                self.gsi_util_obj.create_gsi_indexes(
                    create_queries=create_queries, database=namespace, query_node=query_node)

        self.wait_until_indexes_online(timeout=600)
        self.item_count_related_validations()

        # Validate magma is being used by all index nodes.
        self.assert_all_indexers_metastore_type(expected="magma")

        # Toggle indexer settings to disable magma-backed indexer metadata.
        self.set_indexing_metadata_store_backend(backend="fdb")
        self.sleep(5, "Sleeping after indexer setting change")

        node_in = self.servers[self.nodes_init]
        node_out = index_nodes[0]
        self.log.info(f"Starting swap rebalance. Adding {node_in.ip}, removing {node_out.ip}")
        task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init],
                                           to_add=[node_in],
                                           to_remove=[node_out],
                                           services=["index"])
        task.result()
        self.sleep(5, "Sleeping after swap rebalance")

        # Post-swap validation: swapped-in node should use fdb/forestdb backend (not magma).

        self.assert_all_indexers_metastore_type(expected="fdb", index_nodes=[node_in])
        
        node_in_2 = node_out
        self.log.info(f"Starting rebalance-in. Adding {node_in_2.ip} as index")
        task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init],
                                           to_add=[node_in_2],
                                           to_remove=[],
                                           services=["index"])
        task.result()
        self.sleep(5, "Sleeping after rebalance-in")

        self.assert_all_indexers_metastore_type(expected="fdb", index_nodes=[node_in_2])
        updated_index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        for nodes in original_index_nodes:
            if nodes in updated_index_nodes and nodes != node_in_2:
                rest = RemoteMachineShellConnection(nodes)
                rest.execute_command(command="pkill indexer")
                self.sleep(5, "Sleeping after indexer restart")
                self.assert_all_indexers_metastore_type(expected="magma", index_nodes=[nodes])

        #running scans and mutations post rebalance
        self._run_queries_continously(select_queries=select_queries,verbose=False)
        try:
            event = Event()
            self.perform_continuous_kv_mutations(event=event, timeout=300, num_docs_mutating=self.num_of_docs_per_collection)
            event.set()
            self.drop_index_node_resources_utilization_validations()
        finally:
            event.set()

    def test_10k_indexes(self):
        """
        Creates up to 10k index instances (scalar, composite, bhive) across all indexer nodes,
        runs mutations and scans, and verifies indexer metadata store is using magma.
        """
        max_instances = self.input.param("max_instances", 10000)
        num_replica = self.input.param("num_replica", 1)

        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        self.enable_redistribute_indexes()

        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template=self.json_template,
                                             load_default_coll=False)
        self.sleep(10)

        select_queries = []
        iteration = 0
        index_types = [
            {"scalar": True, "bhive": False, "label": "scalar"},
            {"scalar": False, "bhive": False, "label": "composite"},
            {"scalar": False, "bhive": True, "label": "bhive"},
        ]

        while True:
            total_instances = self._get_total_index_instances(index_nodes)
            if total_instances >= max_instances:
                self.log.info(f"Reached {total_instances} instances (max={max_instances}). Stopping index creation.")
                break

            for idx_type in index_types:
                total_instances = self._get_total_index_instances(index_nodes)
                if total_instances >= max_instances:
                    break

                prefix = f"idx_10k_{idx_type['label']}_{iteration}_"
                definitions = self.gsi_util_obj.get_index_definition_list(
                    dataset=self.json_template, prefix=prefix, array_indexes=False,
                    bhive_index=idx_type["bhive"], scalar=idx_type["scalar"])

                for namespace in self.namespaces:
                    total_instances = self._get_total_index_instances(index_nodes)
                    if total_instances >= max_instances:
                        break

                    create_queries = self.gsi_util_obj.get_create_index_list(
                        definition_list=definitions, namespace=namespace,
                        num_replica=num_replica, bhive_index=idx_type["bhive"])
                    select_queries.extend(
                        self.gsi_util_obj.get_select_queries(definition_list=definitions, namespace=namespace))
                    self.gsi_util_obj.create_gsi_indexes(
                        create_queries=create_queries, database=namespace, query_node=query_node)

            iteration += 1

        self.assertTrue(self.wait_until_indexes_online(timeout=7200),
                        "Indexes did not become Ready after build")
        self.sleep(360)

        total_instances = self._get_total_index_instances(index_nodes)
        self.log.info(f"Final total index instances: {total_instances}")

        # Run scans on all created indexes.
        self.log.info("Running scans on all indexes...")
        n1ql_server = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        self.gsi_util_obj.aysnc_run_select_queries(select_queries=select_queries, query_node=n1ql_server)
        self.log.info("Scans completed successfully")

        # Run mutations.
        self.log.info("Running mutations...")
        try:
            event = Event()
            self.perform_continuous_kv_mutations(event=event, timeout=300,
                                                 num_docs_mutating=self.num_of_docs_per_collection)
            event.set()
        finally:
            event.set()

        # Run scans again after mutations.
        self.log.info("Running scans after mutations...")
        for query in select_queries:
            self.run_cbq_query(query=query, scan_consistency='request_plus', server=n1ql_server)
        self.log.info("Post-mutation scans completed")

        # Validate magma is being used by all index nodes.
        self.assert_all_indexers_metastore_type(expected="magma", index_nodes=index_nodes)

        #drop all indexes and check for disk usage post dropping
        self.drop_index_node_resources_utilization_validations(sleep_time=900)

    def test_indexes_compaction(self):
        """
        Tests metadata compaction by creating, building, and dropping indexes to trigger compaction:
        1. Create bucket and load documents.
        2. Create a large initial batch of indexes (scalar + bhive) over multiple iterations.
        3. Wait for indexes to reach steady state for several minutes.
        4. Capture baseline compaction count (NCompacts) and initial metadata size at steady state.
        5. Create indexes (defer_build=true), both scalar and bhive types, build them, 
           let them run for 2-3 minutes, then drop them. Monitor metadata size after each cycle.
        6. Repeat step 5 until compaction count exceeds baseline (or max_cycles reached).
        7. Validate that final metadata size does not exceed 35% of the initial steady state size.
        """
        steady_state_sleep = self.input.param("steady_state_sleep", 120)
        index_lifespan = self.input.param("index_lifespan", 180)
        num_initial_index_iterations = self.input.param("num_initial_index_iterations", 3)
        max_compaction_cycles = self.input.param("max_compaction_cycles", 20)
        max_size_increase_percent = self.input.param("max_size_increase_percent", 35)

        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        self.enable_redistribute_indexes()

        # Step 1: Create bucket and load documents.
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.test_bucket = self.test_bucket + '_compaction'
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222, bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template=self.json_template,
                                             load_default_coll=False)

        namespace = self.namespaces[0]

        self.log.info("=== Compaction stats before initial index creation ===")
        initial_ncompacts_before_creation, initial_node_ncompacts_before_creation = self._get_total_ncompacts(index_nodes)
        self.log.info(f"Total NCompacts before initial index creation: {initial_ncompacts_before_creation}")
        self.log.info(f"Per-node NCompacts: {initial_node_ncompacts_before_creation}")

        # Step 2: Create a large initial batch of indexes over multiple iterations.
        self.log.info(f"Creating initial batch of indexes over {num_initial_index_iterations} iterations...")
        created_index_names = []
        for iteration in range(num_initial_index_iterations):
            self.log.info(f"--- Initial index creation iteration {iteration + 1}/{num_initial_index_iterations} ---")

            # Create scalar indexes for this iteration
            iteration_scalar_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template,
                prefix=f"idx_initial_scalar_{iteration}_",
                array_indexes=False,
                bhive_index=False,
                scalar=True)
            create_queries = self.gsi_util_obj.get_create_index_list(
                definition_list=iteration_scalar_definitions,
                namespace=namespace)
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=query_node)
            created_index_names.extend([idx.index_name for idx in iteration_scalar_definitions])
            self.log.info(f"Iteration {iteration + 1}: Created {len(iteration_scalar_definitions)} scalar indexes")

            # Create bhive indexes for this iteration
            iteration_bhive_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template,
                prefix=f"idx_initial_bhive_{iteration}_",
                array_indexes=False,
                bhive_index=True,
                scalar=False)
            create_queries = self.gsi_util_obj.get_create_index_list(
                definition_list=iteration_bhive_definitions,
                namespace=namespace,
                bhive_index=True)
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=query_node)
            created_index_names.extend([idx.index_name for idx in iteration_bhive_definitions])
            self.log.info(f"Iteration {iteration + 1}: Created {len(iteration_bhive_definitions)} bhive indexes")

        # Wait for all initial indexes to come online
        self.log.info("Waiting for all initial indexes to come online...")
        self.wait_until_indexes_online(timeout=1200)
        self.log.info(f"All {num_initial_index_iterations} iterations of initial indexes are online")
        indexes_before_comapction = self.get_all_indexes_in_the_cluster()
        self.log.info(f"indexes in the cluster before are{indexes_before_comapction} and no of indexes are {len(indexes_before_comapction)}")
        
        # Log compaction stats after initial index creation
        self.log.info("=== Compaction stats after initial index creation ===")
        initial_ncompacts_after_creation, initial_node_ncompacts_after_creation = self._get_total_ncompacts(index_nodes)
        self.log.info(f"Total NCompacts after initial index creation: {initial_ncompacts_after_creation}")
        self.log.info(f"Per-node NCompacts: {initial_node_ncompacts_after_creation}")

        self.sleep(10)

        # Step 3: Let indexes reach steady state
        self.log.info(f"Letting indexes reach steady state for {steady_state_sleep} seconds...")
        self.sleep(steady_state_sleep, "Waiting for indexes to reach steady state")

        # Step 4: Capture baseline compaction count and initial metadata size after steady state.
        baseline_ncompacts, baseline_node_ncompacts = self._get_total_ncompacts(index_nodes)
        self.log.info(f"Baseline NCompacts at steady state: {baseline_ncompacts}")
        self.log.info(f"Baseline per-node NCompacts: {baseline_node_ncompacts}")
        
        initial_total_size, initial_node_sizes = self._get_total_metastore_size(index_nodes)
        self.log.info(f"Initial metadata size at steady state: {initial_total_size}")
        cycle_metadata_sizes = []
        cycle_metadata_sizes.append(("steady_state", initial_total_size))

        # Step 5: Create, build, and drop indexes until compaction count increases
        self.log.info(f"Starting compaction trigger cycles (max {max_compaction_cycles} cycles)...")
        self.log.info("Each cycle: Create indexes (defer_build=true) -> Build -> Wait -> Drop")
        self.log.info(f"Will run until NCompacts > baseline ({baseline_ncompacts}) or max cycles reached")
        
        cycle = 0
        compaction_triggered = False
        
        while cycle < max_compaction_cycles:
            cycle += 1
            self.log.info(f"\n=== Compaction Cycle {cycle} ===")
            
            # Use defer_build for these indexes
            self.defer_build = True
            
            # Create scalar indexes (deferred build)
            cycle_scalar_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template,
                prefix=f"idx_compact_scalar_{cycle}_",
                array_indexes=False,
                bhive_index=False,
                scalar=True)
            
            scalar_create_queries = self.gsi_util_obj.get_create_index_list(
                definition_list=cycle_scalar_definitions,
                namespace=namespace, defer_build=self.defer_build)
            self.gsi_util_obj.create_gsi_indexes(create_queries=scalar_create_queries, query_node=query_node)
            scalar_index_names = [idx.index_name for idx in cycle_scalar_definitions]
            self.log.info(f"Cycle {cycle}: Created {len(scalar_index_names)} scalar indexes (defer_build=true)")
            
            # Create bhive indexes (deferred build)
            cycle_bhive_definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template,
                prefix=f"idx_compact_bhive_{cycle}_",
                array_indexes=False,
                bhive_index=True,
                scalar=False)
            
            bhive_create_queries = self.gsi_util_obj.get_create_index_list(
                definition_list=cycle_bhive_definitions,
                namespace=namespace,
                bhive_index=True, defer_build=self.defer_build)
            self.gsi_util_obj.create_gsi_indexes(create_queries=bhive_create_queries, query_node=query_node)
            bhive_index_names = [idx.index_name for idx in cycle_bhive_definitions]
            self.log.info(f"Cycle {cycle}: Created {len(bhive_index_names)} bhive indexes (defer_build=true)")
            
            # Query system:indexes to get all deferred index names for this namespace
            self.log.info(f"Cycle {cycle}: Querying for deferred indexes in namespace {namespace}...")
            

            # Build all indexes at once using correct BUILD INDEX syntax
            self.log.info(f"Cycle {cycle}: Building all deferred indexes...")
            index_names_str = ", ".join([f"`{name}`" for name in bhive_index_names+scalar_index_names])
            build_query = f"BUILD INDEX ON {namespace} ({index_names_str})"
            self.log.info(f"Build query: {build_query}")
            self.run_cbq_query(query=build_query, server=query_node)

            # Wait for indexes to build
            self.log.info(f"Cycle {cycle}: Waiting for indexes to come online...")
            self.wait_until_indexes_online(timeout=600)
            
            # Let indexes be in steady state for specified lifespan
            self.sleep(index_lifespan, f"Cycle {cycle}:indexes running")
            
            # Drop all current cycle indexes (both scalar and bhive)
            self.log.info(f"Cycle {cycle}: Dropping indexes created in this cycle...")
            all_cycle_index_names = scalar_index_names + bhive_index_names
            # Accumulate all drop queries
            drop_queries = []
            for idx_name in all_cycle_index_names:
                drop_query = f"DROP INDEX `{idx_name}` ON {namespace}"
                drop_queries.append(drop_query)
            # Execute all drop queries at once using async_create_indexes
            if drop_queries:
                self.log.info(f"Cycle {cycle}: Dropping {len(drop_queries)} indexes asynchronously")
                self.gsi_util_obj.async_create_indexes(create_queries=drop_queries, database=namespace, query_node=query_node)
            self.log.info(f"Cycle {cycle}: Dropped {len(all_cycle_index_names)} indexes ({len(scalar_index_names)} scalar, {len(bhive_index_names)} bhive)")
            self.sleep(30, "Sleeping post index drops")
            
            # Capture metadata size after cycle
            cycle_total_size, _ = self._get_total_metastore_size(index_nodes)
            cycle_metadata_sizes.append((f"cycle_{cycle}", cycle_total_size))
            
            # Calculate size increase from initial steady state
            size_diff = cycle_total_size - initial_total_size
            size_diff_percent = (size_diff / initial_total_size * 100) if initial_total_size > 0 else 0
            
            self.log.info(f"Cycle {cycle}: Metadata size = {cycle_total_size} (diff: {size_diff}, {size_diff_percent:.2f}% from initial)")
            
            # Check if compaction has been triggered
            current_ncompacts, current_node_ncompacts = self._get_total_ncompacts(index_nodes)
            self.log.info(f"Cycle {cycle}: Current NCompacts = {current_ncompacts} (baseline: {baseline_ncompacts})")
            
            if current_ncompacts > baseline_ncompacts:
                compaction_triggered = True
                self.log.info(f"Cycle {cycle}: Compaction triggered! NCompacts increased from {baseline_ncompacts} to {current_ncompacts}")
                break
            
            # Small sleep before next cycle
            self.sleep(10, f"Cycle {cycle}: Brief pause before next cycle")

        indexes_post_comapction = self.get_all_indexes_in_the_cluster()
        self.log.info(
            f"indexes in the cluster after are{indexes_post_comapction} and no of indexes are {len(indexes_post_comapction)}")
        # Step 6: Log cycle completion status
        if compaction_triggered:
            self.log.info(f"\nCompaction was triggered after {cycle} cycles")
        else:
            self.log.info(f"\nCompaction was NOT triggered after {cycle} cycles (max: {max_compaction_cycles})")
        
        # Step 7: Let cluster settle
        self.sleep(120, "Waiting for cluster to settle after compaction cycles")

        # Step 8: Capture final metadata size and validate
        final_total_size, final_node_sizes = self._get_total_metastore_size(index_nodes)
        final_ncompacts, final_node_ncompacts = self._get_total_ncompacts(index_nodes)
        
        self.log.info(f"\n=== Final Results ===")
        self.log.info(f"Total cycles run: {cycle}")
        self.log.info(f"Compaction triggered: {compaction_triggered}")
        self.log.info(f"Baseline NCompacts: {baseline_ncompacts}")
        self.log.info(f"Final NCompacts: {final_ncompacts} (increase: {final_ncompacts - baseline_ncompacts})")
        self.log.info(f"\nMetadata size at each stage:")
        for stage, size in cycle_metadata_sizes:
            diff = size - initial_total_size
            diff_percent = (diff / initial_total_size * 100) if initial_total_size > 0 else 0
            self.log.info(f"  {stage}: {size} (diff: {diff}, {diff_percent:.2f}% from initial)")
        
        self.log.info(f"\nFinal metadata size: {final_total_size}")
        size_diff = final_total_size - initial_total_size
        size_diff_percent = (size_diff / initial_total_size * 100) if initial_total_size > 0 else 0
        
        self.log.info(f"Total size increase: {size_diff} ({size_diff_percent:.2f}%)")
        
        # Log per-node comparison
        for node_ip in initial_node_sizes:
            initial = initial_node_sizes.get(node_ip, 0)
            final = final_node_sizes.get(node_ip, 0)
            diff = final - initial
            diff_percent = (diff / initial * 100) if initial > 0 else 0
            self.log.info(f"  Node {node_ip}: initial={initial}, final={final}, diff={diff} ({diff_percent:.2f}%)")
        
        self.log.info(f"\nNCompacts: {final_ncompacts}")
        
        # Validate that metadata size does not exceed max_size_increase_percent of initial
        self.assertTrue(size_diff_percent <= max_size_increase_percent, 
                        f"Metadata size increased by {size_diff_percent:.2f}%, which exceeds the maximum allowed {max_size_increase_percent}%")
        
        self.log.info(f"Compaction test completed successfully: Metadata size ({size_diff_percent:.2f}% increase) is within acceptable limit of {max_size_increase_percent}%")

    def test_corrupt_sstables_data_files(self):
        """
        Test that corrupts sstable.<number>.data files by truncating them in seqIndex folders.
        Works on both Windows and Linux platforms.
        Steps:
        1. Create all index types (bhive, composite, scalar).
        2. SSH into each indexer node using RemoteMachineShellConnection.
        3. Detect OS type (Windows/Linux) for node.
        4. Navigate to {index_path}/@2i/metadata_repo_v2.
        5. For every kvstore-<number> folder, go into all rev-<number> directories.
        6. In every seqIndex folder, truncate each sstable.<number>.data file using OS-specific commands:
           - Linux: truncate -s 0
           - Windows: Set-Content -Value $null (PowerShell)
        7. Kill indexer process on each node using OS-specific commands.
        """
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        self.enable_redistribute_indexes()

        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template=self.json_template,
                                             load_default_coll=True)
        self.sleep(10)

        select_queries = []
        index_types = [
            {"scalar": True, "bhive": False, "label": "scalar"},
            {"scalar": False, "bhive": False, "label": "composite"},
            {"scalar": False, "bhive": True, "label": "bhive"},
        ]

        for idx_type in index_types:
            prefix = f"idx_corrupt_{idx_type['label']}_"
            definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template, prefix=prefix, array_indexes=False,
                bhive_index=idx_type["bhive"], scalar=idx_type["scalar"])

            for namespace in self.namespaces:
                create_queries = self.gsi_util_obj.get_create_index_list(
                    definition_list=definitions, namespace=namespace,
                    num_replica=1, bhive_index=idx_type["bhive"])
                select_queries.extend(
                    self.gsi_util_obj.get_select_queries(definition_list=definitions, namespace=namespace))
                self.gsi_util_obj.create_gsi_indexes(
                    create_queries=create_queries, database=namespace, query_node=query_node)

        self.assertTrue(self.wait_until_indexes_online(timeout=600),
                        "Indexes did not become Ready after build")
        self.item_count_related_validations()
        self.sleep(10)

        self.log.info("Starting corruption of sstable data files by truncating...")

        corrupted_files_count = 0
        for node in index_nodes:
            self.log.info(f"Processing node: {node.ip}")
            shell = RemoteMachineShellConnection(node)
            data_dir = RestConnection(node).get_index_path()
            metadata_repo_dir = f"{data_dir}/{self.indexer_meta_data_directory}"

            # Detect OS type for this node
            info = shell.extract_remote_info()
            is_windows = info.type.lower() == 'windows'
            self.log.info(f"Node {node.ip}: OS type = {info.type}, is_windows = {is_windows}")

            # Find all sstable.<number>.data files under kvstore-*/rev-*/seqIndex/
            if is_windows:
                # Windows: use PowerShell commands
                find_cmd = f'Get-ChildItem -Path "{metadata_repo_dir}" -Directory -Filter "kvstore-*" -Recurse -ErrorAction SilentlyContinue | Select-Object FullName'
            else:
                # Linux: use find command
                find_cmd = f"find {metadata_repo_dir} -type d -name 'kvstore-*' 2>/dev/null"

            output, error = shell.execute_command(find_cmd)
            kvstore_dirs = []
            if is_windows:
                # Parse PowerShell output
                for line in output:
                    if line.strip():
                        kvstore_dirs.append(line.strip().replace("'", ""))
            else:
                # Parse Linux find output
                kvstore_dirs = [line.strip() for line in output if line.strip()]

            self.log.info(f"Node {node.ip}: Found {len(kvstore_dirs)} kvstore directories")

            for kvstore_dir in kvstore_dirs:
                # Find all rev-* directories within this kvstore
                if is_windows:
                    find_rev_cmd = f'Get-ChildItem -Path "{kvstore_dir}" -Directory -Filter "rev-*" -Recurse -ErrorAction SilentlyContinue | Select-Object FullName'
                else:
                    find_rev_cmd = f"find {kvstore_dir} -type d -name 'rev-*' 2>/dev/null"

                rev_output, rev_error = shell.execute_command(find_rev_cmd)
                rev_dirs = []

                if is_windows:
                    for line in rev_output:
                        if line.strip():
                            rev_dirs.append(line.strip().replace("'", ""))
                else:
                    rev_dirs = [line.strip() for line in rev_output if line.strip()]

                for rev_dir in rev_dirs:
                    seq_index_dir = f"{rev_dir}/seqIndex"
                    # Check if seqIndex directory exists
                    if is_windows:
                        check_cmd = f'Test-Path "{seq_index_dir}"'
                        check_output, _ = shell.execute_command(check_cmd)
                        if not check_output or 'False' in check_output[0]:
                            continue
                    else:
                        check_cmd = f"[ -d '{seq_index_dir}' ] && echo 'EXISTS' || echo 'NOT_FOUND'"
                        check_output, _ = shell.execute_command(check_cmd)
                        if not check_output or 'NOT_FOUND' in check_output[0]:
                            continue

                    # Find all sstable.<number>.data files in seqIndex
                    if is_windows:
                        find_sstable_cmd = f'Get-ChildItem -Path "{seq_index_dir}" -Filter "sstable.*.data" -Recurse -ErrorAction SilentlyContinue | Select-Object FullName'
                    else:
                        find_sstable_cmd = f"find {seq_index_dir} -type f -name 'sstable.*.data' 2>/dev/null"

                    sstable_output, _ = shell.execute_command(find_sstable_cmd)
                    sstable_files = []

                    if is_windows:
                        for line in sstable_output:
                            if line.strip():
                                sstable_files.append(line.strip().replace("'", ""))
                    else:
                        sstable_files = [line.strip() for line in sstable_output if line.strip()]

                    for sstable_file in sstable_files:
                        # Truncate file to corrupt it - OS-specific command
                        if is_windows:
                            # Windows PowerShell: truncate file
                            truncate_cmd = f'Set-Content -Path "{sstable_file}" -Value $null -NoNewline'
                            shell.execute_command(truncate_cmd)
                        else:
                            # Linux: truncate command
                            truncate_cmd = f"truncate -s 0 '{sstable_file}'"
                            shell.execute_command(truncate_cmd)

                        corrupted_files_count += 1
                        self.log.info(f"Corrupted: {sstable_file}")

            shell.disconnect()

        self.log.info(f"Total sstable data files corrupted: {corrupted_files_count}")
        # self.assertTrue(corrupted_files_count > 0, "No sstable data files were corrupted")


        self.log.info("Waiting for indexer to detect corruption...")
        self.sleep(30)
        for node in index_nodes:
            remote_connection = RemoteMachineShellConnection(node)
            info = remote_connection.extract_remote_info()
            is_windows = info.type.lower() == 'windows'

            if is_windows:
                # Windows: kill indexer process
                kill_cmd = "Stop-Process -Name indexer -Force -ErrorAction SilentlyContinue"
                remote_connection.execute_command(kill_cmd)
            else:
                # Linux: pkill indexer
                remote_connection.execute_command("pkill indexer")

            self.sleep(5)
            remote_connection.disconnect()

        try:
            self.log.info(self.index_rest.get_indexer_metadata())
        except Exception as e:
            self.log.info(e)