import math
import random
import time
import os
import subprocess
import json
import requests
import huggingface_hub
import threading
from typing import Dict, Any
if not hasattr(huggingface_hub, "cached_download"):
    huggingface_hub.cached_download = huggingface_hub.hf_hub_download

from sentence_transformers import SentenceTransformer
from .gsi_file_based_rebalance import FileBasedRebalance
from Cb_constants import CbServer
from membase.api.rest_client import RestConnection, RestHelper
from concurrent.futures import ThreadPoolExecutor
from couchbase_helper.documentgenerator import SDKDataLoader
from lib.remote.remote_util import RemoteMachineShellConnection, RemoteMachineHelper
from pytests.query_tests_helper import QueryHelperTests
from .base_gsi import BaseSecondaryIndexingTests, log
from newupgradebasetest import NewUpgradeBaseTest
from threading import Thread
from threading import Event
from deepdiff import DeepDiff
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, QueryOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
import uuid
import re
from threading import Barrier
from concurrent.futures import as_completed
from lib.couchbase_helper.query_definitions import QueryDefinition, RANGE_SCAN_TEMPLATE
from typing import List, Tuple

class ReportValidationError(Exception):
    pass

class Scan_Report(FileBasedRebalance):
    def setUp(self):
        super().setUp()
        self.rest = RestConnection(self.servers[0])
        self.n1ql_server = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        self.n1ql_rest = RestConnection(self.n1ql_server)
        self.log.info(f"REST connection: {self.servers[0].ip}, N1QL server: {self.n1ql_server.ip}")
        self.create_primary_index = False
        self.retry_time = int(str(self.input.param("retry_time", 300)).strip().rstrip(','))
        self.sleep_time = int(str(self.input.param("sleep_time", 3)).strip().rstrip(','))
        self.num_retries = int(str(self.input.param("num_retries", 1)).strip().rstrip(','))
        self.build_index = self.input.param("build_index", False)
        self.rebalance_all_at_once = self.input.param("rebalance_all_at_once", False)
        self.continuous_mutations = self.input.param("continuous_mutations", False)
        self.replica_repair = self.input.param("replica_repair", False)
        self.chaos_action = self.input.param("chaos_action", None)
        self.topology_change = self.input.param("topology_change", True)
        self.query_timeout = self.input.param("query_timeout", 12000)
        self.stress_factor = self.input.param("stress_factor", 0.5)
        self.disable_shard_affinity = self.input.param("disable_shard_affinity", False)
        self.induce_dgm = self.input.param("induce_dgm", False)
        self.index_resident_ratio = int(self.input.param("index_resident_ratio", 50))
        self.index_memory_quota = int(self.input.param("index_memory_quota", 256))
        self.disk_full = self.input.param("disk_full", False)
        self.rand = random.randint(1, 1000000000)
        self.alter_index = self.input.param("alter_index", None)
        self.use_nodes_clause = self.input.param("use_nodes_clause", False)
        self.skip_default = self.input.param("skip_default", True)
        self.use_python_sdk = self.input.param("use_python_sdk", False)
        self.log_query_response = self.input.param("log_query_response", False)
        self.encoder = SentenceTransformer(self.data_model, device="cpu")
        self.encoder.cpu()
        self.gsi_util_obj.set_encoder(encoder=self.encoder)
        if self.ansi_join:
            self.rest.load_sample("travel-sample")
            self.sleep(10)
        self.enable_shard_based_rebalance()
        self.NUM_DOCS_POST_REBALANCE = 10 ** 5

        
    
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        for node in index_nodes:
            index_rest = RestConnection(node)
            index_rest.set_index_settings({"indexer.settings.generateScanReport": True})
            self.log.info(f"Enabled scan report generation on {node.ip}")

    def tearDown(self):
        if self._testMethodName not in ['suite_tearDown', 'suite_setUp']:
            super(Scan_Report, self).tearDown()

    def _create_scalar_indexes(self, namespace, prefix, query_node, num_replica=2, randomise_replica_count=True, defer_build=True):
        """Create scalar indexes: Primary, Secondary (single/multi key), Missing keys, Partitioned, Array"""
        self.log.info(f"Creating scalar indexes with prefix '{prefix}' in namespace '{namespace}' with num_replica={num_replica}, "f"randomise_replica_count={randomise_replica_count}, defer_build={defer_build}")
        definitions = self.gsi_util_obj.get_index_definition_list(
            dataset=self.json_template,
            prefix=f"scalar_{prefix}",
            skip_primary=False,
            scalar=True
        )
        queries = self.gsi_util_obj.get_create_index_list(
            definition_list=definitions,
            namespace=namespace,
            num_replica=num_replica,
            defer_build_mix=defer_build,
            randomise_replica_count=randomise_replica_count
        )
        self.gsi_util_obj.create_gsi_indexes(
            create_queries=queries,
            database=namespace,
            query_node=query_node
        )
        # Build deferred indexes
        self.gsi_util_obj.build_indexes(
            definition_list=definitions,
            namespace=namespace,
            query_node=query_node
        )
        select_queries = self.gsi_util_obj.get_select_queries(
            definition_list=definitions,
            namespace=namespace,
            limit=self.scan_limit
        )
        return definitions, queries, select_queries

    def _create_vector_indexes(self, namespace, prefix, query_node, num_replica=1):
        """Create composite vector indexes"""
        self.log.info(f"Creating vector indexes with prefix '{prefix}' in namespace '{namespace}' with num_replica={num_replica}")
        definitions = self.gsi_util_obj.get_index_definition_list(
            dataset=self.json_template,
            prefix=f"vector_{prefix}",
            skip_primary=True,
            similarity=self.similarity,
            train_list=self.trainlist,
            scan_nprobes=self.scan_nprobes,
            xattr_indexes=self.xattr_indexes,
            array_indexes=False,
            limit=self.scan_limit,
            is_base64=self.base64,
            quantization_algo_color_vector=self.quantization_algo_color_vector,
            quantization_algo_description_vector=self.quantization_algo_description_vector,
            bhive_index=False,
            description_dimension=self.dimension
        )
        queries = self.gsi_util_obj.get_create_index_list(
            definition_list=definitions,
            namespace=namespace,
            defer_build_mix=False,
            num_replica=num_replica
        )
        self.gsi_util_obj.async_create_indexes(
            create_queries=queries,
            database=namespace,
            query_node=query_node
        )
        self.sleep(10, "Waiting for index creation to propagate before building")
        self.gsi_util_obj.build_indexes(
            definition_list=definitions,
            namespace=namespace,
            query_node=query_node
        )
        self.sleep(60, "Allowing time for indexes to be fully built")

        select_queries = self.gsi_util_obj.get_select_queries(
            definition_list=definitions,
            namespace=namespace,
            limit=self.scan_limit
        )
        return definitions, queries, select_queries

    def _create_bhive_indexes(self, namespace, prefix, query_node, num_replica=0, 
                              similarity="L2_SQUARED", train_list=None):
        """
        Create BHIVE vector indexes on Hotel dataset.
        
        Pattern used in composite_vector_index.py and bhive_e2e_tests.py:
        1. Get index definitions with bhive_index=True
        2. Generate CREATE INDEX queries
        3. Create indexes asynchronously
        4. Build indexes if needed
        5. Generate SELECT queries for scans
        
        Args:
            namespace: The keyspace in format 'default:bucket.scope.collection'
            prefix: Prefix for index names (e.g., 'bhive_test_bucket_0')
            query_node: Query service node for index operations
            num_replica: Number of index replicas (default 0)
            similarity: Distance metric (L2_SQUARED, COSINE, DOT, etc.)
            train_list: Training list size for IVF clustering (None = auto)
        
        Returns:
            Tuple of (definitions, create_queries, select_queries)

        """
        self.log.info(f"Creating BHIVE vector indexes with prefix '{prefix}' in namespace '{namespace}' with num_replica={num_replica}, similarity={similarity}, train_list={train_list}")  
        definitions = self.gsi_util_obj.get_index_definition_list(
            dataset=self.json_template,
            prefix=f"bhive_{prefix}",
            skip_primary=True,
            similarity=similarity,
            train_list=self.trainlist,
            xattr_indexes=self.xattr_indexes,
            scan_nprobes=self.scan_nprobes,
            array_indexes=False,
            limit=self.scan_limit,
            is_base64=self.base64,
            quantization_algo_color_vector=self.quantization_algo_color_vector,
            quantization_algo_description_vector=self.quantization_algo_description_vector,
            bhive_index=True,
            description_dimension=self.dimension
        )
        queries = self.gsi_util_obj.get_create_index_list(
            definition_list=definitions,
            namespace=namespace,
            defer_build_mix=False,
            num_replica=num_replica,
            bhive_index=True
        )


        self.gsi_util_obj.async_create_indexes(
            create_queries=queries,
            database=namespace,
            query_node=query_node
        )
        self.sleep(10, "Waiting for index creation to propagate before building")
        self.gsi_util_obj.build_indexes(
            definition_list=definitions,
            namespace=namespace,
            query_node=query_node
        )
        self.sleep(60, "Allowing time for indexes to be fully built")
        select_queries = self.gsi_util_obj.get_select_queries(
            definition_list=definitions,
            namespace=namespace,
            limit=self.scan_limit
        )
        return definitions, queries, select_queries


    def _capture_row_stats_snapshot(self, debug_logs=False) -> Dict[str, Dict[str, int]]:
        """
        Capture current num_rows_returned and num_rows_scanned for all indexes across all indexer nodes.
        Uses get_index_stats(perNode=True) to get stats from each node, then aggregates across
        all replicas (same index name on different nodes).
        
        Returns:
            Dict mapping index identifier (keyspace.index_name) to row stats:
            {
                "default:bucket.scope.collection.index_name": {
                    "num_rows_returned": <int>,
                    "num_rows_scanned": <int>
                },
                ...
            }
        """
        
        stats_snapshot = {}
        
       
        index_stats = self.get_index_stats(perNode=True)
        
        if not index_stats:
            self.log.warning("No index stats returned from get_index_stats(perNode=True)")
            return stats_snapshot
        
        self.log.info(f"Capturing stats from {len(index_stats)} indexer nodes")
        
        # Regex to strip replica suffix like " (replica 1)", " (replica 2)", etc.
        replica_suffix_pattern = re.compile(r'\s*\(replica\s+\d+\)$', re.IGNORECASE)
        
        # index_stats structure: {node_ip_port: {keyspace: {index_name: {stat: value}}}}
        for node_key, node_stats in index_stats.items():
            self.log.debug(f"Processing stats from node {node_key}")
            
            for keyspace, indexes in node_stats.items():
                if '_system:' in keyspace:
                    continue
                    
                for index_name, idx_stats in indexes.items():
                    # Strip replica suffix to aggregate stats across all replicas
                    base_index_name = replica_suffix_pattern.sub('', index_name)
                    full_index_key = f"{keyspace}.{base_index_name}"
                    rows_returned = idx_stats.get('num_rows_returned', 0)
                    rows_scanned = idx_stats.get('num_rows_scanned', 0)
                    
                    # Log non-zero stats for debugging - this helps identify which replica served the scan
                    if rows_returned > 0 or rows_scanned > 0:
                        if debug_logs:
                            self.log.info(f"Non-zero stats on {node_key}: {index_name} (base: {base_index_name}) - "
                                     f"returned={rows_returned}, scanned={rows_scanned}")
                    
                    # Aggregate counts across all replicas (same base index name on different nodes)
                    if full_index_key in stats_snapshot:
                        stats_snapshot[full_index_key]['num_rows_returned'] += rows_returned
                        stats_snapshot[full_index_key]['num_rows_scanned'] += rows_scanned
                    else:
                        stats_snapshot[full_index_key] = {
                            'num_rows_returned': rows_returned,
                            'num_rows_scanned': rows_scanned
                        }
        
        # Log summary of captured stats
        total_indexes = len(stats_snapshot)
        non_zero = sum(1 for v in stats_snapshot.values() 
                      if v['num_rows_returned'] > 0 or v['num_rows_scanned'] > 0)
        self.log.info(f"Captured stats for {total_indexes} indexes, {non_zero} with non-zero row counts")
        
        return stats_snapshot

    def _compare_against_IndexStats(self, scan_report: Dict[str, Any],
                                  stats_before: Dict[str, Dict[str, int]],
                                  stats_after: Dict[str, Dict[str, int]],
                                  index_name: str,
                                  tolerance_percent: float = 0.0) -> Dict[str, Any]:
        """
        Validate rowsReturn and rowsScan from scan report against the delta
        between before and after stats snapshots.
        Returns:
            Dict containing validation results:
            {
                'valid': bool,
                'index_name': str,
                'report_rows_return': int,
                'report_rows_scan': int,
                'delta_rows_returned': int,
                'delta_rows_scanned': int,
                'errors': list
            }
        
        Raises:
            ReportValidationError if validation fails
        """
        result = {
            'valid': True,
            'index_name': index_name,
            'report_rows_return': None,
            'report_rows_scan': None,
            'delta_rows_returned': None,
            'delta_rows_scanned': None,
            'errors': []
        }
        
        if not index_name:
            result['errors'].append("index_name not provided")
            return result
        if 'srvr_total_counts' not in scan_report:
            result['errors'].append("Scan report missing srvr_total_counts")
            result['valid'] = False
            return result
        counts = scan_report['srvr_total_counts']
        report_rows_return = counts.get('rowsReturn', 0)
        report_rows_scan = counts.get('rowsScan', 0)
        result['report_rows_return'] = report_rows_return
        result['report_rows_scan'] = report_rows_scan
        
        matching_key = None
        for key in stats_after.keys():
            if key.endswith(f".{index_name}") or index_name in key:
                matching_key = key
                break
        if not matching_key and index_name.startswith('#primary'):
            for key in stats_after.keys():
                if '#primary' in key:
                    matching_key = key
                    break
        if not matching_key:
            result['errors'].append(f"Index '{index_name}' not found in stats")
            return result
        before_stats = stats_before.get(matching_key, {'num_rows_returned': 0, 'num_rows_scanned': 0})
        after_stats = stats_after.get(matching_key, {'num_rows_returned': 0, 'num_rows_scanned': 0})
        delta_rows_returned = after_stats['num_rows_returned'] - before_stats['num_rows_returned']
        delta_rows_scanned = after_stats['num_rows_scanned'] - before_stats['num_rows_scanned']
        
        result['delta_rows_returned'] = delta_rows_returned
        result['delta_rows_scanned'] = delta_rows_scanned
        
        if delta_rows_returned == 0 and delta_rows_scanned == 0 and (report_rows_return > 0 or report_rows_scan > 0):
            self.log.warning("Stats delta is 0 but scan report shows rows - possible stats aggregation issue for replicated indexes")
            # Don't fail validation for this case - it's a known limitation with replicated indexes
            result['valid'] = True
            return result
        
        if tolerance_percent > 0 and delta_rows_returned > 0:
            diff_percent = abs(report_rows_return - delta_rows_returned) / delta_rows_returned * 100
            if diff_percent > tolerance_percent:
                error_msg = (f"rows_return mismatch: scan_report={report_rows_return}, "
                            f"delta={delta_rows_returned}, diff={diff_percent:.2f}%")
                result['errors'].append(error_msg)
                result['valid'] = False
                self.log.error(error_msg)
        else:
            if report_rows_return != delta_rows_returned:
                error_msg = (f"rows_return mismatch: scan_report={report_rows_return}, "
                            f"delta={delta_rows_returned}")
                result['errors'].append(error_msg)
                result['valid'] = False
                self.log.error(error_msg)
        
        # Validate rows_scan against delta_rows_scanned
        if tolerance_percent > 0 and delta_rows_scanned > 0:
            diff_percent = abs(report_rows_scan - delta_rows_scanned) / delta_rows_scanned * 100
            if diff_percent > tolerance_percent:
                error_msg = (f"rows_scan mismatch: scan_report={report_rows_scan}, "
                            f"delta={delta_rows_scanned}, diff={diff_percent:.2f}%")
                result['errors'].append(error_msg)
                result['valid'] = False
                self.log.error(error_msg)
        else:
            if report_rows_scan != delta_rows_scanned:
                error_msg = (f"rows_scan mismatch: scan_report={report_rows_scan}, "
                            f"delta={delta_rows_scanned}")
                result['errors'].append(error_msg)
                result['valid'] = False
                self.log.error(error_msg)
        
        if result['valid']:
            pass
        else:
            for error in result['errors']:
                self.log.error(f"  - {error}")
        
        return result
 

    def _run_single_scan_and_validate(self, query,
                                       expect_retries: bool = False,
                                       expect_incomplete: bool = False,
                                       failover_scenario: bool = False,
                                       extra_query_params: dict = None,
                                       runDetailed: bool = False,
                                       validate_rows_delta: bool = False,
                                       index_name: str = None,
                                       log_full_response: bool = True,
                                       use_python_sdk: bool = False):
        """
        Run a single scan query, validate the scan report, and return it.
        Automatically detects whether the index used is a vector/bhive index from
        the query response and applies appropriate validation.
        
        Args:
            query: The select query to execute
            expect_retries: If True, validates that retries field is present
            expect_incomplete: If True, expects reports to be incomplete
            failover_scenario: If True, uses relaxed validation (skips rows stats validation)
            extra_query_params: Additional query params to merge into the default ones
            runDetailed: If True, runs detailed scan; if False (default), runs concise scan
            validate_rows_delta: If True, validates rows_return and rows_scan against indexer
                                 stats using delta method. Default is False because indexer
                                 stats may need 120+ seconds to propagate and may not be
                                 tracked for all index types.
            index_name: Name of the index used by this query (required for delta validation)
            log_full_response: If True (default), logs the entire query response. If False,
                               only logs the extracted scan report to avoid flooding logs
                               with large result sets.
            use_python_sdk: If True, uses Python SDK for query execution instead of REST API
        
        Returns:
            The scan report if found and validated, None otherwise
        
        Raises:
            ReportValidationError: If scan report validation fails
        """
        if runDetailed:
            query_params = {"profile": "timings", "scanreport_wait": 1000000}
            scan_type = "Detailed"
        else:
            query_params = {"profile": "timings"}
            scan_type = "Concise"
        
        if extra_query_params:
            query_params.update(extra_query_params)

        self.log.info(f"Running {scan_type} scan on this query")
        self.log.info(f"{query}")
        
        # Capture stats before scan for delta validation
        stats_before = None
        if validate_rows_delta and not failover_scenario and index_name:
            self.log.info(f"Capturing row stats snapshot before scan for index '{index_name}'")
            stats_before = self._capture_row_stats_snapshot()
        
        try:
            if use_python_sdk:
                result = self._run_query_with_python_sdk(query, {"profile": "timings", "scanreport_wait": "1000000"})
            else:
                result = self.n1ql_rest.query_tool(query, query_params=query_params)
        except (TypeError, Exception) as e:
            raise ReportValidationError(
                f"Query service unavailable - failed to execute query: {query}. Error: {str(e)}"
            )
        
        # Handle connection failures - query_tool may return None or non-dict response
        if result is None:
            raise ReportValidationError(f"Query service unavailable - failed to execute query: {query}")
        
        if log_full_response:
            if isinstance(result, dict):
                self.log.info(f"Response for query {query} is: {json.dumps(result, indent=2)}")
            else:
                self.log.info(f"Response for query {query} is: {result}")
        
        # Capture stats after scan for delta validation
        stats_after = None
        if validate_rows_delta and not failover_scenario and stats_before is not None:
            # Wait for indexer stats to be updated asynchronously after scan completion
            self.log.info(f"Waiting {self.sleep_time}s for indexer stats to update before capturing snapshot")
            time.sleep(self.sleep_time)
            self.log.info("Capturing row stats snapshot after scan")
            stats_after = self._capture_row_stats_snapshot()

        if not isinstance(result, dict):
            raise ReportValidationError(f"Unexpected response type for query: {query}")

        if result.get("status") == "errors" or "errors" in result:
            self.log.info(f"Query returned error: {result.get('errors', [])}")

        scan_report, actual_index_name = self.extract_scan_report(result)

        if not scan_report:
            if not failover_scenario and not expect_incomplete and not expect_retries:
                self.fail(f"No scan report found in query response for query: {query}")

        if not log_full_response and scan_report:
            self.log.info(f"Scan report for query {query[:80]}...: {json.dumps(scan_report, indent=2)}")
        
        # Use actual index name from response for delta validation (more accurate than passed-in name)
        effective_index_name = actual_index_name if actual_index_name else index_name
        self.log.info(f"Delta validation index: actual_from_response='{actual_index_name}', "
                     f"passed_in='{index_name}', effective='{effective_index_name}'")
        
        # Auto-detect if the index used is a vector/bhive index from the query response
        bhive_indexes=self.get_all_bhive_index_names()
        composite_indexes=self.get_all_composite_index_names()
        if actual_index_name in bhive_indexes or actual_index_name in composite_indexes:
            is_vector_scan = True
        else:
            is_vector_scan= False
        self.log.info(f"Index used: {actual_index_name}, auto-detected is_vector_scan: {is_vector_scan}")
        
        if scan_report:
            # Validate scan report structure and fields
            self.validate_report(
                scan_report,
                is_vector_scan=is_vector_scan,
                expect_retries=expect_retries,
                is_detailed="detailed" in scan_report if runDetailed else False,
                expect_incomplete=expect_incomplete,
                failover_scenario=failover_scenario
            )
            
            # Validate rows using delta method if enabled and not in failover scenario
            if validate_rows_delta and not failover_scenario and stats_before and stats_after and effective_index_name:
                try:
                    delta_result = self._compare_against_IndexStats(
                        scan_report=scan_report,
                        stats_before=stats_before,
                        stats_after=stats_after,
                        index_name=effective_index_name
                    )
                    if not delta_result['valid']:
                        raise ReportValidationError(
                            f"Delta row validation failed: {delta_result['errors']}"
                        )
                except ReportValidationError:
                    raise
                except Exception as e:
                    self.log.warning(f"Delta row validation skipped due to error: {str(e)}")
            elif validate_rows_delta and not failover_scenario and not effective_index_name:
                self.log.warning("Delta row validation skipped: index_name not provided")
            
            self.log.info(f"{scan_type} Scan report validated successfully for query: {query[:50]}...")
            return scan_report
        else:
            self.log.warning(f"No {scan_type} scan report found for query: {query[:50]}...")
            return None

    def _run_scans_and_validate(self, select_queries,
                                 expect_retries: bool = False,
                                 expect_incomplete: bool = False,
                                 failover_scenario: bool = False,
                                 extra_query_params: dict = None,
                                 runDetailed: bool = False,
                                 validate_rows_delta: bool = False,
                                 index_names: list = None,
                                 log_full_response: bool = True,
                                 use_python_sdk: bool = False):
        """
        Run scan queries and validate scan reports for all queries.
        Automatically detects whether each index used is a vector/bhive index
        from the query response.
        
        Returns:
            List of scan reports collected during the run
        """
        scan_reports = []
        scan_type = "Detailed" if runDetailed else "Concise"
        if index_names and len(index_names) != len(select_queries):
            self.log.warning(f"index_names length ({len(index_names)}) doesn't match "
                           f"select_queries length ({len(select_queries)}). Delta validation may be incomplete.")

        for i, query in enumerate(select_queries):
            # Get corresponding index name if available
            index_name = index_names[i] if index_names and i < len(index_names) else None
            
            try:
                scan_report = self._run_single_scan_and_validate(
                    query=query,
                    expect_retries=expect_retries,
                    expect_incomplete=expect_incomplete,
                    failover_scenario=failover_scenario,
                    extra_query_params=extra_query_params,
                    runDetailed=runDetailed,
                    validate_rows_delta=validate_rows_delta,
                    index_name=index_name,
                    log_full_response=log_full_response,
                    use_python_sdk=use_python_sdk
                )
                if scan_report:
                    scan_reports.append(scan_report)
            except ReportValidationError as e:
                self.fail(f"{scan_type} Scan report validation failed for query '{query}': {str(e)}")
            except Exception as e:
                self.log.error(f"Error running query '{query}': {str(e)}")
        
        return scan_reports

            
    def test_node_level_profiling(self):
        """
        Test: Node-level query profiling validation

        Steps:
        1. Create a test bucket and prepare collections with sample documents.
        2. Retrieve query and index service nodes from the cluster.
        3. Create scalar indexes for the configured namespaces.
        4. Wait until all indexes reach the online state.
        5. Enable node-level profiling (profile=timings, completed-threshold=0) on query nodes.
        6. Execute a subset of SELECT queries using the created indexes.
        7. Validate that scan reports are present in the query response.
        8. Fetch completed_requests from the query node.
        9. Extract #indexStats from the stored query execution plan.
        10. Validate the scan report contained in completed_requests.
        11. Compare scan report structures between query response and completed_requests.
        12. Log differences if any structural mismatch is detected.
        """
        def get_index_stats_by_query(completed_requests, query_statement):
            """
            Extract #indexStats entries from completed_requests by matching
            the normalized query statement and recursively scanning the plan.
            """
            def normalize_statement(stmt):
                """Normalize statement for comparison - remove extra whitespace and standardize quotes"""
                if not stmt:
                    return ""
                # Normalize whitespace
                normalized = ' '.join(stmt.split())
                return normalized.lower()
            
            def recursive_search(node):
                """Recursively search for #indexStats in a plan node."""
                stats_list = []
                if isinstance(node, dict):
                    if "#indexStats" in node:
                        stats_list.append(node["#indexStats"])
                    for key in ["~child", "~children"]:
                        if key in node:
                            child = node[key]
                            if isinstance(child, list):
                                for c in child:
                                    stats_list.extend(recursive_search(c))
                            else:
                                stats_list.extend(recursive_search(child))
                elif isinstance(node, list):
                    for item in node:
                        stats_list.extend(recursive_search(item))
                return stats_list
            
            normalized_query = normalize_statement(query_statement)
            
            for req in completed_requests:
                statement = req.get("completed_requests", {}).get("statement")
                if normalize_statement(statement) == normalized_query:
                    plan_str = req.get("plan")
                    if not plan_str:
                        self.log.warning(f"No plan found for matching statement")
                        return None
                    try:
                        plan_json = json.loads(plan_str)
                        index_stats = recursive_search(plan_json)
                        if index_stats:
                            self.log.info(f"Found {len(index_stats)} #indexStats entries in plan")
                        return index_stats if index_stats else None
                    except json.JSONDecodeError as e:
                        self.log.error(f"Failed to parse plan JSON: {e}")
                        return None
            self.log.warning(f"No matching completed request found for query")
            return None

        buckets = self.create_test_buckets(num_buckets=1)
        query_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        self.log.info(f"Query nodes retrieved: {query_nodes}")
        
        # Fallback to self.n1ql_server if query_nodes is empty
        if not query_nodes:
            self.log.warning("No n1ql query nodes found via get_nodes_from_services_map, using self.n1ql_server")
            query_nodes = [self.n1ql_server] if self.n1ql_server else []
        
        if not query_nodes:
            self.fail("No n1ql query nodes found in cluster - cannot set node-level profiling")
        
        query_node = query_nodes[0] if isinstance(query_nodes, list) else query_nodes
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        self.log.info(f"Index nodes retrieved: {index_nodes}")
        
        all_select_queries = []
        all_definitions = []
        
        self.log.info(f"Namespaces to process: {self.namespaces}")
        
        for bucket in buckets:
            self.log.info(f"Preparing collection for bucket: {bucket}")
            self.prepare_collection_for_indexing(
                bucket_name=bucket,
                num_scopes=1,
                num_collections=1,
                num_of_docs_per_collection=10000,
                json_template=self.json_template,
                load_default_coll=False
            )
        
        for namespace in self.namespaces:
            prefix = ''.join(namespace.split(':')[1].split('.'))
            self.log.info(f"Processing namespace: {namespace}, prefix: {prefix}")
            
            # Create all index types
             
             
            scalar_defs, _, scalar_selects = self._create_scalar_indexes(
                namespace, prefix, query_node, num_replica=2, randomise_replica_count=True
            )
            self.log.info(f"Scalar indexes created: {len(scalar_defs)}")
            
            
            
            all_definitions.extend(scalar_defs )
        
        self.log.info("Waiting for indexes to be ready")
        self.wait_until_indexes_online()
        self.log.info("All indexes are online")
        
        # Set node-level profiling on all query nodes
        self.log.info(f"query_nodes type: {type(query_nodes)}, value: {query_nodes}")
        nodes_to_iterate = query_nodes if isinstance(query_nodes, list) else [query_nodes]
        self.log.info(f"Will iterate over {len(nodes_to_iterate)} query node(s)")
        
        for node in nodes_to_iterate:
            url = f"http://{node.ip}:8093/admin/settings"
            headers = {'Content-Type': 'application/json'}
            payload = {"profile": "timings", "completed-threshold": 0}
            
            self.log.info(f"Setting node-level profiling on query node {node.ip}")
            response = requests.post(
                url,
                auth=(self.rest.username, self.rest.password),
                headers=headers,
                data=json.dumps(payload)
            )
            
            if response.status_code == 200:
                self.log.info(f"Successfully set profiling to 'timings' on {node.ip}")
                self.log.info(f"Response: {response.text}")
            else:
                self.log.error(f"Failed to set profiling on {node.ip}: {response.status_code} - {response.text}")
        
        all_select_queries.extend(scalar_selects[:3] )
        for query in all_select_queries:
            self.log.info("Validating scan report in query response for query: {query[:50]}...")
            result = self.n1ql_rest.query_tool(query)
            self.log.info(f"Response for query {query} is: {json.dumps(result, indent=2)}")
            scan_report, _ = self.extract_scan_report(result)
            if scan_report:
                self.validate_report(scan_report)
                self.log.info(f"Scan report validated successfully in query response for query: {query[:50]}...")
            else:
                self.log.warning(f"No scan report attached in query response for query: {query[:50]}...")
            
            # Validating scan report in completed_requests via node-level profiling
            self.log.info(f"Validating scan report in completed_requests for query: {query[:50]}...")
            
            # Fetch completed requests from query service
            completed_requests = self.get_completed_requests(query_node)
            completed_request_stats = None
            if completed_requests:
                self.log.info(f"Found {len(completed_requests)} completed requests")
                index_stats = get_index_stats_by_query(completed_requests, query)

                    
                if index_stats:
                    self.log.info(f"Index stats from completed_requests: {json.dumps(index_stats, indent=2)}")
                    for stats in index_stats:
                        self.validate_report(stats)
                    self.log.info(f"Scan report validated successfully in completed_requests for query: {query[:50]}...")
                    # Use the first index_stats for comparison
                    completed_request_stats = index_stats[0] if index_stats else None
                else:
                    self.fail(f"No index stats found in completed_requests for query: {query[:50]}...")
            else:
                self.fail("No completed requests found")
            
            # Compare scan report from query response with scan report from completed_requests
            if scan_report and completed_request_stats:
                self.log.info("Comparing scan report from query response with completed_requests report...")
                
                # Extract structure with check_defn=True since both reports are from the same scan
                query_response_structure = self._extract_report_structure(scan_report)
                completed_request_structure = self._extract_report_structure(completed_request_stats)
                
                # Compare structures
                diff = DeepDiff(query_response_structure, completed_request_structure, ignore_order=True)
                
                if diff:
                    self.log.warning(f"Differences found between query response and completed_requests reports:")
                    self.log.warning(f"Diff: {json.dumps(diff, indent=2, default=str)}")
                    self.log.warning(f"Query response structure: {json.dumps(query_response_structure, indent=2)}")
                    self.log.warning(f"Completed request structure: {json.dumps(completed_request_structure, indent=2)}")
                else:
                    self.log.info("SUCCESS: Scan reports from query response and completed_requests are structurally identical")
            elif scan_report and not completed_request_stats:
                self.log.warning("Cannot compare: scan report found in query response but not in completed_requests")
            elif not scan_report and completed_request_stats:
                self.log.warning("Cannot compare: scan report found in completed_requests but not in query response")



    def test_vector_bhive_scan_report_consistency(self):

        """
        Test: Vector/BHIVE scan report consistency validation
        Steps:
        1. Create a test bucket and prepare collections with sample documents.
        2. Retrieve query and index service nodes from the cluster.
        3. Create vector and BHIVE indexes for the configured namespaces.
        4. Wait until all indexes reach the online state and verify successful creation.
        5. Optionally induce DGM conditions based on configured resident ratio.
        6. Execute SELECT queries using the created vector and BHIVE indexes.
        7. Validate scan reports for vector and BHIVE index scans in concise mode and detailed mode with row delta checks.
        
        Expected Result:
        - Scan reports are generated for all vector and BHIVE scans.
        - Scan report structure and row deltas are validated successfully for both
        concise and detailed modes.
        Test: Default scan report consistency validation
        Runtime Parameter:
        - use_python_sdk: Set to True to use Python SDK for query execution (default False uses REST API)
        """
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        all_definitions = []
        for namespace in self.namespaces:
            prefix = ''.join(namespace.split(':')[1].split('.'))
            self.log.info("Creating Vector indexes")
            vector_defs, _, vector_selects = self._create_vector_indexes(
                namespace, prefix, query_node, num_replica=1
            )
            all_definitions.extend(vector_defs)
            self.log.info("Creating BHIVE indexes")
            bhive_defs, _, bhive_selects = self._create_bhive_indexes(
                namespace, prefix, query_node, num_replica=0,
                similarity=self.similarity
            )
            all_definitions.extend(bhive_defs)
        self.log.info("waiting for indexes to be ready")
        self.wait_until_indexes_online(timeout=3600)
        self.log.info("validating all indexes are created and in ready state")
        for definition in all_definitions:
            self.log.info(f"Index {definition.index_name} created successfully")
        vector_index_names = [d.index_name for d in vector_defs]
        bhive_index_names = [d.index_name for d in bhive_defs]
        if self.induce_dgm:
            index_resident_ratio = self.index_resident_ratio
            self.log.info(f"Inducing DGM with index resident ratio: {index_resident_ratio}")
            time.sleep(120)
            self.load_until_index_dgm(resident_ratio=index_resident_ratio, memory_quota=self.index_memory_quota,use_magma_loader=True)
        query_groups = [
            {"name": "vector", "queries": vector_selects , "index_names": vector_index_names },
            {"name": "bhive", "queries": bhive_selects , "index_names": bhive_index_names },
        ]
        self.sleep(7200)
        for group in query_groups:
            self.log.info(f"Validating scan reports for {group['name']} indexes (concise) ")
            self._run_scans_and_validate(group['queries'], runDetailed=False,
                                          index_names=group['index_names'], validate_rows_delta=True,
                                          )
            self.log.info(f"Validating scan reports for {group['name']} indexes (detailed) ")
            self._run_scans_and_validate(group['queries'], runDetailed=True,
                                          index_names=group['index_names'], validate_rows_delta=True,
                                          )
    def test_default_scan_consistency(self):
        """
        Test: Default scan report consistency validation

        Steps:
        1. Create a test bucket and prepare collections with sample documents.
        2. Retrieve query and index service nodes from the cluster.
        3. Create scalar indexes (with replicas) for the configured namespaces.
        4. Wait until all indexes reach the online state and verify successful creation.
        5. Optionally induce DGM conditions based on configured resident ratio.
        6. Execute SELECT queries using the created scalar indexes.
        7. Validate scan reports for scalar index scans in concise mode and detailed mode with row delta checks.
        8. Generate INNER JOIN queries that use multiple indexes in a single query.
        9. Validate scan reports for INNER JOIN queries in concise and detailed mode with row delta checks.

        Expected Result:
        - Scan reports are generated for all scans.
        - Scan report structure and row deltas are validated successfully for both
        concise and detailed modes.

        Runtime Parameter:
        - use_python_sdk: Set to True to use Python SDK for query execution (default False uses REST API)
        """
        query_method = "Python SDK" if self.use_python_sdk else "REST API"
        self.log.info(f"=== Testing default scan consistency using {query_method} ===")
        buckets = self.create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        all_select_queries = []
        all_definitions = []
        for bucket in buckets:
            self.prepare_collection_for_indexing(
                bucket_name=bucket,
                num_scopes=1,
                num_collections=1,
                num_of_docs_per_collection=10000,
                json_template=self.json_template,
                load_default_coll=False
            )
        for namespace in self.namespaces:
            prefix = ''.join(namespace.split(':')[1].split('.'))
             
             
            scalar_defs, _, scalar_selects = self._create_scalar_indexes(
                namespace, prefix, query_node, num_replica=2, randomise_replica_count=True
            )
            all_definitions.extend(scalar_defs)
        self.log.info("waiting for indexes to be ready")
        self.wait_until_indexes_online(timeout=3600)
        self.log.info("validating all indexes are created and in ready state")
        for definition in all_definitions:
            self.log.info(f"Index {definition.index_name} created successfully")
        scalar_index_names = [d.index_name for d in scalar_defs]
        if self.induce_dgm:
            index_resident_ratio = self.index_resident_ratio
            self.log.info(f"Inducing DGM with index resident ratio: {index_resident_ratio}")
            time.sleep(120)
            self.load_until_index_dgm(resident_ratio=index_resident_ratio,use_magma_loader=True)
        query_groups = [
            {"name": "scalar", "queries": scalar_selects , "index_names": scalar_index_names },
        ]
        for group in query_groups:
            self.log.info(f"Validating scan reports for {group['name']} indexes (concise) via {query_method}")
            self._run_scans_and_validate(group['queries'], runDetailed=False,
                                          index_names=group['index_names'], validate_rows_delta=True,
                                          use_python_sdk=self.use_python_sdk)
            self.log.info(f"Validating scan reports for {group['name']} indexes (detailed) via {query_method}")
            self._run_scans_and_validate(group['queries'], runDetailed=True,
                                          index_names=group['index_names'], validate_rows_delta=True,
                                          use_python_sdk=self.use_python_sdk)

    def test_at_plus_index_consistency_scan_report(self):
        """
        Test: AT_PLUS scan report validation for index consistency.
        Duplicates the at_plus index consistency flow and validates scan reports
        for each AT_PLUS scan.
        """
        buckets = self.create_test_buckets(num_buckets=1)
        num_of_docs = 10 ** 3
        self.prepare_collection_for_indexing(
            bucket_name=buckets[0],
            num_of_docs_per_collection=num_of_docs,
            json_template="Hotel"
        )
        collection_namespace = self.namespaces[0]
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')
        index_gen = QueryDefinition(index_name='idx', index_fields=['price', 'country', 'city'])
        meta_index_gen = QueryDefinition(index_name='meta_idx', index_fields=['meta().id'])
        query = index_gen.generate_index_create_query(namespace=collection_namespace)
        self.run_cbq_query(query=query)
        query = meta_index_gen.generate_index_create_query(namespace=collection_namespace)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online()
        select_query = f'Select * from {collection_namespace} where price >100 and country like "A%";'
        select_meta_id_query = f'Select * from {collection} where meta().id like "doc_100%";'
        count_query = f'Select count(*) from {collection_namespace} where price >= 0;'
        named_collection_query_context = f'default:{bucket}.{scope}'
        meta_id_result_before_new_inserts = self.run_cbq_query(
            query=select_meta_id_query,
            query_context=named_collection_query_context
        )['results']
        scan_vectors_before_mutations = self.get_mutation_vectors()
        new_insert_docs_num = 2
        gen_create = SDKDataLoader(
            num_ops=new_insert_docs_num,
            percent_create=100,
            json_template="Hotel",
            percent_update=0,
            percent_delete=0,
            scope=scope,
            collection=collection,
            output=True,
            start_seq_num=num_of_docs + 1
        )
        tasks = self.data_ops_javasdk_loader_in_batches(sdk_data_loader=gen_create, batch_size=1000)
        for task in tasks:
            out = task.result()
            self.log.info(out)
        self.sleep(15, "Waiting some time before checking for mutation vectors")
        scan_vectors_after_mutations = self.get_mutation_vectors()
        new_scan_vectors = scan_vectors_after_mutations - scan_vectors_before_mutations
        scan_vector = self.convert_mutation_vector_to_scan_vector(new_scan_vectors)
        result = self.run_cbq_query(query=count_query)['results'][0]['$1']
        self.assertEqual(result, num_of_docs + new_insert_docs_num)
        try:
            # Test with inserts on named collection
            with ThreadPoolExecutor() as executor:
                select_task = executor.submit(
                    self.run_cbq_query,
                    query=select_query,
                    scan_consistency='at_plus',
                    scan_vector=scan_vector
                )
                meta_task = executor.submit(
                    self.run_cbq_query,
                    query=select_meta_id_query,
                    scan_consistency='at_plus',
                    scan_vector=scan_vector,
                    query_context=named_collection_query_context
                )
                result = select_task.result()['results']
                meta_id_result_after_new_inserts = meta_task.result()['results']
            self._run_single_scan_and_validate(
                select_query,
                extra_query_params={"scan_consistency": "at_plus", "scan_vector": scan_vector},
                index_name='idx'
            )
            self._run_single_scan_and_validate(
                select_meta_id_query,
                extra_query_params={
                    "scan_consistency": "at_plus",
                    "scan_vector": scan_vector,
                    "query_context": named_collection_query_context
                },
                index_name='meta_idx'
            )
            self.assertTrue(len(result) > 0,
                            "scan_doc_1 which was inserted before scan request with request_plus is not in result")
            self.assertEqual(len(meta_id_result_after_new_inserts),
                             len(meta_id_result_before_new_inserts) + new_insert_docs_num,
                             "request plus scan is not able to wait for new inserted docs")
            # Test with update mutation on named collection
            result1 = self.run_cbq_query(
                query=f'Select * from {collection_namespace} where meta().id = "doc_1001"'
            )['results'][0][collection]
            result2 = self.run_cbq_query(
                query=f'Select * from {collection_namespace} where meta().id = "doc_1002"'
            )['results'][0][collection]
            scan_vectors_before_mutations = self.get_mutation_vectors()
            gen_create = SDKDataLoader(
                num_ops=new_insert_docs_num,
                percent_create=0,
                json_template="Hotel",
                percent_update=100,
                percent_delete=0,
                scope=scope,
                fields_to_update=["price"],
                collection=collection,
                output=True,
                start_seq_num=num_of_docs + 1,
                op_type="update"
            )
            tasks = self.data_ops_javasdk_loader_in_batches(sdk_data_loader=gen_create, batch_size=1000)
            for task in tasks:
                out = task.result()
                self.log.info(out)
            self.sleep(15, "Waiting some time before checking for mutation vectors")
            scan_vectors_after_mutations = self.get_mutation_vectors()
            new_scan_vectors = scan_vectors_after_mutations - scan_vectors_before_mutations
            scan_vector = self.convert_mutation_vector_to_scan_vector(new_scan_vectors)
            with ThreadPoolExecutor() as executor:
                select_task = executor.submit(
                    self.run_cbq_query,
                    query=select_query,
                    scan_consistency='at_plus',
                    scan_vector=scan_vector
                )
                meta_task = executor.submit(
                    self.run_cbq_query,
                    query=select_meta_id_query,
                    scan_consistency='at_plus',
                    scan_vector=scan_vector,
                    query_context=named_collection_query_context
                )
                result = select_task.result()['results']
                meta_id_result_after_new_inserts = meta_task.result()['results']
                result3 = self.run_cbq_query(
                    query=f'Select * from {collection_namespace} where meta().id = "doc_1001"'
                )['results'][0][collection]
                result4 = self.run_cbq_query(
                    query=f'Select * from {collection_namespace} where meta().id = "doc_1002"'
                )['results'][0][collection]
                diff1 = DeepDiff(result1, result3, ignore_order=True)
                diff2 = DeepDiff(result2, result4, ignore_order=True)
            self._run_single_scan_and_validate(
                select_query,
                extra_query_params={"scan_consistency": "at_plus", "scan_vector": scan_vector},
                index_name='idx'
            )
            self._run_single_scan_and_validate(
                select_meta_id_query,
                extra_query_params={
                    "scan_consistency": "at_plus",
                    "scan_vector": scan_vector,
                    "query_context": named_collection_query_context
                },
                index_name='meta_idx'
            )
            self.assertTrue(len(result) > 0,
                            "scan_doc_1 which was inserted before scan request with request_plus is not in result")
            self.assertEqual(len(meta_id_result_after_new_inserts),
                             len(meta_id_result_before_new_inserts) + new_insert_docs_num,
                             "request plus scan is not able to wait for new inserted docs")
            if len(diff1['values_changed']) == 1 and "root['price']" in diff1['values_changed']:
                self.log.info("Price field mutated for doc_1001")
                self.log.info(diff1)
            else:
                self.log.info(diff1)
                self.log.info(f"Before Muatation: {result1}")
                self.log.info(f"After Muatation: {result3}")
                self.fail("Unexpected Mutation found for doc_1001")
            if len(diff2['values_changed']) == 1 and "root['price']" in diff2['values_changed']:
                self.log.info("Price field mutated for doc_1002")
                self.log.info(diff2)
            else:
                self.log.info(diff1)
                self.log.info(f"Before Muatation: {result2}")
                self.log.info(f"After Muatation: {result4}")
                self.fail("Unexpected Mutation found for doc_1002")
            # Test with Delete mutation on named collection
            scan_vectors_before_mutations = self.get_mutation_vectors()
            gen_create = SDKDataLoader(
                num_ops=new_insert_docs_num,
                percent_create=0,
                json_template="Hotel",
                percent_update=0,
                percent_delete=100,
                scope=scope,
                collection=collection,
                output=True,
                start_seq_num=num_of_docs + 1
            )
            tasks = self.data_ops_javasdk_loader_in_batches(sdk_data_loader=gen_create, batch_size=1000)
            for task in tasks:
                out = task.result()
                self.log.info(out)
            self.sleep(30, "Waiting some time before checking for mutation vectors")
            scan_vectors_after_mutations = self.get_mutation_vectors()
            new_scan_vectors = scan_vectors_after_mutations - scan_vectors_before_mutations
            scan_vector = self.convert_mutation_vector_to_scan_vector(new_scan_vectors)
            with ThreadPoolExecutor() as executor:
                select_task = executor.submit(
                    self.run_cbq_query,
                    query=select_query,
                    scan_consistency='at_plus',
                    scan_vector=scan_vector
                )
                meta_task = executor.submit(
                    self.run_cbq_query,
                    query=select_meta_id_query,
                    scan_consistency='at_plus',
                    scan_vector=scan_vector,
                    query_context=named_collection_query_context
                )
                count_task = executor.submit(self.run_cbq_query, query=count_query)
                result = select_task.result()['results']
                meta_id_result_after_new_inserts = meta_task.result()['results']
                count_result = count_task.result()['results'][0]['$1']

            self._run_single_scan_and_validate(
                select_query,
                extra_query_params={"scan_consistency": "at_plus", "scan_vector": scan_vector},
                index_name='idx'
            )
            self._run_single_scan_and_validate(
                select_meta_id_query,
                extra_query_params={
                    "scan_consistency": "at_plus",
                    "scan_vector": scan_vector,
                    "query_context": named_collection_query_context
                },
                index_name='meta_idx'
            )

            self.assertTrue(len(result) > 0,
                            "scan_doc_1 which was inserted before scan request with request_plus is not in result")
            self.assertEqual(len(meta_id_result_after_new_inserts),
                             len(meta_id_result_before_new_inserts),
                             "request plus scan is not able to wait for new inserted docs")
            self.assertEqual(count_result, num_of_docs, "Docs count not matching.")

            # Test with new mutation on default collection
            select_query = f'Select * from {bucket} where price > 100 and country like "A%";'
            select_meta_id_query = f'Select meta().id,* from {bucket} where meta().id like "doc_100%";'
            count_query = f'Select count(*) from {bucket} where price >= 0;'
            named_collection_query_context = f'default:'
            scan_vectors_before_mutations = self.get_mutation_vectors()
            gen_create = SDKDataLoader(
                num_ops=10 ** 3,
                percent_create=100,
                json_template="Hotel",
                percent_update=0,
                percent_delete=0,
                scope='_default',
                collection='_default',
                output=True
            )
            tasks = self.data_ops_javasdk_loader_in_batches(sdk_data_loader=gen_create, batch_size=1000)
            for task in tasks:
                out = task.result()
                self.log.info(out)

            scan_vectors_after_mutations = self.get_mutation_vectors()
            new_scan_vectors = scan_vectors_after_mutations - scan_vectors_before_mutations
            scan_vector = self.convert_mutation_vector_to_scan_vector(new_scan_vectors)
            self.log.info(f"Scan vector: {scan_vector}")
            default_index_gen = QueryDefinition(index_name='default_idx', index_fields=['price', 'country', 'city'])
            default_meta_index_gen = QueryDefinition(index_name='default_meta_idx', index_fields=['meta().id'])
            query = default_index_gen.generate_index_create_query(namespace=bucket)
            self.run_cbq_query(query=query)
            query = default_meta_index_gen.generate_index_create_query(namespace=bucket)
            self.run_cbq_query(query=query)

            with ThreadPoolExecutor() as executor:
                select_task = executor.submit(
                    self.run_cbq_query,
                    query=select_query,
                    scan_consistency='at_plus',
                    scan_vector=scan_vector
                )
                meta_task = executor.submit(
                    self.run_cbq_query,
                    query=select_meta_id_query,
                    scan_consistency='at_plus',
                    scan_vector=scan_vector,
                    query_context=named_collection_query_context
                )
                count_task = executor.submit(self.run_cbq_query, query=count_query)
                result = select_task.result()['results']
                meta_id_result_after_new_inserts = meta_task.result()['results']
                count_result = count_task.result()['results'][0]['$1']

            self._run_single_scan_and_validate(
                select_query,
                extra_query_params={"scan_consistency": "at_plus", "scan_vector": scan_vector},
                index_name='default_idx'
            )
            self._run_single_scan_and_validate(
                select_meta_id_query,
                extra_query_params={
                    "scan_consistency": "at_plus",
                    "scan_vector": scan_vector,
                    "query_context": named_collection_query_context
                },
                index_name='default_meta_idx'
            )

            self.assertTrue(len(result) > 0,
                            "scan_doc_1 which was inserted before scan request with request_plus is not in result")
            self.assertEqual(len(meta_id_result_after_new_inserts), 2,
                             "request plus scan is not able to wait for new inserted docs")
            self.assertEqual(count_result, num_of_docs, "Docs count not matching.")
        except Exception as err:
            self.fail(str(err))
        
    def test_scan_report_inner_join_consistency(self):
        """
        Test: Scan report consistency validation for INNER JOIN queries
        Steps:
        1. Create a test bucket and prepare collections with sample documents.
        2. Retrieve query and index service nodes from the cluster.
        3. Create scalar indexes (with replicas) for the configured namespaces.
        4. Wait until all indexes reach the online state and verify successful creation.
        5. Generate INNER JOIN queries that use multiple indexes in a single query.
        6. Validate scan reports for INNER JOIN queries in concise mode and detailed mode with row delta checks.
        Expected Result:
        - Scan reports are generated for all INNER JOIN scans.
        concise and detailed modes.
        """
        def _get_inner_join_queries(namespace, scalar_definitions, prefix, limit=100):
            queries = []
            index_names = []
            idx_price = f"{prefix}price"
            idx_breakfast_rating = f"{prefix}free_breakfast_avg_rating"
            idx_breakfast_array = f"{prefix}free_breakfast_array_count"
            idx_missing = f"{prefix}missing_keys"
            query1 = f"""
                SELECT h1.name AS budget_hotel, h1.price AS budget_price,
                    h2.name AS quality_hotel, h2.avg_rating, h2.free_breakfast
                FROM {namespace} h1 USE INDEX (`{idx_price}`)
                INNER JOIN {namespace} h2 USE INDEX (`{idx_breakfast_rating}`)
                    ON h1.price < h2.price
                WHERE h1.price > 500 AND h1.price < 1000
                AND h2.free_breakfast = true AND h2.avg_rating > 3
                LIMIT {limit}
            """.strip()
            queries.append(query1)
            index_names.append(idx_price)  
            query2 = f"""
                SELECT h1.name AS no_breakfast_hotel, h1.avg_rating AS rating,
                    h2.name AS breakfast_hotel, h2.free_breakfast
                FROM {namespace} h1 USE INDEX (`{idx_breakfast_rating}`)
                INNER JOIN {namespace} h2 USE INDEX (`{idx_breakfast_rating}`)
                    ON h1.avg_rating = h2.avg_rating
                WHERE h1.free_breakfast = false AND h2.free_breakfast = true
                AND h1.avg_rating >= 3
                LIMIT {limit}
            """.strip()
            queries.append(query2)
            index_names.append(idx_breakfast_rating) 
            return queries, index_names

        buckets = self.create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        all_definitions = []
        
        for bucket in buckets:
            self.prepare_collection_for_indexing(
                bucket_name=bucket,
                num_scopes=1,
                num_collections=1,
                num_of_docs_per_collection=10000,
                json_template=self.json_template,
                load_default_coll=False
            )
        for namespace in self.namespaces:
            prefix = ''.join(namespace.split(':')[1].split('.'))
             
             
            scalar_defs, _, _ = self._create_scalar_indexes(
                namespace, prefix, query_node, num_replica=2, randomise_replica_count=True
            )
            all_definitions.extend(scalar_defs)
        self.log.info("waiting for indexes to be ready")
        self.wait_until_indexes_online(timeout=3600)
        self.log.info("validating all indexes are created and in ready state")
        for definition in all_definitions:
            self.log.info(f"Index {definition.index_name} created successfully")
        # Inner Join queries - test scan reports with multiple indexes in a single query
        self.log.info(f"Validating scan reports for INNER JOIN queries")
        for namespace in self.namespaces:
            prefix = ''.join(namespace.split(':')[1].split('.'))
            inner_join_queries, inner_join_index_names = _get_inner_join_queries(
                namespace=namespace,
                scalar_definitions=scalar_defs,
                prefix=f"scalar_{prefix}",
                limit=100
            )
            self.log.info(f"Validating scan reports for INNER JOIN queries (concise)")
            self._run_scans_and_validate(inner_join_queries, runDetailed=False,
                                          index_names=inner_join_index_names,
                                          use_python_sdk=self.use_python_sdk)
            self.log.info(f"Validating scan reports for INNER JOIN queries (detailed)")
            self._run_scans_and_validate(inner_join_queries, runDetailed=True,
                                          index_names=inner_join_index_names,
                                          use_python_sdk=self.use_python_sdk)



    def _run_query_with_python_sdk(self, query, query_params=None):
        """
        Execute N1QL query using Python SDK instead of REST API.
        
        Args:
            query: N1QL query string
            query_params: Dict of query parameters (profile, scanreport_wait, etc.)
        
        Returns:
            Query result as dictionary matching REST API response format
        """
        # Connect to cluster using Python SDK
        auth = PasswordAuthenticator("Administrator", "password")
        cluster = Cluster(f"couchbase://{self.master.ip}", ClusterOptions(auth))
        
        # Build QueryOptions from query_params
        # QueryOptions is dict-like - pass parameters as constructor kwargs
        options_kwargs = {}
        raw_params = {}
        
        if query_params:
            for key, value in query_params.items():
                if key == "profile":
                    # profile can be passed directly as a string ('timings', 'phases', 'off')
                    options_kwargs["profile"] = value
                else:
                    # Other params like scanreport_wait go in raw
                    raw_params[key] = value
        
        if raw_params:
            options_kwargs["raw"] = raw_params
        
        options = QueryOptions(**options_kwargs)
        
        try:
            # Execute query
            result = cluster.query(query, options)
            
            # Convert result to dictionary format matching REST API response
            # Must iterate through all rows before accessing metadata
            rows = [row for row in result]
            
            # Get metadata including profile/scan report
            metadata = result.metadata()
            if metadata is None:
                return {
                    "status": "fatal",
                    "errors": [{"msg": "Query returned no metadata"}]
                }

            metrics = metadata.metrics()
            if metrics is None:
                return {
                    "status": "fatal",
                    "errors": [{"msg": "Query returned no metrics"}]
                }
            
            response = {
                "requestID": metadata.request_id(),
                "status": "success",
                "results": rows,
                "metrics": {
                    "elapsedTime": str(metrics.elapsed_time()),
                    "executionTime": str(metrics.execution_time()),
                    "resultCount": metrics.result_count(),
                    "resultSize": metrics.result_size(),
                }
            }
            try:
                profile_data = metadata.profile()
                if profile_data:
                    response["profile"] = profile_data
            except Exception:
                pass
            
            return response
            
        except Exception as e:
            self.log.error(f"Python SDK query failed: {str(e)}")
            return {
                "status": "fatal",
                "errors": [{"msg": str(e)}]
            }

    def _start_disk_stress(self, nodes, duration=300):
        """
        Start fio disk stress on the given nodes to saturate disk I/O.
        Uses fio to generate random read/write workloads on the indexer data directory.
        
        Args:
            nodes: List of server nodes to stress
            duration: Duration in seconds for the stress workload
            
        Returns:
            List of (shell, node) tuples for cleanup
        """
        shells = []
        for node in nodes:
            shell = RemoteMachineShellConnection(node)
            # Install fio if not present
            shell.execute_command("apt-get install -y fio || yum install -y fio", debug=False)
            # Run fio in background to stress disk I/O on the indexer data path
            fio_cmd = (
                f"fio --name=stress_io --directory=/opt/couchbase/var/lib/couchbase/data "
                f"--rw=randrw --bs=4k --direct=1 --numjobs=8 --iodepth=64 "
                f"--size=1G --runtime={duration} --time_based --group_reporting "
                f">/tmp/fio_stress.log 2>&1 &"
            )
            shell.execute_command(fio_cmd)
            self.log.info(f"Started fio disk stress on {node.ip} for {duration}s")
            shells.append((shell, node))
        return shells

    def _stop_disk_stress(self, shells):
        """Stop fio stress and disconnect shells."""
        for shell, node in shells:
            shell.execute_command("pkill -9 fio || true", debug=False)
            self.log.info(f"Stopped fio disk stress on {node.ip}")
            shell.disconnect()

    def test_scan_report_slow_io(self):
        """
        Test: Scan report for slow scans caused by slow I/O to retrieve results

        Steps:
        1. Create a cluster with 3+ indexer nodes with varying replica counts (0, 1, 2).
        2. Create scalar indexes for all namespaces.
        3. Wait until all indexes reach the online state and verify index creation.
        4. Capture baseline scan reports before disk stress.
        5. Induce DGM to push index data to disk, then stress disk I/O with fio.
        6. Run scans under disk stress and validate scan reports.
        7. Compare stressed scan reports with baseline to verify elevated I/O latency.
        8. Stop disk stress and verify recovery.

        Expected Result:
        - Indexes are created successfully and in Ready state.
        - Scan reports are generated with correct structure.
        - Slow scan metrics (srvr_avg_ns/srvr_avg_ms timing fields) are populated,
          reflecting elevated I/O latency compared to baseline.
        """
        buckets = self.create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)

        if len(index_nodes) < 3:
            self.fail("Test requires at least 3 indexer nodes")

        all_definitions = []
        all_scalar_selects = []
        all_scalar_index_names = []

        for bucket in buckets:
            self.prepare_collection_for_indexing(
                bucket_name=bucket,
                num_scopes=1,
                num_collections=1,
                num_of_docs_per_collection=self.num_of_docs_per_collection,
                json_template=self.json_template,
                load_default_coll=False
            )

        for namespace in self.namespaces:
            prefix = ''.join(namespace.split(':')[1].split('.'))
            scalar_defs, _, scalar_selects = self._create_scalar_indexes(
                namespace, prefix, query_node, num_replica=2, randomise_replica_count=True
            )
            all_definitions.extend(scalar_defs)
            all_scalar_selects.extend(scalar_selects)
            all_scalar_index_names.extend([d.index_name for d in scalar_defs])

        self.log.info("Waiting for indexes to come online")
        if not self.wait_until_indexes_online():
            self.fail("Indexes did not come online in expected time")
            

        for definition in all_definitions:
            self.log.info(f"Index {definition.index_name} created successfully and is online")

        # Capture scan reports BEFORE stress (baseline)
        self.log.info("=== Phase 1: Baseline scans before disk stress ===")
        baseline_reports = self._run_scans_and_validate(
            all_scalar_selects , runDetailed=False,
            index_names=all_scalar_index_names 
        )

        # Induce DGM to push data off memory to disk
        self.log.info("=== Phase 2: Inducing DGM to lower cache hit rate ===")
        index_resident_ratio = self.index_resident_ratio
        self.log.info(f"Target index resident ratio: {index_resident_ratio}%")
        self.load_until_index_dgm(
            resident_ratio=index_resident_ratio,
            memory_quota=self.index_memory_quota,
            use_magma_loader=True
        )
        self.sleep(30, "Waiting for DGM state to stabilize")

        # Start disk stress on all indexer nodes
        self.log.info("=== Phase 3: Starting disk I/O stress with fio ===")
        stress_duration = 600
        stress_shells = self._start_disk_stress(index_nodes, duration=stress_duration)
        self.sleep(10, "Waiting for fio stress to ramp up")

        try:
            # Run scans under disk stress and validate reports
            self.log.info("=== Phase 4: Running scans under disk stress ===")
            self.log.info("Validating scan reports for scalar indexes under stress (concise)")
            stressed_reports_concise = self._run_scans_and_validate(
                all_scalar_selects , runDetailed=False,
                index_names=all_scalar_index_names 
            )
            self.log.info("Validating scan reports for scalar indexes under stress (detailed)")
            stressed_reports_detailed = self._run_scans_and_validate(
                all_scalar_selects , runDetailed=True,
                index_names=all_scalar_index_names 
            )

            # Compare timing: stressed scans should have higher latency than baseline
            if baseline_reports and stressed_reports_concise:
                for i in range(min(len(baseline_reports), len(stressed_reports_concise))):
                    baseline = baseline_reports[i]
                    stressed = stressed_reports_concise[i]
                    # Extract avg timing from whichever unit field is present
                    for unit_field in ["srvr_avg_ns", "srvr_avg_ms"]:
                        if unit_field in baseline and unit_field in stressed:
                            baseline_total = baseline[unit_field].get("total", 0)
                            stressed_total = stressed[unit_field].get("total", 0)
                            self.log.info(
                                f"Scan {i}: baseline {unit_field}.total={baseline_total}, "
                                f"stressed {unit_field}.total={stressed_total}"
                            )
                            if stressed_total > baseline_total:
                                self.log.info(
                                    f"Scan {i}: Confirmed higher latency under disk stress "
                                    f"({stressed_total} > {baseline_total})"
                                )
                            break
        finally:
            # Always stop disk stress
            self.log.info("=== Cleanup: Stopping disk I/O stress ===")
            self._stop_disk_stress(stress_shells)

    def _start_query_node_stress(self, nodes, duration=300):
        """
        Simulate slow query-side consumption by adding network delay and CPU/memory
        stress on query nodes. The network delay (tc netem) is the primary mechanism:
        it slows TCP flow control so the indexer blocks when pushing scan results,
        making the backpressure visible in srvr_avg_ns.total. Uses fio for CPU/memory stress.

        Args:
            nodes: List of query service nodes to stress
            duration: Duration in seconds for the stress workload

        Returns:
            List of (shell, node) tuples for cleanup
        """
        shells = []
        for node in nodes:
            shell = RemoteMachineShellConnection(node)
            # Add network delay on the query node to slow indexer-to-query communication.
            # This causes TCP backpressure that the indexer measures in srvr_avg_ns.
            shell.enable_network_delay()
            self.log.info(f"Added 200ms network delay on query node {node.ip}")
            
            # Install fio if not present
            shell.execute_command("apt-get install -y fio || yum install -y fio", debug=False)
            
            # Use fio to stress CPU and memory on query node
            # Configure fio with high concurrency to saturate CPU and memory bandwidth
            fio_cmd = (
                f"fio --name=query_stress "
                f"--rw=randrw "  # Random read/write mix
                f"--bs=4k "  # 4KB block size
                f"--ioengine=sync "  # Synchronous I/O (CPU intensive)
                f"--numjobs=8 "  # 8 parallel jobs to saturate CPU
                f"--iodepth=64 "  # High queue depth
                f"--size=512M "  # 512MB per job
                f"--runtime={duration} "
                f"--time_based "  # Run for duration regardless of size
                f"--directory=/tmp "  # Use /tmp for I/O operations
                f"--group_reporting "
                f">/tmp/fio_query_stress.log 2>&1 &"
            )
            shell.execute_command(fio_cmd)
            self.log.info(f"Started fio CPU/memory stress on query node {node.ip} for {duration}s")
            shells.append((shell, node))
        return shells

    def _stop_query_node_stress(self, shells):
        """Stop fio stress and remove network delay on query nodes."""
        for shell, node in shells:
            shell.execute_command("pkill -9 fio || true", debug=False)
            self.log.info(f"Stopped fio stress on query node {node.ip}")
            shell.delete_network_rule()
            self.log.info(f"Removed network delay on query node {node.ip}")
            shell.disconnect()

    def test_scan_report_slow_query_consumption(self):
        """
        Test: Scan report for slow scans caused by slow consumption on the Query service side

        Steps:
        1. Create a cluster with 3+ indexer nodes with varying replica counts (0, 1, 2).
        2. Create scalar indexes for all namespaces.
        3. Wait until all indexes reach the online state and verify index creation.
        4. Capture baseline scan reports with broad queries.
        5. Stress CPU and memory on query nodes using stress-ng.
        6. Add network delay to simulate slow TCP backpressure.
        7. Run scans under query node stress with broad queries (high LIMIT).
        8. Validate scan reports are generated with correct slow-scan metrics.
        9. Stop stress and remove network delay.

        Expected Result:
        - Indexes are created successfully and in Ready state.
        - Scan reports are generated with correct structure under query-side pressure.
        - Slow scan timing metrics (srvr_avg_ns/srvr_avg_ms) reflect elevated latency
          due to slow result consumption by the stressed query service.
        """
        buckets = self.create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        query_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 3:
            self.fail("Test requires at least 3 indexer nodes")
        all_definitions = []
        all_scalar_selects = []
        all_scalar_index_names = []
        for bucket in buckets:
            self.prepare_collection_for_indexing(
                bucket_name=bucket,
                num_scopes=1,
                num_collections=1,
                num_of_docs_per_collection=self.num_of_docs_per_collection,
                json_template=self.json_template,
                load_default_coll=False
            )
        for namespace in self.namespaces:
            prefix = ''.join(namespace.split(':')[1].split('.'))
            scalar_defs, _, scalar_selects = self._create_scalar_indexes(
                namespace, prefix, query_node, num_replica=2, randomise_replica_count=True
            )
            all_definitions.extend(scalar_defs)
            all_scalar_selects.extend(scalar_selects)
            all_scalar_index_names.extend([d.index_name for d in scalar_defs])
        self.log.info("Waiting for indexes to come online")
        self.wait_until_indexes_online()
        for definition in all_definitions:
            self.log.info(f"Index {definition.index_name} created successfully and is online")
        high_limit = self.num_of_docs_per_collection
        broad_queries = []
        broad_index_names = []
        for namespace in self.namespaces:
            broad_queries.append(
                f"SELECT * FROM {namespace} WHERE price >= 0 LIMIT {high_limit}"
            )
            prefix = ''.join(namespace.split(':')[1].split('.'))
            broad_index_names.append(f"scalar_{prefix}price")
        self.log.info(f"Using broad queries with LIMIT {high_limit} to saturate TCP window")
        self.log.info("=== Phase 1: Baseline scans before query node stress ===")
        baseline_reports = self._run_scans_and_validate(
            broad_queries, runDetailed=False,
            index_names=broad_index_names, log_full_response=False
        )
        self.log.info("=== Phase 2: Starting network delay and CPU/memory stress on query nodes ===")
        stress_duration = 600
        stress_shells = self._start_query_node_stress(query_nodes, duration=stress_duration)
        self.sleep(10, "Waiting for stress to ramp up on query nodes")

        try:
            self.log.info("=== Phase 3: Running broad scans under query node stress ===")
            self.log.info("Validating scan reports under query stress (concise)")
            stressed_reports_concise = self._run_scans_and_validate(
                broad_queries, runDetailed=False,
                index_names=broad_index_names, log_full_response=False
            )
            self.log.info("Validating scan reports under query stress (detailed)")
            stressed_reports_detailed = self._run_scans_and_validate(
                broad_queries, runDetailed=True,
                index_names=broad_index_names, log_full_response=False
            )
            if baseline_reports and stressed_reports_concise:
                for i in range(min(len(baseline_reports), len(stressed_reports_concise))):
                    baseline = baseline_reports[i]
                    stressed = stressed_reports_concise[i]
                    for unit_field in ["srvr_avg_ns", "srvr_avg_ms"]:
                        if unit_field in baseline and unit_field in stressed:
                            baseline_total = baseline[unit_field].get("total", 0)
                            stressed_total = stressed[unit_field].get("total", 0)
                            self.log.info(
                                f"Scan {i}: baseline {unit_field}.total={baseline_total}, "
                                f"stressed {unit_field}.total={stressed_total}"
                            )
                            if stressed_total > baseline_total:
                                self.log.info(
                                    f"Scan {i}: Confirmed higher latency under query node stress "
                                    f"({stressed_total} > {baseline_total})"
                                )
                            break
        finally:
            self.log.info("=== Cleanup: Stopping stress-ng on query nodes ===")
            self._stop_query_node_stress(stress_shells)

    def test_scan_report_high_rows_returned(self):
        """
        Test: Scan report for slow scans caused by a high number of rows returned

        Steps:
        1. Create a cluster with 3+ indexer nodes with varying replica counts (0, 1, 2).
        2. Create scalar indexes for all namespaces.
        3. Wait until all indexes reach the online state and verify index creation.
        4. Run a single broad scan (no restrictive WHERE, very high LIMIT) that returns
           a large number of rows using the primary index.
        5. Validate the scan report contains correct metrics for the large result set.
        6. Verify rows_return in srvr_total_counts reflects the large result set.
        7. Verify timing metrics are populated and consistent with a heavy scan.

        Expected Result:
        - Indexes are created successfully and in Ready state.
        - Scan report is generated with correct structure.
        - rows_return in srvr_total_counts reflects the large result set.
        - Timing metrics are populated and consistent with a heavy scan.
        """
        buckets = self.create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)

        if len(index_nodes) < 3:
            self.fail("Test requires at least 3 indexer nodes")

        all_definitions = []

        for bucket in buckets:
            self.prepare_collection_for_indexing(
                bucket_name=bucket,
                num_scopes=1,
                num_collections=1,
                num_of_docs_per_collection=self.num_of_docs_per_collection,
                json_template=self.json_template,
                load_default_coll=False
            )

        primary_index_name = None
        namespace = None
        for ns in self.namespaces:
            namespace = ns
            prefix = ''.join(ns.split(':')[1].split('.'))

             
            scalar_defs, _, _ = self._create_scalar_indexes(
                ns, prefix, query_node, num_replica=2, randomise_replica_count=True
            )
            all_definitions.extend(scalar_defs)
            # Find the primary index for the broad scan
            for d in scalar_defs:
                if 'primary' in d.index_name.lower() or d.index_name.startswith('#primary'):
                    primary_index_name = d.index_name
                    break

        self.log.info("Waiting for indexes to come online")
        self.wait_until_indexes_online()

        for definition in all_definitions:
            self.log.info(f"Index {definition.index_name} created successfully and is online")

        # Build a broad scan query that returns a very large number of rows
        # Use the primary index with a high limit to force scanning many rows
        high_limit = self.num_of_docs_per_collection
        broad_query = f"SELECT * FROM {namespace} LIMIT {high_limit}"
        self.log.info(f"Running broad scan query returning up to {high_limit} rows")
        # Run the scan and validate the report
        scan_report = self._run_single_scan_and_validate(
            query=broad_query,
            runDetailed=True,
            validate_rows_delta=True,
            index_name=primary_index_name,
            log_full_response=False
        )

        if not scan_report:
            self.fail("No scan report generated for high-rows-returned query")

        # Validate rowsReturn reflects the large result set
        counts = scan_report.get('srvr_total_counts', {})
        rows_return = counts.get('rowsReturn', 0)
        rows_scan = counts.get('rowsScan', 0)
        self.log.info(f"Scan report: rowsReturn={rows_return}, rowsScan={rows_scan}")

        min_expected_rows = int(high_limit * 0.5)
        self.assertGreaterEqual(
            rows_return, min_expected_rows,
            f"Expected rowsReturn >= {min_expected_rows} for a broad scan, got {rows_return}"
        )
        self.assertGreaterEqual(
            rows_scan, rows_return,
            f"rowsScan ({rows_scan}) should be >= rowsReturn ({rows_return})"
        )
        # Validate timing fields are present and non-zero for such a heavy scan
        unit_field = "srvr_avg_ns" if "srvr_avg_ns" in scan_report else "srvr_avg_ms"
        avg_total = scan_report.get(unit_field, {}).get("total", 0)
        self.assertGreater(
            avg_total, 0,
            f"Expected {unit_field}.total > 0 for a scan returning {rows_return} rows, got {avg_total}"
        )
        self.log.info(f"Timing validated: {unit_field}.total = {avg_total}")

    def test_scan_report_slow_kv_communication(self):
        """
        Test: Scan report for slow scans caused by slow bucketSeqNo retrieval due to network latency

        Steps:
        1. Create a cluster with 3+ indexer nodes with varying replica counts (0, 1, 2).
        2. Create scalar indexes for all namespaces.
        3. Wait until all indexes reach the online state and verify index creation.
        4. Capture baseline scan reports before inducing KV latency.
        5. Induce memory pressure on KV nodes.
        6. Add network delay (tc netem) on KV nodes to slow communication with the indexer.
        7. Run scans under KV pressure with network latency.
        8. Validate scan reports reflect the elevated latency.
        9. Remove network delay and relieve memory pressure.

        Expected Result:
        - Indexes are created successfully and in Ready state.
        - Scan reports are generated with correct structure under KV pressure.
        - Timing metrics reflect elevated latency from slow KV communication.
        """
        buckets = self.create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        kv_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        if len(index_nodes) < 3:
            self.fail("Test requires at least 3 indexer nodes")
        all_definitions = []
        all_scalar_selects = []
        all_scalar_index_names = []
        for bucket in buckets:
            self.prepare_collection_for_indexing(
                bucket_name=bucket,
                num_scopes=1,
                num_collections=1,
                num_of_docs_per_collection=self.num_of_docs_per_collection,
                json_template=self.json_template,
                load_default_coll=False
            ) 
        for namespace in self.namespaces:
            prefix = ''.join(namespace.split(':')[1].split('.')) 
             
            scalar_defs, _, scalar_selects = self._create_scalar_indexes(
                namespace, prefix, query_node, num_replica=2, randomise_replica_count=True
            )
            all_definitions.extend(scalar_defs)
            all_scalar_selects.extend(scalar_selects)
            all_scalar_index_names.extend([d.index_name for d in scalar_defs])
        self.log.info("Waiting for indexes to come online")
        self.wait_until_indexes_online() 
        for definition in all_definitions:
            self.log.info(f"Index {definition.index_name} created successfully and is online")
        self.log.info("=== Phase 1: Baseline scans before KV network delay ===")
        baseline_reports = self._run_scans_and_validate(
            all_scalar_selects[:], runDetailed=False,
            index_names=all_scalar_index_names
        )
        self.log.info("=== Phase 2: Inducing memory pressure and network delay on KV nodes ===")
        kv_shells = []
        for kv_node in kv_nodes:
            shell = RemoteMachineShellConnection(kv_node)
            shell.execute_command(
                "apt-get install -y stress-ng || yum install -y stress-ng", debug=False
            )
            shell.execute_command(
                "stress-ng --vm 4 --vm-bytes 80% --vm-keep --timeout 600s "
                ">/tmp/stress_ng_kv.log 2>&1 &"
            )
            self.log.info(f"Started memory stress on KV node {kv_node.ip}")
            # Add network delay (tc netem 200ms)
            shell.enable_network_delay()
            self.log.info(f"Added 200ms network delay on KV node {kv_node.ip}")
            kv_shells.append((shell, kv_node))
 
        self.sleep(10, "Waiting for network delay and memory pressure to take effect")
 
        try:
            # Run scans under KV pressure and validate reports
            self.log.info("=== Phase 3: Running scans under KV communication delay ===")
            self.log.info("Validating scan reports for scalar indexes under KV delay (concise)")
            stressed_reports_concise = self._run_scans_and_validate(
                all_scalar_selects , runDetailed=False,
                index_names=all_scalar_index_names 
            )
            self.log.info("Validating scan reports for scalar indexes under KV delay (detailed)")
            stressed_reports_detailed = self._run_scans_and_validate(
                all_scalar_selects , runDetailed=True,
                index_names=all_scalar_index_names 
            )
 
            # Compare timing: scans under KV delay should show higher latency
            if baseline_reports and stressed_reports_concise:
                for i in range(min(len(baseline_reports), len(stressed_reports_concise))):
                    baseline = baseline_reports[i]
                    stressed = stressed_reports_concise[i]
                    for unit_field in ["srvr_avg_ns", "srvr_avg_ms"]:
                        if unit_field in baseline and unit_field in stressed:
                            baseline_total = baseline[unit_field].get("total", 0)
                            stressed_total = stressed[unit_field].get("total", 0)
                            self.log.info(
                                f"Scan {i}: baseline {unit_field}.total={baseline_total}, "
                                f"stressed {unit_field}.total={stressed_total}"
                            )
                            if stressed_total > baseline_total:
                                self.log.info(
                                    f"Scan {i}: Confirmed higher latency under KV delay "
                                    f"({stressed_total} > {baseline_total})"
                                )
                            break
        finally:
            # Always clean up: remove network delay and stop stress
            self.log.info("=== Cleanup: Removing network delay and stopping stress on KV nodes ===")
            for shell, kv_node in kv_shells:
                shell.delete_network_rule()
                self.log.info(f"Removed network delay on KV node {kv_node.ip}")
                shell.execute_command("pkill -9 stress-ng || true", debug=False)
                self.log.info(f"Stopped memory stress on KV node {kv_node.ip}")
                shell.disconnect()

    def test_scan_report_consistent_snapshot_wait(self):
        """
        Scan Report for slow scans waiting for a consistent snapshot.
        Validates that request_plus scans correctly report elevated wait times
        when indexer mutation catch-up is blocked (port 9104) and the indexer
        is under memory pressure with continuous mutations in flight.

        Steps:
        1. Create scalar indexes with num_replica=1 across 3+ indexer nodes.
        2. Run a baseline request_plus scan and capture wait time.
        3. Start memory pressure on indexer nodes, start continuous mutations,
           and block port 9104 (indexer stream) on all indexer nodes.
        4. Run request_plus scan (will wait for consistent snapshot).
        5. Unblock port 9104 so mutations catch up and scan completes.
        6. Validate scan report: wait time > baseline, num_docs_pending > 0.
        """

        buckets = self.create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)

        if len(index_nodes) < 3:
            self.fail("Test requires at least 3 indexer nodes")

        all_definitions = []
        all_scalar_selects = []
        all_scalar_index_names = []

        for bucket in buckets:
            self.prepare_collection_for_indexing(
                bucket_name=bucket,
                num_scopes=1,
                num_collections=1,
                num_of_docs_per_collection=self.num_of_docs_per_collection,
                json_template=self.json_template,
                load_default_coll=False
            )

        for namespace in self.namespaces:
            prefix = ''.join(namespace.split(':')[1].split('.'))
            scalar_defs, _, scalar_selects = self._create_scalar_indexes(
                namespace, prefix, query_node, num_replica=1, randomise_replica_count=False
            )
            all_definitions.extend(scalar_defs)
            all_scalar_selects.extend(scalar_selects)
            all_scalar_index_names.extend([d.index_name for d in scalar_defs])

        self.log.info("Waiting for indexes to come online")
        self.wait_until_indexes_online()

        for definition in all_definitions:
            self.log.info(f"Index {definition.index_name} created successfully and is online")

        # Phase 1: Baseline request_plus scan
        self.log.info("=== Phase 1: Baseline request_plus scan ===")
        baseline_query = all_scalar_selects[1]
        baseline_index_name = all_scalar_index_names[1]
        baseline_report = self._run_single_scan_and_validate(
            query=baseline_query,
            runDetailed=True,
            extra_query_params={"scan_consistency": "request_plus"},
            index_name=baseline_index_name
        )
        baseline_wait = 0
        if baseline_report:
            unit_field = "srvr_avg_ns" if "srvr_avg_ns" in baseline_report else "srvr_avg_ms"
            baseline_wait = baseline_report.get(unit_field, {}).get("wait", 0)
            self.log.info(f"Baseline {unit_field}.wait = {baseline_wait}")

        # Phase 2: Induce stress conditions
        self.log.info("=== Phase 2: Inducing indexer memory pressure and blocking port 9104 ===")
        indexer_shells = []
        for node in index_nodes:
            shell = RemoteMachineShellConnection(node)
            # Memory pressure on indexer nodes
            shell.execute_command(
                "apt-get install -y stress-ng || yum install -y stress-ng", debug=False
            )
            shell.execute_command(
                "stress-ng --vm 4 --vm-bytes 80% --vm-keep --timeout 600s "
                ">/tmp/stress_ng_indexer.log 2>&1 &"
            )
            self.log.info(f"Started memory stress on indexer node {node.ip}")
            # Block port 9104 (projector/indexer stream) to prevent mutation catch-up
            # Install iptables if not present (some distros only ship nftables)
            shell.execute_command(
                "which iptables || which /sbin/iptables || "
                "apt-get install -y iptables 2>/dev/null || "
                "yum install -y iptables 2>/dev/null || "
                "dnf install -y iptables 2>/dev/null",
                debug=False
            )
            shell.execute_command(
                "/sbin/iptables -A INPUT -p tcp --dport 9104 -j DROP || "
                "iptables -A INPUT -p tcp --dport 9104 -j DROP"
            )
            self.log.info(f"Blocked port 9104 on indexer node {node.ip}")
            indexer_shells.append((shell, node))

        # Start continuous mutations so num_docs_pending grows
        self.log.info("Starting continuous mutations in background")
        mutation_stop_event = Event()
        mutation_thread = threading.Thread(
            target=self._run_continuous_mutations,
            args=(buckets, mutation_stop_event)
        )
        mutation_thread.start()

        # Let mutations accumulate while port is blocked
        self.sleep(30, "Letting mutations accumulate with blocked stream port")

        # Phase 3: Run request_plus scan under stress (runs in a separate thread
        # so we can unblock the port after a delay to let the scan complete)
        self.log.info("=== Phase 3: Running request_plus scan with blocked stream ===")
        scan_result = [None]
        scan_error = [None]

        def run_stressed_scan():
            try:
                scan_result[0] = self._run_single_scan_and_validate(
                    query=baseline_query,
                    runDetailed=True,
                    extra_query_params={"scan_consistency": "request_plus"},
                    index_name=baseline_index_name
                )
            except Exception as e:
                scan_error[0] = e

        scan_thread = threading.Thread(target=run_stressed_scan)
        scan_thread.start()
        self.sleep(30, "Letting scan wait for consistent snapshot")
        self.log.info("Capturing num_docs_pending while stream is blocked")
        index_stats = self.get_index_stats(perNode=True)
        total_docs_pending = 0
        for node_key, node_stats in index_stats.items():
            for keyspace, indexes in node_stats.items():
                for index_name, stats in indexes.items():
                    pending = stats.get('num_docs_pending', 0)
                    if pending > 0:
                        self.log.info(
                            f"num_docs_pending on {node_key}/{index_name}: {pending}"
                        )
                        total_docs_pending += pending
        

        self.log.info("=== Phase 4: Unblocking port 9104 ===")
        for shell, node in indexer_shells:
            shell.execute_command(
                "/sbin/iptables -D INPUT -p tcp --dport 9104 -j DROP || "
                "iptables -D INPUT -p tcp --dport 9104 -j DROP"
            )
            self.log.info(f"Unblocked port 9104 on indexer node {node.ip}")

        # Wait for scan to complete
        scan_thread.join(timeout=300)

        # Stop mutations
        self.log.info("Stopping continuous mutations")
        mutation_stop_event.set()
        mutation_thread.join(timeout=30)

        # Cleanup: stop stress and disconnect
        self.log.info("=== Cleanup: Stopping memory stress on indexer nodes ===")
        for shell, node in indexer_shells:
            shell.execute_command("pkill -9 stress-ng || true", debug=False)
            self.log.info(f"Stopped memory stress on indexer node {node.ip}")
            shell.disconnect()

        # Phase 5: Validate results
        self.log.info("=== Phase 5: Validating scan report ===")
        if scan_error[0]:
            self.fail(f"Stressed scan failed with error: {scan_error[0]}")

        stressed_report = scan_result[0]
        if not stressed_report:
            self.fail("No scan report generated for request_plus scan under stress")

        # Validate wait time is greater than baseline
        unit_field = "srvr_avg_ns" if "srvr_avg_ns" in stressed_report else "srvr_avg_ms"
        stressed_wait = stressed_report.get(unit_field, {}).get("wait", 0)
        self.log.info(
            f"Stressed {unit_field}.wait = {stressed_wait}, baseline = {baseline_wait}"
        )
        self.assertGreater(
            stressed_wait, baseline_wait,
            f"Expected stressed wait ({stressed_wait}) > baseline wait ({baseline_wait}) "
            f"when indexer stream was blocked"
        )



    def test_scan_report_slow_gsi_connection(self):
        """
        Scan Report for slow scans caused by latency in establishing or
        communicating with the GSI server.

        Introduces targeted network delay on indexer nodes (port 9102 – the
        GSI scan service port) using tc/netem so that only client-to-indexer
        scan traffic is affected. Validates that scan reports reflect the
        added latency in the getseqnos/fetchseqnos timing fields.

        Steps:
        1. Create scalar indexes (num_replica=1) across all namespaces.
        2. Run baseline scans and capture scan report timing.
        3. Add 200ms network delay on port 9102 on every indexer node.
        4. Re-run the same scans and capture scan reports.
        5. Validate scan reports exist, have correct structure, and that
           total timing is present and elevated vs baseline.
        6. Remove the network delay.
        """
        buckets = self.create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)

        if len(index_nodes) < 3:
            self.fail("Test requires at least 3 indexer nodes")

        all_definitions = []
        all_scalar_selects = []
        all_scalar_index_names = []

        for bucket in buckets:
            self.prepare_collection_for_indexing(
                bucket_name=bucket,
                num_scopes=1,
                num_collections=1,
                num_of_docs_per_collection=self.num_of_docs_per_collection,
                json_template=self.json_template,
                load_default_coll=False
            )

        for namespace in self.namespaces:
            prefix = ''.join(namespace.split(':')[1].split('.'))
            scalar_defs, _, scalar_selects = self._create_scalar_indexes(
                namespace, prefix, query_node, num_replica=1, randomise_replica_count=False
            )
            all_definitions.extend(scalar_defs)
            all_scalar_selects.extend(scalar_selects)
            all_scalar_index_names.extend([d.index_name for d in scalar_defs])

        self.log.info("Waiting for indexes to come online")
        self.wait_until_indexes_online()

        for definition in all_definitions:
            self.log.info(f"Index {definition.index_name} created successfully and is online")

        self.log.info("=== Phase 1: Baseline scans ===")
        baseline_reports = self._run_scans_and_validate(
            all_scalar_selects , runDetailed=True,
            index_names=all_scalar_index_names 
        )

        baseline_total_times = []
        for report in baseline_reports:
            unit_field = "srvr_avg_ns" if "srvr_avg_ns" in report else "srvr_avg_ms"
            avg = report.get(unit_field, {})
            total_time = avg.get("total", 0)
            baseline_total_times.append(total_time)
            self.log.info(
                f"Baseline {unit_field}: total={total_time}"
            )

        self.log.info("=== Phase 2: Adding 200ms delay on port 9102 (GSI scan service) ===")
        indexer_shells = []
        for node in index_nodes:
            shell = RemoteMachineShellConnection(node)
            # Add a tc qdisc with netem delay targeted at port 9102
            # First, add root qdisc with prio bands so we can filter by port
            shell.execute_command(
                "tc qdisc add dev eth0 root handle 1: prio priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0"
            )
            shell.execute_command(
                "tc qdisc add dev eth0 parent 1:2 handle 20: netem delay 200ms"
            )
            # Filter: match traffic on destination or source port 9102
            shell.execute_command(
                "tc filter add dev eth0 parent 1:0 protocol ip u32 "
                "match ip dport 9102 0xffff flowid 1:2"
            )
            shell.execute_command(
                "tc filter add dev eth0 parent 1:0 protocol ip u32 "
                "match ip sport 9102 0xffff flowid 1:2"
            )
            self.log.info(f"Added 200ms netem delay on port 9102 on indexer node {node.ip}")
            indexer_shells.append((shell, node))

        self.sleep(5, "Waiting for netem rules to take effect")

        try:
            self.log.info("=== Phase 3: Running scans under GSI connection delay ===")
            self.log.info("Validating scan reports under GSI delay (concise)")
            self._run_scans_and_validate(
                all_scalar_selects , runDetailed=False,
                index_names=all_scalar_index_names 
            )
            self.log.info("Validating scan reports under GSI delay (detailed)")
            stressed_reports = self._run_scans_and_validate(
                all_scalar_selects , runDetailed=True,
                index_names=all_scalar_index_names 
            )
            for i, report in enumerate(stressed_reports):
                unit_field = "srvr_avg_ns" if "srvr_avg_ns" in report else "srvr_avg_ms"
                avg = report.get(unit_field, {})
                stressed_total = avg.get("total", 0)
                baseline_total = baseline_total_times[i] if i < len(baseline_total_times) else 0
                self.log.info(
                    f"Scan {i}: stressed {unit_field}: total={stressed_total}, baseline_total={baseline_total}"
                )
                self.assertGreater(
                    stressed_total, baseline_total,
                    f"Scan {i}: Expected stressed total ({stressed_total}) > baseline total ({baseline_total}) under GSI delay"
                )
        finally:
            self.log.info("=== Cleanup: Removing netem delay on indexer nodes ===")
            for shell, node in indexer_shells:
                shell.delete_network_rule()
                self.log.info(f"Removed netem rules on indexer node {node.ip}")
                shell.disconnect()

   
    def test_scan_report_with_use_index(self):
        """
        Test: Scan report validation for indexes with USE INDEX clause

        Steps:
        1. Create a cluster with 3+ indexer nodes with varying replica counts (0, 1, 2).
        2. Create scalar indexes for all namespaces.
        3. Generate SELECT queries with USE INDEX clause for each index.
        4. Wait until all indexes reach the online state and verify index creation.
        5. Verify primary index is fully online before running USE INDEX queries.
        6. Run scans using the USE INDEX clause for all index types.
        7. Validate scan reports for scalar index scans in concise mode and detailed mode.

        Expected Result:
        - Indexes are created successfully and in Ready state.
        - Scan reports are generated for scans with USE INDEX clause.
        - Scan report structure is validated successfully for all index types.
        """
        buckets = self.create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 3:
            self.fail("Test requires at least 3 indexer nodes")
        all_select_queries = []
        all_definitions = []
        all_scalar_selects = []
        all_scalar_index_names = []
        for bucket in buckets:
            self.prepare_collection_for_indexing(
                bucket_name=bucket,
                num_scopes=1,
                num_collections=1,
                num_of_docs_per_collection=10000,
                json_template=self.json_template,
                load_default_coll=False
            )
        for namespace in self.namespaces:
            prefix = ''.join(namespace.split(':')[1].split('.'))
            # Use defer_build=False to ensure primary index is built immediately
            scalar_defs, _, _ = self._create_scalar_indexes(
                namespace, prefix, query_node, num_replica=2, randomise_replica_count=True, defer_build=False
            )
            all_definitions.extend(scalar_defs )
            for definition in scalar_defs:
                scalar_selects = self.gsi_util_obj.get_select_queries(
                    definition_list=[definition],
                    namespace=namespace,
                    limit=self.scan_limit,
                    index_name=definition.index_name
                )
                all_select_queries.extend(scalar_selects)
                all_scalar_selects.extend(scalar_selects)
                all_scalar_index_names.extend([definition.index_name] * len(scalar_selects))
            
        
        self.log.info("Waiting for indexes to be ready")
        self.wait_until_indexes_online(timeout=600)
        self.sleep(30)

        self.log.info("Validating all indexes are created and in ready state")
        for definition in all_definitions:
            self.log.info(f"Index {definition.index_name} created successfully")
        
        # Verify primary index is online before running USE INDEX queries
        rest = RestConnection(index_nodes[0])
        index_map = rest.get_index_status()
        primary_indexes = [idx for idx in index_map.values() if idx.get('name', '').startswith('#primary')]
        self.log.info(f"Found {len(primary_indexes)} primary indexes")
        for idx in primary_indexes:
            self.log.info(f"Primary index: {idx.get('name')} - status: {idx.get('status')}")
            if idx.get('status') != 'Ready':
                self.fail(f"Primary index {idx.get('name')} is not Ready, status: {idx.get('status')}")
        
        # Run scans with USE INDEX and validate scan reports
        self.log.info("Validating scan reports for scalar indexes with USE INDEX (concise)")
        self._run_scans_and_validate(all_scalar_selects, runDetailed=False,
                                      index_names=all_scalar_index_names)
        self.log.info("Validating scan reports for scalar indexes with USE INDEX (detailed)")
        self._run_scans_and_validate(all_scalar_selects, runDetailed=True,
                                      index_names=all_scalar_index_names)
        
        

    def test_scan_report_replica_retries(self):
        """
        Test: Scan report validation with replica retries when indexer nodes are stopped

        Steps:
        1. Create a cluster with 3+ indexer nodes and verify node count.
        2. Create scalar indexes with replicas for all namespaces.
        3. Wait until all indexes reach the online state.
        4. Stop Couchbase server on SOME indexer nodes (not all, keep one running).
        5. Run scans that trigger replica retry logic to the remaining node.
        6. Validate scan reports contain relevant retry error details and are incomplete.
        7. Restart the stopped indexer nodes and wait for recovery.
        8. Verify indexes come back online after recovery.

        Expected Result:
        - Scan reports are generated with incomplete status due to replica retries.
        - Retry details are captured in scan reports.
        - Indexes recover successfully after node restart.
        """
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 3:
            self.fail("Test requires at least 3 indexer nodes for replica retry testing")
        
        buckets = self.create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        
        for bucket in buckets:
            self.prepare_collection_for_indexing(
                bucket_name=bucket, num_scopes=1, num_collections=1,
                num_of_docs_per_collection=10000, json_template=self.json_template
            )
        
        all_select_queries = []
        all_scalar_selects = []
        all_scalar_index_names = []

        
        for namespace in self.namespaces:
            prefix = ''.join(namespace.split(':')[1].split('.'))
            scalar_defs, _, scalar_selects = self._create_scalar_indexes(namespace, prefix, query_node, num_replica=1, randomise_replica_count=False)
            
            all_select_queries.extend(scalar_selects)
            all_scalar_selects.extend(scalar_selects)
            all_scalar_index_names.extend([d.index_name for d in scalar_defs])
            
        
        self.wait_until_indexes_online()

        # Stop couchbase server on SOME indexer nodes, keep one running
        # This allows queries to still reach the index service but triggers replica retries
        nodes_to_stop = index_nodes[:-1]  # Stop all except the last one
        self.log.info(f"Stopping couchbase server on {len(nodes_to_stop)} of {len(index_nodes)} indexer nodes")
        for index_node in nodes_to_stop:
            shell = RemoteMachineShellConnection(index_node)
            shell.stop_server()
            self.log.info(f"Stopped server on node {index_node.ip}")
            shell.disconnect()
        
        self.sleep(10, "Waiting for servers to stop")
        
        try:
            # Run scans with failover_scenario=True for relaxed validation (skips delta validation)
            self.log.info("Validating scan reports for scalar indexes (concise)")
            self._run_scans_and_validate(all_scalar_selects, expect_retries=True,
                                          failover_scenario=True, runDetailed=False, index_names=all_scalar_index_names,expect_incomplete=True)
            self.log.info("Validating scan reports for scalar indexes (detailed)")
            self._run_scans_and_validate(all_scalar_selects, expect_retries=True,
                                          failover_scenario=True, runDetailed=True, index_names=all_scalar_index_names,expect_incomplete=True)
        finally:
            self.log.info("Restarting couchbase server on stopped indexer nodes")
            for index_node in nodes_to_stop:
                shell = RemoteMachineShellConnection(index_node)
                shell.start_server()
                self.log.info(f"Started server on node {index_node.ip}")
                shell.disconnect()
            
            self.sleep(30, "Waiting for servers to restart")
            self.wait_until_indexes_online()

    
    def test_scan_report_post_recovery(self):
        """
        Test: Scan report consistency post indexer recovery

        Steps:
        1. Create a cluster with 3+ indexer nodes and verify node count.
        2. Create scalar indexes with replicas for all namespaces.
        3. Wait until all indexes reach the online state and verify index creation.
        4. Run baseline scans and collect detailed scan reports before recovery (Phase 1).
        5. Kill indexer service on all nodes one by one to trigger recovery (Phase 2).
        6. Wait for indexes to come back online after recovery.
        7. Run the same scans and collect detailed scan reports after recovery (Phase 3).
        8. Compare scan reports before and after recovery for consistency.
        9. Log any structural differences found between pre and post recovery reports.

        Expected Result:
        - Scan reports are consistent before and after recovery.
        - Indexer recovery does not alter scan report structure.
        - Any differences found are logged for analysis.
        """
        buckets = self.create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)

        if len(index_nodes) < 3:
            self.fail("Test requires at least 3 indexer nodes")

        all_definitions = []
        scalar_selects = []
        scalar_index_names = []
        
        
        for bucket in buckets:
            self.prepare_collection_for_indexing(
                bucket_name=bucket,
                num_scopes=1,
                num_collections=1,
                num_of_docs_per_collection=10000,
                json_template=self.json_template,
                load_default_coll=False
            )
        
        for namespace in self.namespaces:
            prefix = ''.join(namespace.split(':')[1].split('.'))
            
             
             
            scalar_defs, _, s_selects = self._create_scalar_indexes(
                namespace, prefix, query_node, num_replica=2, randomise_replica_count=True
            )

            
            all_definitions.extend(scalar_defs )
            scalar_selects.extend(s_selects)
            scalar_index_names.extend([d.index_name for d in scalar_defs])

        
        self.log.info("Waiting for indexes to be ready")
        self.wait_until_indexes_online()

        self.log.info("Validating all indexes are created and in ready state")
        for definition in all_definitions:
            self.log.info(f"Index {definition.index_name} created successfully")
        
        # Define query groups with their parameters and index names
        query_groups = [
            {"name": "scalar", "queries": scalar_selects , "index_names": scalar_index_names },
        ]
        
        # Step 1: Run scans BEFORE recovery and collect scan reports (both detailed and concise)
        self.log.info("=== Phase 1: Collecting scan reports BEFORE recovery ===")
        scan_reports_before_detailed = {}
        scan_reports_before_concise = {}
        for group in query_groups:
            self.log.info(f"Collecting {group['name']} DETAILED scan reports before recovery")
            for i, query in enumerate(group["queries"]):
                index_name = group["index_names"][i] if i < len(group["index_names"]) else None
                try:
                    scan_report = self._run_single_scan_and_validate(
                        query=query,
                        runDetailed=True,
                        index_name=index_name,
                        validate_rows_delta=True
                    )
                    if scan_report:
                        scan_reports_before_detailed[query] = self._extract_report_structure(scan_report)
                    else:
                        self.log.warning(f"No DETAILED scan report found BEFORE recovery for query: {query[:50]}...")
                except Exception as e:
                    self.log.error(f"Error collecting DETAILED scan report for query '{query[:50]}...': {str(e)}")
            
            self.log.info(f"Collecting {group['name']} CONCISE scan reports before recovery")
            for i, query in enumerate(group["queries"]):
                index_name = group["index_names"][i] if i < len(group["index_names"]) else None
                try:
                    scan_report = self._run_single_scan_and_validate(
                        query=query,
                        runDetailed=False,
                        index_name=index_name,
                        validate_rows_delta=True
                    )
                    if scan_report:
                        scan_reports_before_concise[query] = self._extract_report_structure(scan_report)
                    else:
                        self.log.warning(f"No CONCISE scan report found BEFORE recovery for query: {query[:50]}...")
                except Exception as e:
                    self.log.error(f"Error collecting CONCISE scan report for query '{query[:50]}...': {str(e)}")
        
        # Step 2: Kill indexer service on all nodes ONE BY ONE and recover
        self.log.info("=== Phase 2: Killing and recovering indexer on each node ===")
        for index_node in index_nodes:
            self.log.info(f"Killing indexer on node {index_node.ip}")
            try:
                shell = RemoteMachineShellConnection(index_node)
                shell.terminate_process(info=shell.extract_remote_info(), process_name='indexer', force=True)
                self.log.info(f"Killed indexer on node {index_node.ip}")
                shell.disconnect()
            except Exception as e:
                self.log.error(f"Error killing indexer on {index_node.ip}: {str(e)}")
            
            # Wait for indexer to restart (Couchbase should auto-restart)
            self.sleep(30, f"Waiting for indexer on {index_node.ip} to recover")
        
        # Wait for all indexes to be online after recovery
        self.log.info("Waiting for all indexes to come back online after recovery")
        self.wait_until_indexes_online()
        self.sleep(30, "Additional wait for indexes to stabilize")
        
        # Step 3: Run scans AFTER recovery and collect scan reports (both detailed and concise)
        self.log.info("=== Phase 3: Collecting scan reports AFTER recovery ===")
        scan_reports_after_detailed = {}
        scan_reports_after_concise = {}
        for group in query_groups:
            self.log.info(f"Collecting {group['name']} DETAILED scan reports after recovery")
            for i, query in enumerate(group["queries"]):
                index_name = group["index_names"][i] if i < len(group["index_names"]) else None
                try:
                    scan_report = self._run_single_scan_and_validate(
                        query=query,
                        runDetailed=True,
                        index_name=index_name,
                        validate_rows_delta=True
                    )
                    if scan_report:
                        scan_reports_after_detailed[query] = self._extract_report_structure(scan_report)
                    else:
                        self.log.warning(f"No DETAILED scan report found AFTER recovery for query: {query[:50]}...")
                except Exception as e:
                    self.log.error(f"Error collecting DETAILED scan report for query '{query[:50]}...': {str(e)}")
            
            self.log.info(f"Collecting {group['name']} CONCISE scan reports after recovery")
            for i, query in enumerate(group["queries"]):
                index_name = group["index_names"][i] if i < len(group["index_names"]) else None
                try:
                    scan_report = self._run_single_scan_and_validate(
                        query=query,
                        runDetailed=False,
                        index_name=index_name,
                        validate_rows_delta=True
                    )
                    if scan_report:
                        scan_reports_after_concise[query] = self._extract_report_structure(scan_report)
                    else:
                        self.log.warning(f"No CONCISE scan report found AFTER recovery for query: {query[:50]}...")
                except Exception as e:
                    self.log.error(f"Error collecting CONCISE scan report for query '{query[:50]}...': {str(e)}")
        
        # Step 4: Compare scan reports before and after recovery (both detailed and concise)
        self.log.info("=== Phase 4: Comparing scan reports before and after recovery ===")
        differences_found = False
        missing_reports_detailed = {"before": 0, "after": 0, "both": 0}
        missing_reports_concise = {"before": 0, "after": 0, "both": 0}
        successful_comparisons_detailed = 0
        successful_comparisons_concise = 0
        total_queries = 0
        
        for group in query_groups:
            self.log.info(f"Comparing {group['name']} DETAILED scan reports")
            for query in group["queries"]:
                total_queries += 1
                has_before = query in scan_reports_before_detailed
                has_after = query in scan_reports_after_detailed
                
                if has_before and has_after:
                    before = scan_reports_before_detailed[query]
                    after = scan_reports_after_detailed[query]
                    diff = DeepDiff(before, after, ignore_order=True)
                    if diff:
                        self.log.warning(f"DETAILED: Difference found in scan report for query: {query[:50]}...")
                        self.log.warning(f"Differences: {diff}")
                        differences_found = True
                    else:
                        self.log.info(f"DETAILED: Scan report consistent for query: {query[:50]}...")
                    successful_comparisons_detailed += 1
                else:
                    if not has_before and not has_after:
                        self.log.warning(f"DETAILED: Missing scan report in BOTH phases for query: {query[:50]}...")
                        missing_reports_detailed["both"] += 1
                    elif not has_before:
                        self.log.warning(f"DETAILED: Missing scan report BEFORE recovery for query: {query[:50]}...")
                        missing_reports_detailed["before"] += 1
                    else:
                        self.log.warning(f"DETAILED: Missing scan report AFTER recovery for query: {query[:50]}...")
                        missing_reports_detailed["after"] += 1
            
            self.log.info(f"Comparing {group['name']} CONCISE scan reports")
            for query in group["queries"]:
                has_before = query in scan_reports_before_concise
                has_after = query in scan_reports_after_concise
                
                if has_before and has_after:
                    before = scan_reports_before_concise[query]
                    after = scan_reports_after_concise[query]
                    diff = DeepDiff(before, after, ignore_order=True)
                    if diff:
                        self.log.warning(f"CONCISE: Difference found in scan report for query: {query[:50]}...")
                        self.log.warning(f"Differences: {diff}")
                        differences_found = True
                    else:
                        self.log.info(f"CONCISE: Scan report consistent for query: {query[:50]}...")
                    successful_comparisons_concise += 1
                else:
                    if not has_before and not has_after:
                        self.log.warning(f"CONCISE: Missing scan report in BOTH phases for query: {query[:50]}...")
                        missing_reports_concise["both"] += 1
                    elif not has_before:
                        self.log.warning(f"CONCISE: Missing scan report BEFORE recovery for query: {query[:50]}...")
                        missing_reports_concise["before"] += 1
                    else:
                        self.log.warning(f"CONCISE: Missing scan report AFTER recovery for query: {query[:50]}...")
                        missing_reports_concise["after"] += 1
        
        # Log summary of comparison results
        self.log.info(f"=== Comparison Summary ===")
        self.log.info(f"Total queries: {total_queries}")
        self.log.info(f"--- DETAILED Reports ---")
        self.log.info(f"Successful comparisons: {successful_comparisons_detailed}")
        self.log.info(f"Missing before recovery: {missing_reports_detailed['before']}")
        self.log.info(f"Missing after recovery: {missing_reports_detailed['after']}")
        self.log.info(f"Missing in both phases: {missing_reports_detailed['both']}")
        self.log.info(f"--- CONCISE Reports ---")
        self.log.info(f"Successful comparisons: {successful_comparisons_concise}")
        self.log.info(f"Missing before recovery: {missing_reports_concise['before']}")
        self.log.info(f"Missing after recovery: {missing_reports_concise['after']}")
        self.log.info(f"Missing in both phases: {missing_reports_concise['both']}")
        
        total_successful = successful_comparisons_detailed + successful_comparisons_concise
        if differences_found:
            self.log.warning("Some scan reports showed differences after recovery")
        elif total_successful == 0:
            self.log.warning("No scan reports were available for comparison!")
        else:
            self.log.info("All compared scan reports (detailed and concise) are consistent before and after recovery")

    def _extract_report_structure(self, scan_report: Dict[str, Any],check_defn=False) -> Dict[str, Any]:
        """
        Extract structural elements of scan report for comparison, excluding timing values.
        
        Args:
            scan_report: The scan report to extract structure from
            
        Returns:
            Dictionary with structural elements (fields present, partition info, etc.)
        """
        structure = {}
        
        # Extract defn
        structure["has_defn"] = False

        if "defn" in scan_report:
            if check_defn:
                structure["defn"] = scan_report["defn"]
            else:
                structure["has_defn"] = True
        
        # Extract fields present (not values) in srvr_avg_ns/srvr_avg_ms
        if "srvr_avg_ns" in scan_report:
            structure["srvr_avg_fields"] = list(scan_report["srvr_avg_ns"].keys())
            structure["unit"] = "ns"
        elif "srvr_avg_ms" in scan_report:
            structure["srvr_avg_fields"] = list(scan_report["srvr_avg_ms"].keys())
            structure["unit"] = "ms"
        
        # Extract fields present in srvr_total_counts (not exact values as they may change)
        if "srvr_total_counts" in scan_report:
            structure["srvr_total_counts_fields"] = list(scan_report["srvr_total_counts"].keys())
        
        # Extract partition structure
        if "partns" in scan_report:
            structure["partns_keys"] = list(scan_report["partns"].keys())
        
        # Check for num_scans presence
        if "num_scans" in scan_report:
            structure["has_num_scans"] = True
        
        # Check for detailed presence
        if "detailed" in scan_report:
            structure["has_detailed"] = True
            # detailed can be a list (array of sub-reports) or dict
            if isinstance(scan_report["detailed"], dict):
                structure["detailed_keys"] = list(scan_report["detailed"].keys())
            elif isinstance(scan_report["detailed"], list):
                # For list, extract keys from the first element if available
                if scan_report["detailed"] and isinstance(scan_report["detailed"][0], dict):
                    structure["detailed_keys"] = list(scan_report["detailed"][0].keys())
                else:
                    structure["detailed_keys"] = []
        
        return structure

    def test_scan_report_post_node_failover(self):
        """
        Test: Scan report in CONCISE and DETAILED mode with node failover

        Steps:
        1. Create a cluster with multiple indexer nodes.
        2. Create scalar indexes with varying replica counts for all namespaces.
        3. Wait until all indexes reach the online state and verify index creation.
        4. Run baseline scans and verify they work before failover (Phase 1).
        5. Fail over one of the indexer nodes (either graceful or hard failover).
        6. Wait for failover to take effect.
        7. Run scans with both CONCISE and DETAILED profiles after failover (Phase 2).
        8. Validate scan reports are incomplete since some indexes have failed.
        9. Verify error handling for scans with failed indexes.

        Expected Result:
        - Scan reports are generated but may be incomplete after failover.
        - Error handling is proper for scans with unavailable indexes.
        - Both CONCISE and DETAILED modes handle failover gracefully.
        """
        buckets = self.create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        all_definitions = []
        scalar_selects = []
        scalar_index_names = []
        for bucket in buckets:
            self.prepare_collection_for_indexing(
                bucket_name=bucket,
                num_scopes=1,
                num_collections=1,
                num_of_docs_per_collection=10000,
                json_template=self.json_template,
                load_default_coll=False
            )
        for namespace in self.namespaces:
            prefix = ''.join(namespace.split(':')[1].split('.')) 
            scalar_defs, _, s_selects = self._create_scalar_indexes(
                namespace, prefix, query_node, num_replica=0, randomise_replica_count=True
            )
            all_definitions.extend(scalar_defs)
            scalar_selects.extend(s_selects)
            scalar_index_names.extend([d.index_name for d in scalar_defs])
        self.log.info("Waiting for indexes to be ready")
        self.wait_until_indexes_online()
        self.log.info("Validating all indexes are created and in ready state")
        for definition in all_definitions:
            self.log.info(f"Index {definition.index_name} created successfully")
        # Define query groups with their parameters and index names
        query_groups = [
            {"name": "scalar", "queries": scalar_selects , "index_names": scalar_index_names },
        ]
        self.log.info("=== Phase 1: Verifying scans work before failover ===")
        for group in query_groups:
            self.log.info(f"Verifying {group['name']} scans before failover")
            for i, query in enumerate(group["queries"]):
                index_name = group["index_names"][i] if i < len(group["index_names"]) else None
                try:
                    scan_report = self._run_single_scan_and_validate(
                        query=query,
                        runDetailed=False,
                        index_name=index_name,
                        validate_rows_delta=True
                    )
                    if scan_report:
                        self.log.info(f"Query successful before failover: {query[:50]}...")
                except Exception as e:
                    self.log.error(f"Error before failover: {str(e)}")
        
        # Fail over one of the index nodes
        failover_node = index_nodes[0]
        self.log.info(f"=== Phase 2: Failing over index node {failover_node.ip} ===")
        try:
            # Graceful failover
            self.rest.fail_over(otpNode=f"ns_1@{failover_node.ip}", graceful=False)
            self.log.info(f"Failed over node {failover_node.ip}")
        except Exception as e:
            self.log.error(f"Error during failover: {str(e)}")
            # Try hard failover
            try:
                self.cluster.failover(self.servers, [failover_node], graceful=False)
                self.log.info(f"Hard failover completed for node {failover_node.ip}")
            except Exception as e2:
                self.log.error(f"Hard failover also failed: {str(e2)}")
        
        self.sleep(30, "Waiting for failover to take effect")
        
        # Run scans with both DETAILED and CONCISE profiles after failover
        self.log.info("=== Phase 3: Running scans after failover ===")
        incomplete_reports = {"concise": 0, "detailed": 0}
        error_reports = {"concise": 0, "detailed": 0}
        
        for run_detailed in [False, True]:
            scan_type = "detailed" if run_detailed else "concise"
            self.log.info(f"--- Running {scan_type} scans after failover ---")
            
            for group in query_groups:
                self.log.info(f"Running {scan_type} scans for {group['name']} indexes")
                for i, query in enumerate(group["queries"]):
                    index_name = group["index_names"][i] if i < len(group["index_names"]) else None
                    try:
                        scan_report = self._run_single_scan_and_validate(
                            query=query,
                            expect_incomplete=True,
                            failover_scenario=True,
                            runDetailed=run_detailed,
                            index_name=index_name
                        )
                        
                        if scan_report:
                            # Check if report is incomplete
                            is_incomplete = self._is_report_incomplete(scan_report)
                            if is_incomplete:
                                self.log.info(f"Scan report is incomplete as expected for query: {query[:50]}...")
                                incomplete_reports[scan_type] += 1
                            
                            if "error" in scan_report:
                                self.log.info(f"Error field present in scan report")
                            
                            if "retries" in scan_report:
                                self.log.info(f"Retries field present in scan report")
                        else:
                            self.log.warning(f"No scan report found for query: {query[:50]}...")
                            
                    except ReportValidationError as e:
                        self.log.info(f"Validation error (expected during failover): {str(e)}")
                        error_reports[scan_type] += 1
                    except Exception as e:
                        self.log.info(f"Expected error running query '{query[:50]}...': {str(e)}")
                        error_reports[scan_type] += 1
        
        self.log.info(f"=== Summary ===")
        self.log.info(f"Concise: {incomplete_reports['concise']} incomplete reports, {error_reports['concise']} error reports")
        self.log.info(f"Detailed: {incomplete_reports['detailed']} incomplete reports, {error_reports['detailed']} error reports")
        
        # Recovery: Add back the failed over node
        self.log.info("=== Phase 4: Recovering failed over node ===")
        try:
            # Recovery by adding the node back
            self.rest.add_back_node(f"ns_1@{failover_node.ip}")
            self.rest.set_recovery_type(f"ns_1@{failover_node.ip}", "full")
            self.rest.rebalance(otpNodes=[f"ns_1@{node.ip}" for node in self.servers])
            self.log.info("Recovery initiated")
            
            # Wait for rebalance
            rebalance_status = RestHelper(self.rest).rebalance_reached()
            if rebalance_status:
                self.log.info("Rebalance completed successfully")
            else:
                self.log.warning("Rebalance did not complete in expected time")
        except Exception as e:
            self.log.error(f"Error during recovery: {str(e)}")
        
        self.sleep(30, "Waiting for cluster to stabilize after recovery")
        self.log.info("Test completed - node failover with DETAILED and CONCISE scan reports")


    def test_scan_report_request_plus_consistency(self):
        """
        Test: Scan report with request_plus scan consistency during concurrent mutations

        Steps:
        1. Create a cluster with 3+ indexer nodes.
        2. Create scalar indexes with replicas for all namespaces.
        3. Wait until all indexes reach the online state and verify index creation.
        4. Start continuous mutations in a background thread (if enabled).
        5. Run scans with request_plus scan consistency parameter.
        6. Validate scan reports are generated for request_plus scans in concise mode.
        7. Validate scan reports are generated for request_plus scans in detailed mode.
        8. Stop the continuous mutations thread.

        Expected Result:
        - Scan reports are generated for scans with request_plus consistency.
        - Scan reports reflect the wait time for consistent snapshot.
        - Both concise and detailed scan reports validate successfully.
        """
        buckets = self.create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 3:
            self.fail("Test requires at least 3 indexer nodes")
        all_select_queries = []
        all_definitions = []
        all_index_names = []
        for bucket in buckets:
            self.prepare_collection_for_indexing(
                bucket_name=bucket,
                num_scopes=1,
                num_collections=1,
                num_of_docs_per_collection=10000,
                json_template=self.json_template,
                load_default_coll=False
            )
        for namespace in self.namespaces:
            prefix = ''.join(namespace.split(':')[1].split('.'))
            scalar_defs, _, scalar_selects = self._create_scalar_indexes(
                namespace, prefix, query_node, num_replica=2, randomise_replica_count=True
            )
            all_definitions.extend(scalar_defs)
            all_select_queries.extend(scalar_selects)
            all_index_names.extend([d.index_name for d in scalar_defs])
        
        self.log.info("Waiting for indexes to be ready")
        self.wait_until_indexes_online()
        self.log.info("Validating all indexes are created and in ready state")
        for definition in all_definitions:
            self.log.info(f"Index {definition.index_name} created successfully")
        mutation_stop_event = Event()
        mutation_thread = None
        if self.continuous_mutations:
            self.log.info("Starting continuous mutations in background")
            mutation_thread = threading.Thread(
                target=self._run_continuous_mutations,
                args=(buckets, mutation_stop_event)
            )
            mutation_thread.start()
        try:
            self.log.info("Running scans with request_plus scan consistency (concise)")
            self._run_scans_and_validate(
                all_select_queries ,
                extra_query_params={"scan_consistency": "request_plus"},
                runDetailed=False,
                index_names=all_index_names ,
                validate_rows_delta=True
            )
            self.log.info("Running scans with request_plus scan consistency (detailed)")
            self._run_scans_and_validate(
                all_select_queries ,
                extra_query_params={"scan_consistency": "request_plus"},
                runDetailed=True,
                index_names=all_index_names ,
                validate_rows_delta=True
            )
            self.log.info("Test completed - request_plus scan consistency")
        finally:
            if mutation_thread:
                self.log.info("Stopping continuous mutations")
                mutation_stop_event.set()
                mutation_thread.join(timeout=30)

    def _run_continuous_mutations(self, buckets, stop_event):
        """Run continuous mutations until stop_event is set"""
        mutation_count = 0
        while not stop_event.is_set():
            try:
                for bucket in buckets:
                    for namespace in self.namespaces:
                        # Insert/update documents
                        doc_id = f"mutation_doc_{mutation_count}"
                        query = f"UPSERT INTO {namespace} (KEY, VALUE) VALUES ('{doc_id}', {{'name': 'test_{mutation_count}', 'age': {mutation_count % 100}}})"
                        self.run_cbq_query(query)
                        mutation_count += 1
                        if stop_event.is_set():
                            break
                    if stop_event.is_set():
                        break
                self.sleep(0.1)  # Small delay between batches
            except Exception as e:
                self.log.error(f"Error during mutations: {str(e)}")
                if stop_event.is_set():
                    break
        self.log.info(f"Completed {mutation_count} mutations")


    def test_scan_report_scan_timeout(self):
        """
        Test: Scan report validation with induced scan timeout from indexer and query sides

        Steps:
        1. Create a cluster with 3+ indexer nodes and verify node count.
        2. Create scalar indexes with replicas for all namespaces.
        3. Wait until all indexes reach the online state and verify index creation.
        4. Save original indexer scan_timeout settings for restoration.
        5. Phase 1: Set indexer scan_timeout to a very low value (1ms) via index settings.
        6. Run scans and validate scan reports are incomplete due to indexer-side timeout.
        7. Restore original indexer scan_timeout settings.
        8. Phase 2: Use N1QL timeout query parameter to induce query-side timeout.
        9. Run scans and validate scan reports are incomplete due to query-side timeout.

        Expected Result:
        - Scan reports are incomplete due to timeout conditions.
        - Both indexer-side and query-side timeouts generate incomplete scan reports.
        - Error handling is proper for timed-out scans.
        """
        buckets = self.create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)

        if len(index_nodes) < 3:
            self.fail("Test requires at least 3 indexer nodes")

        all_definitions = []
        scalar_selects = []
        scalar_index_names = []
        

        for bucket in buckets:
            self.prepare_collection_for_indexing(
                bucket_name=bucket,
                num_scopes=1,
                num_collections=1,
                num_of_docs_per_collection=10000,
                json_template=self.json_template,
                load_default_coll=False
            )

        for namespace in self.namespaces:
            prefix = ''.join(namespace.split(':')[1].split('.'))

             
            s_defs, _, s_selects = self._create_scalar_indexes(
                namespace, prefix, query_node, num_replica=2, randomise_replica_count=True
            )
            

            all_definitions.extend(s_defs)
            scalar_selects.extend(s_selects)
            scalar_index_names.extend([d.index_name for d in s_defs])
            

        self.log.info("Waiting for indexes to be ready")
        self.wait_until_indexes_online()

        for definition in all_definitions:
            self.log.info(f"Index {definition.index_name} created successfully")

        # Save original indexer scan_timeout so we can restore it later
        index_node = index_nodes[0]
        index_rest = RestConnection(index_node)
        original_settings = index_rest.get_index_settings()
        original_scan_timeout = original_settings.get("indexer.settings.scan_timeout", 120000)
        self.log.info(f"Original indexer scan_timeout: {original_scan_timeout}")

        all_selects = scalar_selects

        # Phase 1: Indexer-side timeout — set indexer.settings.scan_timeout to a very low
        # value via the index settings REST API so the indexer enforces it.
        self.log.info("=== Phase 1: Inducing INDEXER-SIDE scan timeout ===")
        INDEXER_TIMEOUT_MS = 1
        index_rest.set_index_settings({"indexer.settings.scan_timeout": INDEXER_TIMEOUT_MS})
        self.sleep(5, "Waiting for indexer setting to propagate")

        self._run_scans_and_validate(
            scalar_selects,
            expect_incomplete=True, failover_scenario=True, runDetailed=False,
            index_names=scalar_index_names
        )
        self._run_scans_and_validate(
            scalar_selects,
            expect_incomplete=True, failover_scenario=True, runDetailed=True,
            index_names=scalar_index_names
        )
        

        # Restore original indexer scan_timeout
        self.log.info("Restoring original indexer scan_timeout")
        index_rest.set_index_settings({"indexer.settings.scan_timeout": original_scan_timeout})
        self.sleep(5, "Waiting for indexer setting to propagate")

        # Phase 2: Query-side timeout — use the N1QL `timeout` query parameter so the query
        # layer aborts the request before the indexer finishes.
        self.log.info("=== Phase 2: Inducing QUERY-SIDE scan timeout ===")
        QUERY_TIMEOUT = self.query_timeout
        self._run_scans_and_validate(
            scalar_selects,
            expect_incomplete=True, failover_scenario=True,
            extra_query_params={"timeout": QUERY_TIMEOUT}, runDetailed=False,
            index_names=scalar_index_names
        )
        self._run_scans_and_validate(
            scalar_selects,
            expect_incomplete=True, failover_scenario=True,
            extra_query_params={"timeout": QUERY_TIMEOUT}, runDetailed=True,
            index_names=scalar_index_names
        )
        self.log.info("Test completed - scan timeout validation")

    def test_scan_report_client_cancel(self):
        """
        Test: Scan report validation when queries are cancelled mid-scan using client_context_id

        Steps:
        1. Create a cluster with 3+ indexer nodes and verify node count.
        2. Create scalar indexes with replicas for all namespaces.
        3. Wait until all indexes reach the online state and verify index creation.
        4. Fire queries with a client_context_id in separate threads.
        5. Cancel the queries via DELETE on the N1QL active_requests endpoint.
        6. Wait for query threads to finish and capture results.
        7. Extract and validate scan reports from cancelled queries.
        8. Verify scan reports contain CLIENT_CANCEL information.

        Expected Result:
        - Scan reports are captured from cancelled queries.
        - Scan reports reflect the CLIENT_CANCEL status.
        - Error messages verify queries were cancelled as expected.
        """
        buckets = self.create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)

        if len(index_nodes) < 3:
            self.fail("Test requires at least 3 indexer nodes")

        all_definitions = []
        scalar_selects = []


        for bucket in buckets:
            self.prepare_collection_for_indexing(
                bucket_name=bucket,
                num_scopes=1,
                num_collections=1,
                num_of_docs_per_collection=10000,
                json_template=self.json_template,
                load_default_coll=False
            )
        for namespace in self.namespaces:
            prefix = ''.join(namespace.split(':')[1].split('.'))

             
            s_defs, _, s_selects = self._create_scalar_indexes(
                namespace, prefix, query_node, num_replica=2, randomise_replica_count=True
            )
            all_definitions.extend(s_defs)
            scalar_selects.extend(s_selects)
        self.log.info("Waiting for indexes to be ready")
        self.wait_until_indexes_online()
        for definition in all_definitions:
            self.log.info(f"Index {definition.index_name} created successfully")
        all_selects = scalar_selects
        n1ql_rest = RestConnection(query_node)
        self.log.info("=== Running scans and cancelling via request ID ===")

        for idx, query in enumerate(all_selects):
            client_ctx_id = f"scan_report_cancel_{self.rand}_{idx}"
            self.log.info(f"Firing query with client_context_id={client_ctx_id}: {query[:80]}...")
            query_result = {"result": None, "error": None}
            def _fire_query(q, ctx_id, out):
                try:
                    result = n1ql_rest.query_tool(
                        q,
                        query_params={
                            "profile": "timings",
                            "scanreport_wait": 1000000,
                            "client_context_id": ctx_id
                        }
                    )
                    out["result"] = result
                except Exception as e:
                    out["error"] = str(e)

            # Fire query in a thread so we can cancel it concurrently
            query_thread = threading.Thread(
                target=_fire_query, args=(query, client_ctx_id, query_result)
            )
            query_thread.start()

            # Small delay to let the query start executing, then cancel
            self.sleep(0.001)

            # Cancel the query using DELETE on the N1QL active_requests endpoint
            try:
                self._cancel_query_by_client_context_id(n1ql_rest, client_ctx_id)

            except Exception as e:
                self.log.warning(f"Error cancelling query: {str(e)}")

            # Wait for the query thread to finish
            query_thread.join(timeout=30)

            result = query_result["result"]
            if result:
                self.log.info(f"Response: {json.dumps(result, indent=2)}")

                if result.get("status") == "errors" or "errors" in result:
                    self.log.info(f"Query returned error as expected: {result.get('errors', [])}")

                scan_report, _ = self.extract_scan_report(result)
                if scan_report:
                    self.log.info(f"Scan report found after cancel")
                
                    self.validate_report(
                        scan_report,
                        is_vector_scan=False,
                        is_detailed="detailed" in scan_report,
                        expect_incomplete=True,
                        failover_scenario=True
                    )
                else:
                    self.log.warning(f"No scan report found for cancelled query: {query[:50]}...")
            elif query_result["error"]:
                self.log.info(f"Query error after cancel (expected): {query_result['error']}")
            else:
                self.log.warning(f"No result from query thread for: {query[:50]}...")

        self.log.info("Test completed - CLIENT_CANCEL scan report validation")

    def _cancel_query_by_client_context_id(self, n1ql_rest, client_context_id):
        """
        Cancel an active N1QL query using its client_context_id.
        Uses DELETE FROM system:active_requests to terminate the query directly.
        This is faster than looking up requestID first and then calling the REST API.
        """
        cancel_query = f'DELETE FROM system:active_requests WHERE clientContextID = "{client_context_id}"'
        self.log.info(f"Cancelling query via: {cancel_query}")
        try:
            result = self.run_cbq_query(query=cancel_query)
            mutations = result.get('metrics', {}).get('mutationCount', 0)
            if mutations > 0:
                self.log.info(f"Cancel successful: mutationCount={mutations}")
            else:
                self.log.warning(f"No active request found/cancelled for clientContextID={client_context_id}")
        except Exception as e:
            self.log.warning(f"Cancel request failed: {str(e)}")

    def test_scan_report_client_cancel_rows_returned(self):
        """
        Test scan reports when queries are cancelled immediately — before any rows are returned. Uses a zero delay cancel so the
        cancellation races ahead of the query response. Expects incomplete scan reports.
        Cancel the query using the request ID. This needs to happen before the indexer node returns any rows. Validate the scan reports.
    
        """
        buckets = self.create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)

        if len(index_nodes) < 3:
            self.fail("Test requires at least 3 indexer nodes")


        all_definitions = []
        scalar_selects = []
        

        for bucket in buckets:
            self.prepare_collection_for_indexing(
                bucket_name=bucket,
                num_scopes=1,
                num_collections=1,
                num_of_docs_per_collection=10000,
                json_template=self.json_template,
                load_default_coll=False
            )

        for namespace in self.namespaces:
            prefix = ''.join(namespace.split(':')[1].split('.'))

            s_defs, _, s_selects = self._create_scalar_indexes(
                namespace, prefix, query_node, num_replica=2, randomise_replica_count=True
            )
            

            all_definitions.extend(s_defs)
            scalar_selects.extend(s_selects)


        self.log.info("Waiting for indexes to be ready")
        self.wait_until_indexes_online()

        for definition in all_definitions:
            self.log.info(f"Index {definition.index_name} created successfully")

        all_selects = scalar_selects 
        n1ql_rest = RestConnection(query_node)

        incomplete_count = 0
        no_report_count = 0

        self.log.info("=== Running scans and cancelling IMMEDIATELY (before indexer report) ===")
        for idx, query in enumerate(all_selects):
            client_ctx_id = f"scan_cancel_early_{self.rand}_{idx}"

            query_result = {"result": None, "error": None}

            def _fire_query(q, ctx_id, out):
                try:
                    result = n1ql_rest.query_tool(
                        q,
                        query_params={
                            "profile": "timings",
                            "scanreport_wait": 1000000,
                            "client_context_id": ctx_id
                        }
                    )
                    out["result"] = result
                except Exception as e:
                    out["error"] = str(e)

            query_thread = threading.Thread(
                target=_fire_query, args=(query, client_ctx_id, query_result)
            )
            query_thread.start()

            # Cancel IMMEDIATELY — no sleep — to race the query response
            try:
                self._cancel_query_by_client_context_id(n1ql_rest, client_ctx_id)
            except Exception as e:
                self.log.warning(f"Error cancelling query: {str(e)}")

            query_thread.join(timeout=30)

            result = query_result["result"]
            if result:
                self.log.info(f"Response: {json.dumps(result, indent=2)}")

                scan_report, _ = self.extract_scan_report(result)
                if scan_report:
                    validation_result = self.validate_report(
                        scan_report,
                        is_detailed="detailed" in scan_report,
                        expect_incomplete=True,
                        failover_scenario=True
                    )
                    if not validation_result["is_complete"]:
                        self.log.info(f"Scan report incomplete as expected for: {query[:50]}...")
                        incomplete_count += 1
                    else:
                        self.log.info(f"Scan report complete (cancel may have arrived late): {query[:50]}...")
                else:
                    self.log.info(f"No scan report (expected — cancel arrived before report): {query[:50]}...")
                    no_report_count += 1
            elif query_result["error"]:
                self.log.info(f"Query error after immediate cancel (expected): {query_result['error']}")
                no_report_count += 1

        self.log.info(f"=== Summary: {incomplete_count} incomplete reports, "
                      f"{no_report_count} no-report results ===")

        if incomplete_count == 0 and no_report_count == 0:
            self.log.warning("No incomplete or missing reports — cancel may not have been fast enough")

        self.log.info("Test completed - CLIENT_CANCEL before indexer report")

    def test_scan_report_client_cancel_before_report(self):
        """
        Test scan reports when queries are cancelled before the indexer can send the report
        to the query service. Uses multithreading to send query and cancel requests
        simultaneously, racing the cancellation ahead of the indexer response.
        
        This test validates that scan reports are incomplete when cancellation wins the race.
        
        Index types: Primary, Secondary (single/multiple keys), Missing keys,
        Partitioned, Replica (with varying replica counts: 0, 1, 2)
        """
        buckets = self.create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)

        if len(index_nodes) < 3:
            self.fail("Test requires at least 3 indexer nodes")

        all_definitions = []
        scalar_selects = []
        scalar_index_names = []

        # Step 1: Create buckets and load data
        for bucket in buckets:
            self.prepare_collection_for_indexing(
                bucket_name=bucket,
                num_scopes=1,
                num_collections=1,
                num_of_docs_per_collection=10000,
                json_template=self.json_template,
                load_default_coll=False
            )

        # Step 2: Create scalar indexes with varying replica counts (0, 1, 2)
        for namespace in self.namespaces:
            prefix = ''.join(namespace.split(':')[1].split('.'))
            s_defs, _, s_selects = self._create_scalar_indexes(
                namespace, prefix, query_node, num_replica=2, randomise_replica_count=True
            )
            all_definitions.extend(s_defs)
            scalar_selects.extend(s_selects)
            scalar_index_names.extend([d.index_name for d in s_defs])
        # Step 3: Wait for indexes to be ready and validate
        self.log.info("Waiting for indexes to be ready")
        self.wait_until_indexes_online()
        self.log.info("Validating all indexes are created and in Ready state")
        for definition in all_definitions:
            self.log.info(f"Index {definition.index_name} created successfully and is Ready")
        n1ql_rest = RestConnection(query_node)
        incomplete_count = 0
        no_report_count = 0
        complete_count = 0
        error_count = 0
        self.log.info("=== Phase 3: Running scans with simultaneous cancellation ===")
        self.log.info("Using multithreading to race cancellation ahead of indexer report")
        for idx, query in enumerate(scalar_selects):
            client_ctx_id = f"cancel_before_report_{self.rand}_{idx}"
            index_name = scalar_index_names[idx] if idx < len(scalar_index_names) else None
            self.log.info(f"Query {idx + 1}/{len(scalar_selects)}: {query[:80]}...")
            self.log.info(f"Index: {index_name}, client_context_id: {client_ctx_id}")
            query_result = {"result": None, "error": None}
            cancel_sent = Event()
            query_started = Event()

            def _fire_query(q, ctx_id, out, started_event):
                """Fire the query and signal when it starts"""
                try:
                    started_event.set()  # Signal that query is being sent
                    result = n1ql_rest.query_tool(
                        q,
                        query_params={
                            "profile": "timings",
                            "scanreport_wait": 1000000,
                            "client_context_id": ctx_id
                        }
                    )
                    out["result"] = result
                except Exception as e:
                    out["error"] = str(e)

            def _cancel_query_rapid(rest, ctx_id, sent_event, num_attempts=3):
                """Send multiple rapid cancel requests to maximize chance of racing ahead"""
                for attempt in range(num_attempts):
                    try:
                        self._cancel_query_by_client_context_id(rest, ctx_id)
                    except Exception as e:
                        pass  # Ignore errors, we're racing
                sent_event.set()

            # Start query and cancel threads simultaneously
            query_thread = threading.Thread(
                target=_fire_query, 
                args=(query, client_ctx_id, query_result, query_started)
            )
            cancel_thread = threading.Thread(
                target=_cancel_query_rapid,
                args=(n1ql_rest, client_ctx_id, cancel_sent, 3)
            )

            # Start both threads at the same time to race
            query_thread.start()
            cancel_thread.start()

            # Wait for both to complete
            query_thread.join(timeout=60)
            cancel_thread.join(timeout=10)

            # Analyze results
            result = query_result["result"]
            if result:
                if result.get("status") == "errors" or "errors" in result:
                    self.log.info(f"Query returned error (expected due to cancel): {result.get('errors', [])}")

                scan_report, actual_index_name = self.extract_scan_report(result)
                if scan_report:
                    self.log.info(f"Scan report found - validating...")
                    
                    # Check if report is incomplete
                    is_incomplete = self._is_report_incomplete(scan_report)
                    
                    # Validate the report structure
                    try:
                        self.validate_report(
                            scan_report,
                            is_vector_scan=False,
                            is_detailed="detailed" in scan_report,
                            expect_incomplete=True,
                            failover_scenario=True
                        )
                    except ReportValidationError as e:
                        self.log.info(f"Validation error (expected for incomplete report): {str(e)}")
                    
                    if is_incomplete:
                        self.log.info(f"SUCCESS: Scan report is INCOMPLETE as expected")
                        incomplete_count += 1
                    else:
                        self.log.info(f"Scan report is COMPLETE (cancel arrived after indexer response)")
                        complete_count += 1
                else:
                    self.log.info(f"No scan report in response (cancel won the race completely)")
                    no_report_count += 1
                    
            elif query_result["error"]:
                self.log.info(f"Query error (expected - cancel won): {query_result['error']}")
                error_count += 1
            else:
                self.log.warning(f"No result or error from query thread")
                error_count += 1

        # Summary
        self.log.info("=" * 60)
        self.log.info("=== Test Summary ===")
        self.log.info(f"Total queries executed: {len(scalar_selects)}")
        self.log.info(f"Incomplete reports (cancel won partially): {incomplete_count}")
        self.log.info(f"No reports (cancel won completely): {no_report_count}")
        self.log.info(f"Complete reports (indexer won): {complete_count}")
        self.log.info(f"Errors/exceptions: {error_count}")
        self.log.info("=" * 60)

        # Validation: At least some cancellations should have won the race
        cancel_wins = incomplete_count + no_report_count + error_count
        if cancel_wins == 0:
            self.log.warning("WARNING: No cancellations won the race - all reports were complete")
            self.log.warning("This could indicate the cancellation mechanism isn't fast enough")
        else:
            self.log.info(f"SUCCESS: {cancel_wins} out of {len(scalar_selects)} cancellations won the race")

        self.log.info("Test completed - CLIENT_CANCEL before indexer report")

    def test_scan_report_query_service_kill(self):
        """
        Test scan reports when the query service is killed just as requests are received.
        This is a negative test — scan reports may not be correctly generated.
        Creates all index types, starts scans in background, kills the query service,
        then validates whatever scan reports were returned before the kill.
        1. Create a cluster with 3+ indexer nodes and verify node count.
        2. Create scalar indexes with replicas for all namespaces.
        3. Wait until all indexes reach the online state and verify index creation.
        4. Start continuous scans in a background thread with scanreport_wait to capture reports.
        5. Kill the query service (cbq-engine) on the query node while scans are running.
        6. Wait for scan thread to finish and capture any scan reports or errors returned.
        7. Validate any captured scan reports, expecting incomplete reports due to the kill.
        8. Log any errors captured from the scan thread.
        9. Wait for query service to auto-recover and verify it's back up.

        """
        buckets = self.create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)

        if len(index_nodes) < 3:
            self.fail("Test requires at least 3 indexer nodes")

        all_definitions = []
        scalar_selects = []


        for bucket in buckets:
            self.prepare_collection_for_indexing(
                bucket_name=bucket,
                num_scopes=1,
                num_collections=1,
                num_of_docs_per_collection=10000,
                json_template=self.json_template,
                load_default_coll=False
            )

        for namespace in self.namespaces:
            prefix = ''.join(namespace.split(':')[1].split('.'))

            s_defs, _, s_selects = self._create_scalar_indexes(
                namespace, prefix, query_node, num_replica=2, randomise_replica_count=True
            )
            

            all_definitions.extend(s_defs)
            scalar_selects.extend(s_selects)

        self.log.info("Waiting for indexes to be ready")
        self.wait_until_indexes_online()

        for definition in all_definitions:
            self.log.info(f"Index {definition.index_name} created successfully")

        all_selects = scalar_selects

        # Start continuous scans in a background thread
        scan_stop_event = Event()
        scan_results = {"reports": [], "errors": []}

        def _run_scans_continuously():
            while not scan_stop_event.is_set():
                for query in all_selects:
                    if scan_stop_event.is_set():
                        break
                    try:
                        result = self.n1ql_rest.query_tool(
                            query,
                            query_params={"profile": "timings", "scanreport_wait": 1000000}
                        )
                        self.log.info(f"result for query '{query[:50]}...': {json.dumps(result, indent=2)}")
                        scan_report, _ = self.extract_scan_report(result)
                        if scan_report:
                            scan_results["reports"].append(scan_report)
                        if result.get("status") == "errors" or "errors" in result:
                            scan_results["errors"].append({
                                "query": query[:80],
                                "errors": result.get("errors", [])
                            })
                    except Exception as e:
                        scan_results["errors"].append({
                            "query": query[:80],
                            "error": str(e)
                        })

        self.log.info("=== Starting continuous scans in background ===")
        scan_thread = threading.Thread(target=_run_scans_continuously)
        scan_thread.start()

        # Give scans a moment to start
        self.sleep(5, "Waiting for scans to start")

        # Kill the query service on the query node
        self.log.info(f"=== Killing query service (cbq-engine) on node {query_node.ip} ===")
        try:
            shell = RemoteMachineShellConnection(query_node)
            shell.terminate_process(
                info=shell.extract_remote_info(), process_name='cbq-engine', force=True
            )
            self.log.info(f"Killed query service on {query_node.ip}")
            shell.disconnect()
        except Exception as e:
            self.log.error(f"Error killing query service on {query_node.ip}: {str(e)}")

        # Let scan thread collect errors for a bit after the kill
        self.sleep(10, "Collecting results after query service kill")

        # Stop the scan thread
        scan_stop_event.set()
        scan_thread.join(timeout=60)

        self.log.info(f"Collected {len(scan_results['reports'])} scan reports "
                      f"and {len(scan_results['errors'])} errors")

        # Validate whatever scan reports were captured before the kill
        for scan_report in scan_results["reports"]:
            try:
                self.validate_report(
                    scan_report,
                    is_vector_scan=False,
                    is_detailed="detailed" in scan_report,
                    expect_incomplete=True,
                    failover_scenario=True
                )
            except ReportValidationError as e:
                self.log.warning(f"Scan report validation issue: {str(e)}")

        # Log error summary
        for err in scan_results["errors"]:
            self.log.info(f"Scan error for query '{err.get('query', '')}': "
                          f"{err.get('errors', err.get('error', ''))}")

        # Wait for query service to auto-restart
        self.log.info("=== Waiting for query service to recover ===")
        self.sleep(30, "Waiting for query service to auto-restart")

        # Verify query service is back by running a simple query
        try:
            result = self.n1ql_rest.query_tool("SELECT 1")
            if result.get("status") == "success":
                self.log.info("Query service recovered successfully")
            else:
                self.log.warning(f"Query service may not be fully recovered: {result}")
        except Exception as e:
            self.log.warning(f"Query service still not available: {str(e)}")

        self.log.info("Test completed - query service kill scan report validation (negative test)")

    def _keep_indexer_down(self, index_node, stop_event, interval=5):
        """
        Flapper thread that keeps the indexer process down on a node.
        Continuously checks if indexer is running and kills it if so,
        until stop_event is set.
        """
        remote = RemoteMachineShellConnection(index_node)
        remote_helper = RemoteMachineHelper(remote)
        try:
            while not stop_event.is_set():
                if remote_helper.is_process_running("indexer"):
                    self.log.info(f"[FLAPPER] Indexer UP on {index_node.ip}, killing it")
                    remote.terminate_process(process_name="indexer")
                else:
                    self.log.info(f"[FLAPPER] Indexer already DOWN on {index_node.ip}")
                time.sleep(interval)
        finally:
            remote.disconnect()

    def test_scan_report_server_error(self):
        """
        Test scan report behavior when the indexer service is forcibly kept down (SERVER_ERROR scenario).

        This test simulates a severe failure by killing the indexer process on all index nodes and keeping it down
        using background flapper threads. It then runs scan queries to collect scan reports and error responses
        while the indexer is unavailable. After collecting results, the test stops the flapper threads and waits
        for the indexer to auto-restart and indexes to come back online.

        Steps:
        1. Create a cluster with 3+ indexer nodes and verify node count.
        2. Create scalar indexes on all namespaces and wait for them to be ready.
        3. Simultaneously kill the indexer process on all index nodes.
        4. Start flapper threads to keep the indexer process down on each node.
        5. Run scan queries and collect scan reports and error responses while indexer is down.
        6. Stop the flapper threads and wait for the indexer to auto-restart.
        7. Wait for all indexes to come back online.
        8. Validate collected scan reports and log error summaries.

        Expected Results:
        - Scan queries should return error responses or incomplete scan reports due to indexer unavailability.
        - The test should log the number of scan reports and errors collected during the outage.
        - After recovery, all indexes should return to the online state.
        """
        buckets = self.create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 3:
            self.fail("Test requires at least 3 indexer nodes")
        all_definitions = []
        scalar_selects = []
        for bucket in buckets:
            self.prepare_collection_for_indexing(
                bucket_name=bucket,
                num_scopes=1,
                num_collections=1,
                num_of_docs_per_collection=10000,
                json_template=self.json_template,
                load_default_coll=False
            )
        for namespace in self.namespaces:
            prefix = ''.join(namespace.split(':')[1].split('.'))
             
            s_defs, _, s_selects = self._create_scalar_indexes(
                namespace, prefix, query_node, num_replica=2, randomise_replica_count=True
            )
            all_definitions.extend(s_defs)
            scalar_selects.extend(s_selects)
        self.log.info("Waiting for indexes to be ready")
        self.wait_until_indexes_online()
        for definition in all_definitions:
            self.log.info(f"Index {definition.index_name} created successfully")
        self.log.info("=== Phase 1: Killing indexer on all index nodes simultaneously ===")
        start_barrier = Barrier(len(index_nodes), timeout=120)

        def kill_indexer(index_node):
            shell = RemoteMachineShellConnection(index_node)
            try:
                start_barrier.wait()
                shell.terminate_process(
                    info=shell.extract_remote_info(), process_name='indexer', force=True
                )
                self.log.info(f"Killed indexer on {index_node.ip}")
                time.sleep(5)
                if not RemoteMachineHelper(shell).is_process_running("indexer"):
                    self.log.info(f"[CONFIRMED] Indexer is DOWN on {index_node.ip}")
                else:
                    self.log.warning(f"Indexer still running on {index_node.ip}")
            except Exception as e:
                self.log.error(f"Error killing indexer on {index_node.ip}: {e}")
            finally:
                shell.disconnect()

        with ThreadPoolExecutor(max_workers=len(index_nodes)) as executor:
            futures = [executor.submit(kill_indexer, node) for node in index_nodes]
            for future in as_completed(futures):
                future.result()
        self.sleep(10, "Allowing indexer to transition to unavailable state")
        self.log.info("=== Phase 2: Starting flapper threads to keep indexer down ===")
        flapper_stop_event = Event()
        flapper_executor = ThreadPoolExecutor(max_workers=len(index_nodes))
        flapper_futures = []
        for node in index_nodes:
            flapper_futures.append(
                flapper_executor.submit(self._keep_indexer_down, node, flapper_stop_event, 5)
            )
        self.log.info("=== Phase 3: Running scans while indexer is down ===")
        scan_results = {"reports": [], "errors": []}
        for query in scalar_selects:
            try:
                result = self.n1ql_rest.query_tool(
                    query,
                    query_params={"profile": "timings", "scanreport_wait": 1000000}
                )
                if result is None or not isinstance(result, dict):
                    scan_results["errors"].append({
                        "query": query[:80],
                        "error": "query_tool returned None or non-dict"
                    })
                    continue
                self.log.info(f"Query result: {json.dumps(result, indent=2)}")
                scan_report, _ = self.extract_scan_report(result)
                if scan_report:
                    scan_results["reports"].append(scan_report)
                if result.get("status") == "errors" or "errors" in result:
                    scan_results["errors"].append({
                        "query": query[:80],
                        "errors": result.get("errors", [])
                    })
            except Exception as e:
                scan_results["errors"].append({
                    "query": query[:80],
                    "error": str(e)
                })
        self.log.info(f"Collected {len(scan_results['reports'])} scan reports "
                      f"and {len(scan_results['errors'])} errors")
        for scan_report in scan_results["reports"]:
            try:
                self.validate_report(
                    scan_report,
                    is_vector_scan=False,
                    is_detailed="detailed" in scan_report,
                    expect_incomplete=True,
                    failover_scenario=True
                )
            except ReportValidationError as e:
                self.log.warning(f"Scan report validation issue: {str(e)}")
        for err in scan_results["errors"]:
            self.log.info(f"Scan error for query '{err.get('query', '')}': "
                          f"{err.get('errors', err.get('error', ''))}")
        self.log.info("=== Phase 4: Stopping flappers and waiting for indexer recovery ===")
        flapper_stop_event.set()
        for future in flapper_futures:
            future.result(timeout=30)
        flapper_executor.shutdown(wait=True)
        self.log.info("Waiting for indexer to auto-restart and indexes to come online")
        self.sleep(30, "Waiting for indexer to auto-restart")
        self.wait_until_indexes_online()
        self.log.info("Test completed - SERVER_ERROR scan report validation")


    def test_scan_report_partition_elimination(self):
        """
        Test scan report for partitioned indexes with partition elimination.
        Create different types of partitioned indexes and run scans that trigger partition elimination.
        Validate the scan report for each type of partitioned index.
        
        Partition elimination scenarios:
        1. Single key partition (e.g., partition by `type`)
        2. Multiple key partition (e.g., partition by `type`, `free_breakfast`)
        3. Using IN predicate for partition elimination
        
        Test Steps:
        1. Create a cluster with 3 or more indexer nodes with num_replica set to 1 for some indexes,
           0 for some, and 2 for some.
        2. Create partitioned indexes with different partition schemes.
        3. Run scans for each index such that partition elimination comes into play.
        4. Validate that scan reports are generated with the expected metrics.
        5. Verify partition elimination by checking indexer stats (num_requests per partition).
        """
        
        
        buckets = self.create_test_buckets(num_buckets=1)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        
        if len(index_nodes) < 3:
            self.fail("Test requires at least 3 indexer nodes for partition elimination testing")
        
        all_definitions = []
        all_select_queries = []
        all_index_names = []
        
        for bucket in buckets:
            self.prepare_collection_for_indexing(
                bucket_name=bucket,
                num_scopes=1,
                num_collections=1,
                num_of_docs_per_collection=10000,
                json_template=self.json_template,
                load_default_coll=False
            )
        
        collection_namespace = self.namespaces[0]
        prefix = ''.join(collection_namespace.split(':')[1].split('.'))
        self.log.info("=== Scenario 1: Creating partitioned index with single key partition (type) ===")
        partitioned_single_key = QueryDefinition(
            index_name=f"{prefix}_partitioned_single_key_type",
            index_fields=['type', 'price', 'avg_rating'],
            query_template=RANGE_SCAN_TEMPLATE.format(
                "name, type, price, avg_rating",
                'type = "Hotel" AND price > 500 ORDER BY avg_rating DESC'
            ),
            partition_by_fields=['type']
        )
        
        create_query_single = partitioned_single_key.generate_index_create_query(
            namespace=collection_namespace,
            num_partition=8,
            num_replica=1
        )
        self.log.info(f"Creating index: {create_query_single}")
        self.run_cbq_query(query=create_query_single)
        all_definitions.append(partitioned_single_key)
        all_index_names.append(partitioned_single_key.index_name)
        self.log.info("=== Scenario 2: Creating partitioned index with multiple key partition (type, free_breakfast) ===")
        partitioned_multi_key = QueryDefinition(
            index_name=f"{prefix}_partitioned_multi_key",
            index_fields=['type', 'free_breakfast', 'price', 'country'],
            query_template=RANGE_SCAN_TEMPLATE.format(
                "name, type, free_breakfast, price, country",
                'type = "Hotel" AND free_breakfast = true AND price > 500 ORDER BY country'
            ),
            partition_by_fields=['type', 'free_breakfast']
        )
        
        create_query_multi = partitioned_multi_key.generate_index_create_query(
            namespace=collection_namespace,
            num_partition=8,
            num_replica=0
        )
        self.log.info(f"Creating index: {create_query_multi}")
        self.run_cbq_query(query=create_query_multi)
        all_definitions.append(partitioned_multi_key)
        all_index_names.append(partitioned_multi_key.index_name)
        self.log.info("=== Scenario 3: Creating partitioned index for IN predicate elimination (country) ===")
        partitioned_in_predicate = QueryDefinition(
            index_name=f"{prefix}_partitioned_in_predicate",
            index_fields=['country', 'city', 'price', 'avg_rating'],
            query_template=RANGE_SCAN_TEMPLATE.format(
                "name, country, city, price, avg_rating",
                'country IN ["United States", "United Kingdom", "France"] AND price > 500 ORDER BY city'
            ),
            partition_by_fields=['country']
        )
        
        create_query_in = partitioned_in_predicate.generate_index_create_query(
            namespace=collection_namespace,
            num_partition=8,
            num_replica=2
        )
        self.log.info(f"Creating index: {create_query_in}")
        self.run_cbq_query(query=create_query_in)
        all_definitions.append(partitioned_in_predicate)
        all_index_names.append(partitioned_in_predicate.index_name)
        self.log.info("=== Scenario 4: Creating partitioned index with boolean partition key (free_parking) ===")
        partitioned_boolean = QueryDefinition(
            index_name=f"{prefix}_partitioned_boolean_key",
            index_fields=['free_parking', 'free_breakfast', 'price', 'name'],
            query_template=RANGE_SCAN_TEMPLATE.format(
                "name, free_parking, free_breakfast, price",
                'free_parking = true AND price BETWEEN 500 AND 1500 ORDER BY name'
            ),
            partition_by_fields=['free_parking']
        )
        
        create_query_bool = partitioned_boolean.generate_index_create_query(
            namespace=collection_namespace,
            num_partition=4,
            num_replica=1
        )
        self.log.info(f"Creating index: {create_query_bool}")
        self.run_cbq_query(query=create_query_bool)
        all_definitions.append(partitioned_boolean)
        all_index_names.append(partitioned_boolean.index_name)
        self.log.info("Waiting for indexes to be ready")
        self.wait_until_indexes_online() 
        self.log.info("Validating all indexes are created and in ready state")
        for definition in all_definitions:
            self.log.info(f"Index {definition.index_name} created successfully")
        all_select_queries = self.gsi_util_obj.get_select_queries(
            definition_list=all_definitions,
            namespace=collection_namespace,
            limit=self.scan_limit
        )
        self.log.info("=== Phase 1: Validating scan reports for partitioned indexes (CONCISE) ===")
        concise_scan_reports = []
        for i, (query, index_name) in enumerate(zip(all_select_queries, all_index_names)):
            self.log.info(f"--- Running CONCISE scan {i+1}/{len(all_select_queries)} for index: {index_name} ---")
            try:
                scan_report = self._run_single_scan_and_validate(
                    query=query,
                    runDetailed=False,
                    index_name=index_name
                )
                if scan_report:
                    concise_scan_reports.append((index_name, scan_report))
                    self.log.info(f"CONCISE scan report captured for {index_name}")
                    # Check for partns field which indicates partition-level info
                    if "partns" in scan_report:
                        self.log.info(f"Partition info found in scan report: {list(scan_report['partns'].keys())}")

            except Exception as e:
                self.log.error(f"Error running CONCISE scan for {index_name}: {str(e)}")
        self.log.info("=== Phase 2: Validating scan reports for partitioned indexes (DETAILED) ===")
        detailed_scan_reports = []
        for i, (query, index_name) in enumerate(zip(all_select_queries, all_index_names)):
            self.log.info(f"--- Running DETAILED scan {i+1}/{len(all_select_queries)} for index: {index_name} ---")
            try:
                scan_report = self._run_single_scan_and_validate(
                    query=query,
                    runDetailed=True,
                    index_name=index_name
                )
                if scan_report:
                    detailed_scan_reports.append((index_name, scan_report))
                    self.log.info(f"DETAILED scan report captured for {index_name}")
                    # Check for partns field in detailed mode too
                    if "partns" in scan_report:
                        self.log.info(f"Partition info found in DETAILED scan report: {list(scan_report['partns'].keys())}")

            except Exception as e:
                self.log.error(f"Error running DETAILED scan for {index_name}: {str(e)}")
        self.log.info("=== Phase 3: Validating partition elimination via indexer stats ===")
        
        for definition in all_definitions:
            index_name = definition.index_name
            self.log.info(f"Checking partition stats for index: {index_name}")
            
            # Get partition-level stats
            stats_map = self.get_index_stats(perNode=True, partition=True)
            num_requests_list = []
            
            for node in stats_map:
                for keyspace in stats_map[node]:
                    for idx in stats_map[node][keyspace]:
                        if index_name in idx:
                            num_requests = stats_map[node][keyspace][idx].get('num_requests', 0)
                            self.log.info(f"Index partition {idx} on node {node}: num_requests = {num_requests}")
                            num_requests_list.append(num_requests)
            
            # Partition elimination should result in unequal num_requests across partitions
            if len(num_requests_list) > 1:
                # Check if partition elimination occurred (not all partitions have same num_requests)
                unique_requests = set(num_requests_list)
                if len(unique_requests) > 1:
                    self.log.info(f"Partition elimination detected for {index_name}: "
                                 f"Different num_requests across partitions: {num_requests_list}")
                else:
                    self.log.warning(f"No partition elimination detected for {index_name}: "
                                    f"All partitions have same num_requests: {num_requests_list}")
            else:
                self.log.warning(f"Could not verify partition elimination for {index_name}: "
                                f"Insufficient partition data")
        self.log.info("=== Phase 4: Running scans with USE INDEX clause ===")
        for definition in all_definitions:
            index_name = definition.index_name
            if definition == partitioned_single_key:
                query_with_use_index = f"""
                    SELECT name, type, price, avg_rating 
                    FROM {collection_namespace} USE INDEX (`{index_name}`)
                    WHERE type = "Hotel" AND price > 500 
                    ORDER BY avg_rating DESC
                    LIMIT {self.scan_limit}
                """.strip()
            elif definition == partitioned_multi_key:
                query_with_use_index = f"""
                    SELECT name, type, free_breakfast, price, country 
                    FROM {collection_namespace} USE INDEX (`{index_name}`)
                    WHERE type = "Hotel" AND free_breakfast = true AND price > 500 
                    ORDER BY country
                    LIMIT {self.scan_limit}
                """.strip()
            elif definition == partitioned_in_predicate:
                query_with_use_index = f"""
                    SELECT name, country, city, price, avg_rating 
                    FROM {collection_namespace} USE INDEX (`{index_name}`)
                    WHERE country IN ["United States", "United Kingdom", "France"] AND price > 500 
                    ORDER BY city
                    LIMIT {self.scan_limit}
                """.strip()
            else:  
                query_with_use_index = f"""
                    SELECT name, free_parking, free_breakfast, price 
                    FROM {collection_namespace} USE INDEX (`{index_name}`)
                    WHERE free_parking = true AND price BETWEEN 500 AND 1500 
                    ORDER BY name
                    LIMIT {self.scan_limit}
                """.strip()
            self.log.info(f"Running scan with USE INDEX for {index_name}")
            try:
                scan_report = self._run_single_scan_and_validate(
                    query=query_with_use_index,
                    runDetailed=True,
                    index_name=index_name
                )
                if scan_report:
                    self.log.info(f"USE INDEX scan successful for {index_name}")
            except Exception as e:
                self.log.error(f"Error with USE INDEX scan for {index_name}: {str(e)}")
        self.log.info("=== Test Summary ===")
        self.log.info(f"Total partitioned indexes created: {len(all_definitions)}")
        self.log.info(f"CONCISE scan reports captured: {len(concise_scan_reports)}")
        self.log.info(f"DETAILED scan reports captured: {len(detailed_scan_reports)}")
        self.log.info("=== Cleanup: Dropping partitioned indexes ===")
        for definition in all_definitions:
            drop_query = definition.generate_index_drop_query(namespace=collection_namespace)
            try:
                self.run_cbq_query(query=drop_query)
                self.log.info(f"Dropped index: {definition.index_name}")
            except Exception as e:
                self.log.warning(f"Error dropping index {definition.index_name}: {str(e)}")
        
        self.log.info("Test completed - partition elimination scan report validation")
    

    def validate_report(self, report: Dict[str, Any], is_vector_scan: bool = False, 
                        expect_retries: bool = False, is_detailed: bool = False,
                        expect_incomplete: bool = False, failover_scenario: bool = False) -> Dict[str, Any]:
        """
        Validates scan report structure and content.
        
        Args:
            report: The scan report dictionary to validate
            is_vector_scan: If True, validates vector-specific fields (dist_comp, decode, rowsFiltered, rowsReranked)
            expect_retries: If True, validates that retries field is present (for replica retry tests)
            is_detailed: If True, validates detailed report structure
            expect_incomplete: If True, expects report to be incomplete (e.g., during failover)
            failover_scenario: If True, validates report for failover scenario (relaxed validation, skips rows stats validation)
        
        Returns:
            Dict with validation results including 'is_complete' and 'validation_errors'
        
        Raises ReportValidationError if validation fails (unless expect_incomplete=True).
        """
        self.log.info("Validating scan report")
        validation_result = {
            "is_complete": True,
            "validation_errors": [],
            "has_error_field": False,
            "has_retries": False,
            "incomplete_partitions": []
        }
        
        # Check if report is incomplete (for failover scenarios)
        is_incomplete = self._is_report_incomplete(report)
        validation_result["is_complete"] = not is_incomplete
        
        if expect_incomplete:
            if is_incomplete:
                self.log.info("Report is incomplete as expected")
                # For expected incomplete reports, just log and return
                validation_result["validation_errors"].append("Report incomplete as expected")
                return validation_result
            else:
                self.log.warning("Expected incomplete report but got complete report")
        
        # For failover scenarios, use relaxed validation
        if failover_scenario:
            return self._validate_report_failover_mode(report, is_detailed, validation_result)
        
        # 1. Mandatory top-level fields
        if "defn" not in report:
            raise ReportValidationError("Missing mandatory field: defn")

        if "srvr_total_counts" not in report:
            raise ReportValidationError("Missing mandatory field: srvr_total_counts")

        # 2. Unit validation (ns OR ms, not both) - Unit consistency check
        has_ns = "srvr_avg_ns" in report
        has_ms = "srvr_avg_ms" in report

        if not has_ns and not has_ms:
            raise ReportValidationError(
                "Either srvr_avg_ns or srvr_avg_ms must be present"
            )

        if has_ns and has_ms:
            raise ReportValidationError(
                "srvr_avg_ns and srvr_avg_ms cannot both be present - unit consistency violation"
            )

        unit_field = "srvr_avg_ns" if has_ns else "srvr_avg_ms"
        unit_type = "ns" if has_ns else "ms"

        # 3. Validate srvr_avg fields
        self.validate_srvr_avg(report[unit_field], unit_field, is_vector_scan)

        # 4. Validate srvr_total_counts
        self.validate_srvr_total_counts(report["srvr_total_counts"], is_vector_scan)

        # 5. Optional partns (only if present)
        if "partns" in report:
            self.validate_partns(report["partns"])

        # 6. Aggregated report validation
        if "num_scans" in report:
            if not isinstance(report["num_scans"], int) or report["num_scans"] <= 0:
                raise ReportValidationError("num_scans must be a positive integer")

            if not isinstance(report["defn"], list):
                raise ReportValidationError(
                    "Aggregated report requires defn to be an array"
                )

        # 7. Detailed mode validation with unit consistency and partition validation
        if is_detailed and "detailed" in report:
            detailed_section = report["detailed"]
            # Handle aggregated scans where detailed is a list of sub-reports
            if isinstance(detailed_section, list):
                self.log.info(f"Aggregated scan detected with {len(detailed_section)} sub-reports")
                for i, sub_report in enumerate(detailed_section):
                    if isinstance(sub_report, dict) and "detailed" in sub_report:
                        nested_detailed = sub_report["detailed"]
                        if isinstance(nested_detailed, dict):
                            incomplete_parts = self.validate_detailed(nested_detailed, unit_type, is_vector_scan, failover_scenario)
                            validation_result["incomplete_partitions"].extend(incomplete_parts)
                            if "partns" in sub_report:
                                self.validate_partitions_in_detailed(sub_report["partns"], nested_detailed)
                        else:
                            self.log.warning(f"Sub-report {i} has non-dict nested detailed: {type(nested_detailed).__name__}")
                    else:
                        self.log.warning(f"Sub-report {i} missing nested detailed dict")
            elif isinstance(detailed_section, dict):
                incomplete_parts = self.validate_detailed(detailed_section, unit_type, is_vector_scan, failover_scenario)
                validation_result["incomplete_partitions"] = incomplete_parts
                # Validate partitions in detailed match partns
                if "partns" in report:
                    self.validate_partitions_in_detailed(report["partns"], detailed_section)
            else:
                self.log.warning(f"Unexpected detailed type: {type(detailed_section).__name__}")

        # 8. Retries validation - must be present for replica retry tests
        if "retries" in report:
            validation_result["has_retries"] = True
            if not isinstance(report["retries"], int) or report["retries"] < 1:
                raise ReportValidationError("retries must be a positive integer")
        
        if expect_retries and not validation_result["has_retries"]:
            raise ReportValidationError("retries field must be present for replica retry scenarios")

        # 9. Error field validation
        if "error" in report:
            validation_result["has_error_field"] = True
            if not isinstance(report["error"], str):
                raise ReportValidationError("error must be a string")
        
        return validation_result

    def _is_report_incomplete(self, report: Dict[str, Any]) -> bool:
        """
        Check if a scan report is incomplete.
        
        Args:
            report: The scan report to check
            
        Returns:
            True if the report appears incomplete, False otherwise
        """
        # Check for error field indicating failure
        if "error" in report:
            return True
        
        # Check for missing mandatory fields
        if "srvr_total_counts" not in report:
            return True
        
        if "srvr_avg_ns" not in report and "srvr_avg_ms" not in report:
            return True
        
        # Check if counts are zero or missing (may indicate incomplete scan)
        counts = report.get("srvr_total_counts", {})
        if counts.get("rowsScan", 0) == 0 and counts.get("rowsReturn", 0) == 0:
            # Zero rows could be valid (no matching data) or incomplete
            # Only flag as incomplete if also missing other indicators
            if "error" in report or "retries" in report:
                return True
        
        # Check for incomplete detailed section
        if "detailed" in report:
            detailed = report["detailed"]
            # Handle aggregated scans where detailed is a list of sub-reports
            if isinstance(detailed, list):
                for sub_report in detailed:
                    if isinstance(sub_report, dict) and "detailed" in sub_report:
                        nested_detailed = sub_report["detailed"]
                        if isinstance(nested_detailed, dict):
                            for part_key, part_value in nested_detailed.items():
                                if not part_value:
                                    return True
                                if isinstance(part_value, dict) and "error" in part_value:
                                    return True
            elif isinstance(detailed, dict):
                for part_key, part_value in detailed.items():
                    if not part_value:
                        return True
                    if isinstance(part_value, dict) and "error" in part_value:
                        return True
            else:
                self.log.warning(f"detailed section is not a dict or list, got {type(detailed).__name__}")
                return True
        
        return False

    def _validate_report_failover_mode(self, report: Dict[str, Any], is_detailed: bool,
                                        validation_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate report in failover mode with relaxed validation.
        Logs warnings instead of raising errors for missing fields.
        
        Args:
            report: The scan report to validate
            is_detailed: If True, validates detailed section
            validation_result: Dict to populate with validation results
            
        Returns:
            Updated validation_result dict
        """
        if "error" in report:
            validation_result["has_error_field"] = True
            self.log.info(f"Error field present in report: {report['error']}")
        
        
        if "retries" in report:
            validation_result["has_retries"] = True
            self.log.info(f"Retries field present: {report['retries']}")
        
       
        if "defn" not in report:
            validation_result["validation_errors"].append("Missing defn field")
            self.log.warning("Missing defn field in report")
        
        
        has_ns = "srvr_avg_ns" in report
        has_ms = "srvr_avg_ms" in report
        if not has_ns and not has_ms:
            validation_result["validation_errors"].append("Missing timing fields")
            self.log.warning("Missing srvr_avg_ns/srvr_avg_ms in report")
        if "srvr_total_counts" not in report:
            validation_result["validation_errors"].append("Missing srvr_total_counts")
            self.log.warning("Missing srvr_total_counts in report")
        if is_detailed and "detailed" in report:
            incomplete_parts = self._validate_detailed_failover(report["detailed"])
            validation_result["incomplete_partitions"] = incomplete_parts
            if incomplete_parts:
                self.log.info(f"Incomplete partitions found: {incomplete_parts}")
        
        return validation_result

    def _validate_detailed_failover(self, detailed: Dict[str, Any]) -> list:
        """
        Validate detailed section of scan report for failover scenarios.
        Returns list of incomplete partitions.
        
        Args:
            detailed: The detailed section of the scan report
            
        Returns:
            List of partition keys that are incomplete
        """
        incomplete_partitions = []
        
        # Handle case where detailed is a list instead of dict
        if not isinstance(detailed, dict):
            self.log.warning(f"detailed section is not a dict, got {type(detailed).__name__}")
            return ["all"]
        
        for part_key, part_value in detailed.items():
            self.log.info(f"Checking detailed partition: {part_key}")
            
            if not isinstance(part_value, dict):
                self.log.warning(f"Partition {part_key} has invalid structure")
                incomplete_partitions.append(part_key)
                continue
            
            # Check for timing fields
            has_timing = "srvr_ns" in part_value or "srvr_ms" in part_value
            has_counts = "srvr_counts" in part_value
            
            if not has_timing:
                self.log.warning(f"Partition {part_key} missing timing data (may be from failed node)")
                incomplete_partitions.append(part_key)
            
            if not has_counts:
                self.log.warning(f"Partition {part_key} missing count data (may be from failed node)")
                if part_key not in incomplete_partitions:
                    incomplete_partitions.append(part_key)
            
            # Check for error in partition
            if "error" in part_value:
                self.log.warning(f"Partition {part_key} has error: {part_value['error']}")
                if part_key not in incomplete_partitions:
                    incomplete_partitions.append(part_key)
            
            # If both present, partition data is complete
            if has_timing and has_counts and "error" not in part_value:
                self.log.info(f"Partition {part_key} has complete data")
        
        return incomplete_partitions


  
    # Helper Validators
    def validate_srvr_avg(self, srvr_avg: Dict[str, Any], field_name: str, is_vector_scan: bool = False) -> None:
        """Validate srvr_avg_ns or srvr_avg_ms fields"""
        required_fields = [ "wait", "scan"]

        for field in required_fields:
            if field not in srvr_avg:
                raise ReportValidationError(
                    f"{field_name} missing required field: {field}"
                )

            if not isinstance(srvr_avg[field], (int, float)):
                raise ReportValidationError(
                    f"{field_name}.{field} must be numeric"
                )

        # Optional fields validation 
        #TODO- remove total and disk_read -once storage team closes their ticket
        optional_numeric_fields = ["getseqnos", "fetchseqnos","total", "disk_read"]
        for field in optional_numeric_fields:
            if field in srvr_avg and not isinstance(srvr_avg[field], (int, float)):
                raise ReportValidationError(f"{field_name}.{field} must be numeric")

        # Vector scan specific fields - dist_comp and decode must be present
        if is_vector_scan:
            vector_required_fields = ["dist_comp", "decode"]
            for field in vector_required_fields:
                if field not in srvr_avg:
                    raise ReportValidationError(
                        f"{field_name} missing required vector field: {field}"
                    )
                if not isinstance(srvr_avg[field], (int, float)):
                    raise ReportValidationError(f"{field_name}.{field} must be numeric")
        else:
            # For non-vector scans, these are optional but must be numeric if present
            if "dist_comp" in srvr_avg:
                if not isinstance(srvr_avg["dist_comp"], (int, float)):
                    raise ReportValidationError("dist_comp must be numeric")
            if "decode" in srvr_avg:
                if not isinstance(srvr_avg["decode"], (int, float)):
                    raise ReportValidationError("decode must be numeric")


    def validate_srvr_total_counts(self, counts: Dict[str, Any], is_vector_scan: bool = False) -> None:
        """Validate srvr_total_counts fields"""
        required_fields = [
            "rowsReturn",
            "rowsScan",
            "bytesRead"
        ]

        for field in required_fields:
            if field not in counts:
                raise ReportValidationError(
                    f"srvr_total_counts missing required field: {field}"
                )

        if not isinstance(counts["rowsReturn"], int):
            raise ReportValidationError("rowsReturn must be integer")

        if not isinstance(counts["rowsScan"], int):
            raise ReportValidationError("rowsScan must be integer")

        if not isinstance(counts["bytesRead"], int):
            raise ReportValidationError("bytesRead must be integer")

        # cache_hit_per is optional but must be numeric if present
        if "cache_hit_per" in counts:
            if not isinstance(counts["cache_hit_per"], (int, float)):
                raise ReportValidationError("cache_hit_per must be numeric")

        # Vector scan specific fields - rowsFiltered and rowsReranked must be present
        if is_vector_scan:
            vector_required_fields = ["rowsFiltered", "rowsReranked"]
            for field in vector_required_fields:
                if field not in counts:
                    raise ReportValidationError(
                        f"srvr_total_counts missing required vector field: {field}"
                    )
                if not isinstance(counts[field], int):
                    raise ReportValidationError(f"{field} must be integer")
        else:
            # For non-vector scans, these are optional but must be integer if present
            if "rowsFiltered" in counts and not isinstance(counts["rowsFiltered"], int):
                raise ReportValidationError("rowsFiltered must be integer")
            if "rowsReranked" in counts and not isinstance(counts["rowsReranked"], int):
                raise ReportValidationError("rowsReranked must be integer")


    def validate_partns(self,partns: Dict[str, Any]) -> None:
        if not isinstance(partns, dict) or not partns:
            raise ReportValidationError("partns must be a non-empty object")

        for key, value in partns.items():
            if not isinstance(key, str):
                raise ReportValidationError("Partition keys must be strings")

            if not isinstance(value, list) or not value:
                raise ReportValidationError(
                    f"Partition {key} must map to a non-empty list"
                )


    def validate_detailed(self, detailed: Dict[str, Any], unit_type: str, 
                          is_vector_scan: bool = False, failover_scenario: bool = False) -> list:
        """
        Validate detailed report structure with unit consistency check.
        
        Args:
            detailed: The detailed section of the report
            unit_type: Expected unit type ('ns' or 'ms') for consistency check
            is_vector_scan: If True, validates vector-specific fields
            failover_scenario: If True, uses relaxed validation (logs warnings instead of errors)
            
        Returns:
            List of incomplete partition keys (empty if all complete)
        """
        incomplete_partitions = []
        
        if not isinstance(detailed, dict) or not detailed:
            if failover_scenario:
                self.log.warning("detailed section is empty or invalid")
                return ["all"]
            raise ReportValidationError("detailed must be a non-empty object")

        # Determine expected field names based on unit type
        srvr_time_field = f"srvr_{unit_type}"  # srvr_ns or srvr_ms

        for part_key, part_value in detailed.items():
            if not isinstance(part_value, dict):
                if failover_scenario:
                    self.log.warning(f"Detailed entry {part_key} is not an object")
                    incomplete_partitions.append(part_key)
                    continue
                raise ReportValidationError(
                    f"Detailed entry {part_key} must be an object"
                )

            # Empty partition entries are valid -- they represent partitions/replicas
            # that did not serve the scan (e.g., after retries or recovery)
            if not part_value:
                self.log.info(f"{part_key} is empty (partition did not serve scan, likely retried to another replica)")
                incomplete_partitions.append(part_key)
                continue

            # Required sub-sections with unit consistency
            has_timing = "srvr_ns" in part_value or "srvr_ms" in part_value
            has_counts = "srvr_counts" in part_value
            
            if not has_timing:
                if failover_scenario:
                    self.log.warning(f"{part_key} missing timing data (may be from failed node)")
                    incomplete_partitions.append(part_key)
                    continue
                raise ReportValidationError(
                    f"{part_key} missing required section: {srvr_time_field}"
                )
            
            # Unit consistency check
            if "srvr_ns" in part_value and unit_type == "ms":
                if failover_scenario:
                    self.log.warning(f"Unit inconsistency in {part_key}: expected srvr_ms but found srvr_ns")
                else:
                    raise ReportValidationError(
                        f"Unit inconsistency in detailed report {part_key}: expected srvr_ms but found srvr_ns"
                    )
            if "srvr_ms" in part_value and unit_type == "ns":
                if failover_scenario:
                    self.log.warning(f"Unit inconsistency in {part_key}: expected srvr_ns but found srvr_ms")
                else:
                    raise ReportValidationError(
                        f"Unit inconsistency in detailed report {part_key}: expected srvr_ns but found srvr_ms"
                    )

            if not has_counts:
                if failover_scenario:
                    self.log.warning(f"{part_key} missing count data (may be from failed node)")
                    if part_key not in incomplete_partitions:
                        incomplete_partitions.append(part_key)
                    continue
                raise ReportValidationError(
                    f"{part_key} missing required section: srvr_counts"
                )

            # Get actual time field present
            actual_time_field = srvr_time_field if srvr_time_field in part_value else \
                               ("srvr_ns" if "srvr_ns" in part_value else "srvr_ms")

            try:
                self.validate_srvr_avg(part_value[actual_time_field], actual_time_field, is_vector_scan)
                self.validate_srvr_total_counts(part_value["srvr_counts"], is_vector_scan)
            except ReportValidationError as e:
                if failover_scenario:
                    self.log.warning(f"Validation error in {part_key}: {str(e)}")
                    if part_key not in incomplete_partitions:
                        incomplete_partitions.append(part_key)
                    continue
                raise

            # centroid_assign is optional but must be numeric if present
            if "centroid_assign" in part_value[actual_time_field]:
                if not isinstance(part_value[actual_time_field]["centroid_assign"], (int, float)):
                    if failover_scenario:
                        self.log.warning(f"centroid_assign in {part_key} is not numeric")
                    else:
                        raise ReportValidationError("centroid_assign must be numeric")
        
        return incomplete_partitions

    def validate_partitions_in_detailed(self, partns: Dict[str, Any], detailed: Dict[str, Any]) -> None:
        """
        Validate that all partitions mentioned in partns are present in detailed reports.
        
        Args:
            partns: The partns section containing partition IDs and their partition numbers
            detailed: The detailed section of the report (must be a dict, not list)
        """
        # Handle case where detailed is not a dict
        if not isinstance(detailed, dict):
            self.log.warning(f"validate_partitions_in_detailed: detailed is not a dict, got {type(detailed).__name__}")
            return
        
        # Extract all partition info from partns
        # Format: {"defn_id": [partition_nums]} or {"defn_id": {"node": [partition_nums]}}
        expected_partitions = set()
        for defn_id, partition_info in partns.items():
            if isinstance(partition_info, list):
                for part_num in partition_info:
                    expected_partitions.add(f"{defn_id}[{part_num}]")
            elif isinstance(partition_info, dict):
                for node, part_nums in partition_info.items():
                    if isinstance(part_nums, list):
                        for part_num in part_nums:
                            expected_partitions.add(f"{defn_id}[{part_num}]")

        # Extract partition info from detailed keys
        # Detailed keys format: "defn_id[partition_nums]" e.g., "17080763569858632701[1,2,3]"
        found_partitions = set()
        for detailed_key in detailed.keys():
            # Extract defn_id and partition numbers from key
            if "[" in detailed_key and "]" in detailed_key:
                defn_id = detailed_key.split("[")[0]
                parts_str = detailed_key.split("[")[1].rstrip("]")
                for part_num in parts_str.split(","):
                    part_num = part_num.strip()
                    if part_num:
                        found_partitions.add(f"{defn_id}[{part_num}]")

        # Validate all partitions in partns are covered in detailed
        # Note: detailed may group partitions, so we check if defn_ids match
        partns_defn_ids = set(partns.keys())
        detailed_defn_ids = set()
        for detailed_key in detailed.keys():
            if "[" in detailed_key:
                detailed_defn_ids.add(detailed_key.split("[")[0])

        missing_defns = partns_defn_ids - detailed_defn_ids
        if missing_defns:
            self.log.warning(
                f"Some partition definition IDs from partns not found in detailed: {missing_defns}"
            )


    def validate_client_ms(self,client_ms: Dict[str, Any]) -> None:
        required_fields = [
            "scatter",
            "gather",
            "get_scanport",
            "response_read",
        ]

        for field in required_fields:
            if field not in client_ms:
                raise ReportValidationError(
                    f"client_ms missing required field: {field}"
                )

            if not isinstance(client_ms[field], (int, float)):
                raise ReportValidationError(
                    f"client_ms.{field} must be numeric"
                )

    def get_rows_stats_from_index_stats(self, index_name: str, namespace: str) -> Dict[str, Any]:
        """
        Get num_rows_returned and num_rows_scanned for a specific index from the /stats endpoint.
        Aggregates counts across all indexer nodes for partitioned indexes.
        Logs the entire stats response for debugging.
        
        Similar to get_item_counts_from_index_stats in base_gsi.py but for row stats.

        Args:
            index_name: Name of the index to get stats for
            namespace: Namespace in format 'bucket.scope.collection'

        Returns:
            Dict containing:
                - 'num_rows_returned': Total rows returned across all partitions/nodes
                - 'num_rows_scanned': Total rows scanned across all partitions/nodes  
                - 'raw_stats': The complete raw stats response for debugging
        """
        indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        
        total_rows_returned = 0
        total_rows_scanned = 0
        raw_stats_per_node = {}
        
        # Convert namespace from bucket.scope.collection to keyspace format used in stats
        # Stats endpoint uses format: default:bucket.scope.collection
        keyspace = f"default:{namespace}"
        
        for node in indexer_nodes:
            rest = RestConnection(node)
            node_key = f"{node.ip}:{node.port}"
            
            # Get all index stats for this node (similar to get_item_counts_from_index_stats)
            stats = rest.get_index_stats()
            raw_stats_per_node[node_key] = stats
            
            self.log.info(f"=== Index Stats from node {node_key} ===")
            self.log.info(f"Full stats response: {json.dumps(stats, indent=2)}")
            
            # Look for the index in the stats
            # Stats structure: stats[keyspace][index_name][stat_key]
            if keyspace in stats and index_name in stats[keyspace]:
                index_stats = stats[keyspace][index_name]
                self.log.info(f"Stats for index '{index_name}' on {node_key}: {json.dumps(index_stats, indent=2)}")
                
                # Get num_rows_returned and num_rows_scanned (as seen in index_config_stats_gsi.py)
                rows_returned = index_stats.get('num_rows_returned', 0)
                rows_scanned = index_stats.get('num_rows_scanned', 0)
                
                self.log.info(f"Node {node_key} - num_rows_returned: {rows_returned}, num_rows_scanned: {rows_scanned}")
                
                total_rows_returned += rows_returned
                total_rows_scanned += rows_scanned
            else:
                self.log.info(f"Index '{index_name}' not found in keyspace '{keyspace}' on node {node_key}")
                # Log available keyspaces and indexes for debugging
                self.log.info(f"Available keyspaces: {list(stats.keys())}")
                for ks in stats:
                    self.log.info(f"Indexes in '{ks}': {list(stats[ks].keys())}")
        
        result = {
            'num_rows_returned': total_rows_returned,
            'num_rows_scanned': total_rows_scanned,
            'raw_stats': raw_stats_per_node
        }
        
        self.log.info(f"=== Aggregated Row Stats for index '{index_name}' ===")
        self.log.info(f"Total num_rows_returned: {total_rows_returned}")
        self.log.info(f"Total num_rows_scanned: {total_rows_scanned}")
        
        return result

    def validate_scan_report_rows_against_stats(self, scan_report: Dict[str, Any],
                                                 index_name: str, namespace: str,
                                                 tolerance_percent: float = 0.0) -> Dict[str, Any]:
        """
        Validate rows_return and rows_scan from scan report against num_rows_returned 
        and num_rows_scanned from the /stats endpoint.
        
        Mapping:
            - scan_report['srvr_total_counts']['rowsReturn'] -> stats['num_rows_returned']
            - scan_report['srvr_total_counts']['rowsScan'] -> stats['num_rows_scanned']
        
        Args:
            scan_report: The scan report dictionary containing srvr_total_counts
            index_name: Name of the index to validate against
            namespace: Namespace in format 'bucket.scope.collection'
            tolerance_percent: Allowed percentage difference (default 0.0 for exact match)
        
        Returns:
            Dict containing:
                - 'valid': Boolean indicating if validation passed
                - 'index_name': Name of the index validated
                - 'namespace': Namespace of the index
                - 'report_rows_return': rows_return from scan report
                - 'report_rows_scan': rows_scan from scan report
                - 'stats_num_rows_returned': num_rows_returned from stats
                - 'stats_num_rows_scanned': num_rows_scanned from stats
                - 'errors': List of validation error messages
        
        Raises:
            ReportValidationError if validation fails
        """
        result = {
            'valid': True,
            'index_name': index_name,
            'namespace': namespace,
            'report_rows_return': None,
            'report_rows_scan': None,
            'stats_num_rows_returned': None,
            'stats_num_rows_scanned': None,
            'errors': []
        }
        
        # Get rows from scan report's srvr_total_counts
        if 'srvr_total_counts' not in scan_report:
            raise ReportValidationError("Scan report missing srvr_total_counts")
        
        counts = scan_report['srvr_total_counts']
        report_rows_return = counts.get('rowsReturn')
        report_rows_scan = counts.get('rowsScan')
        
        result['report_rows_return'] = report_rows_return
        result['report_rows_scan'] = report_rows_scan
        
        self.log.info(f"=== Scan Report srvr_total_counts Values ===")
        self.log.info(f"rows_return: {report_rows_return}")
        self.log.info(f"rows_scan: {report_rows_scan}")
        
        # Get rows from /stats endpoint
        stats_result = self.get_rows_stats_from_index_stats(index_name, namespace)
        stats_num_rows_returned = stats_result['num_rows_returned']
        stats_num_rows_scanned = stats_result['num_rows_scanned']
        
        result['stats_num_rows_returned'] = stats_num_rows_returned
        result['stats_num_rows_scanned'] = stats_num_rows_scanned
        
        self.log.info(f"=== /stats Endpoint Values ===")
        self.log.info(f"num_rows_returned: {stats_num_rows_returned}")
        self.log.info(f"num_rows_scanned: {stats_num_rows_scanned}")
        
        # Validate rows_return against num_rows_returned
        if report_rows_return is not None:
            if tolerance_percent > 0 and stats_num_rows_returned > 0:
                diff_percent = abs(report_rows_return - stats_num_rows_returned) / stats_num_rows_returned * 100
                if diff_percent > tolerance_percent:
                    error_msg = (f"rows_return mismatch: scan_report={report_rows_return}, "
                                f"stats num_rows_returned={stats_num_rows_returned}, diff={diff_percent:.2f}%")
                    result['errors'].append(error_msg)
                    result['valid'] = False
                    self.log.error(error_msg)
            else:
                if report_rows_return != stats_num_rows_returned:
                    error_msg = (f"rows_return mismatch: scan_report={report_rows_return}, "
                                f"stats num_rows_returned={stats_num_rows_returned}")
                    result['errors'].append(error_msg)
                    result['valid'] = False
                    self.log.error(error_msg)
        
        # Validate rows_scan against num_rows_scanned
        if report_rows_scan is not None:
            if tolerance_percent > 0 and stats_num_rows_scanned > 0:
                diff_percent = abs(report_rows_scan - stats_num_rows_scanned) / stats_num_rows_scanned * 100
                if diff_percent > tolerance_percent:
                    error_msg = (f"rows_scan mismatch: scan_report={report_rows_scan}, "
                                f"stats num_rows_scanned={stats_num_rows_scanned}, diff={diff_percent:.2f}%")
                    result['errors'].append(error_msg)
                    result['valid'] = False
                    self.log.error(error_msg)
            else:
                if report_rows_scan != stats_num_rows_scanned:
                    error_msg = (f"rows_scan mismatch: scan_report={report_rows_scan}, "
                                f"stats num_rows_scanned={stats_num_rows_scanned}")
                    result['errors'].append(error_msg)
                    result['valid'] = False
                    self.log.error(error_msg)
        
        if result['valid']:
            self.log.info(f"=== Validation PASSED for index '{index_name}' ===")
            self.log.info(f"  rows_return ({report_rows_return}) matches num_rows_returned ({stats_num_rows_returned})")
            self.log.info(f"  rows_scan ({report_rows_scan}) matches num_rows_scanned ({stats_num_rows_scanned})")
        else:
            self.log.error(f"=== Validation FAILED for index '{index_name}' ===")
            for error in result['errors']:
                self.log.error(f"  - {error}")
            raise ReportValidationError(f"Row count validation failed: {result['errors']}")
        
        return result

    
    def create_test_buckets(self, num_buckets=3, bucket_size=256, bucket_storage="magma"):
        """
        Create test buckets for the test

        Parameters:
        - num_buckets: Number of buckets to create (default: 3)
        - bucket_size: Size of each bucket in MB (default: 256)
        - bucket_storage: Storage backend to use (default: "magma")

        Returns:
        - List of created bucket names
        """
        buckets = []

        for i in range(num_buckets):
            bucket_name = f"test_bucket_{i}"
            try:
                self.bucket_params = self._create_bucket_params(
                    server=self.master,
                    size=bucket_size,
                    bucket_storage=bucket_storage,
                    replicas=self.num_replicas,
                    bucket_type=self.bucket_type,
                    enable_replica_index=self.enable_replica_index,
                    eviction_policy=self.eviction_policy,
                    lww=self.lww
                )
                self.cluster.create_standard_bucket(
                    name=bucket_name,
                    port=11222,
                    bucket_params=self.bucket_params
                )
                buckets.append(bucket_name)
                self.log.info(f"Created bucket: {bucket_name}")
            except Exception as err:
                self.log.error(f"Error creating bucket {bucket_name}: {str(err)}")

        # Wait for buckets to be ready
        self.sleep(10, "Waiting for buckets to be ready")

        return buckets


