import logger
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from remote.remote_util import RemoteMachineShellConnection

from pytests.xdcr.xdcrnewbasetests import XDCRNewBaseTest


class TenKCollectionHelper:
    """
    Utility helper class for managing 10K collection scale testing in XDCR.
    Provides functions to create, manage, and verify collections at scale.
    """

    @staticmethod
    def create_10k_collections(server_info, bucket_name, num_scopes=100, collections_per_scope=100,
                                scope_prefix="scope_", collection_prefix="col_",
                                preserve_existing=True):
        """
        Creates 10K collections (100 scopes x 100 collections each) on a bucket.

        Args:
            server_info: Server object with cluster connection details
            bucket_name: Name of the bucket to create collections in
            num_scopes: Number of scopes to create (default 100 for 10K total)
            collections_per_scope: Collections per scope (default 100 for 10K total)
            scope_prefix: Prefix for scope names
            collection_prefix: Prefix for collection names
            preserve_existing: Whether to preserve existing collections on the bucket

        Returns:
            bool: True if creation successful, False otherwise
        """
        log = logger.Logger.get_logger()
        log.info(f"Creating {num_scopes} scopes with {collections_per_scope} collections each "
                 f"(Total: {num_scopes * collections_per_scope} collections)")

        try:
            status = BucketOperationHelper.bulk_create_collection_on_bucket(
                server_info=server_info,
                bucket_name=bucket_name,
                scopes=num_scopes,
                collections_per_scope=collections_per_scope,
                preserve_og_manifest=preserve_existing,
                scope_prefix=scope_prefix,
                collection_prefix=collection_prefix,
                ensure_manifest=True
            )

            total_collections = num_scopes * collections_per_scope
            if status:
                log.info(f"Successfully created {total_collections} collections on bucket '{bucket_name}'")
            else:
                log.error(f"Failed to create {total_collections} collections on bucket '{bucket_name}'")

            return status
        except Exception as e:
            log.error(f"Exception creating 10K collections: {e}")
            return False

    @staticmethod
    def create_missing_collections(source_server, dest_server, bucket_name,
                                   batch_scopes=10):
        """
        Creates missing collections on destination by comparing manifests.
        Applies changes in batches of scopes to avoid REST payload/timeout
        failures at 10K collection scale.

        Args:
            source_server: Source cluster server object
            dest_server: Destination cluster server object
            bucket_name: Name of the bucket
            batch_scopes: Number of scopes per REST batch (default 10)

        Returns:
            tuple: (bool success, int missing_count, int created_count)
        """
        log = logger.Logger.get_logger()
        log.info("Checking for missing collections on destination for bucket '{}'".format(
            bucket_name))

        try:
            src_manifest = BucketOperationHelper.get_api_manifest_json_from_bucket(
                source_server, bucket_name
            )
            dest_manifest = BucketOperationHelper.get_api_manifest_json_from_bucket(
                dest_server, bucket_name
            )

            dest_scope_map = {s["name"]: {c["name"] for c in s.get("collections", [])}
                              for s in dest_manifest.get("scopes", [])}

            missing_by_scope = {}
            missing_count = 0

            for src_scope in src_manifest.get("scopes", []):
                scope_name = src_scope["name"]
                if scope_name.startswith("_"):
                    continue
                src_collections = {c["name"] for c in src_scope.get("collections", [])}
                dest_collections = dest_scope_map.get(scope_name, set())
                missing_in_scope = list(src_collections - dest_collections)
                if missing_in_scope:
                    missing_by_scope[scope_name] = missing_in_scope
                    missing_count += len(missing_in_scope)

            if not missing_by_scope:
                log.info("No missing collections found")
                return True, 0, 0

            log.info("Found {} missing collections across {} scopes".format(
                missing_count, len(missing_by_scope)))

            tasks = []
            for scope_name, col_names in missing_by_scope.items():
                for col_name in col_names:
                    tasks.append((scope_name, col_name))

            max_workers = min(16, max(1, len(tasks)))
            log.info("Creating {} collections with {} workers".format(
                len(tasks), max_workers))

            created_count = 0
            all_success = True
            lock = threading.Lock()

            def _create_one(scope_name, col_name):
                rest = RestConnection(dest_server)
                try:
                    rest.create_collection(bucket_name, scope_name, col_name)
                    return scope_name, col_name, True, None
                except Exception as e:
                    if "already exists" in str(e).lower():
                        return scope_name, col_name, True, None
                    return scope_name, col_name, False, str(e)

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(_create_one, s, c) for s, c in tasks]
                for fut in as_completed(futures):
                    scope_name, col_name, ok, err = fut.result()
                    with lock:
                        if ok:
                            created_count += 1
                        else:
                            all_success = False
                            log.warning("Failed to create {}.{}: {}".format(
                                scope_name, col_name, err))

            log.info("Created {}/{} missing collections (success={})".format(
                created_count, missing_count, all_success))
            return all_success, missing_count, created_count

        except Exception as e:
            log.error("Exception creating missing collections: {}".format(e))
            return False, 0, 0

    @staticmethod
    def get_collection_pairs_on_dest(source_server, dest_server, bucket_name):
        """
        Returns two disjoint lists of (scope, collection) pairs:
        - existing_on_dest: pairs present on BOTH src and dest
        - missing_on_dest: pairs present on src but NOT on dest

        Used by backfill tests to construct explicit load targets.

        Returns:
            tuple: (existing_pairs, missing_pairs)
              each is a list of (scope_name, collection_name)
        """
        log = logger.Logger.get_logger()
        src_manifest = BucketOperationHelper.get_api_manifest_json_from_bucket(
            source_server, bucket_name)
        dest_manifest = BucketOperationHelper.get_api_manifest_json_from_bucket(
            dest_server, bucket_name)

        dest_scope_map = {s["name"]: {c["name"] for c in s.get("collections", [])}
                          for s in dest_manifest.get("scopes", [])}

        existing_pairs = []
        missing_pairs = []

        for src_scope in src_manifest.get("scopes", []):
            scope_name = src_scope["name"]
            if scope_name.startswith("_"):
                continue
            dest_cols = dest_scope_map.get(scope_name, set())
            for col in src_scope.get("collections", []):
                col_name = col["name"]
                if col_name in dest_cols:
                    existing_pairs.append((scope_name, col_name))
                else:
                    missing_pairs.append((scope_name, col_name))

        log.info("Pair split: {} existing on dest, {} missing on dest".format(
            len(existing_pairs), len(missing_pairs)))
        return existing_pairs, missing_pairs

    @staticmethod
    def get_collection_doc_counts_on_node(node, bucket_name):
        """
        Returns per-collection item counts from a single node via cbstats.

        Args:
            node: Server object for the node to query
            bucket_name: Name of the bucket (string) or Bucket object

        Returns:
            dict: {(scope_name, collection_name): item_count}

        Notes:
            Pairs cbstats lines by the <scope_id>:<col_id> prefix rather
            than relying on grep+zip ordering. Tolerates collection names
            containing dots, colons, or any other punctuation.
        """
        log = logger.Logger.get_logger()
        if isinstance(bucket_name, str):
            class _Bucket:
                def __init__(self, name):
                    self.name = name
            bucket_obj = _Bucket(bucket_name)
            bucket_str = bucket_name
        else:
            bucket_obj = bucket_name
            bucket_str = bucket_name.name

        shell = RemoteMachineShellConnection(node)
        try:
            output, _ = shell.execute_cbstats(
                bucket_obj, "collections",
                cbadmin_user="Administrator")
        finally:
            shell.disconnect()

        # cbstats lines: "<scope_id>:<col_id>:<key>: <value>"
        # We only care about lines with key in {name, scope_name, items}.
        # Pair them by the (scope_id, col_id) tuple.
        by_cid = {}  # (scope_id, col_id) -> {"name": str, "scope_name": str, "items": int}
        for raw in (output or []):
            line = raw.strip()
            if not line:
                continue
            parts = line.split(":", 3)
            if len(parts) < 4:
                continue
            scope_id, col_id, key, value = parts[0], parts[1], parts[2], parts[3].strip()
            if key not in ("name", "scope_name", "items"):
                continue
            cid = (scope_id, col_id)
            slot = by_cid.setdefault(cid, {})
            if key == "items":
                try:
                    slot["items"] = int(value)
                except ValueError:
                    log.warning("Could not parse cbstats items value %r for cid %s",
                                value, cid)
            else:
                slot[key] = value

        # If cbstats didn't include scope_name, fall back to the manifest mapping.
        needs_manifest = any("scope_name" not in slot for slot in by_cid.values())
        scope_id_to_name = {}
        if needs_manifest:
            try:
                manifest = BucketOperationHelper.get_api_manifest_json_from_bucket(
                    node, bucket_str)
                for s in manifest.get("scopes", []):
                    scope_id_to_name[s["uid"]] = s["name"]
            except Exception as e:
                log.warning("Manifest fallback for scope-id mapping failed: %s", e)

        counts = {}
        for (scope_id, col_id), slot in by_cid.items():
            col_name = slot.get("name")
            if col_name is None:
                continue
            scope_name = slot.get("scope_name")
            if scope_name is None:
                scope_name = scope_id_to_name.get(scope_id.lstrip("0x") or "0",
                                                  scope_id_to_name.get(scope_id, scope_id))
            counts[(scope_name, col_name)] = slot.get("items", 0)
        return counts

    @staticmethod
    def get_collection_doc_counts(nodes, bucket_name):
        """
        Aggregates per-collection item counts across multiple nodes.

        Args:
            nodes: list of server objects (all nodes in a cluster)
            bucket_name: Name of the bucket

        Returns:
            dict: {(scope_name, collection_name): total_item_count}
        """
        aggregated = {}
        for node in nodes:
            node_counts = TenKCollectionHelper.get_collection_doc_counts_on_node(
                node, bucket_name)
            for col_name, col_items in node_counts.items():
                aggregated[col_name] = aggregated.get(col_name, 0) + col_items
        return aggregated

    @staticmethod
    def get_bucket_item_count(server, bucket_name="default"):
        """
        Returns bucket-level active item count with fallback.

        Args:
            server: Server object (master node)
            bucket_name: Name of the bucket

        Returns:
            int: total active item count
        """
        log = logger.Logger.get_logger()
        rest = RestConnection(server)
        try:
            return int(rest.get_active_key_count(bucket_name))
        except Exception:
            bucket_info = rest.get_bucket_json(bucket_name)
            total = 0
            for node in bucket_info.get("nodes", []):
                stats = node.get("interestingStats") or {}
                if "curr_items" not in stats:
                    log.warning("Node %s missing interestingStats.curr_items; "
                                "treating as 0" % node.get("hostname", "?"))
                    continue
                total += stats["curr_items"]
            return total

    @staticmethod
    def get_manifest_collection_count(server, bucket_name):
        """
        Returns total collection count from the bucket manifest.

        Args:
            server: Server object
            bucket_name: Name of the bucket

        Returns:
            int: total number of collections across all scopes
        """
        manifest = BucketOperationHelper.get_api_manifest_json_from_bucket(
            server, bucket_name)
        return sum(
            len(scope.get("collections", []))
            for scope in manifest.get("scopes", [])
        )

    @staticmethod
    def read_10k_params(input_obj):
        """
        Reads standard 10K collection test params from the test input.
        Returns a dict suitable for passing to other helper methods.

        Args:
            input_obj: TestInput object (_input from the test class)

        Returns:
            dict with keys: num_scopes, collections_per_scope, scope_prefix,
                  collection_prefix, sample_collections, docs_per_collection,
                  doc_load_mode, max_workers, docid_prefix
        """
        return {
            "num_scopes": input_obj.param("num_scopes", 100),
            "collections_per_scope": input_obj.param("collections_per_scope", 100),
            "scope_prefix": input_obj.param("scope_prefix", "s10k_"),
            "collection_prefix": input_obj.param("collection_prefix", "c10k_"),
            "sample_collections": input_obj.param("sample_collections", 200),
            "docs_per_collection": input_obj.param("docs_per_collection", 5),
            "doc_load_mode": input_obj.param("doc_load_mode", "sample"),
            "max_workers": input_obj.param("max_workers", 0),
            "docid_prefix": input_obj.param("docid_prefix", "xdcr10k"),
        }

    @staticmethod
    def select_and_load(server, bucket_name, params, run_id=None, mode=None,
                        sample_size=None, docs_per_collection=None):
        """
        Select collections and load docs via XDCRNewBaseTest.load_collections_with_sdk
        (Java SDK doc-loader jar invoked directly as subprocesses, parallelised by
        a ThreadPoolExecutor — the framework TaskManager is bypassed because it
        runs SDK tasks serially with a 30s ES-indexing sleep per task).

        Routing:
            mode == "all"    -> bulk fast-path: all_collections=True, ONE Java
                                invocation iterates every non-system collection.
                                LoadResult.success_pairs is populated by enumerating
                                the bucket manifest after the load completes.
            mode == "sample" -> XDCRNewBaseTest.select_collections picks `sample_size`
                                random (scope, collection) pairs from the manifest;
                                load_collections_with_sdk fans them out across
                                `parallel_loaders` (default 8) Java subprocesses.

        Args:
            server: Server object to load data on (source cluster master).
            bucket_name: Target bucket name.
            params: dict from read_10k_params() — supplies sample_collections,
                    docs_per_collection, doc_load_mode, docid_prefix.
            run_id: Optional run identifier; threaded into SDKDataLoader.key_prefix
                    so doc keys are unique across reruns and (in sample mode) per pair.
            mode: "sample" or "all"; overrides params["doc_load_mode"] when set.
            sample_size: Override for number of collections to sample (sample mode only).
            docs_per_collection: Override for docs created per collection.

        Returns:
            LoadResult from XDCRNewBaseTest with success_pairs / failed_pairs /
            errors / total_docs_loaded / elapsed_seconds.
        """
        log = logger.Logger.get_logger()
        load_mode = mode if mode else params.get("doc_load_mode", "sample")
        size = sample_size if sample_size else params.get("sample_collections", 200)
        dpc = docs_per_collection if docs_per_collection else params.get("docs_per_collection", 5)

        if load_mode == "all":
            result = XDCRNewBaseTest.load_collections_with_sdk(
                server=server,
                bucket=bucket_name,
                all_collections=True,
                docs_per_collection=dpc,
                docid_prefix=params.get("docid_prefix", "xdcr10k"),
                run_id=run_id,
            )
        else:
            pairs = XDCRNewBaseTest.select_collections(
                server, bucket_name, mode="sample", sample_size=size)
            result = XDCRNewBaseTest.load_collections_with_sdk(
                server=server,
                bucket=bucket_name,
                pairs=pairs,
                docs_per_collection=dpc,
                docid_prefix=params.get("docid_prefix", "xdcr10k"),
                run_id=run_id,
            )
        log.info("Load result: {}/{} succeeded, {} docs in {:.1f}s".format(
            len(result.success_pairs), result.total_attempted,
            result.total_docs_loaded, result.elapsed_seconds))
        return result

    @staticmethod
    def load_into_pairs(server, bucket_name, pairs, docs_per_collection,
                        run_id="load", docid_prefix="xdcr10k", max_workers=0):
        """
        Load docs into an explicit list of (scope, collection) pairs via
        XDCRNewBaseTest.load_collections_with_sdk. One Java subprocess per pair,
        parallelised by a ThreadPoolExecutor (default 8 concurrent loaders).

        Args:
            server: Source server object (cluster master).
            bucket_name: Bucket name.
            pairs: list of (scope_name, collection_name) tuples to load into.
            docs_per_collection: number of docs to create per collection.
            run_id: label threaded into the doc-key prefix for uniqueness across reruns.
            docid_prefix: prefix for document keys (default "xdcr10k").
            max_workers: deprecated; retained for backwards compatibility with callers.
                Ignored — load_collections_with_sdk uses its own `parallel_loaders`
                pool to bound concurrent JVMs.

        Returns:
            LoadResult from XDCRNewBaseTest with success_pairs / failed_pairs /
            errors / total_docs_loaded / elapsed_seconds.
        """
        log = logger.Logger.get_logger()
        log.info("Loading {} docs into {} explicit pairs (run_id={})".format(
            docs_per_collection, len(pairs), run_id))
        result = XDCRNewBaseTest.load_collections_with_sdk(
            server=server,
            bucket=bucket_name,
            pairs=pairs,
            docs_per_collection=docs_per_collection,
            docid_prefix=docid_prefix,
            run_id=run_id,
        )
        log.info("load_into_pairs result: {}/{} succeeded, {} docs in {:.1f}s".format(
            len(result.success_pairs), result.total_attempted,
            result.total_docs_loaded, result.elapsed_seconds))
        return result
