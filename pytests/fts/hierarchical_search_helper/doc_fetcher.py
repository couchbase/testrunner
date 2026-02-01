"""
Document Fetcher for Hierarchical Search Validation

Fetches documents from Couchbase cluster for validation with hs_validator.
Supports batch fetching for memory efficiency with large datasets (700-800k docs).
"""

import logging
import time
from datetime import timedelta
from typing import Iterator, Tuple, Dict, Optional, List, Any
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions, QueryOptions
from couchbase.exceptions import CouchbaseException


class HierarchicalDocumentFetcher:
    """
    Fetches documents from Couchbase for hierarchical search validation
    """

    def __init__(self, node_ip: str, username: str = "Administrator",
                 password: str = "password", use_ssl: bool = False):
        """
        Initialize document fetcher

        Args:
            node_ip: Couchbase node IP address
            username: Cluster username
            password: Cluster password
            use_ssl: Whether to use SSL connection
        """
        self.node_ip = node_ip
        self.username = username
        self.password = password
        self.use_ssl = use_ssl
        self.log = logging.getLogger("HierarchicalDocumentFetcher")

        # Connection string
        protocol = "couchbases" if use_ssl else "couchbase"
        self.connection_string = f"{protocol}://{node_ip}"

    def fetch_documents_iterator(
        self,
        bucket: str,
        scope: str = "_default",
        collection: str = "_default",
        doc_id_prefix: Optional[str] = None,
        batch_size: int = 1000,
        limit: Optional[int] = None,
        query_timeout_ms: int = 75_000,
        max_retries: int = 5,
        retry_backoff_s: float = 1.0
    ) -> Iterator[Tuple[str, Dict]]:
        """
        Fetch documents from Couchbase and yield as iterator

        Args:
            bucket: Bucket name
            scope: Scope name (default: _default)
            collection: Collection name (default: _default)
            doc_id_prefix: Filter documents by ID prefix (e.g., "hvec_")
            batch_size: Number of documents to fetch per query
            limit: Maximum number of documents to fetch (None = all)
            query_timeout_ms: Per-page N1QL timeout in milliseconds
            max_retries: Maximum retries per page for transient failures
            retry_backoff_s: Base backoff (seconds) between retries

        Yields:
            Tuple of (doc_id, doc_content_dict)
        """
        def _is_retryable_cb_error(exc: BaseException) -> bool:
            # We specifically see 12008 bulk-get i/o timeouts surfaced as ec=202 (index_failure).
            # Treat these as transient and retry the page.
            msg = str(exc).lower()
            retryable_markers = (
                "12008",
                "i/o timeout",
                "temporary_failure",
                "index_failure",
                "ambiguous_timeout",
                "unambiguous_timeout",
                "\"retry\":true",
            )
            return isinstance(exc, CouchbaseException) and any(m in msg for m in retryable_markers)

        try:
            # Connect to cluster
            auth = PasswordAuthenticator(self.username, self.password)
            cluster = Cluster(self.connection_string, ClusterOptions(auth))

            # Build N1QL query
            keyspace = f"`{bucket}`.`{scope}`.`{collection}`"

            docs_fetched = 0
            last_doc_id: Optional[str] = None

            while True:
                # Prefer keyset pagination over OFFSET for stability/performance at high counts.
                where_parts: List[str] = []
                named_parameters: Dict[str, Any] = {}

                if doc_id_prefix:
                    where_parts.append("META().id LIKE $prefix")
                    named_parameters["prefix"] = f"{doc_id_prefix}%"

                if last_doc_id is not None:
                    where_parts.append("META().id > $last_doc_id")
                    named_parameters["last_doc_id"] = last_doc_id

                where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

                query = f"""
                    SELECT META().id AS doc_id, *
                    FROM {keyspace}
                    {where_clause}
                    ORDER BY META().id
                    LIMIT {batch_size}
                """

                self.log.debug(
                    f"Executing query (last_doc_id={last_doc_id}, batch_size={batch_size}, docs_fetched={docs_fetched})"
                )

                attempt = 0
                rows: List[Dict[str, Any]] = []
                while True:
                    try:
                        result = cluster.query(
                            query,
                            QueryOptions(
                                timeout=timedelta(milliseconds=query_timeout_ms),
                                named_parameters=named_parameters or None,
                            ),
                        )
                        # Materialize the page so failures don't occur mid-yield.
                        rows = list(result)
                        break
                    except Exception as e:
                        if not _is_retryable_cb_error(e) or attempt >= max_retries:
                            raise
                        sleep_s = retry_backoff_s * (2 ** attempt)
                        self.log.warning(
                            f"Transient error fetching page (attempt {attempt + 1}/{max_retries}); "
                            f"sleeping {sleep_s:.1f}s then retrying. Error: {e}"
                        )
                        time.sleep(sleep_s)
                        attempt += 1

                if not rows:
                    self.log.info(f"Finished fetching {docs_fetched} documents")
                    break

                # Track pagination cursor from the last row.
                last_doc_id = rows[-1].get("doc_id")

                for row in rows:
                    doc_id = row["doc_id"]
                    # Remove meta fields from document content
                    doc_content = {k: v for k, v in row.items() if k != "doc_id"}
                    yield (doc_id, doc_content)

                    docs_fetched += 1
                    if limit and docs_fetched >= limit:
                        self.log.info(f"Reached limit of {limit} documents")
                        return

                    if docs_fetched % 10_000 == 0:
                        self.log.info(f"Fetched {docs_fetched} documents so far...")

        except Exception as e:
            self.log.error(f"Error fetching documents: {e}")
            raise
        finally:
            try:
                cluster.close()  # type: ignore[attr-defined]
            except Exception:
                pass

    def fetch_specific_docs(
        self,
        bucket: str,
        doc_ids: List[str],
        scope: str = "_default",
        collection: str = "_default"
    ) -> Iterator[Tuple[str, Dict]]:
        """
        Fetch specific documents by ID (useful for validating FTS results)

        Args:
            bucket: Bucket name
            doc_ids: List of document IDs to fetch
            scope: Scope name
            collection: Collection name

        Yields:
            Tuple of (doc_id, doc_content_dict)
        """
        try:
            auth = PasswordAuthenticator(self.username, self.password)
            cluster = Cluster(self.connection_string, ClusterOptions(auth))
            cb_bucket = cluster.bucket(bucket)
            cb_collection = cb_bucket.scope(scope).collection(collection)

            for doc_id in doc_ids:
                try:
                    result = cb_collection.get(doc_id)
                    doc_content = result.content_as[dict]
                    yield (doc_id, doc_content)
                except Exception as e:
                    self.log.warning(f"Could not fetch doc {doc_id}: {e}")
                    continue

        except Exception as e:
            self.log.error(f"Error fetching specific documents: {e}")
            raise


def fetch_documents_for_validation(
    node_ip: str,
    bucket: str,
    scope: str = "_default",
    collection: str = "_default",
    username: str = "Administrator",
    password: str = "password",
    doc_id_prefix: Optional[str] = None,
    batch_size: int = 1000,
    limit: Optional[int] = None,
    use_ssl: bool = False,
    query_timeout_ms: int = 75_000,
    max_retries: int = 5,
    retry_backoff_s: float = 1.0
) -> Iterator[Tuple[str, Dict]]:
    """
    Convenience function to fetch documents for validation

    Args:
        node_ip: Couchbase node IP
        bucket: Bucket name
        scope: Scope name
        collection: Collection name
        username: Cluster username
        password: Cluster password
        doc_id_prefix: Filter by document ID prefix
        batch_size: Batch size for fetching
        limit: Max documents to fetch
        use_ssl: Use SSL connection
        query_timeout_ms: Per-page N1QL timeout in milliseconds
        max_retries: Maximum retries per page for transient failures
        retry_backoff_s: Base backoff (seconds) between retries

    Returns:
        Iterator of (doc_id, doc_content) tuples
    """
    fetcher = HierarchicalDocumentFetcher(
        node_ip=node_ip,
        username=username,
        password=password,
        use_ssl=use_ssl
    )

    return fetcher.fetch_documents_iterator(
        bucket=bucket,
        scope=scope,
        collection=collection,
        doc_id_prefix=doc_id_prefix,
        batch_size=batch_size,
        limit=limit,
        query_timeout_ms=query_timeout_ms,
        max_retries=max_retries,
        retry_backoff_s=retry_backoff_s,
    )