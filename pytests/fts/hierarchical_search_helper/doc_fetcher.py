"""
Document Fetcher for Hierarchical Search Validation

Fetches documents from Couchbase cluster for validation with hs_validator.
Supports batch fetching for memory efficiency with large datasets (700-800k docs).
"""

import logging
from typing import Iterator, Tuple, Dict, Optional, List
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions, QueryOptions
import json


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
        limit: Optional[int] = None
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

        Yields:
            Tuple of (doc_id, doc_content_dict)
        """
        try:
            # Connect to cluster
            auth = PasswordAuthenticator(self.username, self.password)
            cluster = Cluster(self.connection_string, ClusterOptions(auth))

            # Build N1QL query
            keyspace = f"`{bucket}`.`{scope}`.`{collection}`"

            if doc_id_prefix:
                where_clause = f"WHERE META().id LIKE '{doc_id_prefix}%'"
            else:
                where_clause = ""

            limit_clause = f"LIMIT {limit}" if limit else ""

            # Use SELECT with LIMIT and OFFSET for batching
            offset = 0
            docs_fetched = 0

            while True:
                query = f"""
                    SELECT META().id AS doc_id, *
                    FROM {keyspace}
                    {where_clause}
                    ORDER BY META().id
                    LIMIT {batch_size} OFFSET {offset}
                """

                self.log.debug(f"Executing query: {query}")

                result = cluster.query(query)
                batch_count = 0

                for row in result:
                    doc_id = row['doc_id']
                    # Remove meta fields from document content
                    doc_content = {k: v for k, v in row.items() if k != 'doc_id'}

                    yield (doc_id, doc_content)

                    batch_count += 1
                    docs_fetched += 1

                    if limit and docs_fetched >= limit:
                        self.log.info(f"Reached limit of {limit} documents")
                        return

                # If we got fewer docs than batch_size, we've reached the end
                if batch_count < batch_size:
                    self.log.info(f"Finished fetching {docs_fetched} documents")
                    break

                offset += batch_size

                if batch_count % 10000 == 0:
                    self.log.info(f"Fetched {docs_fetched} documents so far...")

        except Exception as e:
            self.log.error(f"Error fetching documents: {e}")
            raise

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
    use_ssl: bool = False
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
        limit=limit
    )