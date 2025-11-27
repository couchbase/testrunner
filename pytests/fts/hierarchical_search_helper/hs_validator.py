"""
Hierarchical Search Validator

Validates that documents match hierarchical/nested query conditions.
Designed to work with batched document processing for memory efficiency.
"""

import json
from typing import Any, List, Dict, Iterator, Set, Tuple, Optional
import logging


class Node:
    def __init__(self, key: str, value: Any):
        self.key = key
        self.value = value
        self.children: List['Node'] = []
        self._build_children()

    def _build_children(self):
        if isinstance(self.value, dict):
            for k, v in self.value.items():
                self.children.append(Node(k, v))
        elif isinstance(self.value, list):
            for i, v in enumerate(self.value):
                self.children.append(Node(f"{self.key}[{i}]", v))

    def __repr__(self):
        return f"Node({self.key}={type(self.value).__name__})"


class QueryNode:
    def __init__(self, key: str, condition: Any):
        self.key = key
        self.condition = condition
        self.children: List['QueryNode'] = []
        if isinstance(condition, dict):
            for k, v in condition.items():
                self.children.append(QueryNode(k, v))

    def __repr__(self):
        return f"QueryNode({self.key})"


def match_tree(data_node: Node, query_node: QueryNode) -> bool:
    if data_node.key.split("[")[0] != query_node.key:
        return False

    if not query_node.children:
        return data_node.value == query_node.condition

    if isinstance(data_node.value, list):
        return any(match_subtree(child, query_node) for child in data_node.children)

    for qchild in query_node.children:
        matching_children = [
            dchild for dchild in data_node.children
            if match_tree(dchild, qchild)
        ]
        if not matching_children:
            return False
    return True

def match_subtree(data_node: Node, query_node: QueryNode) -> bool:
    if match_tree(data_node, query_node):
        return True
    return any(match_subtree(child, query_node) for child in data_node.children)


class HierarchicalSearchValidator:
    def __init__(self, batch_size: int = 100_000):
        self.batch_size = batch_size
        self.log = logging.getLogger("HierarchicalSearchValidator")

    def _create_document_tree(self, doc: Dict, root_key: str = "root") -> Node:
        return Node(root_key, doc)

    def _process_batch(
        self,
        batch: List[Tuple[str, Dict]],
        query_trees: List[QueryNode]
    ) -> List[Set[str]]:
        batch_results = [set() for _ in query_trees]

        for doc_id, doc_content in batch:
            data_tree = self._create_document_tree(doc_content)

            for twig_idx, query_tree in enumerate(query_trees):
                if match_subtree(data_tree, query_tree):
                    batch_results[twig_idx].add(doc_id)

        return batch_results

    def _batch_iterator(
        self,
        doc_source: Iterator[Tuple[str, Dict]]
    ) -> Iterator[List[Tuple[str, Dict]]]:
        batch = []
        for doc in doc_source:
            batch.append(doc)
            if len(batch) >= self.batch_size:
                yield batch
                batch = []

        if batch:
            yield batch

    def validate(
        self,
        doc_source: Iterator[Tuple[str, Dict]],
        query: Dict,
        verbose: bool = False
    ) -> Set[str]:
        if not query:
            return set()

        query_trees = []
        twig_keys = []
        for twig_key, twig_condition in query.items():
            query_trees.append(QueryNode(twig_key, twig_condition))
            twig_keys.append(twig_key)

        if verbose:
            self.log.info(f"Processing query with {len(query_trees)} twig(s): {twig_keys}")

        twig_results: List[Set[str]] = [set() for _ in query_trees]

        batch_count = 0
        total_docs = 0

        for batch in self._batch_iterator(doc_source):
            batch_count += 1
            total_docs += len(batch)

            if verbose:
                self.log.info(f"  Processing batch {batch_count} ({len(batch)} docs, {total_docs} total)...")

            batch_results = self._process_batch(batch, query_trees)

            for twig_idx, batch_set in enumerate(batch_results):
                twig_results[twig_idx].update(batch_set)

        if verbose:
            self.log.info(f"Completed processing {total_docs} documents in {batch_count} batch(es)")
            for idx, twig_key in enumerate(twig_keys):
                self.log.info(f"  Twig '{twig_key}': {len(twig_results[idx])} matches")

        # Intersect all twig results
        final_result = self._intersect_results(twig_results)

        if verbose:
            self.log.info(f"Final result (intersection): {len(final_result)} documents")

        return final_result

    def _intersect_results(self, twig_results: List[Set[str]]) -> Set[str]:
        if not twig_results:
            return set()

        result = twig_results[0].copy()
        for twig_set in twig_results[1:]:
            result &= twig_set

        return result


def validate_documents(
    doc_source: Iterator[Tuple[str, Dict]],
    query: Dict,
    batch_size: int = 100_000,
    verbose: bool = False
) -> Set[str]:
    validator = HierarchicalSearchValidator(batch_size=batch_size)
    return validator.validate(doc_source, query, verbose=verbose)


# This is an example of how to use the class. Temporary for now, will be changed as the other stuff is integrated into the codebase.
if __name__ == "__main__":
    # These docs are just a placeholder for now. Actual docs will be fed to the program from the cluster where the docs will be loaded onto.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    log = logging.getLogger("main")
    example_docs = {
        "doc1": {
            "company": {
                "id": "c1",
                "name": "TechCorp",
                "departments": [
                    {
                        "name": "Marketing",
                        "budget": 2000000,
                        "employees": [[[
                            {"name": "Alice", "role": "Manager", "home": "Chennai"},
                            {"name": "Alice", "role": "Salesperson"},
                            {"name": "Bob", "role": "Manager"}
                        ]]]
                    },
                    {
                        "name": "Sales",
                        "budget": 300000,
                        "employees": [
                            {"name": "Charlie", "role": "Salesperson"}
                        ]
                    }
                ],
                "locations": [
                    {"city": "Chennai", "country": "India"},
                    {"city": "London", "country": "UK"}
                ]
            }
        },
        "doc2": {
            "company": {
                "id": "c2",
                "name": "BizInc",
                "departments": [
                    {
                        "name": "Marketing",
                        "budget": 800000,
                        "employees": [
                            {"name": "Eve", "role": "Salesperson"},
                            {"name": "Bob", "role": "Manager"}
                        ]
                    },
                    {
                        "name": "Engineering",
                        "budget": 1500000,
                        "employees": [
                            {"name": "Alice", "role": "Engineer"}
                        ]
                    }
                ],
                "locations": [
                    {"city": "Athens", "country": "USA"},
                    {"city": "London", "country": "India"}
                ]
            },
        },
        "doc3": {
            "company": {
                "id": "c3",
                "name": "StartupXYZ",
                "departments": [
                    {
                        "name": "Marketing",
                        "budget": 100000,
                        "employees": [
                            {"name": "Alice", "role": "Manager"}
                        ]
                    }
                ],
                "locations": [
                    {"city": "Athens", "country": "USA"},
                    {"city": "London", "country": "UK"}
                ]
            }
        }
    }

    def doc_source_from_dict(docs: Dict[str, Dict]) -> Iterator[Tuple[str, Dict]]:
        for doc_id, doc_content in docs.items():
            yield (doc_id, doc_content)

    query = {
        "departments": {
            "employees": {
                "name": "Alice"
            }
        },
        "locations": {
            "country": "India"
        }
    }

    log.info("=" * 70)
    log.info("Hierarchical Search Validator - Example")
    log.info("=" * 70)
    log.info(f"\nQuery: {json.dumps(query, indent=2)}\n")

    results = validate_documents(
        doc_source=doc_source_from_dict(example_docs),
        query=query,
        batch_size=2,
        verbose=True
    )

    if results:
        log.info(f"\n✓ {len(results)} document(s) match the query:")
        for doc_id in sorted(results):
            doc = example_docs[doc_id]
            log.info(f"  - {doc_id}: {doc['company']['name']}")
    else:
        log.info("\n✗ No documents match the query.")

    non_matching = set(example_docs.keys()) - results
    if non_matching:
        log.info(f"\n✗ {len(non_matching)} document(s) do NOT match:")
        for doc_id in sorted(non_matching):
            doc = example_docs[doc_id]
            log.info(f"  - {doc_id}: {doc['company']['name']}")
