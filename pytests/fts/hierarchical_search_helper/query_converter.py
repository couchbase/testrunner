"""
Query Converter for Hierarchical Search

Converts shorthand hierarchical queries from conf files to:
1. FTS nested queries (with full field paths)
2. hs_validator queries (nested dict structure)
"""

import json
from typing import Dict, List, Any, Union


HIERARCHICAL_FIELD_MAPPING = {
    "name": "company.departments.employees.name",
    "role": "company.departments.employees.role",
    "home": "company.departments.employees.home",

    "project_title": "company.departments.projects.title",
    "project_status": "company.departments.projects.status",

    "dept_name": "company.departments.name",
    "budget": "company.departments.budget",

    "city": "company.locations.city",
    "country": "company.locations.country",

    "company_name": "company.name",

    "id": "id"
}


def convert_to_fts_query(shorthand_query: Union[str, List[Dict]],
                         size: int = 10,
                         explain: bool = False,
                         fields: List[str] = None) -> Dict:
    """
    Convert shorthand query to FTS query format (QUERY OBJECT ONLY, not full search request)

    Args:
        shorthand_query: List of conjunct dicts with short field names
                        e.g., [{"name":"Alice","home":"Chennai"},{"city":"Athens"}]
        size: Number of results to return (ignored, kept for compatibility)
        explain: Include explain in query (ignored, kept for compatibility)
        fields: Fields to return (ignored, kept for compatibility)

    Returns:
        FTS query object (just the "query" part, not the full search request)

    Example:
        Input: [{"name":"Alice","home":"Chennai"},{"city":"Athens"}]
        Output: {
            "conjuncts": [
                {
                    "conjuncts": [
                        {"field": "company.departments.employees.name", "match": "Alice"},
                        {"field": "company.departments.employees.home", "match": "Chennai"}
                    ]
                },
                {"field": "company.locations.city", "match": "Athens"}
            ]
        }
    """
    if isinstance(shorthand_query, str):
        shorthand_query = json.loads(shorthand_query)

    # Build conjuncts
    conjuncts = []

    for conjunct_group in shorthand_query:
        if len(conjunct_group) == 1:
            # Single field - add directly
            short_field, value = list(conjunct_group.items())[0]
            full_field = HIERARCHICAL_FIELD_MAPPING.get(short_field, short_field)
            conjuncts.append({
                "field": full_field,
                "match": value
            })
        else:
            # Multiple fields - nested conjunct
            nested_conjuncts = []
            for short_field, value in conjunct_group.items():
                full_field = HIERARCHICAL_FIELD_MAPPING.get(short_field, short_field)
                nested_conjuncts.append({
                    "field": full_field,
                    "match": value
                })

            conjuncts.append({
                "conjuncts": nested_conjuncts
            })

    # Return ONLY the query object (not the full search request)
    # The FTS framework will wrap this with explain, fields, size, etc.
    fts_query = {
        "conjuncts": conjuncts
    }

    return fts_query


def path_to_nested_dict(path: str, value: Any) -> Dict:
    """
    Convert dot-notation path to nested dict

    Example:
        path_to_nested_dict("company.departments.employees.name", "Alice")
        Returns: {"company": {"departments": {"employees": {"name": "Alice"}}}}
    """
    parts = path.split('.')
    result = {}
    current = result

    for part in parts[:-1]:
        current[part] = {}
        current = current[part]

    current[parts[-1]] = value
    return result


def merge_nested_dicts(dict1: Dict, dict2: Dict) -> Dict:
    """
    Deep merge two nested dictionaries

    Example:
        merge_nested_dicts(
            {"a": {"b": 1}},
            {"a": {"c": 2}}
        )
        Returns: {"a": {"b": 1, "c": 2}}
    """
    result = dict1.copy()

    for key, value in dict2.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_nested_dicts(result[key], value)
        else:
            result[key] = value

    return result


def convert_to_validator_query(shorthand_query: Union[str, List[Dict]]) -> Dict:
    """
    Convert shorthand query to hs_validator query format

    Args:
        shorthand_query: List of conjunct dicts with short field names

    Returns:
        Single nested dict query with all fields merged

    Example:
        Input: [{"name":"Alice","home":"Chennai"},{"city":"Athens"}]
        Output: {
            "company": {
                "departments": {
                    "employees": {
                        "name": "Alice",
                        "home": "Chennai"
                    }
                },
                "locations": {
                    "city": "Athens"
                }
            }
        }
    """
    if isinstance(shorthand_query, str):
        shorthand_query = json.loads(shorthand_query)

    # Merge all conjunct groups into a single nested dict
    final_dict = {}

    for conjunct_group in shorthand_query:
        for short_field, value in conjunct_group.items():
            full_field = HIERARCHICAL_FIELD_MAPPING.get(short_field, short_field)
            nested = path_to_nested_dict(full_field, value)
            final_dict = merge_nested_dicts(final_dict, nested)

    return final_dict


# Helper function for test files
def parse_hierarchical_query_param(param_value: str) -> tuple:
    """
    Parse hierarchical query parameter and return both FTS and validator queries

    Args:
        param_value: JSON string from conf file

    Returns:
        (fts_query, validator_query)
    """
    fts_query = convert_to_fts_query(param_value)
    validator_query = convert_to_validator_query(param_value)

    return fts_query, validator_query