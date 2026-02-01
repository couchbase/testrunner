"""
Query Converter for Hierarchical Search

Converts shorthand hierarchical queries from conf files to:
1. FTS nested queries (with full field paths)
2. hs_validator queries (nested dict structure)
"""

import ast
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

def _parse_shorthand(s: str) -> List[Dict[str, str]]:
    """
    Parse shorthand syntax (avoids commas and shell-special chars for conf compatibility).

    Format: key:value+key2:value2__key3:value3
      - `__` (double underscore) separates conjunct groups (AND between groups)
      - `+` separates fields within a conjunct group (AND within group)
      - `key:value` is a field match

    Example:
        "name:Alice+role:Manager__city:Athens"
        -> [{"name": "Alice", "role": "Manager"}, {"city": "Athens"}]
    """
    result = []
    groups = s.split('__')
    for group in groups:
        group = group.strip()
        if not group:
            continue
        fields = group.split('+')
        group_dict = {}
        for field in fields:
            field = field.strip()
            if not field:
                continue
            if ':' not in field:
                raise ValueError(f"Invalid field format '{field}' - expected 'key:value'")
            key, val = field.split(':', 1)
            group_dict[key.strip()] = val.strip()
        if group_dict:
            result.append(group_dict)
    return result


def _is_shorthand(s: str) -> bool:
    """
    Detect if string is shorthand syntax vs JSON/python-literal.

    Shorthand characteristics:
    - Contains `+` or `__` as separators
    - Does NOT start with `[` or `{` (those are JSON/dict)
    - Contains `key:value` patterns
    """
    s = s.strip()
    if not s:
        return False
    # If it starts with [ or {, it's likely JSON or python literal
    if s.startswith('[') or s.startswith('{'):
        return False
    # If it contains + or __ with key:value patterns, it's shorthand
    if ('+' in s or '__' in s) and ':' in s:
        return True
    # Single key:value without separators is also valid shorthand
    if ':' in s and '=' not in s and not s.startswith('"'):
        # Make sure it's not a JSON-like structure
        return True
    return False


def _parse_param_value(value: Any) -> Any:
    """
    Parse parameter value that may arrive as:
    - already-parsed python objects (list/dict)
    - shorthand (no commas, shell-safe): "name:Alice+role:Manager;city:Athens"
    - strict JSON string (double quotes)
    - python-literal string (single quotes), e.g. "[{'name':'Alice'}]"
    """
    if isinstance(value, (list, dict)):
        return value
    if value is None:
        return None
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return s
        # Check for shorthand (comma-free, shell-safe syntax for conf files)
        if _is_shorthand(s):
            return _parse_shorthand(s)
        # Try strict JSON first.
        try:
            return json.loads(s)
        except Exception:
            pass
        # Fall back to python-literal parsing (common in our test param pipeline).
        try:
            return ast.literal_eval(s)
        except Exception:
            pass
        # If all else fails, try shorthand as last resort
        # (handles edge cases where detection didn't trigger)
        if ':' in s:
            try:
                return _parse_shorthand(s)
            except Exception:
                pass
        raise ValueError(
            f"Cannot parse hierarchical_query: {s!r}\n"
            f"Supported formats:\n"
            f"  - Shorthand (recommended for conf): name:Alice+role:Manager__city:Athens\n"
            f"  - JSON (if no commas): [{{\"name\":\"Alice\"}}]"
        )
    return value


def _as_conjunct_groups(shorthand_query: Any) -> List[Dict[str, Any]]:
    """
    Normalize shorthand query to list-of-dicts (conjunct groups).
    Accepts:
    - list[dict]
    - dict (single conjunct group)
    """
    parsed = _parse_param_value(shorthand_query)
    if parsed is None or parsed == "":
        return []
    if isinstance(parsed, list):
        if not all(isinstance(x, dict) for x in parsed):
            raise ValueError(f"Expected list of dicts for hierarchical_query, got list: {parsed!r}")
        return parsed  # type: ignore[return-value]
    if isinstance(parsed, dict):
        return [parsed]
    raise ValueError(f"Unsupported hierarchical_query type: {type(parsed).__name__} ({parsed!r})")


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
    shorthand_groups = _as_conjunct_groups(shorthand_query)
    if not shorthand_groups:
        return {"conjuncts": []}

    # Build conjuncts
    conjuncts = []

    for conjunct_group in shorthand_groups:
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
    parsed = _parse_param_value(shorthand_query)
    # If the param is already a nested validator query (dict-of-dicts), accept it as-is.
    # Heuristic: nested validator queries have dict/list values; shorthand conjunct groups typically don't.
    if isinstance(parsed, dict) and any(isinstance(v, (dict, list)) for v in parsed.values()):
        return parsed

    shorthand_groups = _as_conjunct_groups(parsed)
    if not shorthand_groups:
        return {}

    # Merge all conjunct groups into a single nested dict
    final_dict = {}

    for conjunct_group in shorthand_groups:
        for short_field, value in conjunct_group.items():
            full_field = HIERARCHICAL_FIELD_MAPPING.get(short_field, short_field)
            nested = path_to_nested_dict(full_field, value)
            final_dict = merge_nested_dicts(final_dict, nested)

    return final_dict


# Helper function for test files
def parse_hierarchical_query_param(param_value: Any) -> tuple:
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