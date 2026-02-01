"""
FTS Query Definitions corresponding to the eventing handler queries
"""
MATCH_QUERY = {
    "query": {
        "match": "location hostel",
        "field": "reviews.content",
        "analyzer": "standard",
        "fuzziness": 2,
        "prefix_length": 4,
        "operator": "and"
    },
    "expected_hits": 40
}

MATCH_PHRASE_QUERY = {
    "query": {
        "match_phrase": "very nice location",
        "field": "reviews.content"
    },
    "expected_hits": 14
}

REGEX_QUERY = {
    "query": {
        "regexp": "inter.+",
        "field": "reviews.content"
    },
    "expected_hits": 401
}

QUERY_STRING_QUERY = {
    "query": {
        "query": "Cleanliness"
    },
    "expected_hits": 61
}

NUMERIC_RANGE_QUERY = {
    "query": {
        "min": 5,
        "inclusive_min": True,
        "field": "reviews.ratings.Cleanliness"
    },
    "expected_hits": 731
}

CONJUNCTS_QUERY = {
    "query": {
        "conjuncts": [
            {
                "match": "location hostel",
                "field": "reviews.content"
            },
            {
                "bool": True,
                "field": "free_breakfast"
            }
        ]
    },
    "expected_hits": 455
}

DISJUNCTS_QUERY = {
    "query": {
        "disjuncts": [
            {
                "match": "very nice location",
                "field": "reviews.content"
            },
            {
                "bool": False,
                "field": "free_breakfast"
            }
        ]
    },
    "expected_hits": 801
}

BOOLEAN_QUERY = {
    "query": {
        "must": {
            "conjuncts": [
                {
                    "match": "location",
                    "field": "reviews.content"
                }
            ]
        },
        "should": {
            "disjuncts": [
                {
                    "bool": True,
                    "field": "free_breakfast"
                }
            ]
        },
        "must_not": {
            "disjuncts": [
                {
                    "bool": False,
                    "field": "free_breakfast"
                }
            ]
        }
    },
    "expected_hits": 453
}

WILDCARD_QUERY = {
    "query": {
        "wildcard": "inter*",
        "field": "reviews.content"
    },
    "expected_hits": 401
}

DOC_IDS_QUERY = {
    "query": {
        "ids": ["airport_8850", "airport_8851"]
    },
    "expected_hits": 2
}

TERM_QUERY = {
    "query": {
        "term": "locate",
        "field": "reviews.content"
    },
    "expected_hits": 12
}

PHRASE_QUERY = {
    "query": {
        "terms": ["nice", "view"],
        "field": "reviews.content"
    },
    "expected_hits": 33
}

PREFIX_QUERY = {
    "query": {
        "prefix": "inter",
        "field": "reviews.content"
    },
    "expected_hits": 401
}

MATCH_ALL_QUERY = {
    "query": {
        "match_all": {}
    },
    "expected_hits": 31591
}

MATCH_NONE_QUERY = {
    "query": {
        "match_none": {}
    },
    "expected_hits": 0
}

# Dictionary mapping query names to their definitions
ALL_QUERIES = {
    "match_query": MATCH_QUERY,
    "match_phrase_query": MATCH_PHRASE_QUERY,
    "regex_query": REGEX_QUERY,
    "query_string_query": QUERY_STRING_QUERY,
    "numeric_range_query": NUMERIC_RANGE_QUERY,
    "conjuncts_query": CONJUNCTS_QUERY,
    "disjuncts_query": DISJUNCTS_QUERY,
    "boolean_query": BOOLEAN_QUERY,
    "wildcard_query": WILDCARD_QUERY,
    "docids_query": DOC_IDS_QUERY,
    "term_query": TERM_QUERY,
    "phrase_query": PHRASE_QUERY,
    "prefix_query": PREFIX_QUERY,
    "match_all_query": MATCH_ALL_QUERY,
    "match_none_query": MATCH_NONE_QUERY
}