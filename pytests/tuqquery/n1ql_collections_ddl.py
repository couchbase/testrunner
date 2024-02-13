from collection.collections_cli_client import CollectionsCLI
from collection.collections_n1ql_client import CollectionsN1QL
from collection.collections_rest_client import CollectionsRest
from membase.api.exception import CBQError
from membase.helper.bucket_helper import BucketOperationHelper

from .tuq import QueryTests


class QueryCollectionsDDLTests(QueryTests):
    """
    Tests descriptors.
    Format:
        "test_name":{
            objects hierarchy to be created
            "buckets": [
                {collections_with_same_names_same_scope
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1", "collections":[]}
                    ]
                }
            ],
            objects to be tested for successful creation
            "tests": [
                {
                    "expected_result": "positive",
                    "object_type": "scope",
                    "object_name": "scope1",
                    "object_container": "bucket1"
                }
            ]
        },
    """
    tests_objects = {
        "massive_test": {
            "buckets": [{"name": "bucket1",
                         "scopes": [{"name": "scope1",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope2",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope3",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope4",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope5",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope6",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope7",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope8",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope9",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope10",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    ]
                         },
                        {"name": "bucket2",
                         "scopes": [{"name": "scope1",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope2",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope3",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope4",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope5",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope6",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope7",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope8",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope9",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope10",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    ]
                         },
                        {"name": "bucket3",
                         "scopes": [{"name": "scope1",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope2",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope3",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope4",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope5",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope6",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope7",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope8",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope9",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope10",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    ]
                         },
                        {"name": "bucket4",
                         "scopes": [{"name": "scope1",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope2",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope3",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope4",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope5",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope6",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope7",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope8",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope9",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope10",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    ]
                         },
                        {"name": "bucket5",
                         "scopes": [{"name": "scope1",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope2",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope3",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope4",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope5",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope6",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope7",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope8",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope9",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope10",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    ]
                         },
                        {"name": "bucket6",
                         "scopes": [{"name": "scope1",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope2",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope3",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope4",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope5",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope6",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope7",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope8",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope9",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope10",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    ]
                         },
                        {"name": "bucket7",
                         "scopes": [{"name": "scope1",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope2",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope3",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope4",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope5",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope6",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope7",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope8",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope9",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    {"name": "scope10",
                                     "collections": [{"name": "collection1"}, {"name": "collection2"},
                                                     {"name": "collection3"}, {"name": "collection4"},
                                                     {"name": "collection5"}, {"name": "collection6"},
                                                     {"name": "collection7"}, {"name": "collection8"},
                                                     {"name": "collection9"}, {"name": "collection10"}]
                                     },
                                    ]
                         }
                        ]
        },
        "single_scope": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1", "collections": []}
                    ]
                }
            ],
            "tests": [
                {
                    "expected_result": "positive",
                    "object_type": "scope",
                    "object_name": "scope1",
                    "object_container": "bucket1",
                    "object_scope": "scope1"
                }
            ]
        },
        "multiple_scopes_different_names_same_bucket": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1", "collections": []},
                        {"name": "scope2", "collections": []}
                    ]
                }
            ],
            "tests": [
                {
                    "expected_result": "positive",
                    "object_type": "scope",
                    "object_name": "scope1",
                    "object_container": "bucket1",
                    "object_scope": "scope1",
                },
                {
                    "expected_result": "positive",
                    "object_type": "scope",
                    "object_name": "scope2",
                    "object_container": "bucket1",
                    "object_scope": "scope2",
                }
            ]
        },
        "multiple_scopes_same_name_different_buckets": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1", "collections": []}
                    ]
                },
                {
                    "name": "bucket2",
                    "scopes": [
                        {"name": "scope1", "collections": []}
                    ]
                }
            ],
            "tests": [
                {
                    "expected_result": "positive",
                    "object_type": "scope",
                    "object_name": "scope1",
                    "object_container": "bucket1",
                    "object_scope": "scope1",
                },
                {
                    "expected_result": "positive",
                    "object_type": "scope",
                    "object_name": "scope1",
                    "object_container": "bucket2",
                    "object_scope": "scope1",
                }
            ]
        },
        "single_collection_not_default_scope": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1",
                         "collections": [
                             {"name": "collection1"}
                         ]
                         }
                    ]
                }
            ],
            "tests": [
                {
                    "expected_result": "positive",
                    "object_type": "collection",
                    "object_name": "collection1",
                    "object_container": "bucket1",
                    "object_scope": "scope1"
                }
            ]
        },
        "two_collections_not_default_scope": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1",
                         "collections": [
                             {"name": "collection1"},
                             {"name": "collection2"}
                         ]
                         }
                    ]
                }
            ],
            "tests": [
                {
                    "expected_result": "positive",
                    "object_type": "collection",
                    "object_name": "collection1",
                    "object_container": "bucket1",
                    "object_scope": "scope1"
                },
                {
                    "expected_result": "positive",
                    "object_type": "collection",
                    "object_name": "collection2",
                    "object_container": "bucket1",
                    "object_scope": "scope1"
                }
            ]
        },
        "two_collections_same_name_different_scopes": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1",
                         "collections": [
                             {"name": "collection1"}
                         ]
                         },
                        {"name": "scope2",
                         "collections": [
                             {"name": "collection1"}
                         ]
                         }
                    ]
                }
            ],
            "tests": [
                {
                    "expected_result": "positive",
                    "object_type": "collection",
                    "object_name": "collection1",
                    "object_container": "bucket1",
                    "object_scope": "scope1"
                },
                {
                    "expected_result": "positive",
                    "object_type": "collection",
                    "object_name": "collection1",
                    "object_container": "bucket1",
                    "object_scope": "scope2"
                }
            ]
        },
        "single_collection_default_scope": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "_default",
                         "collections": [
                             {"name": "collection1"}
                         ]
                         }
                    ]
                }
            ],
            "tests": [
                {
                    "expected_result": "positive",
                    "object_type": "collection",
                    "object_name": "collection1",
                    "object_container": "bucket1",
                    "object_scope": "_default"
                }
            ]
        },
        "multiple_collections_default_scope": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "_default",
                         "collections": [
                             {"name": "collection1"},
                             {"name": "collection2"}
                         ]
                         }
                    ]
                }
            ],
            "tests": [
                {
                    "expected_result": "positive",
                    "object_type": "collection",
                    "object_name": "collection1",
                    "object_container": "bucket1",
                    "object_scope": "_default"
                },
                {
                    "expected_result": "positive",
                    "object_type": "collection",
                    "object_name": "collection2",
                    "object_container": "bucket1",
                    "object_scope": "_default"
                },
            ]
        },
        "multiple_collections_same_name_default_scope_different_buckets": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "_default",
                         "collections": [
                             {"name": "collection1"}
                         ]
                         }
                    ]
                },
                {
                    "name": "bucket2",
                    "scopes": [
                        {"name": "_default",
                         "collections": [
                             {"name": "collection1"}
                         ]
                         }
                    ]
                },
            ],
            "tests": [
                {
                    "expected_result": "positive",
                    "object_type": "collection",
                    "object_name": "collection1",
                    "object_container": "bucket1",
                    "object_scope": "_default"
                },
                {
                    "expected_result": "positive",
                    "object_type": "collection",
                    "object_name": "collection1",
                    "object_container": "bucket2",
                    "object_scope": "_default"
                },
            ]
        },
    }

    negative_tests_objects = {
        "same_name_scopes_same_bucket": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1", "collections": []}
                    ]
                }
            ],
            "test_queries": [
                {
                    "text": "create scope bucket1.scope1",
                    "expected_error": "Scope with name \"scope1\" already exists"
                }
            ]

        },
        "scope_in_scope": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1", "collections": []}
                    ]
                }
            ],
            "test_queries": [
                {
                    "text": "create scope bucket1.scope1.scope2",
                    "expected_error": "syntax error"
                }
            ]
        },
        "scope_in_collection": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1",
                         "collections": [{"name": "collection1"}]
                         }
                    ]
                }
            ],
            "test_queries": [
                {
                    "text": "create scope bucket1.scope1.collection1.scope2",
                    "expected_error": "syntax error"
                }
            ]
        },
        "scope_in_missed_keyspace": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": []
                }
            ],
            "test_queries": [
                {
                    "text": "create scope mykeyspace:bucket1.scope1",
                    "expected_error": "3 path parts are expected"
                }
            ]
        },
        "collection_in_missed_scope": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": []
                }
            ],
            "test_queries": [
                {
                    "text": "create collection bucket1.scope1.collection1",
                    "expected_error": "Scope not found in CB datastore"
                }
            ]
        },
        "collections_with_same_names_same_scope": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1",
                         "collections": [{"name": "collection1"}]
                         }
                    ]
                }
            ],
            "test_queries": [
                {
                    "text": "create collection bucket1.scope1.collection1",
                    "expected_error": "Collection with name \"collection1\" in scope \"scope1\" already exists"
                }
            ]
        },
        "collection_missed_bucket_default_scope": {
            "buckets": [
            ],
            "test_queries": [
                {
                    "text": "create collection bucket1.collection1",
                    "expected_error": "only 2 or 4 parts are valid"
                }
            ]
        },
        "collections_with_same_names_same_default_scope": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "_default",
                         "collections": [{"name": "collection1"}]
                         }
                    ]
                }
            ],
            "test_queries": [
                {
                    "text": "create collection bucket1.collection1",
                    "expected_error": "only 2 or 4 parts are valid"
                }
            ]
        },
        "collection_in_collection": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1",
                         "collections": [{"name": "collection1"}]
                         }
                    ]
                }
            ],
            "test_queries": [
                {
                    "text": "create collection bucket1.scope1.collection1.collection2",
                    "expected_error": "syntax error"
                }
            ]
        },
        "collection_in_missed_keyspace": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1",
                         "collections": [{"name": "collection1"}]
                         }
                    ]
                }
            ],
            "test_queries": [
                {
                    "text": "create collection mykeyspace.bucket1.scope1.collection2",
                    "expected_error": "syntax error"
                }
            ]
        },
        "incorrect_collection_drop": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1",
                         "collections": [{"name": "collection1"}]
                         }
                    ]
                }
            ],
            "test_queries": [
                {
                    "text": "drop collection collection1",
                    "expected_error": "4 path parts are expected"
                }
            ]
        },
        "incorrect_collection_drop_ver2": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1",
                         "collections": [{"name": "collection1"}]
                         }
                    ]
                }
            ],
            "test_queries": [
                {
                    "text": "drop collection scope1.collection1",
                    "expected_error": "only 2 or 4 parts are valid"
                }
            ]
        },
        "incorrect_scope_drop": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1",
                         "collections": []
                         }
                    ]
                }
            ],
            "test_queries": [
                {
                    "text": "drop scope scope1",
                    "expected_error": "3 path parts are expected"
                }
            ]
        },
    }

    bucket_params = {}

    def setUp(self):
        super(QueryCollectionsDDLTests, self).setUp()
        self.log.info("==============  QueryCollectionsDDLTests setup has started ==============")
        self.log_config_info()
        bucket_type = self.input.param("bucket_type", self.bucket_type)

        self.collections_helper = CollectionsN1QL(self.master)
        self.cli_client = CollectionsCLI(self.master)
        self.rest_client = CollectionsRest(self.master)

        eviction_policy = "noEviction" if bucket_type == "ephemeral" else self.eviction_policy
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=eviction_policy, lww=self.lww)
        self.log.info("==============  QueryCollectionsDDLTests setup has completed ==============")

    def suite_setUp(self):
        self.log.info("==============  QueryCollectionsDDLTests suite_setup has started ==============")
        super(QueryCollectionsDDLTests, self).suite_setUp()
        self.log.info("==============  QueryCollectionsDDLTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueryCollectionsDDLTests tearDown has started ==============")
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        super(QueryCollectionsDDLTests, self).tearDown()
        self.log.info("==============  QueryCollectionsDDLTests tearDown has completed ==============")

    def suite_tearDown(self):
        self.log.info("==============  QueryCollectionsDDLTests suite_tearDown has started ==============")
        super(QueryCollectionsDDLTests, self).suite_tearDown()
        self.log.info("==============  QueryCollectionsDDLTests suite_tearDown has completed ==============")

    def test_create(self):
        test_name = self.input.param("test_name", '')
        if test_name == '':
            self.fail("Test name cannot be empty, please fix .conf file")

        test_data = self.tests_objects[test_name]

        test_objects_created, error = \
            self.collections_helper.create_bucket_scope_collection_multi_structure(cluster=self.cluster,
                                                                                   existing_buckets=self.buckets,
                                                                                   bucket_params=self.bucket_params,
                                                                                   data_structure=test_data)
        if not test_objects_created:
            self.assertEquals(True, False, f"Test objects load is failed: {error}")
        result, message = self._perform_test(test_data)
        self.assertEquals(result, True, message)

    def test_scope_name_special_chars(self):
        special_chars = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '_', '-', '%']
        tick_chars = ['-', '%']
        bucket_name = 'bucket1'
        errors = []

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)
        for special_char in special_chars:
            tick_char = '`'
            scope_name = f"scope{special_char}"
            query = f"create scope {bucket_name}.{tick_char}{scope_name}{tick_char}"
            try:
                self.run_cbq_query(query)
            except CBQError as err:
                errors.append(f"Cannot run query: {query} \nError is:{str(err)}")
                continue
            bucket_scopes = self.rest.get_bucket_scopes(bucket_name)
            if scope_name not in bucket_scopes:
                errors.append(f"Cannot create scope {scope_name} in bucket {bucket_name}")

        self.cluster.bucket_delete(self.master, bucket_name)
        self.wait_for_bucket_delete(bucket_name, 3, 100)
        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)

        for special_char in special_chars:
            if special_char in tick_chars:
                continue
            scope_name = f"scope{special_char}"
            query = f"create scope {bucket_name}.{scope_name}"
            try:
                self.run_cbq_query(query)
            except CBQError as err:
                errors.append(f"Cannot run query: {query} \nError is:{str(err)}")
                continue
            bucket_scopes = self.rest.get_bucket_scopes(bucket_name)
            if scope_name not in bucket_scopes:
                errors.append(f"Cannot create scope {scope_name} in bucket {bucket_name}")

        self.cluster.bucket_delete(self.master, bucket_name)
        self.wait_for_bucket_delete(bucket_name, 3, 100)
        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)

        for special_char in special_chars:
            if special_char in tick_chars:
                continue
            scope_name = f"scope{special_char}test"
            query = f"create scope {bucket_name}.{scope_name}"
            try:
                self.run_cbq_query(query)
            except CBQError as err:
                errors.append(f"Cannot run query: {query} \nError is:{str(err)}")
                continue
            bucket_scopes = self.rest.get_bucket_scopes(bucket_name)
            if scope_name not in bucket_scopes:
                errors.append(f"Cannot create scope {scope_name} in bucket {bucket_name}")

        self.cluster.bucket_delete(self.master, bucket_name)
        self.wait_for_bucket_delete(bucket_name, 3, 100)
        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)

        for special_char in special_chars:
            if special_char not in tick_chars:
                continue
            tick_char = '`'
            scope_name = f"scope{special_char}test"
            query = f"create scope {bucket_name}.{tick_char}{scope_name}{tick_char}"
            try:
                self.run_cbq_query(query)
            except CBQError as err:
                errors.append(f"Cannot run query: {query} \nError is:{str(err)}")
                continue
            bucket_scopes = self.rest.get_bucket_scopes(bucket_name)
            if scope_name not in bucket_scopes:
                errors.append(f"Cannot create scope {scope_name} in bucket {bucket_name}")

        for error in errors:
            self.log.info(error)
        self.assertEquals(len(errors), 0, "Creation of scope with special chars is failed.")

    def test_collection_name_special_chars(self):
        special_chars = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '_', '-', '%']
        tick_chars = ['-', '%']
        bucket_name = 'bucket1'
        scope_name = "_default"
        errors = []

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)
        for special_char in special_chars:
            tick_char = '`'
            collection_name = f"collection{special_char}"
            query = f"create collection {bucket_name}.{scope_name}.{tick_char}{collection_name}{tick_char}"
            try:
                self.run_cbq_query(query)
            except CBQError as err:
                errors.append(f"Cannot run query: {query} \nError is:{str(err)}")
                continue
            scope_collections = self.rest.get_scope_collections(bucket_name, scope_name)
            if collection_name not in scope_collections:
                errors.append(f"Cannot create collection {collection_name} in bucket {bucket_name}")

        self.cluster.bucket_delete(self.master, bucket_name)
        self.wait_for_bucket_delete(bucket_name, 3, 100)

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)

        for special_char in special_chars:
            if special_char in tick_chars:
                continue
            collection_name = f"collection{special_char}"
            query = f"create collection {bucket_name}.{scope_name}.{collection_name}"
            try:
                self.run_cbq_query(query)
            except CBQError as err:
                errors.append(f"Cannot run query: {query} \nError is:{str(err)}")
                continue
            scope_collections = self.rest.get_scope_collections(bucket_name, scope_name)
            if collection_name not in scope_collections:
                errors.append(f"Cannot create collection {collection_name} in bucket {bucket_name}")

        self.cluster.bucket_delete(self.master, bucket_name)
        self.wait_for_bucket_delete(bucket_name, 3, 100)
        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)

        for special_char in special_chars:
            tick_char = '`'
            collection_name = f"collection{special_char}test"
            query = f"create collection {bucket_name}.{scope_name}.{tick_char}{collection_name}{tick_char}"
            try:
                self.run_cbq_query(query)
            except CBQError as err:
                errors.append(f"Cannot run query: {query} \nError is:{str(err)}")
                continue
            scope_collections = self.rest.get_scope_collections(bucket_name, scope_name)
            if collection_name not in scope_collections:
                errors.append(f"Cannot create collection {collection_name} in bucket {bucket_name}")

        for error in errors:
            self.log.info(error)
        self.assertEquals(len(errors), 0, "Creation of collections with special chars is failed.")

    def test_create_negative(self):
        test_name = self.input.param("test_name", '')
        if test_name == '':
            self.fail("Test name cannot be empty, please fix .conf file")

        test_data = self.negative_tests_objects[test_name]
        test_objects_created, error = \
            self.collections_helper.create_bucket_scope_collection_multi_structure(cluster=self.cluster,
                                                                                   existing_buckets=self.buckets,
                                                                                   bucket_params=self.bucket_params,
                                                                                   data_structure=test_data)
        if not test_objects_created:
            self.assertEquals(True, False, f"Test objects load is failed: {error}")

        test_fails = []

        for test_query in test_data["test_queries"]:
            wrong_object_created = False
            query = test_query['text']
            expected_error = test_query["expected_error"]
            try:
                self.run_cbq_query(test_query["text"])
                wrong_object_created = True
            except CBQError as err:
                err_msg = str(err)
                if expected_error not in err_msg:
                    test_fails.append(f"Unexpected error message found while executing query: {query}"
                                      f"\nFound:{str(err)}"
                                      f"\nExpected message fragment is: {expected_error}")
            if wrong_object_created:
                test_fails.append(f"Unexpected success while executing query: {query}")

        for fail in test_fails:
            self.log.info(fail)
        self.assertEquals(len(test_fails), 0, "See logs for test fails information")

    def test_incorrect_scope_naming_negative(self):
        special_chars = ['_', '%']
        bucket_name = 'bucket1'
        errors = []

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)
        for special_char in special_chars:
            tick_char = '`'
            scope_name = f"{special_char}scope"
            query = f"create scope {bucket_name}.{tick_char}{scope_name}{tick_char}"
            try:
                self.run_cbq_query(query)
            except CBQError as err:
                continue
            bucket_scopes = self.rest.get_bucket_scopes(bucket_name)
            if scope_name in bucket_scopes:
                errors.append(f"Can create scope {scope_name} in bucket {bucket_name}")

        for error in errors:
            self.log.info(error)
        self.assertEquals(len(errors), 0, "Creation of scope with incorrect name is successful.")

    def test_create_default_scope(self):
        bucket_name = 'bucket1'
        scope_name = "_default"

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)
        query = f"create scope {bucket_name}.{scope_name}"
        try:
            self.run_cbq_query(query)
        except CBQError as err:
            self.assertEquals(True, True, "Cannot create scope named _default")
            return

        self.assertEquals(True, False, "Creation of scope with name _default is successful.")

    def test_incorrect_scope_naming_not_allowed_symbols_negative(self):
        special_chars = ["~", "!", "#", "$", "^", "&", "*", "(", ")", "+", "=", "{", "[", "}", "]", "|", "\\", ":",
                         ";", "\"", "'", "<", ",", ">", ".", "?", "/"]
        bucket_name = 'bucket1'
        errors = []

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)
        for special_char in special_chars:
            tick_char = '`'
            scope_name = f"scope{special_char}test"
            query = f"create scope {bucket_name}.{tick_char}{scope_name}{tick_char}"
            try:
                self.run_cbq_query(query)
            except CBQError as err:
                continue
            bucket_scopes = self.rest.get_bucket_scopes(bucket_name)
            if scope_name in bucket_scopes:
                errors.append(f"Can create scope {scope_name} in bucket {bucket_name}")

        for error in errors:
            self.log.info(error)
        self.assertEquals(len(errors), 0, "Creation of scope with incorrect name is successful.")

    def test_incorrect_collection_naming_not_allowed_symbols_negative(self):
        special_chars = ["~", "!", "#", "$", "^", "&", "*", "(", ")", "+", "=", "{", "[", "}", "]", "|", "\\", ":",
                         ";", "\"", "'", "<", ",", ">", ".", "?", "/"]
        bucket_name = 'bucket1'
        scope_name = "_default"
        errors = []

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)
        for special_char in special_chars:
            tick_char = '`'
            collection_name = f"collection{special_char}test"
            query = f"create collection {bucket_name}.{tick_char}{collection_name}{tick_char}"
            try:
                self.run_cbq_query(query)
            except CBQError as err:
                continue
            bucket_collections = self.rest.get_scope_collections(bucket_name, scope_name)
            if collection_name in bucket_collections:
                errors.append(f"Can create collection {collection_name} in bucket {bucket_name}")

        for error in errors:
            self.log.info(error)
        self.assertEquals(len(errors), 0, "Creation of collection with incorrect name is successful.")

    def test_drop_cli_collection(self):
        bucket_name = "bucket1"
        scope_name = "scope1"
        collection_name = "collection1"
        keyspace_name = "default"

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)
        collection_created = self.cli_client.create_scope_collection(bucket=bucket_name, scope=scope_name,
                                                                     collection=collection_name)
        if collection_created:
            self.collections_helper.delete_collection(keyspace=keyspace_name, bucket_name=bucket_name,
                                                      scope_name=scope_name, collection_name=collection_name)

            objects = self.rest.get_scope_collections(bucket_name, scope_name)
            if collection_name in objects:
                self.assertTrue(False, "Collection still exists after collection drop.")
            result, _ = self.collections_helper.find_object_in_all_keyspaces(type="collection", name=collection_name, bucket=bucket_name, scope=scope_name)
            self.assertFalse(result, "Collection still exists in system:all_keyspaces after collection drop.")
        else:
            self.assertTrue(False, "Failed to create collection using CLI. Test is failed.")

    def test_drop_rest_collection(self):
        bucket_name = "bucket1"
        scope_name = "scope1"
        collection_name = "collection1"
        keyspace_name = "default"

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)

        self.rest_client.create_scope_collection(bucket=bucket_name, scope=scope_name, collection=collection_name)
        # Allow time for scope and collection to be reflected in manifest after creation
        self.sleep(8)
        self.collections_helper.delete_collection(keyspace=keyspace_name, bucket_name=bucket_name,
                                                  scope_name=scope_name, collection_name=collection_name)

        objects = self.rest.get_scope_collections(bucket_name, scope_name)
        if collection_name in objects:
            self.assertEquals(True, False, "Collection still exists after collection drop.")
        result, _ = self.collections_helper.find_object_in_all_keyspaces(type="collection", name=collection_name, bucket=bucket_name, scope=scope_name)
        self.assertFalse(result, "Collection still exists in system:all_keyspaces after collection drop.")


    def test_drop_collection(self):
        bucket_name = "bucket1"
        scope_name = "scope1"
        collection_name = "collection1"
        keyspace_name = "default"

        # creating all DB objects
        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)
        self.collections_helper.create_scope(bucket_name=bucket_name, scope_name=scope_name)
        self.collections_helper.create_collection(bucket_name=bucket_name, scope_name=scope_name,
                                                  collection_name=collection_name)

        #Allow time for scope and collection to be reflected in manifest after creation
        self.sleep(8)

        # load document into collection
        try:
            self.run_cbq_query(
                "INSERT INTO "+keyspace_name+":" + bucket_name + "." + scope_name + "."
                + collection_name + "(KEY, VALUE) VALUES ('id1', { 'name' : 'name1' })")
            result = self.run_cbq_query(f"select name from {keyspace_name}:{bucket_name}.{scope_name}.{collection_name}"
                                        f" use keys 'id1'")['results'][0]['name']
            self.assertEquals(result, "name1", "Insert and select results do not match!")
        except CBQError as e:
            self.assertEquals(True, False, "Failed to perform insert into collection")
        except KeyError as err:
            self.assertEquals(True, False, "Failed to perform insert into collection")

        # dropping collection
        self.collections_helper.delete_collection(keyspace=keyspace_name, bucket_name=bucket_name,
                                                  scope_name=scope_name, collection_name=collection_name)

        # test that collection is dropped
        objects = self.rest.get_scope_collections(bucket_name, scope_name)
        if collection_name in objects:
            self.assertEquals(True, False, "Collection still exists after collection drop.")
        result, _ = self.collections_helper.find_object_in_all_keyspaces(type="collection", name=collection_name,
                                                                         bucket=bucket_name, scope=scope_name)
        self.assertFalse(result, "Collection still exists in system:all_keyspaces after collection drop.")

        # test that collection document is deleted
        result = self.run_cbq_query(f"select count(*) as cnt from {bucket_name}")['results'][0]['cnt']
        self.assertEquals(result, 0, "Collection document was not deleted after collection drop")

    # test always fails because information about scopes is not going to be updated after manipulations with scopes. Waiting for fix.
    def test_drop_cli_scope(self):
        keyspace_name = "default"
        bucket_name = "bucket1"
        scope_name = "scope1"

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)
        scope_created = self.cli_client.create_scope(bucket=bucket_name, scope=scope_name)
        if scope_created:
            self.collections_helper.delete_scope(keyspace=keyspace_name, bucket_name=bucket_name, scope_name=scope_name)
            self.sleep(5,"Wait before checking for scope deletion")

            scope_exists = self.collections_helper.check_if_scope_exists_in_scopes(keyspace=keyspace_name, bucket=bucket_name,
                                                      scope=scope_name)

            self.assertFalse(scope_exists, "Scope still exists in system:scopes after scope drop.")

        else:
            self.assertTrue(False, "Failed to create scope using CLI. Test is failed.")

    def test_drop_rest_scope(self):
        keyspace_name = "default"
        bucket_name = "bucket1"
        scope_name = "scope1"

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)

        self.rest_client.create_scope(bucket=bucket_name, scope=scope_name)
        self.sleep(3)
        self.collections_helper.delete_scope(keyspace=keyspace_name, bucket_name=bucket_name, scope_name=scope_name)
        self.sleep(3)
        objects = self.rest.get_bucket_scopes(bucket_name)
        if scope_name in objects:
            self.assertEquals(True, False, "Scope still exists after scope drop.")

        scope_exists = self.collections_helper.check_if_scope_exists_in_scopes(keyspace=keyspace_name,
                                                                               bucket=bucket_name,
                                                                               scope=scope_name)

        self.assertFalse(scope_exists, "Scope still exists in system:scopes after scope drop.")

    def test_drop_scope_cascade(self):
        bucket_name = "bucket1"
        scope_name = "scope1"
        collection_name = "collection1"
        keyspace_name = "default"

        # creating all DB objects
        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)
        self.collections_helper.create_scope(bucket_name=bucket_name, scope_name=scope_name)
        self.collections_helper.create_collection(bucket_name=bucket_name, scope_name=scope_name,
                                                  collection_name=collection_name)

        self.sleep(8)

        # load document into collection
        try:
            self.run_cbq_query(
                "INSERT INTO " + bucket_name + "." + scope_name + "." + collection_name +
                " (KEY, VALUE) VALUES ('id1', { 'name' : 'name1' })")
            result = self.run_cbq_query(
                f"select name from {keyspace_name}:{bucket_name}.{scope_name}.{collection_name} use keys 'id1'")[
                'results'][0]['name']
            self.assertEquals(result, "name1", "Insert and select results do not match!")
        except CBQError as e:
            self.assertEquals(True, False, "Failed to perform insert into collection")
        except KeyError as err:
            self.assertEquals(True, False, "Failed to perform insert into collection")

        # dropping scope
        self.collections_helper.delete_scope(keyspace=keyspace_name, bucket_name=bucket_name, scope_name=scope_name)

        # check that collection is dropped
        #this check is still invalid since we don't update bucket manifest after dropping scope by n1ql query
        #objects = self.rest.get_scope_collections(bucket_name, scope_name)
        #if collection_name in objects:
        #    self.assertEquals(True, False, "Collection still exists after scope drop.")

        result, _ = self.collections_helper.find_object_in_all_keyspaces(type="collection", name=collection_name,
                                                                         bucket=bucket_name, scope=scope_name)
        self.assertFalse(result, "Collection still exists in system:all_keyspaces after scope drop.")

        # check that scope is dropped
        objects = self.rest.get_bucket_scopes(bucket_name)
        if scope_name in objects:
            self.assertEquals(True, False, "Scope still exists after scope drop.")

        # check that collection document is dropped
        result = self.run_cbq_query(f"select count(*) as cnt from {bucket_name}")['results'][0]['cnt']
        self.assertEquals(result, 0, "Collection document was not deleted after scope drop")

    def test_create_n1ql_collection_in_cli_scope(self):
        bucket_name = "bucket1"
        scope_name = "scope1"
        collection_name = "collection1"

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)
        scope_created = self.cli_client.create_scope(bucket=bucket_name, scope=scope_name)

        # Allow time for scope and collection to be reflected in manifest after creation
        self.sleep(8)

        if scope_created:
            self.collections_helper.create_collection(bucket_name=bucket_name, scope_name=scope_name,
                                                      collection_name=collection_name)
            scope_collections = self.rest.get_scope_collections(bucket_name, scope_name)
            if collection_name not in scope_collections:
                self.assertEquals(True, False, "Cannot create collection via N1QL in a scope created via CLI.")
            result, error = self.collections_helper.find_object_in_all_keyspaces(type="collection", name=collection_name,
                                                                         bucket=bucket_name, scope=scope_name)
            self.assertTrue(result, f"Cannot find collection, created in scope, created via CLI in system:all_keyspaces. Error is: {error}")

        else:
            self.assertEquals(True, False, "Failed to create scope using CLI. Test is failed")

    def test_create_n1ql_collection_in_rest_scope(self):
        bucket_name = "bucket1"
        scope_name = "scope1"
        collection_name = "collection1"

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)
        self.rest_client.create_scope(bucket=bucket_name, scope=scope_name)

        # Allow time for scope and collection to be reflected in manifest after creation
        self.sleep(8)

        self.collections_helper.create_collection(bucket_name=bucket_name, scope_name=scope_name,
                                                  collection_name=collection_name)
        scope_collections = self.rest.get_scope_collections(bucket_name, scope_name)
        if collection_name not in scope_collections:
            self.assertEquals(True, False,
                              "Cannot create collection via N1QL in a scope created via REST. Test is failed.")
        result, error = self.collections_helper.find_object_in_all_keyspaces(type="collection", name=collection_name,
                                                                         bucket=bucket_name, scope=scope_name)
        self.assertTrue(result, f"Cannot find collection, created in scope, created via REST in system:all_keyspaces. Error is: {error}")

    def test_recreate_collection(self):
        bucket_name = "bucket1"
        scope_name = "scope1"
        collection_name = "collection1"
        objects = {"buckets": [{"name": f"{bucket_name}", "scopes": [
            {"name": f"{scope_name}", "collections": [{"name": f"{collection_name}"}]}]}]}
        self.collections_helper.create_bucket_scope_collection_multi_structure(cluster=self.cluster,
                                                                               existing_buckets=self.buckets,
                                                                               bucket_params=self.bucket_params,
                                                                               data_structure=objects)
        # todo: remove this sleep after fix of https://issues.couchbase.com/browse/MB-39500
        self.sleep(30)

        self.run_cbq_query(
            "INSERT INTO " + bucket_name + "." + scope_name + "." + collection_name +
            " (KEY, VALUE) VALUES ('id1', { 'name' : 'name1' })")

        self.collections_helper.delete_collection(bucket_name=bucket_name, scope_name=scope_name,
                                                  collection_name=collection_name)
        result, _ = self.collections_helper.find_object_in_all_keyspaces(type="collection", name=collection_name,
                                                                             bucket=bucket_name, scope=scope_name)
        self.assertFalse(result,
                        "Collection still exists in system:all_keyspaces after drop")

        result = self.collections_helper.create_collection(bucket_name=bucket_name, scope_name=scope_name,
                                                           collection_name=collection_name)
        self.assertEquals(result, True, "Cannot re-create collection. Test is failed.")
        result, error = self.collections_helper.find_object_in_all_keyspaces(type="collection", name=collection_name,
                                                                             bucket=bucket_name, scope=scope_name)
        self.assertTrue(result, f"Collection cannot be found in system:all_keyspaces after re-creation. Error: {error}")

    def test_recreate_scope(self):
        bucket_name = "bucket1"
        scope_name = "scope1"
        collection_name = "collection1"
        objects = {"buckets": [{"name": f"{bucket_name}", "scopes": [
            {"name": f"{scope_name}", "collections": [{"name": f"{collection_name}"}]}]}]}
        self.collections_helper.create_bucket_scope_collection_multi_structure(cluster=self.cluster,
                                                                               existing_buckets=self.buckets,
                                                                               bucket_params=self.bucket_params,
                                                                               data_structure=objects)
        self.run_cbq_query(
            "INSERT INTO " + bucket_name + "." + scope_name + "." + collection_name +
            " (KEY, VALUE) VALUES ('id1', { 'name' : 'name1' })")

        self.collections_helper.delete_scope(bucket_name=bucket_name, scope_name=scope_name)
        result, _ = self.collections_helper.find_object_in_all_keyspaces(type="scope", name=scope_name,
                                                                             bucket=bucket_name, scope=scope_name)
        self.assertFalse(result, "Scope still exists in system:all_keyspaces after drop")


        result = self.collections_helper.create_scope(bucket_name=bucket_name, scope_name=scope_name)
        self.assertEquals(result, True, "Cannot re-create scope. Test is failed.")
        result, error = self.collections_helper.find_object_in_all_keyspaces(type="scope", name=scope_name,
                                                                             bucket=bucket_name, scope=scope_name)
        self.assertTrue(result, f"Scope cannmot be found in system:all_keyspaces after re-creation. Error: {error}")


    def test_1000_objects_test(self):
        bucket = "bucket1"
        self._create_bucket(bucket)
        for i in range(1, 100):
            scope = f"scope_{i}"
            scope_created = self.collections_helper.create_scope(bucket_name=bucket, scope_name=scope)
            for j in range(1, 10):
                collection = f"collection_{j}"
                collection_created = self.collections_helper.create_collection(bucket_name=bucket, scope_name=scope, collection_name=collection)

        for i in range(1, 100):
            scope = f"scope_{i}"
            for j in range(1, 10):
                collection = f"collection_{j}"
                result, error = self.collections_helper.find_object_in_all_keyspaces(type="collection", name=collection, bucket=bucket, scope=scope)
                self.assertTrue(result, f"Collection {collection} cannot be found in system:all_keyspaces. Error: {error}")


    def test_massive_test(self):
        buckets = ["bucket1", "bucket2", "bucket3", "bucket4", "bucket5", "bucket6", "bucket7"]
        scopes = ["scope1", "scope2", "scope3", "scope4", "scope5", "scope6", "scope7", "scope8", "scope9", "scope10"]
        collections = ["collection1", "collection2", "collection3", "collection4", "collection5", "collection6",
                       "collection7", "collection8", "collection9", "collection10"]
        keyspace_name = "default"

        # create schema
        self.collections_helper.create_bucket_scope_collection_multi_structure(cluster=self.cluster,
                                                                               existing_buckets=self.buckets,
                                                                               bucket_params=self.bucket_params,
                                                                               data_structure=self.tests_objects[
                                                                                   "massive_test"])
        # fill collections:
        for bucket in buckets:
            for scope in scopes:
                for collection in collections:
                    self.run_cbq_query(
                        "INSERT INTO " + bucket + "." + scope + "." + collection + " (KEY, VALUE) VALUES ('" + bucket
                        + "_" + scope + "_" + collection + "', { 'name' : '" + bucket + "_" + scope + "_"
                        + collection + "' })")

        # selects
        for bucket in buckets:
            for scope in scopes:
                for collection in collections:
                    result = self.run_cbq_query(
                        f"select name from {keyspace_name}:{bucket}.{scope}.{collection} use keys '" + bucket +
                        "_" + scope + "_" + collection + "'")[
                        'results'][0]['name']
                    self.assertEquals(result, bucket + "_" + scope + "_" + collection,
                                      f"Data in {keyspace_name}:{bucket}.{scope}.{collection} "
                                      f"is incorrect. Test is failed")

    """ Test is invalid until we will start using python SDK 3.0 
    def test_create_scope_sdk(self):

        bucket_name = "bucket1"
        scope_name = "scope1"
        collection_name = "collection1"

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)

        from couchbase.cluster import Cluster
        from couchbase.cluster import PasswordAuthenticator
        cl = Cluster("couchbase://"+self.master.ip)
        authenticator = PasswordAuthenticator("Administrator", "password")
        cl.authenticate(authenticator)
        cb = cl.open_bucket("bucket1")


        scope = cb.scope("scope1")
        collection = scope.collection("collection1")
    """

    def test_create_cbq(self):
        bucket_name = "bucket1"
        scope_name = "scope1"
        collection_name = "collection1"
        keyspace_name = "default"

        rest_collections_helper = CollectionsN1QL(self.master)

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)
        self.collections_helper.use_rest = False
        scope_created = self.collections_helper.create_scope(bucket_name=bucket_name, scope_name=scope_name)
        self.assertEquals(scope_created, True, "Cannot create scope via CBQ console")
        result, error = rest_collections_helper.find_object_in_all_keyspaces(type="scope", name=scope_name, bucket=bucket_name, scope=scope_name)
        self.assertTrue(result, f"Scope {scope_name}, created via CBQ cannot be found in system:keyspaces. Error: {error}")

        collection_created = self.collections_helper.create_collection(bucket_name=bucket_name, scope_name=scope_name,
                                                                       collection_name=collection_name)
        self.assertEquals(collection_created, True, "Cannot create collection via CBQ console")


        result, error = rest_collections_helper.find_object_in_all_keyspaces(type="collection", name=collection_name, bucket=bucket_name, scope=scope_name)
        self.assertTrue(result, f"Collection {collection_name}, created via CBQ cannot be found in system:all_keyspaces. Error: {error}")

        # Trying to insert doc into created collection
        try:
            self.run_cbq_query(
                "INSERT INTO " + bucket_name + "." + scope_name + "." + collection_name +
                " (KEY, VALUE) VALUES ('id1', { 'name' : 'name1' })")
            result = self.run_cbq_query(
                f"select name from {keyspace_name}:{bucket_name}.{scope_name}.{collection_name} use keys 'id1'")[
                'results'][0]['name']
            self.assertEquals(result, "name1", "Insert and select results do not match!")
        except CBQError as e:
            self.assertEquals(True, False, "Failed to perform insert into collection created via CBQ console")
        except KeyError as err:
            self.assertEquals(True, False, "Failed to perform insert into collection created via CBQ console")

    def _perform_test(self, data_structure=None):
        if data_structure is None:
            raise Exception("Empty value for data_structure parameter")
        tests = data_structure["tests"]
        for test in tests:
            object_type = test["object_type"]
            object_name = test["object_name"]
            object_bucket = test["object_container"]
            object_scope = test["object_scope"] if "object_scope" in test else None


            result, error = self.collections_helper.find_object_in_all_keyspaces(type=object_type,
                                                               name=object_name,
                                                               bucket=object_bucket,
                                                               scope = object_scope)
            if not result:
                return False, error

            if object_type == "scope":
                objects = self.rest.get_bucket_scopes(object_bucket)
            else:
                objects = self.rest.get_scope_collections(object_bucket, test["object_scope"])

            if not test["object_name"] in objects:
                return False, f"{object_type} {object_name} is not found in bucket {object_bucket}. Test is failed"
        return True, ""


    def _create_bucket(self, bucket_name):
        bucket_params = self._create_bucket_params(server=self.master, size=256,
                                                   replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                   enable_replica_index=self.enable_replica_index,
                                                   eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(bucket_name, 11222, bucket_params)
