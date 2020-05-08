from couchbase_helper.tuq_helper import N1QLHelper
from membase.api.exception import CBQError
import logger

class CollectionsN1QL(object):
    def __init__(self, node):
        self.log = logger.Logger.get_logger()
        self.node = node
        self.n1ql_helper = N1QLHelper(use_rest=True, log=self.log)

    def create_collection(self, keyspace="default", bucket_name="", scope_name="", collection_name="", poll_interval=1, timeout=30):
        return self.n1ql_helper.create_collection(server=self.node, keyspace=keyspace, bucket_name=bucket_name,
                                           scope_name=scope_name, collection_name=collection_name,
                                           poll_interval=poll_interval, timeout=timeout)

    def create_scope(self, keyspace="default", bucket_name="", scope_name="", poll_interval=1, timeout=30):
        return self.n1ql_helper.create_scope(server=self.node, keyspace=keyspace, bucket_name=bucket_name, scope_name=scope_name,
                                      poll_interval=poll_interval, timeout=timeout)

    def delete_collection(self, keyspace="default", bucket_name="", scope_name="", collection_name="",
                        poll_interval=1, timeout=30):
        return self.n1ql_helper.delete_collection(server=self.node, keyspace=keyspace, bucket_name=bucket_name,
                                           scope_name=scope_name, collection_name=collection_name,
                                           poll_interval=poll_interval, timeout=timeout)

    def delete_scope(self, keyspace="default", bucket_name="", scope_name="", poll_interval=1, timeout=30):
        return self.n1ql_helper.delete_scope(server=self.node, keyspace=keyspace, bucket_name=bucket_name,
                                      scope_name=scope_name, poll_interval=poll_interval, timeout=timeout)

    """
        data_structure format:
        {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1", "collections":[{"name": "collection1"}, {"name": "collection2"}]}
                    ]
                },
                {
                    "name": "bucket2",
                    "scopes": [
                        {"name": "scope1", "collections": []}
                    ]
                }
            ]
        }            
    """
    def create_bucket_scope_collection_multi_structure(self, cluster=None, existing_buckets=[], bucket_params={}, data_structure={}):
        try:
            buckets = data_structure["buckets"]
            for bucket in buckets:
                if bucket not in existing_buckets:
                    cluster.create_standard_bucket(bucket["name"], 11222, bucket_params)
                scopes = bucket["scopes"]
                for scope in scopes:
                    if not scope["name"] == "_default":
                        result = self.create_scope(bucket_name=bucket["name"], scope_name=scope["name"])
                        if not result:
                            return False, f"Scope {scope['name']} creation is failed."
                    collections = scope["collections"]
                    for collection in collections:
                        result = self.create_collection(bucket_name=bucket["name"], scope_name=scope["name"],
                                               collection_name=collection["name"])
                        if not result:
                            return False, f"Collection {collection['name']} creation is failed."
        except CBQError as err:
            return False, str(err)
        return True, ""
