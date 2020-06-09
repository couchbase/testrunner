from couchbase_helper.tuq_helper import N1QLHelper
from membase.api.exception import CBQError
import logger
import time

class CollectionsN1QL(object):
    def __init__(self, node):
        self.log = logger.Logger.get_logger()
        self.node = node
        self.use_rest = True
        self.n1ql_helper = N1QLHelper(use_rest=True, log=self.log)

    def create_collection(self, keyspace="default", bucket_name="", scope_name="", collection_name="", poll_interval=1, timeout=30):
        self.n1ql_helper.use_rest = self.use_rest
        return self.n1ql_helper.create_collection(server=self.node, keyspace=keyspace, bucket_name=bucket_name,
                                           scope_name=scope_name, collection_name=collection_name,
                                           poll_interval=poll_interval, timeout=timeout)

    def create_scope(self, keyspace="default", bucket_name="", scope_name="", poll_interval=1, timeout=30):
        self.n1ql_helper.use_rest = self.use_rest
        result =  self.n1ql_helper.create_scope(server=self.node, keyspace=keyspace, bucket_name=bucket_name, scope_name=scope_name,
                                      poll_interval=poll_interval, timeout=timeout)
        if result:
            # waiting for additional time according to https://issues.couchbase.com/browse/MB-39500
            time.sleep(10)
        return result

    def delete_collection(self, keyspace="default", bucket_name="", scope_name="", collection_name="",
                        poll_interval=1, timeout=30):
        self.n1ql_helper.use_rest = self.use_rest
        return self.n1ql_helper.delete_collection(server=self.node, keyspace=keyspace, bucket_name=bucket_name,
                                           scope_name=scope_name, collection_name=collection_name,
                                           poll_interval=poll_interval, timeout=timeout)

    def delete_scope(self, keyspace="default", bucket_name="", scope_name="", poll_interval=1, timeout=30):
        self.n1ql_helper.use_rest = self.use_rest
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

    def find_object_in_all_keyspaces(self, type=None, name=None, bucket=None, scope=None):
        path = ""
        if type == 'scope':
            path = f"default:{bucket}.{name}"
        else:
            path = f"default:{bucket}.{scope}.{name}"

        query = f"select count(*) from system:all_keyspaces where path='{path}'"
        result = self.n1ql_helper.run_cbq_query(query)

        if result['results'][0]['$1'] == 0:
            return False, f"Object {path} is not found in system:all_keyspaces."
        elif result['results'][0]['$1'] > 1:
            return False, f" More than one object {path} is found in system:all_keyspaces."
        else:
            return True, ""
