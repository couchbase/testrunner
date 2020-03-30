from membase.api.rest_client import RestConnection

import logger
import threading


class Collections_Rest(object):
    def __init__(self, node):
        self.log = logger.Logger.get_logger()
        self.rest = RestConnection(node)

    def create_collection(self, bucket="default", scope="scope0", collection="mycollection0", params=None):
        self.rest.create_collection(bucket, scope, collection, params)

    def create_scope(self, bucket="default", scope="scope0", params=None):
        self.rest.create_scope(bucket, scope, params)

    def delete_collection(self, bucket="default", scope='_default', collection='_default'):
        self.rest.delete_collection(bucket, scope, collection)

    def delete_scope(self, scope, bucket="default"):
        self.rest.delete_scope(bucket, scope)

    def create_scope_collection(self, bucket, scope, collection, params=None):
        self.create_scope(bucket, scope)
        self.create_collection(bucket, scope, collection, params)

    def delete_scope_collection(self, bucket, scope, collection):
        self.rest.delete_collection(bucket, scope, collection)
        self.delete_scope(bucket, scope)

    def create_scope_collection(self, scope_num, collection_num, bucket="default"):
        for s in range(1, scope_num):
            scope = "scope_" + str(s)
            self.create_scope(bucket, scope)
            for c in range(1, collection_num):
                self.create_collection(bucket, scope, "collection_" + str(c))

    class CollectionFactory(threading.Thread):
        def __init__(self, bucket, scope, collection, rest):
            threading.Thread.__init__(self)
            self.bucket_name = bucket
            self.scope_name = scope
            self.collection_name = collection
            self.rest_handle = rest

        def run(self):
            self.rest_handle.create_collection(self.bucket_name, self.scope_name, self.collection_name)

    # Multithreaded for bulk creation
    def async_create_scope_collection(self, scope_num, collection_num, bucket="default"):
        tasks = []
        for s in range(1, scope_num):
            scope = "scope_" + str(s)
            self.create_scope(bucket, scope)
            for c in range(1, collection_num):
                task = self.CollectionFactory(bucket, scope, "collection_" + str(c), self.rest)
                task.start()
                tasks.append(task)

        for task in tasks:
            task.join()

    def get_bucket_scopes(self, bucket):
        return self.rest.get_bucket_scopes(bucket)

    def get_bucket_collections(self, bucket):
        return self.rest.get_bucket_collections(bucket)

    def get_scope_collections(self, bucket, scope):
        return self.rest.get_scope_collections(bucket, scope)