from couchbase_cli import CouchbaseCLI
import logger

class Collections_CLI(object):
    def __init__(self, node):
        self.log = logger.Logger.get_logger()
        self.cli = CouchbaseCLI(node)

    def create_collection(self, bucket="default", scope="scope0", collection="mycollection0"):
        return self.cli.create_collection(bucket, scope, collection)

    def create_scope(self, bucket="default", scope="scope0"):
        return self.cli.create_scope(bucket, scope)

    def delete_collection(self, bucket="default", scope='_default', collection='_default'):
        return self.cli.delete_collection(bucket, scope, collection)

    def delete_scope(self, scope, bucket="default"):
        return self.cli.delete_scope(bucket, scope)

    def create_scope_collection(self, bucket, scope, collection):
        self.cli.create_scope(bucket, scope)
        return self.cli.create_collection(bucket, scope, collection)

    def delete_scope_collection(self, bucket, scope, collection):
        self.cli.delete_collection(bucket, scope, collection)
        self.cli.delete_scope(bucket, scope)

    def get_bucket_scopes(self, bucket):
        return self.cli.get_bucket_scopes(bucket)

    def get_bucket_collections(self, bucket):
        return self.cli.get_bucket_collections(bucket)

    def get_scope_collections(self, bucket, scope):
        return self.cli.get_scope_collections(bucket, scope)