from couchbase_cli import CouchbaseCLI
import logger


class CollectionsCLI(object):
    def __init__(self, node):
        self.log = logger.Logger.get_logger()
        self.cli = CouchbaseCLI(node)

    def create_collection(self, bucket="default", scope="scope0", collection="mycollection0"):
        status, content, success = self.cli.create_collection(bucket, scope, collection)
        if success:
            self.log.info("Collection created {}->{}->{}".format(bucket, scope, collection))
        else:
            raise Exception("Create collection failed : status:{0},content:{1}".format(status, content))
        return success

    def create_scope(self, bucket="default", scope="scope0"):
        status, content, success = self.cli.create_scope(bucket, scope)
        if success:
            self.log.info("Scope created {}->{}".format(bucket, scope))
        else:
            raise Exception("Create scope failed : status:{0},content:{1}".format(status, content))
        return success

    def delete_collection(self, bucket="default", scope='_default', collection='_default'):
        status, content, success = self.cli.delete_collection(bucket, scope, collection)
        if success:
            self.log.info("Collection dropped {}->{}->{}".format(bucket, scope, collection))
        else:
            raise Exception("Drop collection failed : status:{0},content:{1}".format(status, content))
        return success

    def delete_scope(self, scope, bucket="default"):
        status, content, success = self.cli.delete_scope(scope, bucket)
        if success:
            self.log.info("Scope dropped {}->{}".format(bucket, scope))
        else:
            raise Exception("Drop scope failed : status:{0},content:{1}".format(status, content))
        return success

    def create_scope_collection(self, bucket, scope, collection):
        if self.cli.create_scope(bucket, scope):
            if self.cli.create_collection(bucket, scope, collection):
                return True
        return False

    def delete_scope_collection(self, bucket, scope, collection):
        if self.cli.delete_collection(bucket, scope, collection):
            if self.cli.delete_scope(bucket, scope):
                return True
        return False

    def get_bucket_scopes(self, bucket):
        return self.cli.get_bucket_scopes(bucket)

    def get_bucket_collections(self, bucket):
        return self.cli.get_bucket_collections(bucket)

    def get_scope_collections(self, bucket, scope):
        return self.cli.get_scope_collections(bucket, scope)
