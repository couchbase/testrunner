from couchbase_cli import CouchbaseCLI
import logger


class CollectionsCLI(object):
    def __init__(self, node):
        self.log = logger.Logger.get_logger()
        self.cli = CouchbaseCLI(node)

    def create_collection(self, bucket="default", scope="scope0", collection="mycollection0", max_ttl=None,
                          admin_tools_package=False):
        status, content, success = self.cli.create_collection(bucket, scope, collection, max_ttl=max_ttl,
                                                              admin_tools_package=admin_tools_package)
        if success:
            self.log.info("Collection created {}->{}->{}".format(bucket, scope, collection))
        else:
            raise Exception("Create collection failed : status:{0},content:{1}".format(status, content))
        return success

    def create_scope(self, bucket="default", scope="scope0", admin_tools_package=False):
        status, content, success = self.cli.create_scope(bucket, scope, admin_tools_package)
        if success:
            self.log.info("Scope created {}->{}".format(bucket, scope))
        else:
            raise Exception("Create scope failed : status:{0},content:{1}".format(status, content))
        return success

    def delete_collection(self, bucket="default", scope='_default', collection='_default', admin_tools_package=False):
        status, content, success = self.cli.delete_collection(bucket, scope, collection, admin_tools_package)
        if success:
            self.log.info("Collection dropped {}->{}->{}".format(bucket, scope, collection))
        else:
            raise Exception("Drop collection failed : status:{0},content:{1}".format(status, content))
        return success

    def delete_scope(self, scope, bucket="default", admin_tools_package=False):
        status, content, success = self.cli.delete_scope(scope, bucket, admin_tools_package)
        if success:
            self.log.info("Scope dropped {}->{}".format(bucket, scope))
        else:
            raise Exception("Drop scope failed : status:{0},content:{1}".format(status, content))
        return success

    def create_scope_collection(self, bucket, scope, collection, admin_tools_package=False):
        if self.cli.create_scope(bucket, scope, admin_tools_package):
            if self.cli.create_collection(bucket, scope, collection, admin_tools_package):
                return True
        return False

    def delete_scope_collection(self, bucket, scope, collection, admin_tools_package=False):
        if self.cli.delete_collection(bucket, scope, collection, admin_tools_package):
            if self.cli.delete_scope(bucket, scope, admin_tools_package):
                return True
        return False

    def get_bucket_scopes(self, bucket, admin_tools_package=False):
        return self.cli.get_bucket_scopes(bucket, admin_tools_package)

    def get_bucket_collections(self, bucket, admin_tools_package=False):
        return self.cli.get_bucket_collections(bucket, admin_tools_package)

    def get_scope_collections(self, bucket, scope, admin_tools_package=False):
        return self.cli.get_scope_collections(bucket, scope, admin_tools_package)
