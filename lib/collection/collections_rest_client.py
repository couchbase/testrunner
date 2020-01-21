from membase.api.rest_client import RestConnection
import re
import logger

class Collections_Rest(object):
    def __init__(self, node):
        self.log = logger.Logger.get_logger()
        self.rest = RestConnection(node)

    def create_collection(self, bucket="default", scope="scope0", collection="mycollection0"):
        return self.rest.create_collection(bucket, scope, collection)

    def create_scope(self, bucket="default", scope="scope0"):
        return self.rest.create_scope(bucket, scope)

    def delete_collection(self, bucket="default", scope='_default', collection='_default'):
        return self.rest.delete_collection(bucket, scope, collection)

    def delete_scope(self, scope, bucket="default"):  # scope should be passed as default scope can not be deleted
        return self.rest.delete_collection(bucket, scope)

    def create_scope_collection(self, collection_name, scope_num, collection_num, bucket="default"):
        collection_name[bucket] = ["_default._default"]
        for i in range(scope_num):
            if i == 0:
                scope_name = "_default"
            else:
                scope_name = bucket + str(i)
                self.create_scope(bucket, scope=scope_name)
            try:
                if i == 0:
                    num = int(collection_num[i] - 1)
                else:
                    num = int(collection_num[i])
            except:
                num = 2
            for n in range(num):
                collection = 'collection' + str(n)
                self.create_collection(bucket=bucket, scope=scope_name, collection=collection)
                collection_name[bucket].append(scope_name + '.' + collection)

        self.log.info("created collections for the bucket {} are {}".format(bucket, collection_name[bucket]))