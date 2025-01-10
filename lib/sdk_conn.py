from datetime import timedelta

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
import couchbase.subdocument as SD


class SDKClient(object):
    def __init__(self, server_ip, username, password, bucket_name=None):
        options = ClusterOptions(PasswordAuthenticator(username, password))

        self.cluster = Cluster(f"couchbases://{server_ip}?ssl=no_verify",
                               options)
        self.cluster.wait_until_ready(timedelta(seconds=5))

        self.bucket = None
        self.collection = None

        if bucket_name:
            self.bucket = self.cluster.bucket(bucket_name)
            self.select_collection()

    def close(self):
        self.cluster.close()

    def select_collection(self, scope=None, collection=None):
        if collection is None:
            self.collection = self.bucket.default_collection()
        else:
            self.collection = self.bucket.scope(scope).collection(collection)

    def get_doc(self, doc_key):
        return self.collection.get(doc_key)

    def upsert_doc(self, doc_key, doc):
        return self.collection.upsert(doc_key, doc)

    def get_sub_doc_as_dict(self, doc_key, sub_doc_path):
        value = self.collection.lookup_in(doc_key, [SD.get(sub_doc_path)])
        return value.content_as[dict](0)

    def get_sub_doc_as_list(self, doc_key, sub_doc_path):
        value = self.collection.lookup_in(doc_key, [SD.get(sub_doc_path)])
        return value.content_as[list](0)

    def insert_sub_doc(self, key, sub_doc_path, sub_doc_value,
                       create_parents=False):
        self.collection.mutate_in(
            key, [SD.insert(sub_doc_path, sub_doc_value,
                  create_parents=create_parents)])

    def upsert_sub_doc(self, key, sub_doc_path, sub_doc_value,
                       create_parents=False):
        self.collection.mutate_in(
            key, [SD.upsert(sub_doc_path, sub_doc_value,
                            create_parents=create_parents)])

    def sub_doc_prepend_array(self, key, sub_doc_path, value,
                              create_parents=False):
        self.collection.mutate_in(
            key, (SD.array_prepend(sub_doc_path, value,
                                   create_parents=create_parents)))
