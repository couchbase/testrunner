import copy
from enum import (
    Enum
)


class Tag(Enum):
    CREATED = 1
    DELETED = 2
    CHANGED = 3
    UNCHANGED = 4
    IMMUTABLE = 5


class Taggable:
    """ A object can be marked as one of Tags to track change """

    def __init__(self, tag=Tag.CREATED):
        self.tag = tag


class Document(Taggable):
    """ Represents a Couchbase document """

    def __init__(self, key, value, **kwargs):
        """ Represents a document

        Args:
            collection (Collection): The collection this document is in.
        """
        super().__init__(**kwargs)
        self.key = key
        self.value = value


class Collection(Taggable):
    """ A collection """

    def __init__(self, collection_string, uid, **kwargs):
        """

        Args:
            uid (int):
            collection_name (CollectionString): The collection name of this collection.
        """
        super().__init__(**kwargs)
        self.uid = uid
        self.documents = {}
        self.collection_string = collection_string

    def set_document(self, key, value):
        if key in self.documents:
            self.documents[key].value = value

        self.documents[key] = Document(key, value)

    def get_document(self, key):
        if key not in self.documents:
            raise ValueError(f"The document key {key} does not exist")

        return self.documents[key]


class Bucket(Taggable):
    """ Represents a bucket """

    def __init__(self, bucket_name, collections={}, **kwargs):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.collections = collections

        self.add_collection(Collection(CollectionString(f"{self.bucket_name}._default._default"), 0))

    def add_collection(self, collection):
        key = str(collection.collection_string)

        if key in self.collections:
            raise ValueError(f"The collection {key} already exists")

        self.collections[key] = collection

    def get_collection(self, collection_string):
        key = str(collection_string)

        if key not in self.collections:
            raise ValueError(f"The collection {key} does not exist")

        return self.collections[key]


class Backup:
    """ Describes a backup which turns into a physical backup on disk """

    def __init__(self, previous_backup=None, buckets={}):
        """ Forms a linked list of backups """
        self.buckets = buckets
        self.previous_backup = previous_backup

    def add_bucket(self, bucket):
        if bucket.bucket_name in self.buckets:
            raise ValueError(f"The bucket {bucket.bucket_name} is already exists.")

        self.buckets[bucket.bucket_name] = bucket

    def del_bucket(self, bucket_name):
        if bucket_name not in self.buckets:
            raise ValueError(f"The bucket {bucket_name} does not exist.")

        self.buckets.pop(bucket.bucket_name)

    def get_bucket(self, bucket_name):
        if bucket_name not in self.buckets:
            raise ValueError(f"The bucket {bucket_name} does not exist.")

        return self.buckets[bucket_name]

    def get_collection(self, collection_string):
        return self.get_bucket(collection_string.bucket_name).get_collection(collection_string)

    def get_collection_uid(self, collection_string):
        return self.get_collection(collection_string).uid

    def set_document(self, collection_string, key, value):
        return self.get_collection(collection_string).set_document(key, value)

    def get_document(self, collection_string, key, value):
        return self.get_collection(collection_string).get_document(key)

    @classmethod
    def from_backup(cls, previous_backup):
        """ Copy construct from the previous Backup object """
        buckets = copy.deepcopy(previous_backup.buckets)
        return cls(buckets=buckets, previous_backup=previous_backup)

    def get_collection_strings(self):
        """ Gets all collection strings """
        return (CollectionString(cs) for b in self.buckets.values() for cs in b.collections)
