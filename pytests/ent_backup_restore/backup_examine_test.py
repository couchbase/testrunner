import copy
import json
import math
import couchbase.subdocument as SD
try:
    from couchbase.options import MutateInOptions  # SDK 4.x
except ImportError:
    # Older SDK (if it ever had this symbol here)
    from couchbase.collection import MutateInOptions

from enum import (
    Enum
)
from ent_backup_restore.enterprise_backup_restore_base import (
    EnterpriseBackupRestoreBase
)
from membase.api.rest_client import (
    RestConnection
)
from membase.helper.rebalance_helper import (
    RebalanceHelper
)
from couchbase_helper.documentgenerator import (
    BlobGenerator,
    DocumentGenerator
)
from remote.remote_util import (
    RemoteMachineShellConnection
)
from testconstants import (
    CLUSTER_QUOTA_RATIO
)
from lib import (
    memcacheConstants
)
from lib.memcached.helper.data_helper import (
    MemcachedClientHelper
)
from TestInput import (
    TestInputSingleton
)
from couchbase.cluster import (
    Cluster
)
from couchbase.auth import (
    PasswordAuthenticator
)
from couchbase.durability import (
    Durability,
    ServerDurability,
    ClientDurability,
    ReplicateTo,
    PersistTo
)


class Tag(Enum):
    CREATED = 1
    DELETED = 2
    CHANGED = 3
    UNCHANGED = 4
    IMMUTABLE = 5
    XATTR_CHANGED = 6


class Taggable:
    """ A object can be marked as one of Tags to track change """

    def __init__(self, tag=Tag.CREATED):
        self.tag = tag


class Document(Taggable):
    """ Represents a Couchbase document """

    def __init__(self, key, value, xattrs=None, **kwargs):
        """ Represents a document

        Args:
            collection (Collection): The collection this document is in.
        """
        super().__init__(**kwargs)
        self.key = key
        self.value = value
        self.xattrs = xattrs
        if self.xattrs is None:
            self.xattrs = {}


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

    def set_document(self, key, value, new_tag=Tag.CHANGED):
        if key in self.documents:
            self.documents[key].append(Document(key, value, tag=new_tag, xattrs=self.documents[key][-1].xattrs))
        else:
            self.documents[key] = [Document(key, value)]

    def get_document(self, key):
        if key not in self.documents:
            raise ValueError(f"The document key {key} does not exist")

        return self.documents[key]

    def delete_document(self, key):
        if key not in self.documents:
            raise ValueError(f"The document key {key} does not exist")

        self.documents[key].append(Document(key, self.documents[key][-1].value, tag=Tag.DELETED))

    def set_subdoc(self, key, path, value, tag=Tag.CHANGED):
        subdoc_path_split = path.split('.')
        if key not in self.documents:
            # TODO: Add support for creating keys with xattrs
            raise ValueError(f"The document key {key} does not exist")
        else:
            self.documents[key].append(copy.deepcopy(self.documents[key][-1]))
            # Build a dictionary tree using the path provided
            tmp = self.documents[key][-1].xattrs
            for level in subdoc_path_split[:-1]:
                tmp.setdefault(level, {})
                tmp = tmp[level]
            tmp[subdoc_path_split[-1]] = value

            self.documents[key][-1].tag = tag

    def mark_unchanged(self, key, entry=-1):
        self.documents[key][entry].tag = Tag.UNCHANGED


class Bucket(Taggable):
    """ Represents a bucket """

    def __init__(self, bucket_name, collections=None, **kwargs):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.collections = collections
        if self.collections is None:
            self.collections = {}

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

    def __init__(self, previous_backup=None, buckets=None):
        """ Forms a linked list of backups """
        self.buckets = buckets
        if self.buckets is None:
            self.buckets = {}
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

    def set_document(self, collection_string, key, value, tag=Tag.CHANGED):
        return self.get_collection(collection_string).set_document(key, value, tag)

    def get_document(self, collection_string, key, value):
        return self.get_collection(collection_string).get_document(key)

    def delete_document(self, collection_string, key):
        return self.get_collection(collection_string).delete_document(key)

    def set_subdoc(self, collection_string, key, path, value, tag=Tag.CHANGED):
        return self.get_collection(collection_string).set_subdoc(key, path, value, tag)

    @classmethod
    def from_backup(cls, previous_backup):
        """ Copy construct from the previous Backup object """
        buckets = copy.deepcopy(previous_backup.buckets)
        return cls(buckets=buckets, previous_backup=previous_backup)

    def get_collection_strings(self):
        """ Gets all collection strings """
        return (CollectionString(cs) for b in self.buckets.values() for cs in b.collections)


class ExamineSimulation:

    def __init__(self, backup_base):
        self.backups = []
        # The backup base class which allows us to run cbbackupmgr commands
        self.backup_base = backup_base
        # Create a backup repository
        self.backup_base.backup_create()
        # An object to run cbbackupmgr examine
        self.examine = Examine(self.backup_base.backupset.backup_host)

    def run(self, mutations, optional_args=None):
        """ Advance a step in the simulation """
        # Create a new backup, copy construct from the previous backup if it exists
        if self.backups:
            backup = Backup.from_backup(self.backups[-1])
        else:
            backup = Backup()

        for cs in backup.get_collection_strings():
            for doc in backup.get_collection(cs).documents.values():
                backup.get_collection(cs).mark_unchanged(doc[-1].key)

        # Mutate the cluster to reflect what we want in the backup
        for mutation in mutations:
            mutation.apply(self.backup_base, backup)

        for bucket_name in backup.buckets.keys():
            if not RebalanceHelper.wait_for_stats_on_all(self.backup_base.master, bucket_name, 'ep_queue_size', 0, timeout_in_seconds=10):
                return False, f"Timed out waiting for ep_queue_size to reach 0"

        # Take a backup of the simulated documents
        stdout, stderr = self.backup_base.backup_cluster()
        if stderr:
            return False, f"Error: {stderr}"

        backup.date = self.backup_base.backups[-1]

        # Save the backup
        self.backups.append(backup)

        # Run examine
        self.run_examine(backup, optional_args)

        return True, None

    def steps(self):
        """ The number of steps that have taken place in the simulation """
        return len(self.backups)

    def run_examine(self, backup, optional_args=None):
        """ Examines things and check they match the contents of the backup object
        Event types (THESE MAY CHANGE):
        1: Created
        2: Full Backup
        3: Rollback
        4: Changed
        5: Deleted
        6: Unchanged
        """
        if optional_args is None:
            final = {}
        else:
            final = dict(optional_args)
        stop = None
        # Make sure that we don't try to read from backups we haven't taken yet
        if "start" in final and "end" in final:
            for s in ["start", "end"]:
                if final[s].isdigit():
                    final[s] = str(min(int(final[s]), self.steps()))
                    stop = int(final[s]) - 1

        for cs in backup.get_collection_strings():
            for doc in backup.get_collection(cs).documents.values():
                # Grab the "end" document if we've specified "end"
                if stop and stop < len(doc):
                    doc = doc[stop]
                else:
                    doc = doc[-1]
                examine_results = self.examine.examine(ExamineArguments(self.backup_base.backupset, str(cs), doc.key, objstore_provider=self.backup_base.objstore_provider, **final))
                print(examine_results[-1].__dict__)

                if doc.tag == Tag.DELETED:
                    self.backup_base.assertTrue(examine_results[-1].document.deleted)
                elif doc.tag == Tag.UNCHANGED:
                    # May not be the most effective method of checking, could do with looking into further
                    self.backup_base.assertEqual(examine_results[-1].event_type, 6)
                elif doc.tag == Tag.XATTR_CHANGED:
                    self.backup_base.assertEqual(examine_results[-1].document.xattrs, doc.xattrs)
                else:
                    if isinstance(examine_results[-1].document.value, dict):
                        self.backup_base.assertEqual(examine_results[-1].document.value, json.loads(doc.value))
                    else:
                        self.backup_base.assertEqual(examine_results[-1].document.value, doc.value)
                # TODO add more checkers


class Examine:

    def __init__(self, server):
        self.remote_connection = RemoteMachineShellConnection(server)

    def examine(self, examine_arguments):
        """ Returns an ExamineResults given an ExamineArguments object """
        if not examine_arguments.json:
            raise ValueError("Currently the non-JSON data output from the examine sub-command is not supported for testing.")

        output, error, exit_code = self.remote_connection.execute_command(examine_arguments.to_command(), get_exit_code=True)

        if exit_code != 0 or not output:
            return None, error

        return ExamineResult.from_output(output[0])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.remote_connection.disconnect()


class ExamineMetadata:

    def __init__(self, flags=None, expiry=None, locktime=None, cas=None, revseqno=None, datatype=None):
        self.flags = flags
        self.expiry = expiry
        self.locktime = locktime
        self.cas = cas
        self.revseqno = revseqno
        self.datatype = datatype


class ExamineDocument:

    def __init__(self, key=None, sequence_number=None, value=None, metadata=None, deleted=None, extended_attributes=None):
        self.key = key
        self.sequence_number = sequence_number
        self._value = value
        self.metadata = metadata if metadata is None else ExamineMetadata(**metadata)
        self.deleted = deleted
        self.xattrs = extended_attributes

    @property
    def value(self):
        if self.metadata.datatype == 0:
            return bytes.fromhex(self._value).decode()
        else:
            return self._value

    @value.setter
    def value(self, value):
        self._value = value


class ExamineResult:
    """ Represents a single entry in the examine sub-command's output """

    def __init__(self, version=None, backup=None, event_type=None, event_description=None, cluster_uuid=None, bucket_uuid=None, scope_id=None, collection_id=None, document=None):
        self.version = version
        self.backup = backup
        self.event_type = event_type
        self.event_description = event_description
        self.cluster_uuid = cluster_uuid
        self.bucket_uuid = bucket_uuid
        self.scope_id = scope_id
        self.collection_id = collection_id
        self.document = None if document is None else ExamineDocument(**document)

    @staticmethod
    def from_output(json_data):
        """ Returns a list of ExamineResult objects given the json output from the examine sub-command """
        return [ExamineResult( **entry) for entry in json.loads(json_data)]


class ExamineArguments:

    def __init__(self, backupset, collection_string, key, start=None, end=None, search_partial_backups=None, json=True, objstore_provider=None):
        self.backupset = backupset
        self.collection_string = collection_string
        self.key = key
        self.start = start
        self.end = end
        self.search_partial_backups = search_partial_backups
        self.json = json
        self.objstore_provider = objstore_provider

    def to_command(self, path="/opt/couchbase/bin"):
        # Required arguments
        command = (
            f"{path}/cbbackupmgr examine "
            f"--archive {self.objstore_provider.schema_prefix() + self.backupset.objstore_bucket + '/' if self.objstore_provider else ''}{self.backupset.directory} "
            f"--repo {self.backupset.name} "
            f"--collection-string {self.collection_string} "
            f"--key {self.key}"
        )

        # Optional arguments
        command += (
            f"{' --start ' + self.start if self.start else ''}"
            f"{' --end ' + self.end if self.end else ''}"
            f"{' --search-partial-backups ' if self.search_partial_backups else ''}"
            f"{' --json ' if self.json else ''}"
        )

        # Cloud arguments
        command += self.backupset.common_objstore_arguments(
            self.objstore_provider)

        return command.strip()


class BackupExamineTest(EnterpriseBackupRestoreBase):

    def setUp(self):
        """ The setup. """
        self.input = TestInputSingleton.input

        # Do not create the 'default' bucket automatically.
        self.input.test_params["default_bucket"] = False
        super().setUp()

        # Create a dictionary of configuration options for examine tests
        self.populate_config()

        # Set the memory quota based on the `memory` configuration option
        self.set_memory_quota(self.config['memory'])

        # Holds a dictionary of mapping buckets to their MemcachedClients
        self.clients = Clients(self.master)

    def tearDown(self):
        """ The teardown. """
        super().tearDown()

    def populate_config(self):
        """ Populates the config dictionary """
        self.config = {}
        # The memory available on the node with the lowest memory
        self.config['memory'] = self.get_available_memory()
        # The size of each bucket that is created
        self.config['bucket_size'] = 256
        # The max buckets that can be created given the available memory and bucket size
        self.config['max_buckets'] = self.config['memory'] // self.config['bucket_size']

    def get_available_memory(self):
        """ Gets the available memory based on the node with the lowest available memory """
        return math.floor(min(RestConnection(server).get_nodes_self().mcdMemoryReserved for server in self.servers) * CLUSTER_QUOTA_RATIO)

    def set_memory_quota(self, memory):
        """ Assign the data service memory """
        self.log.info(f"Setting memoryQuota to: {memory}")
        RestConnection(self.master).set_service_memoryQuota(service='memoryQuota', memoryQuota=memory)

    def run_simulation(self, mutations, optional_args=None):
        """ Runs a simulation given a list of a list of mutations.

        The i'th element in the list `mutations` contains a list of mutation at the i'th simulation step.

        Args:
            mutations list(list(Mutation)): Runs a step in the simulation for each list element in this list.
        """
        simulation = ExamineSimulation(self)

        for mutations in mutations:
            success, error_message = simulation.run(mutations, optional_args)
            self.assertTrue(success, error_message)

    def test_sample(self):
        """ Single key test """
        mutations = []

        mutations.append([
            CreateBucketMutation("default"),
            CreateCollectionMutation("default.fruit.red_fruit"),
            CreateCollectionMutation("default.fruit.green_fruit"),
            SetDocumentMutation("default.fruit.red_fruit", "my_key", "my_value")
        ])

        mutations.append([
            SetDocumentMutation("default.fruit.red_fruit", "my_key", "a_different_value")
        ])

        self.run_simulation(mutations)

    def test_start_end(self):
        """ Collection string at collection level """
        mutations = []

        optional_args = {
            "start": "1",
            "end": "3"
        }

        mutations.append([
            CreateBucketMutation("default"),
            CreateCollectionMutation("default.fruit.red_fruit"),
            SetDocumentMutation("default.fruit.red_fruit", "my_key", "my_value")
        ])

        mutations.append([
            SetDocumentMutation("default.fruit.red_fruit", "my_key", "a_different_value"),
            SetDocumentMutation("default.fruit.red_fruit", "my_key2", "another_one"),
            CreateCollectionMutation("default.fruit.green_fruit")
        ])

        mutations.append([
            SetDocumentMutation("default.fruit.green_fruit", "eyes", "twoofem")
        ])

        self.run_simulation(mutations, optional_args)

    def test_delete_document(self):
        """ Add document then delete in a later backup """
        mutations = []

        mutations.append([
            CreateBucketMutation("default"),
            CreateCollectionMutation("default.fruit.red_fruit"),
            SetDocumentMutation("default.fruit.red_fruit", "my_key", "my_value")
        ])

        mutations.append([
            DeleteDocumentMutation("default.fruit.red_fruit", "my_key")
        ])

        mutations.append([
            SetDocumentMutation("default.fruit.red_fruit", "another_key", "another_value")
        ])

        self.run_simulation(mutations)

    def test_reinsert_document(self):
        """ Add a document, delete it then reinsert it """
        mutations = []

        mutations.append([
            CreateBucketMutation("default"),
            CreateCollectionMutation("default.fruit.red_fruit"),
            SetDocumentMutation("default.fruit.red_fruit", "my_key", "my_value")
        ])

        mutations.append([
            DeleteDocumentMutation("default.fruit.red_fruit", "my_key")
        ])

        mutations.append([
            SetDocumentMutation("default.fruit.red_fruit", "my_key", "my_value")
        ])

        self.run_simulation(mutations)

    def test_with_merge(self):
        mutations = []

        mutations.append([
            CreateBucketMutation("default"),
            CreateCollectionMutation("default.fruit.red_fruit"),
            SetDocumentMutation("default.fruit.red_fruit", "my_key", "my_value")
        ])

        mutations.append([
            SetDocumentMutation("default.fruit.red_fruit", "my_key", "new_value"),
            MergeBackupMutation(),
            SetDocumentMutation("default.fruit.red_fruit", "my_key2", "my_value2")
        ])


        self.run_simulation(mutations)

    def test_change_document_type(self):
        mutations = []
        jsonIn = {
            "my": "value",
            "number": 15,
            "another": "one"
        }

        mutations.append([
            CreateBucketMutation("default"),
            CreateCollectionMutation("default.fruit.red_fruit"),
            SetDocumentMutation("default.fruit.red_fruit", "my_key", "my_value")
        ])


        mutations.append([
            SetDocumentMutation("default.fruit.red_fruit", "my_key", json.dumps(jsonIn)),
        ])

        self.run_simulation(mutations)

    def test_set_xattr(self):
        mutations = []
        jsonIn = {
            "name": "jef"
        }

        mutations.append([
            CreateBucketMutation("default"),
            CreateCollectionMutation("default.fruit.red_fruit"),
            SetDocumentMutation("default.fruit.red_fruit", "my_key", json.dumps(jsonIn))
        ])

        mutations.append([
            SetSubDocMutation("default.fruit.red_fruit", "my_key", "xattr_path.subdoc1", "sdval1")
        ])

        self.run_simulation(mutations)

    def test_large_value(self):
        mutations = []
        gen = BlobGenerator("examine-test", "examine-test-", 20, end=1)

        mutations.append([
            CreateBucketMutation("default"),
            CreateCollectionMutation("default.fruit.red_fruit"),
            SetDocumentMutation("default.fruit.red_fruit", *next(gen))
        ])

        self.run_simulation(mutations)

class SetSubDocMutation:
    """ Set a sub-document
    TODO: Add support for create_as_deleted
    As of 27/04/21 does not exist in python SDK
    Main point of contact is Jared Casey """


    def __init__(self, collection_string, key, path, value):
        self.collection_string, self.key, self.path, self.value = CollectionString(collection_string), key, path, value

    def apply(self, backup_base, backup):
        uid = backup.get_collection_uid(self.collection_string)
        cluster = Cluster(
            "couchbase://" + backup_base.master.ip,
            authenticator=PasswordAuthenticator(
            "Administrator",
            "password")
        )
        bucket = cluster.bucket(self.collection_string.bucket_name)
        scope = bucket.scope(self.collection_string.scope_name)
        collection = scope.collection(self.collection_string.collection_name)

        set_result = collection.mutate_in(self.key,
            [SD.upsert(self.path, self.value, xattr=True, create_parents=True)]
        )
        backup.set_subdoc(self.collection_string, self.key, self.path, self.value, Tag.XATTR_CHANGED)

        #SDK options utilising existing testrunner implementations
        #Not recommended, do not support all operations
        #bucketIn = backup_base.buckets[-1]
        #sdk_client = backup_base._get_python_sdk_client(backup_base.master.ip, bucketIn, backup_base.backupset.cluster_host)
        #client2 = SDKClient(bucket=self.collection_string.bucket_name, hosts=[backup_base.master.ip])
        #set_result = sdk_client.mutate_in(self.key,
        #                                  [SD.upsert(self.path, self.value, xattr=True, create_parents=True)],
        #                                  create_as_deleted=True,
        #                                  collection=self.collection_string.collection_name,
        #                                  scope=self.collection_string.scope_name
        #                                  )

class MergeBackupMutation:
    """ Merge backups """

    def apply(self, backup_base, backup):
        backup_base.backup_merge()

class CreateBucketMutation:
    """ Create a bucket """

    def __init__(self, name):
        self.name = name

    def apply(self, backup_base, backup):
        backup_base._create_buckets(backup_base.master, [self.name], bucket_size=backup_base.config['bucket_size'])
        backup.add_bucket(Bucket(self.name))


class CreateCollectionMutation:
    """ Create the a collection_string a collection represents """

    def __init__(self, collection_string):
        self.collection_string = CollectionString(collection_string)

    def apply(self, backup_base, backup):
        # TODO We can model this entire mutation as operations on a collections manifest
        rest_connection = RestConnection(backup_base.master)
        bucket, scope, collection = self.collection_string.bucket_name, self.collection_string.scope_name, self.collection_string.collection_name

        rest_connection.create_scope(bucket, scope)
        rest_connection.create_collection(bucket, scope, collection)

        uid = int(rest_connection.get_collection_uid(bucket, scope, collection), 16)
        backup.get_bucket(bucket).add_collection(Collection(self.collection_string, uid))


class SetDocumentMutation:
    """ Set a document in a collection given a key and value """

    def __init__(self, collection_string, key, value):
        self.collection_string, self.key, self.value = CollectionString(collection_string), key, value

    def apply(self, backup_base, backup):
        uid = backup.get_collection_uid(self.collection_string)
        backup_base.clients.get_client(self.collection_string.bucket_name).set(self.key, 0, 0, self.value, collection=uid)
        backup.set_document(self.collection_string, self.key, self.value)

class DeleteDocumentMutation:
    """ Delete a document from a collection given a key """

    def __init__(self, collection_string, key):
        self.collection_string, self.key = CollectionString(collection_string), key

    def apply(self, backup_base, backup):
        uid = backup.get_collection_uid(self.collection_string)
        backup_base.clients.get_client(self.collection_string.bucket_name).delete(self.key, collection=uid)
        backup.delete_document(self.collection_string, self.key)

class Clients:
    """ Gets you a MemcachedClient given a bucket """

    def __init__(self, server):
        self.server, self.clients = server, {}

    def get_client(self, bucket):
        if bucket not in self.clients:
            self.clients[bucket] = MemcachedClientHelper().direct_client(self.server, bucket)

        self.clients[bucket].hello(memcacheConstants.FEATURE_COLLECTIONS)

        return self.clients[bucket]

    def close(self):
        for client in self.clients:
            try:
                client.close()
            except Exception as e:
                print(f"Error closing client: {e}")

    def __del__(self):
        self.close()


class CollectionString:
    """ Manipulate a collection string """

    def __init__(self, collection_string):
        collection_string_split = collection_string.split('.')

        if len(collection_string_split) != 3:
            raise ValueError(f"{collection_string} is not a valid collection string")

        self.bucket_name, self.scope_name, self.collection_name = collection_string_split

    @property
    def collection_string(self):
        return f"{self.bucket_name}.{self.scope_name}.{self.collection_name}"

    def __str__(self):
        return self.collection_string
