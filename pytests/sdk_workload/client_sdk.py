import logging
import time
import multiprocessing
import traceback

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.durability import ServerDurability, Durability
from couchbase.options import ClusterOptions, InsertMultiOptions, UpsertMultiOptions, GetMultiOptions
from couchbase.options import TLSVerifyMode
from datetime import timedelta
from couchbase.exceptions import CouchbaseException, BucketNotFoundException, ScopeNotFoundException, CollectionNotFoundException,  AmbiguousTimeoutException, UnAmbiguousTimeoutException

CREATE, UPDATE, READ, DELETE = "create", "delete", "read", "update"

class SDKCounter(object):
    def __init__(self, initval=0):
        self.create_failure = multiprocessing.RawValue('i', initval)
        self.read_failure = multiprocessing.RawValue('i', initval)
        self.delete_failure = multiprocessing.RawValue('i', initval)
        self.update_failure = multiprocessing.RawValue('i', initval)
        self.lock = multiprocessing.Lock()

    def increment(self, method):
        """ increment in increases the counter for a object
        with method options - like create, delete, read, update"""
        if method == CREATE:
            with self.lock:
                self.create_failure.value += 1

        if method == DELETE:
            with self.lock:
                self.delete_failure.value += 1

        if method == READ:
            with self.lock:
                self.read_failure.value += 1

        if method == UPDATE:
            with self.lock:
                self.update_failure.value += 1

    def value(self, method):
        """ value gives current value of counter for methods like - like create, delete, read, update"""
        if method == CREATE:
            return self.create_failure.value

        if method == DELETE:
            return self.delete_failure.value

        if method == READ:
            return self.read_failure.value

        if method == UPDATE:
            return self.update_failure.value


class SDKClient(object):
    def __int__(self):
        pass

    def sdk_client_pool(self, cluster_srv, user_name, password):
        auth = PasswordAuthenticator(user_name, password)
        options = ClusterOptions(auth, tls_verify=TLSVerifyMode.NONE, log_redaction=True)
        endpoint = cluster_srv
        logging.info("endpoint is : {}".format(endpoint))
        retries = 5
        while retries > 0:
            try:
                cluster = Cluster.connect(endpoint, options)
                cluster.wait_until_ready(timedelta(seconds=5))
                break
            except CouchbaseException:
                logging.info("Retrying for SDK connection after 15 seconds.......")
                retries = retries - 1
                time.sleep(15)

        return cluster

    def get_bucket(self, cluster, bucket_name):
        try:
            bucket = cluster.bucket(bucket_name)
        except BucketNotFoundException:
            return BucketNotFoundException.error_code
        return bucket

    def get_scope(self, bucket, scope_name):
        try:
            scope = bucket.scope(scope_name)
        except ScopeNotFoundException:
            return ScopeNotFoundException.error_code
        return scope

    def get_collection(self, scope, collection_name):
        try:
            collection = scope.collection(collection_name)
        except CollectionNotFoundException:
            return CollectionNotFoundException.error_code
        return collection

    def insert_multi(self, collection, counter_obj, batch, logger):
        key_doc_dict = batch
        retries = 5
        while retries > 0:
            retries_batch = dict()
            result = collection.insert_multi(key_doc_dict, InsertMultiOptions(timeout=timedelta(seconds=60)))
            if not result.all_ok:
                logger.error(traceback.format_exc())
            for key in result.exceptions.keys():
                exception = result.exceptions[key]
                if exception == AmbiguousTimeoutException or exception == UnAmbiguousTimeoutException:
                    retries_batch[key] = key_doc_dict[key]
                else:
                    counter_obj.increment(CREATE)
                    logging.critical("Create document failed with exception: {}".format(exception))
            key_doc_dict = retries_batch
            if len(key_doc_dict.keys()) == 0:
                break
            logging.info("Retries insert doc due ambiguous timeout failures......")
        return True

    def upsert_multi(self, collection, counter_obj, batch, logger):
        key_doc_dict = batch
        retries = 5
        while retries > 0:
            retries_batch = dict()
            result = collection.upsert_multi(key_doc_dict, UpsertMultiOptions(timeout=timedelta(seconds=60)),
                                             durability=ServerDurability(Durability.PERSIST_TO_MAJORITY.value))
            if not result.all_ok:
                logger.error(traceback.format_exc())
            for key in result.exceptions.keys():
                exception = result.exceptions[key]
                if exception == AmbiguousTimeoutException or exception == UnAmbiguousTimeoutException:
                    retries_batch[key] = key_doc_dict[key]
                else:
                    counter_obj.increment(UPDATE)
                    logging.critical("Update document failed with exception: {}".format(exception))
            key_doc_dict = retries_batch
            if len(key_doc_dict.keys()) == 0:
                break
            logging.info("Retries upsert doc due ambiguous timeout failures......")

    def read_multi(self, collection, counter_obj, batch, logger):
        retries = 5
        key_list = list(batch.keys())
        while retries > 0:
            retries_keys = list()
            result = collection.get_multi(key_list, GetMultiOptions(timeout=timedelta(seconds=60)))
            if not result.all_ok:
                logger.error(traceback.format_exc())
            for key in result.exceptions.keys():
                exception = result.exceptions[key]
                if exception == AmbiguousTimeoutException or exception == UnAmbiguousTimeoutException:
                    retries_keys.append(key)
                else:
                    counter_obj.increment(READ)
                    logging.critical("Read document failed with exception: {}".format(exception))
            key_list = retries_keys
            if len(key_list) == 0:
                break
            logging.info("Retries read doc due ambiguous timeout failures......")