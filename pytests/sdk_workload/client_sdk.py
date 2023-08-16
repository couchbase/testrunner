import logging
import time
import multiprocessing
import traceback

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.durability import ServerDurability, Durability
from couchbase.options import ClusterOptions, UpsertMultiOptions, GetMultiOptions, RemoveMultiOptions
from couchbase.options import TLSVerifyMode
from datetime import timedelta
from couchbase.exceptions import CouchbaseException, BucketNotFoundException, ScopeNotFoundException, CollectionNotFoundException,  AmbiguousTimeoutException, UnAmbiguousTimeoutException, RequestCanceledException
# from couchbase import enable_protocol_logger_to_save_network_traffic_to_file
# enable_protocol_logger_to_save_network_traffic_to_file('pytests/sdk_workload/sdk_logs.log')

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
        options = ClusterOptions(auth, tls_verify=TLSVerifyMode.NONE, log_redaction=True, dump_configuration=True)
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

    def delete_multi(self, collection, counter_obj, batch, number_of_retries, delay_time):
        retries = number_of_retries
        key_list = list(batch.keys())
        while retries > 0:
            retries_keys = list()
            result = collection.remove_multi(key_list, RemoveMultiOptions(timeout=timedelta(seconds=60)))
            for key in result.exceptions.keys():
                exception = result.exceptions[key]
                if exception.error_code == 2 or exception.error_code == 14 or exception.error_code == 15:
                    retries_keys.append(key)
                else:
                    counter_obj.increment(READ)
                    logging.critical("Delete document failed with exception: {}".format(exception))
            key_list = retries_keys
            retries = retries - 1
            if len(key_list) == 0:
                logging.info("Delete operation gets success after {} retries".format(number_of_retries - retries))
                break
            if retries == 0:
                counter_obj.increment(DELETE)
                logging.critical("Delete document failed with exception: {} after {} retries".format(result.exceptions, number_of_retries))
            time.sleep(delay_time)
            logging.info("Retries Delete doc due timeout failures......")

    def upsert_multi(self, collection, counter_obj, batch, number_of_retries, delay_time):
        key_doc_dict = batch
        retries = number_of_retries
        while retries > 0:
            retries_batch = dict()
            result = collection.upsert_multi(key_doc_dict, UpsertMultiOptions(timeout=timedelta(seconds=60)),
                                             durability=ServerDurability(Durability.PERSIST_TO_MAJORITY.value))
            for key in result.exceptions.keys():
                exception = result.exceptions[key]
                if exception.error_code == 2 or exception.error_code == 14  or exception.error_code == 15:
                    retries_batch[key] = key_doc_dict[key]
                else:
                    counter_obj.increment(UPDATE)
                    logging.critical("Update document failed with exception: {}".format(exception))
            key_doc_dict = retries_batch
            retries = retries - 1
            if len(key_doc_dict.keys()) == 0:
                logging.info("Upsert operation gets success after {} retries".format(number_of_retries - retries))
                break
            if retries == 0:
                counter_obj.increment(UPDATE)
                logging.critical("Update document failed with exception: {} after {} retries".format(result.exceptions, number_of_retries))
            time.sleep(delay_time)
            logging.info("Retries upsert doc due timeout failures......")

    def read_multi(self, collection, counter_obj, batch, number_of_retries, delay_time):
        retries = number_of_retries
        key_list = list(batch.keys())
        while retries > 0:
            retries_keys = list()
            result = collection.get_multi(key_list, GetMultiOptions(timeout=timedelta(seconds=60)))
            for key in result.exceptions.keys():
                exception = result.exceptions[key]
                if exception.error_code == 2 or exception.error_code == 14  or exception.error_code == 15:
                    retries_keys.append(key)
                else:
                    counter_obj.increment(READ)
                    logging.critical("Read document failed with exception: {}".format(exception))
            key_list = retries_keys
            retries = retries - 1
            if len(key_list) == 0:
                logging.info("Read operation gets success after {} retries".format(number_of_retries-retries))
                break
            if retries == 0:
                counter_obj.increment(READ)
                logging.critical("Read document failed with exception: {} after retries {}".format(result.exceptions, number_of_retries))
            time.sleep(delay_time)
            logging.info("Retries read doc due timeout failures......")