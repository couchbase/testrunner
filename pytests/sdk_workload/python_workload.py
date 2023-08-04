import logging
import unittest
import couchbase
import json
import threading
import time
from TestInput import TestInputSingleton

from lib.capella.utils import CapellaAPI, CapellaCredentials

from couchbase.exceptions import BucketNotFoundException, ScopeNotFoundException, CollectionNotFoundException
from lib.couchbase_helper.documentgenerator import BatchedDocumentGenerator
from pytests.serverless.dapi.dapi_helper import doc_generator
from pytests.sdk_workload.client_sdk import SDKClient
from pytests.sdk_workload.client_sdk import SDKCounter

failed_with_errors = dict()
CREATE, UPDATE, READ, DELETE = "create", "delete", "read", "update"


def crud_thread(collection, key_prefix, number_of_docs, run, counter_obj, logger):
    doc_gen = doc_generator(key_prefix, 12, 1024, number_of_docs)
    batched_gen_obj = BatchedDocumentGenerator(doc_gen,1000)
    sdk_client = SDKClient()
    # logging.info("key_prefix: {}, run: {} for collection: {}".format(key_prefix, run, collection.name))
    while batched_gen_obj.has_next():
        current_batch = batched_gen_obj.next_batch()
        logging.info("Inserting {} in collection: {}".format(number_of_docs, collection.name))
        key_doc_dict = dict()
        for key in current_batch:
            doc = current_batch[key]
            new_doc = json.loads(doc)
            new_doc['upsert'] = True
            key_doc_dict[key] = new_doc
        result = sdk_client.insert_multi(collection, counter_obj, key_doc_dict, logger)
        if result:
            logging.info("Upserting {} in collection: {}".format(number_of_docs, collection.name))
            sdk_client.upsert_multi(collection, counter_obj, current_batch, logger)
            logging.info("Reading {} in collection: {}".format(number_of_docs, collection.name))
            sdk_client.read_multi(collection, counter_obj, current_batch, logger)


def mutate_doc_thread(collection, counter_obj, logger, number_of_docs):
    num_of_docs_per_thread, number_of_threads = number_of_docs, 10
    threading_list,key = list(), "key"
    run = 0
    for i in range(number_of_threads):
        key = time.time()
        key = int(key)
        key = str(key)
        thread = threading.Thread(target=crud_thread, args=(collection, key, num_of_docs_per_thread, run, counter_obj, logger))
        threading_list.append(thread)
        time.sleep(1)
        run = run + 1

    for thread in threading_list:
        thread.start()

    for thread in threading_list:
        thread.join()


def start_workload(collection_obj_list, logger, number_of_dos):
    thread_list = list()
    counter_obj = SDKCounter()
    logging.info("Starting {} number of threads to start the workload".format(len(collection_obj_list)))

    logging.info("Running SDK Workload")
    for collection in collection_obj_list:
        thread = threading.Thread(target=mutate_doc_thread, args=(collection,counter_obj, logger, number_of_dos))
        thread_list.append(thread)

    for thread in thread_list:
        thread.start()

    for thread in thread_list:
        thread.join()

    print("Create failures: {}".format(counter_obj.value(CREATE)))
    print("Upsert failures: {}".format(counter_obj.value(UPDATE)))
    print("Read failures: {}".format(counter_obj.value(READ)))
    print("Delete failures: {}".format(counter_obj.value(DELETE)))


class PythonSdkWorkload(unittest.TestCase):
    def setUp(self):
        self.input = TestInputSingleton.input
        print("We are in base test case setup")
        capella_credentials = CapellaCredentials(self.input.capella)
        self.capella_api = CapellaAPI(capella_credentials)
        self.cluster_id = self.input.capella.get('cluster_id', None)
        self.bucket_list = self.input.capella.get("bucket_list",
                                                  '{"bucket-1":{"saurabh": ["col-1", "col-2", "col-3", "col-4", "col-5"]},"bucket-2": {"saurabh": ["col-1", "col-2", "col-3", "col-4", "col-5"]}}')
        self.user_name = self.input.param("username", "RunWorkloadUser-1")
        self.user_pass = self.input.param("password", "RunWorkloadUser@123")
        self.duration = self.input.capella.get("duration", 3600)
        self.bucket_list = json.loads(self.bucket_list)
        self.log_level = self.input.param("log_level", None)
        self.sdk_logger = self.get_sdk_logger()
        self.number_of_docs_per_collection = self.input.param("num_of_docs_per_coll", 10000)
        self.sdk_client = SDKClient()

    def allow_current_ip(self):
        try:
            _ = self.capella_api.allow_my_ip(self.cluster_id,self.user_name,self.user_pass)
        except Exception as e:
            logging.info("Exception while allow current IP: {}".format(e))

    def get_sdk_logger(self):
        filehandler = logging.FileHandler('pytests/sdk_workload/sdk_logs.log', 'a')
        formatter = logging.Formatter('%(levelname)s::%(asctime)s::%(message)s')
        filehandler.setFormatter(formatter)
        sdk_logger = logging.getLogger('sdk_log')
        for hdlr in sdk_logger.handlers[:]:  # remove the existing file handlers
            if isinstance(hdlr, logging.FileHandler):
                sdk_logger.removeHandler(hdlr)
        sdk_logger.addHandler(filehandler)

        if self.log_level == "debug":
            sdk_logger.setLevel(logging.DEBUG)
        else:
            sdk_logger.setLevel(logging.INFO)
        couchbase.configure_logging(sdk_logger.name, level=sdk_logger.level)
        return sdk_logger

    def create_db_user(self):
        logging.info("Creating DB user")
        try:
            _ = self.capella_api.create_db_user(self.cluster_id,self.user_name,self.user_pass)
        except Exception as e:
            logging.info("Exception while creating user: {}".format(e))


    def flush_bucket(self, bucket_name):
        bucket_id = self.capella_api.get_bucket_id(self.cluster_id, bucket_name)
        resp = self.capella_api.flush_bucket(self.cluster_id, bucket_id)
        return resp

    def run_workload(self):
        logging.info("Step-1----------------Creating DB user")
        self.create_db_user()
        logging.info("Step-2----------------Allowing current IP")
        resp = self.allow_current_ip()
        logging.info("response for allow current ip: {}".format(resp))
        logging.info("Step-3----------------Creating SDK client")
        self.cluster_srv = self.input.capella.get("connection_string", None)
        self.cluster = self.sdk_client.sdk_client_pool(self.cluster_srv, self.user_name, self.user_pass)
        logging.info("Step-4----------------Running workload for duration: {}".format(self.duration))
        collection_obj_list = list()
        for bucket_name in self.bucket_list.keys():
            bucket = self.sdk_client.get_bucket(self.cluster, bucket_name)
            if bucket == BucketNotFoundException.error_code:
                logging.critical("Bucket does not exists")
                break
            else:
                scope_dict = self.bucket_list[bucket_name]
                for scope_name in scope_dict.keys():
                    scope = self.sdk_client.get_scope(bucket, scope_name)
                    if scope == ScopeNotFoundException.error_code:
                        logging.critical("Scope does not exists")
                        break
                    else:
                        collection_list = scope_dict[scope_name]
                        for collection_name in collection_list:
                            logging.info("bucket: {}, scope: {}, collection: {}".format(bucket_name, scope_name, collection_name))
                            collection = self.sdk_client.get_collection(scope, collection_name)
                            if collection == CollectionNotFoundException.error_code:
                                logging.critical("Collection does exists")
                                break
                            else:
                                collection_obj_list.append(collection)

        start_workload(collection_obj_list, self.sdk_logger, self.number_of_docs_per_collection)
        logging.info("Step-5----------------Flush buckets".format(self.duration))
        for bucket_name in self.bucket_list.keys():
            resp = self.flush_bucket(bucket_name)
            if resp.status_code != 200:
                raise Exception("Response: {} when trying to flush bucket: {}".format(resp.status_code, bucket_name))
