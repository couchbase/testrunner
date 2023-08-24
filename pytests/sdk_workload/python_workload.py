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
from pytests.sdk_workload.query_workload import RunQueryWorkload

failed_with_errors = dict()
CREATE, UPDATE, READ, DELETE, QUERY = "create", "delete", "read", "update", "query"


def crud_thread(collection, key_prefix, number_of_docs, run, counter_obj, number_of_retries, duration, delay_time):
    doc_gen = doc_generator(key_prefix, 12, 1024, number_of_docs)
    sdk_client = SDKClient()
    t_end = time.time() + int(duration)
    revert_value = True
    while time.time() < t_end:
        batched_gen_obj = BatchedDocumentGenerator(doc_gen, 1000)
        while batched_gen_obj.has_next():
            current_batch = batched_gen_obj.next_batch()
            key_doc_dict = dict()
            for key in current_batch:
                doc = current_batch[key]
                new_doc = json.loads(doc)
                new_doc['upsert'] = revert_value
                revert_value != revert_value
                key_doc_dict[key] = new_doc

            logging.info("Upserting {} in collection: {}".format(number_of_docs, collection.name))
            sdk_client.upsert_multi(collection, counter_obj, key_doc_dict, number_of_retries, delay_time)
            logging.info("Reading {} in collection: {}".format(number_of_docs, collection.name))
            sdk_client.read_multi(collection, counter_obj, key_doc_dict, number_of_retries, delay_time)
        doc_gen.reset()

    doc_gen.reset()
    batched_gen_obj = BatchedDocumentGenerator(doc_gen, 1000)
    while batched_gen_obj.has_next():
        current_batch = batched_gen_obj.next_batch()
        logging.info("Deleting {} in collection: {}".format(number_of_docs, collection.name))
        sdk_client.delete_multi(collection, counter_obj, current_batch, number_of_retries, delay_time)


def mutate_doc_thread(collection, counter_obj, number_of_docs, number_of_threads, number_of_retries, duration, delay_time):
    num_of_docs_per_thread = number_of_docs
    threading_list,key = list(), "key"
    run = 0
    for i in range(number_of_threads):
        key = time.time()
        key = int(key)
        key = str(key)
        thread = threading.Thread(target=crud_thread, args=(collection, key, num_of_docs_per_thread, run, counter_obj, number_of_retries, duration, delay_time))
        threading_list.append(thread)
        time.sleep(1)
        run = run + 1

    for thread in threading_list:
        thread.start()

    for thread in threading_list:
        thread.join()


def run_query_thread(cluster, bucket_name, scope_name, collection_name, duration, counter_obj):
    logging.info("Running Query workload on {}-{}-{}".format(bucket_name, scope_name, collection_name))
    t_end = time.time() + int(duration)
    query_workload_obj = RunQueryWorkload()
    query_workload_obj.build_indexes(cluster, bucket_name, scope_name, collection_name, counter_obj)
    while time.time() < t_end:
        query_workload_obj.run_query(cluster, bucket_name, scope_name, collection_name, counter_obj)


def start_workload(workload_type, collection_obj_list, query_run_list, number_of_dos, number_of_threads, number_of_retries,
                   duration, delay_time):
    thread_list = list()
    counter_obj = SDKCounter()
    logging.info("Starting {} number of threads to start the workload".format(len(collection_obj_list)))

    logging.info("Running SDK Workload")
    for i in range(len(collection_obj_list)):
        if 'kv' in workload_type:
            collection = collection_obj_list[i]
            thread1 = threading.Thread(target=mutate_doc_thread, args=(collection, counter_obj, number_of_dos, number_of_threads, number_of_retries, duration, delay_time))
            thread_list.append(thread1)
        if 'query' in workload_type:
            param_dict = query_run_list[i]
            thread2 = threading.Thread(target=run_query_thread, args=(param_dict['cluster'], param_dict['bucket'], param_dict['scope'], param_dict['collection'], duration, counter_obj))
            thread_list.append(thread2)

    for thread in thread_list:
        thread.start()

    for thread in thread_list:
        thread.join()

    if 'kv' in workload_type:
        logging.info("Create failures: {}".format(counter_obj.value(CREATE)))
        logging.info("Upsert failures: {}".format(counter_obj.value(UPDATE)))
        logging.info("Read failures: {}".format(counter_obj.value(READ)))
        logging.info("Delete failures: {}".format(counter_obj.value(DELETE)))
    if 'query' in workload_type:
        logging.info("Query failures: {}".format(counter_obj.value(QUERY)))


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
        self.number_of_threads = self.input.param("number_of_threads", 10)
        self.number_of_retries = self.input.param("number_of_retries", 100)
        self.log_level = self.input.param("log_level", None)
        self.sdk_logger = self.get_sdk_logger()
        self.number_of_docs_per_collection = self.input.param("num_of_docs_per_coll", 10000)
        self.delay_time = self.input.param("delay_time", 0.1)
        self.workload_type = self.input.param("workload_type", ['kv'])
        self.sdk_client = SDKClient()

    def allow_current_ip(self):
        try:
            _ = self.capella_api.allow_my_ip(self.cluster_id)
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
        query_run_list = list()
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
                            collection = self.sdk_client.get_collection(scope, collection_name)
                            collection.insert_multi
                            if collection == CollectionNotFoundException.error_code:
                                logging.critical("Collection does exists")
                                break
                            else:
                                collection_obj_list.append(collection)
                                query_run_list.append({'cluster': self.cluster,
                                                       'bucket': bucket_name, 'scope': scope_name,
                                                       'collection': collection_name})

        start_workload(self.workload_type, collection_obj_list, query_run_list, self.number_of_docs_per_collection,
                       self.number_of_threads, self.number_of_retries, self.duration, self.delay_time)