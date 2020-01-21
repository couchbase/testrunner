import queue
import copy
import json
import threading
from multiprocessing import Process

import couchbase.subdocument as SD
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import VBucketAwareMemcached

from lib.couchbase_helper.random_gen import RandomDataGenerator
from lib.couchbase_helper.subdoc_helper import SubdocHelper
from .subdoc_base import SubdocBaseTest


class SubdocAutoTestGenerator(SubdocBaseTest):
    def setUp(self):
        super(SubdocAutoTestGenerator, self).setUp()
        self.prepopulate_data = self.input.param("prepopulate_data", False)
        self.verify_data_without_paths = self.input.param("verify_data_without_paths", True)
        self.number_of_arrays = self.input.param("number_of_arrays", 1)
        self.verbose_func_usage = self.input.param("verbose_func_usage", False)
        self.nesting_level = self.input.param("nesting_level", 0)
        self.mutation_operation_type = self.input.param("mutation_operation_type", "any")
        self.force_operation_type = self.input.param("force_operation_type", None)
        self.run_data_verification = self.input.param("run_data_verification", True)
        self.prepopulate_item_count = self.input.param("prepopulate_item_count", 10000)
        self.seed = self.input.param("seed", 0)
        self.run_mutation_mode = self.input.param("run_mutation_mode", "seq")
        self.client = self.direct_client(self.master, self.buckets[0])
        self.build_kv_store = self.input.param("build_kv_store", False)
        self.total_writer_threads = self.input.param("total_writer_threads", 10)
        self.number_of_documents = self.input.param("number_of_documents", 10)
        self.concurrent_threads = self.input.param("concurrent_threads", 10)
        self.randomDataGenerator = RandomDataGenerator()
        self.subdoc_gen_helper = SubdocHelper()
        self.kv_store = {}
        self.load_thread_list = []
        if self.prepopulate_data:
            self.run_sync_data()

    def tearDown(self):
        super(SubdocAutoTestGenerator, self).tearDown()

    def test_readonly(self):
        self.client = self.direct_client(self.master, self.buckets[0])
        error_result = {}
        data_set = self.generate_json_for_nesting()
        base_json = self.generate_json_for_nesting()
        json_document = self.generate_nested(base_json, data_set, self.nesting_level)
        data_key = "test_readonly"
        self.set(self.client, data_key, json_document)
        pairs = {}
        self.subdoc_gen_helper.find_pairs(json_document, "", pairs)
        for path in list(pairs.keys()):
            data = self.get(self.client, key=data_key, path=path)
            if data != pairs[path]:
                error_result[path] = "expected {0}, actual = {1}".format(pairs[path], data)
        self.assertTrue(len(error_result) == 0, error_result)

    def test_exists(self):
        self.client = self.direct_client(self.master, self.buckets[0])
        error_result = {}
        data_set = self.generate_json_for_nesting()
        base_json = self.generate_json_for_nesting()
        json_document = self.generate_nested(base_json, data_set, self.nesting_level)
        data_key = "test_readonly"
        self.set(self.client, data_key, json_document)
        pairs = {}
        self.subdoc_gen_helper.find_pairs(json_document, "", pairs)
        for path in list(pairs.keys()):
            try:
                self.exists(self.client, data_key, path, xattr=self.xattr)
            except Exception as ex:
                error_result[path] = str(ex)
        self.assertTrue(len(error_result) == 0, error_result)

    def test_seq_mutations_dict(self):
        self.mutation_operation_type = "dict"
        self.test_seq_mutations()

    def test_seq_mutations_array(self):
        self.mutation_operation_type = "array"
        self.test_seq_mutations()

    def test_multi_seq_mutations(self):
        self.verify_result = self.input.param("verify_result", False)
        queue = queue.Queue()
        number_of_times = (self.number_of_documents // self.concurrent_threads)
        process_list = []
        number_of_buckets = len(self.buckets)
        for x in range(self.concurrent_threads):
            base_json = self.generate_json_for_nesting()
            data_set = self.generate_nested(base_json, base_json, 2)
            json_document = self.generate_nested(base_json, data_set, 10)
            bucket_number = x % number_of_buckets
            prefix = self.buckets[bucket_number].name + "_" + str(x) + "_"
            p = Process(target=self.test_seq_mutations,
                        args=(queue, number_of_times, prefix, json_document, self.buckets[bucket_number]))
            p.start()
            process_list.append(p)
        for p in process_list:
            p.join()
        if self.verify_result:
            filename = "/tmp/" + self.randomDataGenerator.random_uuid() + "_dump_failure.txt"
            queue_size = queue.qsize()
            if not queue.empty():
                self._dump_data(filename, queue)
            self.assertTrue(queue_size == 0, "number of failures {0}, check file {1}".format(queue.qsize(), filename))

    def test_seq_mutations(self, queue, number_of_times, prefix, json_document, bucket):
        client = self.direct_client(self.master, bucket)
        for x in range(number_of_times):
            self.number_of_operations = self.input.param("number_of_operations", 50)
            data_key = prefix + self.randomDataGenerator.random_uuid()
            self.set(client, data_key, json_document)
            operations = self.subdoc_gen_helper.build_sequence_operations(json_document, self.number_of_operations,
                                                                          seed=self.seed,
                                                                          mutation_operation_type=self.mutation_operation_type,
                                                                          force_operation_type=self.force_operation_type)
            for operation in operations:
                function = getattr(self, operation["subdoc_api_function_applied"])
                try:
                    data_value = operation["data_value"]
                    if not self.use_sdk_client:
                        data_value = json.dumps(data_value)
                    function(client, data_key, operation["new_path_impacted_after_mutation_operation"], data_value)
                except Exception as ex:
                    queue.put("bucket {0}, error {1}".format(bucket.name, str(ex)))
            data = self.get_all(client, data_key)
            error_result = None
            if data != json_document:
                error_result = "bucket {0}, expected {1}, actual = {2}".format(bucket.name, json_document, data)
            if error_result != None:
                queue.put(error_result)

    def test_concurrent_mutations_dict(self):
        self.mutation_operation_type = "dict"
        self.test_concurrent_mutations()

    def test_concurrent_mutations_array(self):
        self.mutation_operation_type = "array"
        self.test_concurrent_mutations()

    def test_concurrent_mutations(self):
        randomDataGenerator = RandomDataGenerator()
        randomDataGenerator.set_seed(self.seed)
        base_json = randomDataGenerator.random_json()
        data_set = randomDataGenerator.random_json()
        json_document = self.generate_nested(base_json, data_set, self.nesting_level)
        data_key = "test_concurrent_mutations"
        self.run_mutation_concurrent_operations(self.buckets[0], data_key, json_document)

    def run_mutation_concurrent_operations(self, bucket=None, document_key="", json_document={}):
        client = self.direct_client(self.master, bucket)
        self.number_of_operations = self.input.param("number_of_operations", 10)
        # INSERT INTO  COUCHBASE
        self.set(client, document_key, json_document)
        # RUN PARALLEL OPERATIONS
        operations = self.subdoc_gen_helper.build_concurrent_operations(
            json_document, self.number_of_operations, seed=self.seed,
            mutation_operation_type=self.mutation_operation_type,
            force_operation_type=self.force_operation_type)
        # RUN CONCURRENT THREADS
        thread_list = []
        result_queue = queue.Queue()
        self.log.info(" number of operations {0}".format(len(operations)))
        for operation in operations:
            client = self.direct_client(self.master, bucket)
            t = Process(target=self.run_mutation_operation, args=(client, document_key, operation, result_queue))
            t.start()
            thread_list.append(t)
        for t in thread_list:
            t.join()
        queue_data = []
        while not result_queue.empty():
            queue_data.append(result_queue.get())
        self.assertTrue(len(queue_data) == 0, queue_data)
        # CHECK RESULT IN THE END
        json_document = copy.deepcopy(operations[len(operations) - 1]["mutated_data_set"])
        pairs = {}
        error_result = {}
        self.subdoc_gen_helper.find_pairs(json_document, "", pairs)
        for path in list(pairs.keys()):
            data = self.get(client, document_key, path)
            if data != pairs[path]:
                error_result[path] = "expected {0}, actual = {1}".format(pairs[path], data)
        self.assertTrue(len(error_result) == 0, error_result)

    def run_mutation_operation(self, client, document_key, operation, result_queue):
        function = getattr(self, operation["subdoc_api_function_applied"])
        try:
            data_value = operation["data_value"]
            if not self.use_sdk_client:
                data_value = json.dumps(data_value)
            function(client, document_key, operation["new_path_impacted_after_mutation_operation"], data_value)
        except Exception as ex:
            self.log.info(str(ex))
            result_queue.put({"error": str(ex), "operation_type": operation["subdoc_api_function_applied"]})

    ''' Generic Test case for running sequence operations based tests '''

    def run_mutation_operations_for_situational_tests(self):
        self.run_load_during_mutations = self.input.param("run_load_during_mutations", False)
        self.number_of_documents = self.input.param("number_of_documents", 10)
        self.number_of_operations = self.input.param("number_of_operations", 10)
        self.concurrent_threads = self.input.param("concurrent_threads", 10)
        error_queue = queue.Queue()
        document_info_queue = queue.Queue()
        thread_list = []
        # RUN INPUT FILE READ THREAD
        document_push = threading.Thread(target=self.push_document_info,
                                         args=(self.number_of_documents, document_info_queue))
        document_push.start()
        # RUN WORKER THREADS
        for x in range(self.concurrent_threads):
            t = Process(target=self.worker_operation_run, args=(
            document_info_queue, error_queue, self.buckets[0], self.mutation_operation_type, self.force_operation_type))
            t.start()
            thread_list.append(t)
        for t in thread_list:
            t.join()
        # ERROR ANALYSIS
        error_msg = ""
        error_count = 0
        if not error_queue.empty():
            # Dump Re-run file
            dump_file = open('/tmp/dump_failure.txt', 'wb')
            while not error_queue.empty():
                error_count += 1
                error_data = error_queue.get()
                dump_file.write(json.dumps(error_data["error_result"]))
            dump_file.close()
            # Fail the test with result count
            self.assertTrue(error_count == 0, "error count {0}".format(error_count))

    ''' Generic Test case for running sequence operations based tests '''

    def test_mutation_operations(self):
        self.run_load_during_mutations = self.input.param("run_load_during_mutations", False)
        self.number_of_documents = self.input.param("number_of_documents", 10)
        self.number_of_operations = self.input.param("number_of_operations", 10)
        self.concurrent_threads = self.input.param("concurrent_threads", 10)
        error_queue = queue.Queue()
        document_info_queue = queue.Queue()
        thread_list = []
        # RUN INPUT FILE READ THREAD
        document_push = threading.Thread(target=self.push_document_info,
                                         args=(self.number_of_documents, document_info_queue))
        document_push.start()
        document_push.join()
        self.sleep(2)
        # RUN WORKER THREADS
        for bucket in self.buckets:
            for x in range(self.concurrent_threads * len(self.buckets)):
                # use thread instead of process because Process did not return an updated error queue
                t = threading.Thread(target=self.worker_operation_run, args=(
                document_info_queue, error_queue, bucket, self.mutation_operation_type, self.force_operation_type))
                t.start()
                thread_list.append(t)
        if self.run_load_during_mutations:
            self.run_async_data()
        for t in thread_list:
            t.join()
        for t in self.load_thread_list:
            if t.is_alive():
                if t is not None:
                    t.signal = False
        # ERROR ANALYSIS
        queue_size = error_queue.qsize()
        filename = '/tmp/dump_failure_{0}.txt'.format(self.randomDataGenerator.random_uuid())
        if not error_queue.empty():
            self._dump_data(filename, error_queue)
        self.assertTrue(queue_size == 0, "number of failures {0}, check file {1}".format(error_queue.qsize(), filename))

    ''' Generate Sample data for testing '''

    def push_document_info(self, number_of_documents, document_info_queue):
        for x in range(number_of_documents):
            document_info = {}
            randomDataGenerator = RandomDataGenerator()
            randomDataGenerator.set_seed(self.seed)
            document_info["document_key"] = self.randomDataGenerator.random_uuid() + "_key_" + str(x)
            document_info["seed"] = randomDataGenerator.random_int()
            base_json = randomDataGenerator.random_json(random_array_count=self.number_of_arrays)
            data_set = randomDataGenerator.random_json(random_array_count=self.number_of_arrays)
            json_document = self.generate_nested(base_json, data_set, self.nesting_level)
            document_info["json_document"] = json_document
            document_info_queue.put(document_info)

    ''' Worker for sequence operations on JSON '''

    def worker_operation_run(self,
                             queue,
                             error_queue,
                             bucket,
                             mutation_operation_type="any",
                             force_operation_type=None):
        client = self.direct_client(self.master, bucket)
        while not queue.empty():
            document_info = queue.get()
            document_key = document_info["document_key"]
            json_document = document_info["json_document"]
            seed = document_info["seed"]
            logic, error_result = self.run_mutation_operations(client, bucket,
                                                               document_key=document_key, json_document=json_document,
                                                               seed=seed,
                                                               number_of_operations=self.number_of_operations,
                                                               mutation_operation_type=mutation_operation_type,
                                                               force_operation_type=force_operation_type)
            if not logic:
                error_queue.put({"error_result": error_result, "seed": seed})

    ''' Method to run sequence operations for a given JSON document '''

    def run_mutation_operations(self,
                                client,
                                bucket,
                                document_key="document_key",
                                json_document={},
                                seed=0,
                                number_of_operations=10,
                                mutation_operation_type="any",
                                force_operation_type=None):
        original_json_copy = copy.deepcopy(json_document)
        self.set(client, document_key, json_document)
        self.log.info(" Test ON KEY :: {0}".format(document_key))
        if self.run_mutation_mode == "seq":
            operations = self.subdoc_gen_helper.build_sequence_operations(
                json_document,
                max_number_operations=number_of_operations,
                seed=seed,
                mutation_operation_type=mutation_operation_type,
                force_operation_type=force_operation_type)
            # self.log.info("TOTAL OPERATIONS CALCULATED {0} ".format(len(operations)))
            operation_index = 1
            for operation in operations:
                function = getattr(self, operation["subdoc_api_function_applied"])
                try:
                    data_value = operation["data_value"]
                    if not self.use_sdk_client:
                        data_value = json.dumps(operation["data_value"])
                    function(client, document_key, operation["new_path_impacted_after_mutation_operation"], data_value)
                    operation_index += 1
                except Exception as ex:
                    return False, operation
        else:
            logic, result = self.run_concurrent_mutation_operations(document_key, bucket, seed, json_document,
                                                                    number_of_operations, mutation_operation_type,
                                                                    force_operation_type)
            if not logic:
                return False, logic
        # self.log.info(" END WORKING ON {0}".format(document_key))
        # json_document = operations[len(operations)-1]["mutated_data_set"]
        if self.build_kv_store:
            self.kv_store[document_key] = operations[len(operations) - 1]["mutated_data_set"]
        error_result = {}
        if self.run_data_verification:
            if self.verify_data_without_paths:
                data = self.get_all(client, document_key)
                if data != json_document:
                    error_result = "expected {0}, actual = {1}".format(json_document, data)
                    return False, error_result
            else:
                pairs = {}
                self.subdoc_gen_helper.find_pairs(json_document, "", pairs)
                for path in list(pairs.keys()):
                    # self.log.info(" Analyzing path {0}".format(path))
                    check_data = True
                    try:
                        data = self.get(client, document_key, path)
                    except Exception as ex:
                        check_data = False
                        error_result[path] = "expected {0}, actual = {1}".format(pairs[path], str(ex))
                        self.print_operations(operations)
                        self.log.info("_______________________________________________")
                        self.log.info(" path in question {0} ".format(path))
                        self.log.info("ORIGINAL {0} ".format(original_json_copy))
                        self.log.info("EXPECTED {0} ".format(json_document))
                        self.log.info("ACTUAL {0} ".format(self.get_all(client, document_key)))
                        self.log.info("_______________________________________________")
                        return False, error_result
                    if check_data and data != pairs[path]:
                        error_result[path] = "expected {0}, actual = {1}".format(pairs[path], data)
                if len(error_result) != 0:
                    return False, error_result
        return True, error_result

    def print_operations(self, operations):
        index = 0
        for operation in operations:
            index += 1
            self.log.info(" +++++++++++++++++++++++++ mutation # {0} +++++++++++++++++++++++++ ".format(index))
            for k, v in operation.items():
                self.log.info("{0} :: {1}".format(k, v))

    def run_concurrent_mutation_operations(self, document_key, bucket, seed, json_document, number_of_operations,
                                           mutation_operation_type, force_operation_type):
        result_queue = queue.Queue()
        operations = self.subdoc_gen_helper.build_concurrent_operations(
            json_document,
            max_number_operations=number_of_operations,
            seed=seed,
            mutation_operation_type=mutation_operation_type,
            force_operation_type=force_operation_type)
        self.log.info("TOTAL OPERATIONS CALCULATED {0} ".format(len(operations)))
        thread_list = []
        for operation in operations:
            client = self.direct_client(self.master, bucket)
            t = Process(target=self.run_mutation_operation, args=(client, document_key, operation, result_queue))
            t.start()
            thread_list.append(t)
        for t in thread_list:
            t.join()
        if result_queue.empty():
            return True, None
        return False, result_queue

    def run_sync_data(self):
        self.load_thread_list = []
        randomDataGenerator = RandomDataGenerator()
        randomDataGenerator.set_seed(self.seed)
        base_json = randomDataGenerator.random_json(random_array_count=self.number_of_arrays)
        data_set = randomDataGenerator.random_json(random_array_count=self.number_of_arrays)
        json_document = self.generate_nested(base_json, data_set, self.nesting_level)
        if self.prepopulate_data:
            self.load_thread_list = []
            for bucket in self.buckets:
                for x in range(self.total_writer_threads):
                    client = VBucketAwareMemcached(RestConnection(self.master), bucket)
                    t = Process(target=self.run_populate_data_per_bucket, args=(
                    client, bucket, json_document, (self.prepopulate_item_count // self.total_writer_threads), x))
                    t.start()
                    self.load_thread_list.append(t)
        for t in self.load_thread_list:
            t.join()

    def run_async_data(self):
        self.load_thread_list = []
        randomDataGenerator = RandomDataGenerator()
        randomDataGenerator.set_seed(self.seed)
        base_json = randomDataGenerator.random_json(random_array_count=self.number_of_arrays)
        data_set = randomDataGenerator.random_json(random_array_count=self.number_of_arrays)
        json_document = self.generate_nested(base_json, data_set, self.nesting_level)
        if self.prepopulate_data:
            self.load_thread_list = []
            for bucket in self.buckets:
                for x in range(self.total_writer_threads):
                    client = VBucketAwareMemcached(RestConnection(self.master), bucket)
                    t = Process(target=self.run_populate_data_per_bucket, args=(
                    client, bucket, json_document, (self.prepopulate_item_count // self.total_writer_threads), x))
                    t.start()
                    self.load_thread_list.append(t)

    def run_populate_data_per_bucket(self, client, bucket, json_document, prepopulate_item_count, prefix):
        for x in range(prepopulate_item_count):
            key = str(prefix) + "_subdoc_" + str(x)
            try:
                client.set(key, 0, 0, json.dumps(json_document))
            except Exception as ex:
                self.log.info(ex)

    def _dump_data(self, filename, queue):
        target = open(filename, 'w')
        while queue.empty():
            data = queue.get()
            target.write(data)
        target.close()

    ''' Method to verify kv store data set '''

    def run_verification(self, bucket, kv_store={}):
        client = self.direct_client(self.master, bucket)
        error_result = {}
        for document_key in list(kv_store.keys()):
            pairs = {}
            json_document = kv_store[document_key]
            self.subdoc_gen_helper.find_pairs(json_document, "", pairs)
            for path in list(pairs.keys()):
                opaque, cas, data = client.get_sd(document_key, path)
                data = json.loads(data)
                if data != pairs[path]:
                    error_result[path] = "key = {0}, expected {1}, actual = {2}".format(document_key, pairs[path], data)
        self.assertTrue(len(error_result) != 0, error_result)

    # DOC COMMANDS
    def set(self, client, key, value):
        try:
            if self.verbose_func_usage:
                self.log.info(" set ----> {0} ".format(key))
            if self.use_sdk_client:
                client.set(key, value)
            else:
                jsonDump = json.dumps(value)
                client.set(key, 0, 0, jsonDump)
        except Exception:
            raise
        # SUB DOC COMMANDS

        # GENERIC COMMANDS

    def delete(self, client, key='', path='', value=None):
        try:
            if self.verbose_func_usage:
                self.log.info(" delete ----> {0} ".format(path))
            if self.use_sdk_client:
                client.mutate_in(key, SD.remove(path, xattr=self.xattr))
            else:
                client.delete_sd(key, path)
        except Exception:
            raise

    def replace(self, client, key='', path='', value=None):
        try:
            if self.verbose_func_usage:
                self.log.info(" replace ----> {0} :: {1}".format(path, value))
            if self.use_sdk_client:
                client.mutate_in(key, SD.replace(path, value, xattr=self.xattr))
            else:
                client.replace_sd(key, path, value)
        except Exception:
            raise

    def get_all(self, client, key='', path='', value=None):
        try:
            if self.verbose_func_usage:
                self.log.info(" get ----> {0} ".format(key))
            if self.use_sdk_client:
                r, v, d = client.get(key)
                return d
            else:
                r, v, d = client.get(key)
                return json.loads(d)
        except Exception:
            raise

    def get(self, client, key='', path='', value=None):
        try:
            if self.verbose_func_usage:
                self.log.info(" get ----> {0} :: {1}".format(key, path))
            if self.use_sdk_client:
                d = client.cb.retrieve_in(key, path).get(0)[1]
                return d
            else:
                r, v, d = client.get_sd(key, path)
                return json.loads(d)
        except Exception:
            raise

    def exists(self, client, key='', path='', value=None):
        try:
            if self.use_sdk_client:
                client.lookup_in(key, SD.exists(path))  # xattr not supported?
            else:
                client.exists_sd(key, path)
        except Exception:
            raise

    def counter(self, client, key='', path='', value=None):
        try:
            if self.verbose_func_usage:
                self.log.info(" counter ----> {0} :: {1} + {2}".format(key, path, value))
            if self.use_sdk_client:
                client.cb.mutate_in(key, SD.counter(path, int(value), xattr=self.xattr))
            else:
                client.counter_sd(key, path, value)
        except Exception:
            raise

        # DICTIONARY SPECIFIC COMMANDS

    def dict_add(self, client, key='', path='', value=None):
        try:
            if self.verbose_func_usage:
                self.log.info(" dict_add ----> {0} :: {1}".format(path, value))
            if self.use_sdk_client:
                client.mutate_in(key, SD.insert(path, value, xattr=self.xattr))
            else:
                client.dict_add_sd(key, path, value)
        except Exception:
            raise

    def dict_upsert(self, client, key='', path='', value=None):
        try:
            if self.verbose_func_usage:
                self.log.info(" dict_upsert ----> {0} :: {1}".format(path, value))
            if self.use_sdk_client:
                client.mutate_in(key, SD.upsert(path, value, xattr=self.xattr))
            else:
                client.dict_upsert_sd(key, path, value)
        except Exception:
            raise


        # ARRAY SPECIFIC COMMANDS

    def array_add_last(self, client, key='', path='', value=None):
        try:
            if self.verbose_func_usage:
                self.log.info(" array_add_last ----> {0} :: {1}".format(path, value))
            if self.use_sdk_client:
                client.mutate_in(key, SD.array_append(path, value, xattr=self.xattr))
            else:
                client.array_push_last_sd(key, path, value)
        except Exception:
            raise

    def array_add_first(self, client, key='', path='', value=None):
        try:
            if self.verbose_func_usage:
                self.log.info(" array_add_first ----> {0} :: {1}".format(path, value))
            if self.use_sdk_client:
                client.mutate_in(key, SD.array_prepend(path, value, xattr=self.xattr))
            else:
                client.array_push_first_sd(key, path, value)
        except Exception:
            raise

    def array_add_unique(self, client, key='', path='', value=None):
        try:
            if self.verbose_func_usage:
                self.log.info(" array_add_unique ----> {0} :: {1}".format(path, value))
            if self.use_sdk_client:
                client.mutate_in(key, SD.array_addunique(path, value, xattr=self.xattr))
            else:
                client.array_add_unique_sd(key, path, value)
        except Exception:
            raise

    def array_add_insert(self, client, key='', path='', value=None):
        try:
            if self.verbose_func_usage:
                self.log.info(" array_add_insert ----> {0} :: {1}".format(path, value))
            if self.use_sdk_client:
                client.mutate_in(key, SD.array_insert(path, value, xattr=self.xattr))
            else:
                client.array_add_insert_sd(key, path, value)
        except Exception:
            raise
