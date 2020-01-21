import argparse
import multiprocessing
import time
from copy import deepcopy
from multiprocessing.dummy import Pool

from couchbase.bucket import Bucket, LOCKMODE_WAIT, CouchbaseError, \
    ArgumentError, NotFoundError, TimeoutError
from couchbase.cluster import Cluster, PasswordAuthenticator
from decorator import decorator

description = """
Upsert some documents into a bucket using the couchbase python client.
The tool can create documents and update them based on the count parameter.
For pure creates, give passes=1. For both create and updates, give passes=(num of mutations required + 1).
For pure updates, include update_counter = (value of update in document + 1).
For deletes, include delete and num_delete parameters.
The tool can also validate the data loaded. Include validation parameter for validation.
For no validation, give validation=0
For validation at the end of the load generation, give validation=1
For validation after each pass of mutation, give validation=2
For only validation and no mutations, give validation=3 
With validation=3, pass -update_counter=value of update in a document (and --deleted and --deleted_items=docs_deleted)
The rate of mutations can be limited using rate_limit (to the best of ability)
Expiry of documents can be set using ttl parameter
Validation of expiry of documents can be done by using --validate_expired
"""

@decorator
def with_sleep(method, *args):
    self = args[0]
    start_time = time.time()
    return_value = method(self, *args[1:])
    end_time = time.time()
    exec_time = end_time - start_time
    if self.rate_limited and exec_time < self.thread_min_time:
        time.sleep(self.thread_min_time - exec_time)
    return return_value

UPSERT = "upsert"
DELETE = "delete"
VALIDATE = "validate"

def parseArguments():
    parser = argparse.ArgumentParser(description=description, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--spec', '-U', default="couchbase://localhost", help='Cluster connections string. ['
                                                                              'Default=couchbase://localhost]')
    parser.add_argument('--bucket', '-b', default="default", help='Bucket to connect to')
    parser.add_argument('--password', '-p', default="password", help='User password')
    parser.add_argument('--user', '-u', default="Administrator", help='Username')
    parser.add_argument('--batch_size', '-B', default=5000, help="Batch size of eatch inserts")
    parser.add_argument('--prefix', '-k', default="Key_", help='Key Prefix')
    parser.add_argument('--timeout', '-t', default=5, type=int, help='KV Operation Timeout')
    parser.add_argument('--count', '-c', default=1000, type=int, help='Number of documents in the bucket already')
    parser.add_argument('--start_document', default=0, type=int, help="Starting document count to start updating from")
    parser.add_argument('--passes', '-P', default=0, type=int, help="Number of mutation cycles to perform per "
                                                                    "document (including create)")
    parser.add_argument('--update_counter', default=0, type=int, help="Starting update counter to start updating from")
    parser.add_argument('--cb_version', default='5.0', help="Current version of the couchbase cluster")
    parser.add_argument('--size', default=100, type=int, help="Size of the document to be inserted, in bytes")
    parser.add_argument('--validation', '-v', default=0, type=int, help="Validate the documents. 0=No validation, "
                                                                        "1=Validate at end, 2=Validate after each "
                                                                        "pass, 3=Perform only validation and no "
                                                                        "mutation. [Default=0]")
    parser.add_argument('--replicate_to', default=0, type=int, help="Perform durability checking on this many "
                                                                    "replicas for presence in memory")
    parser.add_argument('--ttl', default=0, type=int, help="Set expiry timer for documents")
    parser.add_argument('--delete', default=False, action='store_true', help="Delete documents from bucket")
    parser.add_argument('--num_delete', default=0, type=int, help='Number of documents to delete')
    parser.add_argument('--deleted', default=False, action='store_true', help="Was delete of documents run before "
                                                                              "validation")
    parser.add_argument('--deleted_items', default=0, type=int, help="Number of documents that were deleted")
    parser.add_argument('--validate_expired', default=False, action='store_true', help="Validate if documents have "
                                                                                    "expired")
    parser.add_argument('--rate_limit', '-r', default=0, type=int, help="Set operations per second limit")
    parser.add_argument('--threads', '-T', default=5, type=int, help="Number of threads per worker.")
    parser.add_argument('--max_worker', default=25, type=int, help="Maximum workers to create. warning: Can cause "
                                                                   "performance degradation with high number")
    parser.add_argument('--workers', default=5, type=int, help="Number of workers to create")
    parser.add_argument('--remove_limit', default=False, action='store_true', help="Remove the rate limiter to run "
                                                                                   "at max capacity")
    return parser.parse_args()

class Document:
    def __init__(self, value, size):
        body_length = size - str(value).__len__() - "val".__len__() - "update".__len__() - "body".__len__()
        body = "".rjust(body_length, 'a')
        self.val = int(value)
        self.update = 0
        self.body = body

class Batch:
    def __init__(self):
        self.start = 0
        self.end = 0
        self.operation = ""

class DocumentGenerator:
    def __init__(self, args):
        self.spec = args.spec
        self.bucket_name = args.bucket
        self.user = args.user
        self.password = args.password
        self.cb_version = args.cb_version
        self.num_items = args.count
        self.mutations = args.passes
        self.batch_size = int(args.batch_size)
        self.key_prefix = args.prefix
        self.start_document = int(args.start_document)
        self.previous_mutation_count = int(args.update_counter)
        self.validation = int(args.validation)
        self.replicate_to = int(args.replicate_to)
        self.size = int(args.size)
        self.timeout = int(args.timeout)
        self.ttl = int(args.ttl)
        self.delete = args.delete
        self.num_delete = int(args.num_delete)
        self.deleted = args.deleted
        self.deleted_items = int(args.deleted_items)
        self.validate_expired = args.validate_expired
        self.threads = args.threads
        self.rate_limit = args.rate_limit
        self.rate_limited = not args.remove_limit
        self.workers = args.workers
        self.connections = []
        self.current_update_counter = 0
        self.batches = []
        self.retry_batches = []
        self.num_completed = 0
        self.key_exists_error = 0
        self.wrong_keys = []
        self.missing_key_val = []
        self.wrong_keys_replica = []
        self.missing_key_val_replica = []
        self.thread_min_time = float(self.threads * self.workers * self.batch_size) / float(self.rate_limit)
        if "couchbase://" not in self.spec:
            self.spec = "couchbase://{}".format(self.spec)
        self.create_connections()

    def create_connections(self):
        """
        Create bucket connections. 5 bucket connections are created per instance.
        :return: Nothing
        """
        for i in range(0, self.threads):
            if self.cb_version > '5':
                cluster = Cluster(self.spec)
                auth = PasswordAuthenticator(self.user, self.password)
                cluster.authenticate(auth)
                bucket = cluster.open_bucket(self.bucket_name, lockmode=LOCKMODE_WAIT)
                bucket.timeout = self.timeout
                self.connections.append(bucket)
            else:
                bucket = Bucket('{0}/{1}'.format(self.spec, self.bucket_name), lockmode=LOCKMODE_WAIT)
                bucket.timeout = self.timeout
                self.connections.append(bucket)

    def create_upsert_batches(self):
        """
        Create the upsert batches for this instance. Each batch contains start and end counter.
        :return: Nothing
        """
        for i in range(self.start_document, self.start_document + self.num_items, self.batch_size):
            batch = Batch()
            batch.start = i
            batch.operation = UPSERT
            if i + self.batch_size > self.start_document + self.num_items:
                batch.end = self.start_document + self.num_items
            else:
                batch.end = i + self.batch_size
            self.batches.append(batch)

    def create_delete_batches(self):
        """
        Create delete batches for this instance. Each batch contains start and end counter
        :return:
        """
        if self.num_items < self.num_delete:
            self.num_delete = self.num_items
        for i in range(self.start_document, self.start_document + self.num_delete, self.batch_size):
            batch  = Batch()
            batch.start = i
            batch.operation = DELETE
            if i + self.batch_size > self.start_document + self.num_delete:
                batch.end = self.start_document + self.num_delete
            else:
                batch.end = i + self.batch_size
            self.batches.append(batch)

    def get_upsert_items(self, start, end):
        """
        Get the upsert items.
        :param start: Starting document
        :param end: End document
        :return: (dict) Key-value pair of all documents from start to end - 1
        """
        items = {}
        for x in range(start, end):
            key = "{}{}".format(self.key_prefix, x)
            document = Document(x, self.size)
            document.update = self.current_update_counter
            items[key] = document.__dict__
        return items

    def get_retry_upsert_items(self):
        """
        Get the items that must be retried for upsert.
        :return: (dict) key-value pair of all documents that should be retried
        """
        items = {}
        for item in self.retry_batches:
            doc_num = int(item.split('_')[1])
            document = Document(doc_num, self.size)
            document.update = self.current_update_counter
            items[item] = document.__dict__
        return items

    def get_delete_retry_keys(self):
        """
        Get the items that must be retried for deletes.
        :return: (list) keys of all documents that should be retried for delete
        """
        items = self.retry_batches
        return items

    def get_keys(self, start, end):
        """
        Get the keys for get or delete
        :param start: Starting document
        :param end: End document
        :return: (list) Keys of documents to be retrieved or deleted
        """
        keys = []
        for i in range(start, end):
            keys.append('{}{}'.format(self.key_prefix, i))
        return keys

    def get_items(self, connection, keys, replica=False):
        """
        Get items from couchbase bucket.
        :param connection: Bucket object for connection
        :param keys: (list) List of keys to be retrieved
        :param replica: (bool) Specify if replica has to be retrieved instead of active.
        :return: (dict) Successful result of get_multi
        """
        try:
            result = connection.get_multi(keys, replica=replica)
            return result
        except TimeoutError as e:
            ok, fail = e.split_results()
            failed_keys = [key for key in fail]
            result = self.get_items(connection, failed_keys, replica)
            result.update(ok)
            return result
        except CouchbaseError as e:
            ok, fail = e.split_results()
            return ok

    @with_sleep
    def upsert_items(self, connection, items):
        """
        Upsert items into couchbase bucket
        :param connection:  Bucket object for connection
        :param items: (dict) Key-value pairs of documents to be inserted
        :return: number of items successfully upserted.
        """
        try:
            result = connection.upsert_multi(items, ttl=self.ttl, replicate_to=self.replicate_to)
            return result.__len__()
        except ArgumentError:
            self.replicate_to = 0
            return self.upsert_items(connection, items)
        except CouchbaseError as e:
            ok, fail = e.split_results()
            num_completed = ok.__len__()
            for key in fail:
                self.retry_batches.append(key)
            self.key_exists_error += 1
            return num_completed

    @with_sleep
    def delete_items(self, connection, keys):
        """
        Delete items from couchbase bucket
        :param connection: Bucket object for connection
        :param keys: (list) List of keys to be deleted
        :return: number of items successfully deleted.
        """
        try:
            result = connection.remove_multi(keys)
            return result.__len__()
        except NotFoundError as e:
            ok, fail = e.split_results()
            return ok.__len__()
        except CouchbaseError as e:
            ok, fail = e.split_results()
            for key in fail:
                self.retry_batches.append(key)
            self.key_exists_error += 1
            return ok.__len__()

    def upsert_thread(self, connection, batch):
        items = self.get_upsert_items(batch.start, batch.end)
        completed = self.upsert_items(connection, items)
        return completed

    def upsert_thread_pool(self, args):
        return self.upsert_thread(*args)

    def validate_items(self, connection, start, end, replica=False):
        """
        Validate items in the bucket
        :param connection: Bucket object for connection
        :param start: Start document number
        :param end: End document number
        :param replica: (bool) Specify if replica has to be validated instead of active.
        :return: Nothing
        """
        keys = self.get_keys(start, end)
        result = self.get_items(connection, keys, replica=replica)
        if self.validate_expired > 0:
            if result:
                for key in list(result.keys()):
                    if replica:
                        self.missing_key_val_replica.append(key)
                        return
                    else:
                        self.missing_key_val.append(key)
                        return
            else:
                return
        for i in range(start, end):
            key = "{}{}".format(self.key_prefix, i)
            document = Document(i, self.size)
            document.update = self.current_update_counter
            value = document.__dict__
            if key in result:
                if self.deleted and (self.start_document + self.deleted_items) > int(key.split("_")[1]):
                    if replica:
                        self.missing_key_val_replica.append(key)
                    else:
                        self.missing_key_val.append(key)
                    continue
                val = result[key].value
                for k in list(value.keys()):
                    if k in val and val[k] == value[k]:
                        continue
                    else:
                        if replica:
                            self.wrong_keys_replica.append(key)
                        else:
                            self.wrong_keys.append(key)
            else:
                if self.deleted and (self.start_document + self.deleted_items) > int(key.split("_")[1]):
                    continue
                if replica:
                    self.missing_key_val_replica.append(key)
                else:
                    self.missing_key_val.append(key)

    def validate_thread(self, connection, batch):
        self.validate_items(connection, batch.start, batch.end)
        if self.replicate_to:
            self.validate_items(connection, batch.start, batch.end, replica=True)

    def validate_thread_pool(self, args):
        return self.validate_thread(*args)

    def delete_thread(self, connection, batch):
        keys = self.get_keys(batch.start, batch.end)
        completed = self.delete_items(connection, keys)
        return completed

    def delete_thread_pool(self, args):
        return self.delete_thread(*args)

    def single_upsert_pass(self):
        """
        Upsert round. Upsert the required documents and retry if any failures. The method retries till all the
        documents are successfully upserted without any error.
        :return: Nothing
        """
        self.batches = []
        self.create_upsert_batches()
        args = []
        num_of_connections_available = self.connections.__len__()
        for i in range(0, self.batches.__len__()):
            connection = self.connections[i % num_of_connections_available]
            args.append((connection, self.batches[i]))
        thread_pool = Pool(self.threads)
        result = thread_pool.map(self.upsert_thread_pool, args)
        thread_pool.close()
        thread_pool.join()
        for res in result:
            self.num_completed += res
        while self.retry_batches:
            items = self.get_retry_upsert_items()
            self.retry_batches = []
            completed = self.upsert_items(self.connections[0], items)
            self.num_completed += completed

    def single_validate_pass(self):
        """
        Validation round. Validate the documents in the bucket.
        :return: Nothing
        """
        self.batches = []
        self.create_upsert_batches()
        args = []
        num_of_connections_available = self.connections.__len__()
        for i in range(0, self.batches.__len__()):
            connection = self.connections[i % num_of_connections_available]
            args.append((connection, self.batches[i]))
        thread_pool = Pool(self.threads)
        result = thread_pool.map(self.validate_thread_pool, args)
        thread_pool.close()
        thread_pool.join()

    def single_delete_pass(self):
        """
        Deletion round. Delete documents in bucket and retry if any failures. The method retries till all required
        documents are successfully deleted without any error.
        :return: Nothing
        """
        self.batches = []
        self.create_delete_batches()
        args = []
        num_of_connections_available = self.connections.__len__()
        for i in range(0, self.batches.__len__()):
            connection = self.connections[i % num_of_connections_available]
            args.append((connection, self.batches[i]))
        thread_pool = Pool(self.threads)
        result = thread_pool.map(self.delete_thread_pool, args)
        thread_pool.close()
        thread_pool.join()
        for res in result:
            self.num_completed += res
        while self.retry_batches:
            keys = self.get_delete_retry_keys()
            self.retry_batches = []
            completed = self.delete_items(self.connections[0], keys)
            self.num_completed += completed
        self.deleted = True
        self.deleted_items = self.num_delete

    def print_validation_stats(self):
        if self.missing_key_val:
            print(("Missing keys count: {}".format(self.missing_key_val.__len__())))
            print(("Missing keys: {}".format(self.missing_key_val.__str__())))
        if self.wrong_keys:
            print(("Mismatch keys count: {}".format(self.wrong_keys.__len__())))
            print(("Mismatch keys: {}".format(self.wrong_keys.__str__())))
        if self.replicate_to > 0 and self.missing_key_val_replica:
            print(("Missing keys count from replicas: {}".format(self.missing_key_val_replica.__len__())))
            print(("Missing keys from replicas: {}".format(self.missing_key_val_replica.__str__())))
        if self.replicate_to > 0 and self.wrong_keys_replica:
            print(("Mismatch keys count from replicas: {}".format(self.wrong_keys_replica.__len__())))
            print(("Mismatch keys from replicas: {}".format(self.wrong_keys_replica.__str__())))
        if not self.missing_key_val and not self.wrong_keys:
            print(("Validated documents: {}".format(self.num_items)))

    def print_upsert_stats(self):
        print(("Upserted documents: {}".format(self.num_completed)))

    def print_delete_stats(self):
        print(("Deleted documents: {}".format(self.num_completed)))

    def generate(self):
        """
        Load generation phase. Based on input, perform various load generation and validations.
        :return:
        """
        if self.validation == 3:
            self.current_update_counter = self.previous_mutation_count
            self.single_validate_pass()
            self.print_validation_stats()
            return
        for i in range(0, self.mutations):
            self.current_update_counter = i + self.previous_mutation_count
            self.num_completed = 0
            self.single_upsert_pass()
            self.print_upsert_stats()
            if self.validation == 2:
                self.single_validate_pass()
                self.print_validation_stats()
        if self.delete:
            self.num_completed = 0
            self.single_delete_pass()
            self.print_delete_stats()
            if self.validation == 2:
                self.single_validate_pass()
                self.print_validation_stats()
        if self.validation == 1:
            self.single_validate_pass()
            self.print_validation_stats()

if __name__ == "__main__":
    args = parseArguments()
    rate = int(args.rate_limit)
    #  Calculate the number of workers required. Since each thread in a worker could take atleast 1 sec and each
    #  thread loads batch_size documents each time, number of workers can be calculated given rate and batch size.
    num_workers = int(args.workers)
    workers = []
    arguments = []
    #  Split the items to be loaded among the workers.
    for i in range(0, num_workers):
        #  Calculate the number of items in each worker and the start documents for each worker.
        arg = deepcopy(args)
        items = int(arg.count) / num_workers
        start = items * i + int(arg.start_document)
        if i == num_workers - 1:
            items = int(arg.count) - (items * i)
        arg.count = items
        arg.start_document = start
        arguments.append(arg)
    if args.delete:
        #    If there are deletes, divide the number of documents to be divided among the workers.
        #    Calculate the workers that should perform the deletes and the workers that should not.
        #    ex: If num_delete=2500 and count=10000 and num_workers=10, only first 3 workers will have deletes,
        #    i.e first 2 workers will have 1000 deletes and 3rd will have 500 deletes.
        items_per_worker = int(args.count) / num_workers
        args_counter = 0
        num_delete = int(args.num_delete)
        for i in range(0, num_delete, items_per_worker):
            arg = arguments[args_counter]
            if num_delete < items_per_worker:
                arg.num_delete = num_delete
            elif i + items_per_worker > num_delete:
                arg.num_delete = i + items_per_worker - num_delete
            else:
                arg.num_delete = items_per_worker
            args_counter += 1
        for arg in arguments[args_counter:]:
            arg.delete = False
            arg.num_delete = 0
    for i in range(0, num_workers):
        arg = arguments[i]
        doc_loader = DocumentGenerator(arg)
        worker = multiprocessing.Process(target=doc_loader.generate, name="Worker {}".format(i))
        worker.daemon = True
        workers.append(worker)
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()

