import time

from .tuq import QueryTests
from lib.collection.collections_n1ql_client import CollectionsN1QL
import threading
from membase.api.exception import CBQError

class IndexerParallel(QueryTests):
    def setUp(self):
        super(IndexerParallel, self).setUp()
        self.bucket_count=self.input.param("bucket_count", 2)
        self.scope_count=self.input.param("scope_count", 10)
        self.collection_count=self.input.param("collection_count", 10)
        self.doc_count = self.input.param("doc_count", 10)
        self.thread_max = self.input.param("thread_max", 75)
        self.primary = self.input.param("primary", False)
        self.collections_helper = CollectionsN1QL(self.master)
        self.sem = threading.Semaphore(self.thread_max)
        self.times = {}

    def suite_setUp(self):
        super(IndexerParallel, self).suite_setUp()

    def tearDown(self):
        super(IndexerParallel, self).tearDown()

    def suite_tearDown(self):
        super(IndexerParallel, self).suite_tearDown()

    def create_index(self, primary, bucket, scope, collection):
        self.sem.acquire()
        self.log.info(f'Thread started for {bucket}.{scope}.{collection}')
        if primary:
            create_index = f'CREATE PRIMARY INDEX on {bucket}.{scope}.{collection} with {{"num_replica": 1}}'
        else:
            create_index = f'CREATE INDEX idx_{bucket}_{scope}_{collection} on {bucket}.{scope}.{collection}(name) with {{"num_replica": 1}}'
        try:
            result = self.run_cbq_query(create_index)
            self.times[f'idx_{bucket}_{scope}_{collection}'] = result['metrics']['executionTime']
        except CBQError as ex:
            self.log.warn(f"CREATE INDEX ERROR with {ex}")
        self.sem.release()
        self.log.info(f'Thread ended for {bucket}.{scope}.{collection}')

    def test_create_index(self):
        # create buckets
        buckets = [f'bucket{i}' for i in range(self.bucket_count)]
        self._create_buckets(self.master, bucket_list=buckets, server_id=None, bucket_size=256)

        # Create collections and thread
        scopes = [f'scope{i}' for i in range(self.scope_count)]
        collections = [f'collection{i}' for i in range(self.collection_count)]
        threads = []
        for bucket in buckets:
            for scope in scopes:
                self.collections_helper.create_scope(bucket_name=bucket, scope_name=scope)
                time.sleep(3)
                for collection in collections:
                    self.collections_helper.create_collection(bucket_name=bucket, scope_name=scope, collection_name=collection)
                    time.sleep(3)
                    self.run_cbq_query(f'INSERT INTO {bucket}.{scope}.{collection} (key k, value v) SELECT uuid() as k , {{"name": "San Francisco"}} as v FROM array_range(0,{self.doc_count}) d')
                    thread = threading.Thread(target=self.create_index,args=(self.primary, bucket, scope, collection))
                    threads.append(thread)

        # Start threads
        for i in range(len(threads)):
            threads[i].start()

        # Wait for threads to finish
        for i in range(len(threads)):
            threads[i].join()

        # Log index timings
        self.log.info(f'INDEX TIMINGS: {self.times}')

        # Run simple query to check query service is still able to handle queries
        result = self.run_cbq_query('SELECT raw count(*) FROM bucket0.scope0.collection0')
        self.assertEqual(result['results'], [self.doc_count])

        # Check all indexes are online
        expected_count = self.bucket_count*self.scope_count*self.collection_count
        result = self.run_cbq_query('SELECT raw count(*) FROM system:indexes WHERE state = online')
        self.assertTrue(result['results'], [expected_count])
