from .tuq import QueryTests
import threading
from membase.api.exception import CBQError

class QuerySeqScanTests(QueryTests):
    def setUp(self):
        super(QuerySeqScanTests, self).setUp()
        self.bucket = "default"
        self.doc_count = self.input.param("doc_count", 1000)
        self.thread_count = self.input.param("thread_count", 5)
        self.run_cbq_query(f'DELETE FROM {self.bucket}')
        self.run_cbq_query(f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {{"name": "San Francisco"}} as v FROM array_range(0,{self.doc_count}) d')
        self.sleep(2)

    def suite_setUp(self):
        super(QuerySeqScanTests, self).suite_setUp()

    def tearDown(self):
        super(QuerySeqScanTests, self).tearDown()

    def suite_tearDown(self):
        super(QuerySeqScanTests, self).suite_tearDown()

    def test_index_precedence_primary(self):
        try:
            self.run_cbq_query(f'CREATE PRIMARY INDEX ON {self.bucket}')
            explain = self.run_cbq_query(f'EXPLAIN SELECT * FROM {self.bucket} LIMIT 1')
            self.assertEqual(explain['results'][0]['plan']['~children'][0]['~children'][0]['index'], '#primary')
        finally:
            self.run_cbq_query(f'DROP PRIMARY INDEX ON {self.bucket}')

    def test_index_precedence_secondary(self):
        try:
            self.run_cbq_query(f'CREATE INDEX idx_name ON {self.bucket}(name)')
            explain = self.run_cbq_query(f'EXPLAIN SELECT * FROM {self.bucket} WHERE name is not null LIMIT 1')
            self.assertEqual(explain['results'][0]['plan']['~children'][0]['~children'][0]['index'], 'idx_name')
        finally:
            self.run_cbq_query(f'DROP INDEX idx_name ON {self.bucket}')

    def test_basic_read(self):
        explain = self.run_cbq_query(f'EXPLAIN SELECT * FROM {self.bucket} LIMIT 1')
        self.assertEqual(explain['results'][0]['plan']['~children'][0]['~children'][0]['index'], '#sequentialscan')

        scan_before = self.get_index_scan(self.bucket, '#sequentialscan')
        result = self.run_cbq_query(f'SELECT * FROM {self.bucket} data LIMIT 1' )
        scan_after = self.get_index_scan(self.bucket, '#sequentialscan')
        self.assertEqual(result['results'], [{'data': {'name': 'San Francisco'}}])
        self.assertEqual(scan_after, scan_before + 1)

    def test_basic_write(self):
        scan_before = self.get_index_scan(self.bucket, '#sequentialscan')
        update = self.run_cbq_query(f'UPDATE {self.bucket} SET name = "Paris" LIMIT 1')
        delete = self.run_cbq_query(f'DELETE FROM {self.bucket} WHERE name = "Paris"')
        scan_after = self.get_index_scan(self.bucket, '#sequentialscan')
        self.assertEqual(scan_after, scan_before + 2)

    def test_join(self):
        scan_before = self.get_index_scan(self.bucket, '#sequentialscan')
        result = self.run_cbq_query(f'SELECT count(*) as count FROM {self.bucket} a JOIN {self.bucket} b ON a.name = b.name')
        scan_after = self.get_index_scan(self.bucket, '#sequentialscan')
        self.assertEqual(result['results'], [{"count": self.doc_count * self.doc_count}])
        self.assertEqual(scan_after, scan_before + 2)

    def test_cte(self):
        scan_before = self.get_index_scan(self.bucket, '#sequentialscan')
        result = self.run_cbq_query(f'WITH city as (SELECT t.* FROM {self.bucket} t) SELECT city.name FROM city')
        scan_after = self.get_index_scan(self.bucket, '#sequentialscan')
        self.assertEqual(result['metrics']['resultCount'], self.doc_count)
        self.assertEqual(result['results'][0], {'name': 'San Francisco'})
        self.assertEqual(scan_after, scan_before + 1)

    def test_subquery(self):
        scan_before = self.get_index_scan(self.bucket, '#sequentialscan')
        result = self.run_cbq_query(f'SELECT s.* FROM {self.bucket} s WHERE name in (SELECT raw t.name FROM {self.bucket} t LIMIT 1)')
        scan_after = self.get_index_scan(self.bucket, '#sequentialscan')
        self.assertEqual(result['metrics']['resultCount'], self.doc_count)
        self.assertEqual(result['results'][0], {'name': 'San Francisco'})
        self.assertEqual(scan_after, scan_before + 2)

    def test_subquery_correlated(self):
        error_code = 5370
        error_message = "Unable to run subquery - cause: No secondary index available for keyspace t2 in correlated subquery."
        try:
            result = self.run_cbq_query(f'SELECT t1.*, array_length(details) FROM {self.bucket} t1 LET details = (SELECT t2.name FROM {self.bucket} t2 WHERE t2.name = t1.name LIMIT 2)')
            self.fail(f'We expected query to fail with {error_message}')
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error_code, error['code'], f'Error code is wrong please check the error: {error}')
            self.assertEqual(error_message, error['msg'], f"The error message is not what we expected please check error: {error}")

    def test_advise(self):
        advise = self.run_cbq_query(f'ADVISE SELECT * FROM {self.bucket} LIMIT 1')
        self.assertEqual(advise['results'][0]['advice']['adviseinfo'], {'recommended_indexes': 'No secondary index recommendation at this time, primary index may apply.'})

    def test_explain(self):
        explain = self.run_cbq_query(f'EXPLAIN SELECT * FROM {self.bucket} LIMIT 1')
        self.assertEqual(explain['results'][0]['plan']['~children'][0]['~children'][0]['index'], '#sequentialscan')

    def test_using(self):
        try:
            self.run_cbq_query(f'CREATE PRIMARY INDEX ON {self.bucket}')
            explain_primary = self.run_cbq_query(f'EXPLAIN SELECT * FROM {self.bucket} LIMIT 1')
            self.assertEqual(explain_primary['results'][0]['plan']['~children'][0]['~children'][0]['index'], '#primary')

            explain_seqscan = self.run_cbq_query(f'EXPLAIN SELECT * FROM {self.bucket} USE INDEX (`#sequentialscan`) LIMIT 1')
            self.assertEqual(explain_seqscan['results'][0]['plan']['~children'][0]['~children'][0]['index'], '#sequentialscan')

            scan_before = self.get_index_scan(self.bucket, '#sequentialscan')
            result = self.run_cbq_query(f'SELECT * FROM {self.bucket} USE INDEX (`#sequentialscan`) LIMIT 1')
            scan_after = self.get_index_scan(self.bucket, '#sequentialscan')
            self.assertEqual(scan_after, scan_before + 1)
        finally:
            self.run_cbq_query(f'DROP PRIMARY INDEX ON {self.bucket}')

    def test_system_catalog(self):
        system_indexes = self.run_cbq_query(f'SELECT * FROM system:indexes WHERE bucket_id = "{self.bucket}"')
        system_all_indexes = self.run_cbq_query(f'SELECT name, `using` FROM system:all_indexes WHERE bucket_id = "{self.bucket}" AND scope_id = "_default" AND keyspace_id = "_default"')
        self.assertEqual(system_indexes['results'], [])
        self.assertEqual(system_all_indexes['results'], [{'name': '#sequentialscan', 'using': 'sequentialscan'}])

    def test_inline_udf(self):
        udf = self.run_cbq_query(f'CREATE or REPLACE FUNCTION f1() {{(SELECT * FROM {self.bucket} LIMIT 1)}}')

        scan_before = self.get_index_scan(self.bucket, '#sequentialscan')
        result = self.run_cbq_query(f'EXECUTE FUNCTION f1()')
        scan_after = self.get_index_scan(self.bucket, '#sequentialscan')
        self.assertEqual(result['results'], [[{'default': {'name': 'San Francisco'}}]])
        self.assertEqual(scan_after, scan_before + 1)

    def test_js_udf(self):
        udf = self.run_cbq_query(f"CREATE or REPLACE FUNCTION select_js(city, num) LANGUAGE JAVASCRIPT as 'function select_js(city, num) {{\
            var query = SELECT name FROM {self.bucket} WHERE name = $city LIMIT $num;\
            var acc = [];\
            for (const row of query) {{ acc.push(row); }}\
            return acc;}}'")
        scan_before = self.get_index_scan(self.bucket, '#sequentialscan')
        result = self.run_cbq_query(f'EXECUTE FUNCTION select_js("San Francisco", 5)')
        scan_after = self.get_index_scan(self.bucket, '#sequentialscan')
        self.assertEqual(result['results'], [[{'name': 'San Francisco'}, {'name': 'San Francisco'}, {'name': 'San Francisco'}, {'name': 'San Francisco'}, {'name': 'San Francisco'}]])
        self.assertEqual(scan_after, scan_before + 1)

    def test_collection(self):
        try:
            result = self.run_cbq_query(f'CREATE SCOPE {self.bucket}.scope1')
            result = self.run_cbq_query(f'CREATE COLLECTION {self.bucket}.scope1.collection1')
            self.run_cbq_query(f'INSERT INTO {self.bucket}.scope1.collection1 (key k, value v) SELECT uuid() as k , {{"name": "San Francisco"}} as v FROM array_range(0,{self.doc_count}) d')
            scan_before = self.get_index_scan(self.bucket, '#sequentialscan', "scope1", "collection1")
            result = self.run_cbq_query(f'SELECT * FROM {self.bucket}.scope1.collection1')
            scan_after = self.get_index_scan(self.bucket, '#sequentialscan', "scope1", "collection1")
            self.assertEqual(result['metrics']['resultCount'], self.doc_count)
            self.assertEqual(scan_after, scan_before + 1)
        finally:
            result = self.run_cbq_query(f'DROP SCOPE {self.bucket}.scope1 IF EXISTS')

    def test_infer(self):
        infer = self.run_cbq_query(f'INFER {self.bucket} WITH {{"flags":["no_random_entry"]}}')
        self.assertEqual(infer['results'][0][0]['#docs'], self.doc_count)

    def test_update_statistics(self):
        result = self.run_cbq_query(f'UPDATE STATISTICS FOR {self.bucket}(name)')

    def test_transaction(self):
        scan_before = self.get_index_scan(self.bucket, '#sequentialscan')
        begin = self.run_cbq_query('BEGIN WORK')
        txid = begin['results'][0]['txid']
        result = self.run_cbq_query(f'UPDATE {self.bucket} SET name = "Paris" LIMIT 1', txnid=txid)
        self.assertEqual(result['metrics']['mutationCount'], 1)
        rollback = self.run_cbq_query('ROLLBACK', txnid=txid)
        scan_after = self.get_index_scan(self.bucket, '#sequentialscan')
        self.assertEqual(scan_after, scan_before + 1)

    def sequential_scan(self):
        self.log.info("Scan started")
        try:
            result = self.run_cbq_query(f'SELECT * FROM {self.bucket}')
            self.assertEqual(result['metrics']['resultCount'], self.doc_count)
        except:
            self.warn
        self.log.info("Scan ended")

    def test_parallel_scan(self):
        threads = []
        for i in range(self.thread_count):
            thread = threading.Thread(target=self.sequential_scan,args=())
            threads.append(thread)

        # Start threads
        for i in range(len(threads)):
            threads[i].start()

        # Wait for threads to finish
        for i in range(len(threads)):
            threads[i].join()

    def get_index_scan(self, bucket, name, scope='_default', collection='_default'):
        total_scans = self.run_cbq_query(f'SELECT raw metadata.total_scans FROM system:all_indexes WHERE name = "{name}" AND bucket_id = "{bucket}" AND scope_id = "{scope}" AND keyspace_id = "{collection}"')
        return total_scans['results'][0]
