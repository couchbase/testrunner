from .tuq import QueryTests
import threading
from lib.membase.api.rest_client import RestHelper
from membase.api.exception import CBQError

class QuerySeqScanTests(QueryTests):
    def setUp(self):
        super(QuerySeqScanTests, self).setUp()
        self.bucket = "default"
        self.doc_count = self.input.param("doc_count", 1000)
        self.thread_count = self.input.param("thread_count", 5)
        self.thread_max = self.input.param("thread_max", 10)
        self.rebalance = self.input.param("rebalance", False)
        self.role = self.input.param("role", "select")
        self.sem = threading.Semaphore(self.thread_max)
        self.thread_errors = 0
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
        scan_before = self.get_index_scan(self.bucket, '#sequentialscan')
        result = self.run_cbq_query(f'SELECT t1.*, array_length(details) FROM {self.bucket} t1 LET details = (SELECT t2.name FROM {self.bucket} t2 WHERE t2.name = t1.name LIMIT 2)')
        scan_after = self.get_index_scan(self.bucket, '#sequentialscan')
        self.assertEqual(result['metrics']['resultCount'], self.doc_count)
        self.assertEqual(result['results'][0], {'name': 'San Francisco', '$1': 2})
        self.assertEqual(scan_after, scan_before + 1001)        

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
            self.sleep(3)
            result = self.run_cbq_query(f'CREATE COLLECTION {self.bucket}.scope1.collection1')
            self.sleep(3)
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
        self.sem.acquire()
        self.log.info("Scan started")
        try:
            result = self.run_cbq_query(f'SELECT * FROM {self.bucket}')
            self.assertEqual(result['metrics']['resultCount'], self.doc_count)
        except Exception as ex:
            self.thread_errors += 1
            self.log.warn(f'Thread failed: {ex}')
        self.sem.release()
        self.log.info("Scan ended")

    def remove_node(self):
        self.log.info("Wait 30s seconds before doing rebalance out")
        self.sleep(30)
        rebalance = self.cluster.async_rebalance(servers=self.servers, to_add=[], to_remove=[self.servers[-1]])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

    def test_parallel_scan(self):
        threads = []

        if self.rebalance:
            thread = threading.Thread(target=self.remove_node, args=())
            threads.append(thread)

        for i in range(self.thread_count):
            thread = threading.Thread(target=self.sequential_scan,args=())
            threads.append(thread)

        # Start threads
        for i in range(len(threads)):
            threads[i].start()

        # Wait for threads to finish
        for i in range(len(threads)):
            threads[i].join()

        if self.thread_errors > 0:
            self.fail(f'{self.thread_errors} thread/s had an error. See warning in log above.')

    def get_index_scan(self, bucket, name, scope='_default', collection='_default'):
        total_scans = self.run_cbq_query(f'SELECT raw metadata.total_scans FROM system:all_indexes WHERE name = "{name}" AND bucket_id = "{bucket}" AND scope_id = "{scope}" AND keyspace_id = "{collection}"')
        return total_scans['results'][0]
    
    def test_rbac_no_seq(self):
        queries = {
            "select" : f"SELECT * FROM {self.bucket}",
            "update" : f"UPDATE {self.bucket} SET name = 'Paris'",
            "delete" : f"DELETE FROM {self.bucket} WHERE name = 'Paris'"
        }
        self.run_cbq_query(f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {{"name": "San Francisco"}} as v FROM array_range(0,{self.doc_count}) d')
        self.users = [{"id": "jacknoseq", "name": "Jack Noseq", "password": "password1"}]
        self.create_users()
        user_id, user_pwd = self.users[0]['id'], self.users[0]['password']
        self.run_cbq_query(query=f"GRANT query_select on `{self.bucket}` to {user_id}")
        self.run_cbq_query(query=f"GRANT query_update on `{self.bucket}` to {user_id}")
        self.run_cbq_query(query=f"GRANT query_delete on `{self.bucket}` to {user_id}")

        try:
            self.run_cbq_query(query=queries[self.role], username=user_id, password=user_pwd)
            self.fail("Query did not fail as expected")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 13014)
            self.assertEqual(error['msg'].replace('`', ''), f'User does not have credentials to use sequential scans. Add role query_use_sequential_scans on default:{self.bucket} to allow the statement to run.')

    def test_rbac_collection(self):
        result = self.run_cbq_query(f'DROP SCOPE {self.bucket}.scope1 IF EXISTS')
        self.run_cbq_query(f'CREATE SCOPE {self.bucket}.scope1 IF not exists')
        self.sleep(3)
        self.run_cbq_query(f'CREATE COLLECTION {self.bucket}.scope1.collection1 IF not exists')
        self.run_cbq_query(f'CREATE COLLECTION {self.bucket}.scope1.collection2 IF not exists')
        self.run_cbq_query(f'INSERT INTO {self.bucket}.scope1.collection1 (key k, value v) SELECT uuid() as k , {{"name": "San Francisco"}} as v FROM array_range(0,{self.doc_count}) d')

        self.users = [{"id": "jackCollection", "name": "Jack Collection", "password": "password1"}]
        self.create_users()
        user_id, user_pwd = self.users[0]['id'], self.users[0]['password']
        # Grant select on all collections
        self.run_cbq_query(query=f"GRANT query_select on `{self.bucket}` to {user_id}")
        # grant sequential scans only on collection1
        self.run_cbq_query(query=f"GRANT query_use_sequential_scans on `{self.bucket}`.scope1.collection1 to {user_id}")

        # select on collection1 should work
        scan_before = self.get_index_scan(self.bucket, '#sequentialscan', "scope1", "collection1")
        result = self.run_cbq_query(f'SELECT * FROM {self.bucket}.scope1.collection1', username=user_id, password=user_pwd)
        scan_after = self.get_index_scan(self.bucket, '#sequentialscan', "scope1", "collection1")
        self.assertEqual(result['metrics']['resultCount'], self.doc_count)
        self.assertEqual(scan_after, scan_before + 1)

        # select on collection2 should fail with missing sequential scans access
        try:
            self.run_cbq_query(f'SELECT * FROM {self.bucket}.scope1.collection2', username=user_id, password=user_pwd)
            self.fail("Query did not fail as expected")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 13014)
            self.assertEqual(error['msg'], f'User does not have credentials to use sequential scans. Add role query_use_sequential_scans on default:default.scope1.collection2 to allow the statement to run.')

    def test_rbac_bucket(self):
        result = self.run_cbq_query(f'DROP SCOPE {self.bucket}.scope1 IF EXISTS')
        self.run_cbq_query(f'CREATE SCOPE {self.bucket}.scope1 IF not exists')
        self.sleep(3)
        self.run_cbq_query(f'CREATE COLLECTION {self.bucket}.scope1.collection1 IF not exists')
        self.run_cbq_query(f'CREATE COLLECTION {self.bucket}.scope1.collection2 IF not exists')
        self.sleep(3)
        self.run_cbq_query(f'INSERT INTO {self.bucket}.scope1.collection1 (key k, value v) SELECT uuid() as k , {{"name": "San Francisco"}} as v FROM array_range(0,{self.doc_count}) d')
        self.run_cbq_query(f'INSERT INTO {self.bucket}.scope1.collection2 (key k, value v) SELECT uuid() as k , {{"name": "San Francisco"}} as v FROM array_range(0,{self.doc_count}) d')

        self.users = [{"id": "jackBucket", "name": "Jack Bucket", "password": "password1"}]
        self.create_users()
        user_id, user_pwd = self.users[0]['id'], self.users[0]['password']
        # Grant select on all collections
        self.run_cbq_query(query=f"GRANT query_select on `{self.bucket}` to {user_id}")
        # grant sequential scans only on collection1
        self.run_cbq_query(query=f"GRANT query_use_sequential_scans on `{self.bucket}` to {user_id}")

        # select on collection1 should work
        scan_before = self.get_index_scan(self.bucket, '#sequentialscan', "scope1", "collection1")
        result = self.run_cbq_query(f'SELECT * FROM {self.bucket}.scope1.collection1', username=user_id, password=user_pwd)
        scan_after = self.get_index_scan(self.bucket, '#sequentialscan', "scope1", "collection1")
        self.assertEqual(result['metrics']['resultCount'], self.doc_count)
        self.assertEqual(scan_after, scan_before + 1)

        # select on collection2 should work
        scan_before = self.get_index_scan(self.bucket, '#sequentialscan', "scope1", "collection2")
        result = self.run_cbq_query(f'SELECT * FROM {self.bucket}.scope1.collection2', username=user_id, password=user_pwd)
        scan_after = self.get_index_scan(self.bucket, '#sequentialscan', "scope1", "collection2")
        self.assertEqual(result['metrics']['resultCount'], self.doc_count)
        self.assertEqual(scan_after, scan_before + 1)

    def test_rbac_scope(self):
        result = self.run_cbq_query(f'DROP SCOPE {self.bucket}.scope1 IF EXISTS')
        self.run_cbq_query(f'CREATE SCOPE {self.bucket}.scope1 IF not exists')
        self.sleep(3)
        self.run_cbq_query(f'CREATE COLLECTION {self.bucket}.scope1.collection1 IF not exists')
        self.run_cbq_query(f'CREATE COLLECTION {self.bucket}.scope1.collection2 IF not exists')
        self.run_cbq_query(f'INSERT INTO {self.bucket}.scope1.collection1 (key k, value v) SELECT uuid() as k , {{"name": "San Francisco"}} as v FROM array_range(0,{self.doc_count}) d')
        self.run_cbq_query(f'INSERT INTO {self.bucket}.scope1.collection2 (key k, value v) SELECT uuid() as k , {{"name": "San Francisco"}} as v FROM array_range(0,{self.doc_count}) d')

        self.users = [{"id": "jackScope", "name": "Jack Scope", "password": "password1"}]
        self.create_users()
        user_id, user_pwd = self.users[0]['id'], self.users[0]['password']
        # Grant select on all collections
        self.run_cbq_query(query=f"GRANT query_select on `{self.bucket}` to {user_id}")
        # grant sequential scans only on collection1
        self.run_cbq_query(query=f"GRANT query_use_sequential_scans on default:`{self.bucket}`.scope1 to {user_id}")

        # select on collection1 should work
        scan_before = self.get_index_scan(self.bucket, '#sequentialscan', "scope1", "collection1")
        result = self.run_cbq_query(f'SELECT * FROM {self.bucket}.scope1.collection1', username=user_id, password=user_pwd)
        scan_after = self.get_index_scan(self.bucket, '#sequentialscan', "scope1", "collection1")
        self.assertEqual(result['metrics']['resultCount'], self.doc_count)
        self.assertEqual(scan_after, scan_before + 1)

        # select on collection2 should work
        scan_before = self.get_index_scan(self.bucket, '#sequentialscan', "scope1", "collection2")
        result = self.run_cbq_query(f'SELECT * FROM {self.bucket}.scope1.collection2', username=user_id, password=user_pwd)
        scan_after = self.get_index_scan(self.bucket, '#sequentialscan', "scope1", "collection2")
        self.assertEqual(result['metrics']['resultCount'], self.doc_count)
        self.assertEqual(scan_after, scan_before + 1)

    def test_rbac_udf(self):
        self.run_cbq_query(f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {{"name": "San Francisco"}} as v FROM array_range(0,{self.doc_count}) d')
        self.users = [{"id": "jackudf", "name": "Jack UDF", "password": "password1"}]
        self.create_users()
        user_id, user_pwd = self.users[0]['id'], self.users[0]['password']
        self.run_cbq_query(query=f"GRANT query_select on `{self.bucket}` to {user_id}")
        self.run_cbq_query(query=f"GRANT query_execute_global_functions to {user_id}")
        self.run_cbq_query(query=f"CREATE OR REPLACE FUNCTION f1() {{( SELECT * FROM {self.bucket} )}} ")
        self.run_cbq_query(query="EXECUTE FUNCTION f1()")
        try:
            self.run_cbq_query(query="EXECUTE FUNCTION f1()", username=user_id, password=user_pwd)
            self.fail("Query did not fail as expected")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['reason']['code'], 13014)
            self.assertEqual(error['reason']['message'], f'User does not have credentials to use sequential scans. Add role query_use_sequential_scans on `default`:`{self.bucket}` to allow the statement to run.')

    def test_rbac_prepare(self):
        self.run_cbq_query(f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {{"name": "San Francisco"}} as v FROM array_range(0,{self.doc_count}) d')
        self.users = [{"id": "jackprep", "name": "Jack Prepare", "password": "password1"}]
        self.create_users()
        user_id, user_pwd = self.users[0]['id'], self.users[0]['password']
        self.run_cbq_query(query=f"GRANT query_select on `{self.bucket}` to {user_id}")
        self.run_cbq_query(query=f"PREPARE P11 AS SELECT * FROM {self.bucket}")
        try:
            self.run_cbq_query(query="EXECUTE P11", username=user_id, password=user_pwd)
            self.fail("Query did not fail as expected")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 13014)
            self.assertEqual(error['msg'], f'User does not have credentials to use sequential scans. Add role query_use_sequential_scans on default:{self.bucket} to allow the statement to run.')

    def test_rbac_subquery(self):
        self.run_cbq_query(f'INSERT INTO {self.bucket} (key k, value v) SELECT uuid() as k , {{"name": "San Francisco"}} as v FROM array_range(0,{self.doc_count}) d')
        self.users = [{"id": "jackprep", "name": "Jack Prepare", "password": "password1"}]
        self.create_users()
        user_id, user_pwd = self.users[0]['id'], self.users[0]['password']
        self.run_cbq_query(query=f"GRANT query_select on `{self.bucket}` to {user_id}")
        try:
            self.run_cbq_query(query=f"SELECT 1 FROM (SELECT * FROM {self.bucket}) d", username=user_id, password=user_pwd)
            self.fail("Query did not fail as expected")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 13014)
            self.assertEqual(error['msg'], f'User does not have credentials to use sequential scans. Add role query_use_sequential_scans on default:{self.bucket} to allow the statement to run.')
