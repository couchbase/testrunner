from .tuq import QueryTests
import threading
from membase.api.exception import CBQError
from remote.remote_util import RemoteMachineShellConnection


class QuerySequenceTests(QueryTests):
    def setUp(self):
        super(QuerySequenceTests, self).setUp()
        self.bucket = "default"
        self.start = self.input.param("start", 0)
        self.increment = self.input.param("increment", 1)
        self.cache = self.input.param("cache", 50)
        self.min = self.input.param("min", -9223372036854775808)
        self.max = self.input.param("max", +9223372036854775807)
        self.cycle = self.input.param("cycle", False)
        self.alter_field = self.input.param("alter_field", "max")
        self.alter_value = self.input.param("alter_value", 100)
        self.use_options = self.input.param("use_options", False)
        self.thread_count = self.input.param("thread_count", 10)
        self.thread_max = self.input.param("thread_max", 5)
        self.sem = threading.Semaphore(self.thread_max)
        self.thread_errors = 0

    def suite_setUp(self):
        super(QuerySequenceTests, self).suite_setUp()

    def tearDown(self):
        super(QuerySequenceTests, self).tearDown()

    def suite_tearDown(self):
        super(QuerySequenceTests, self).suite_tearDown()

    def test_default_options(self):
        sequence_name = "seq_default_option"
        expected_default = [{'cache': 50, 'cycle': False, 'increment': 1, 'max': 9223372036854775807, 'min': -9223372036854775808, 'path': f'`default`:`default`.`_default`.`{sequence_name}`'}]
        self.run_cbq_query(f"DROP SEQUENCE {self.bucket}.`_default`.{sequence_name} IF EXISTS")
        self.run_cbq_query(f"CREATE SEQUENCE {self.bucket}.`_default`.{sequence_name}")

        result = self.run_cbq_query(f"SELECT `cache`, `cycle`, `increment`, `max`, `min`, `path` FROM system:sequences WHERE name = '{sequence_name}'")
        self.assertEqual(result['results'], expected_default)

        nextval = self.run_cbq_query(f"SELECT NEXTVAL FOR {self.bucket}.`_default`.{sequence_name} as val")
        self.assertEqual(nextval['results'][0]['val'], 0)

        nextval = self.run_cbq_query(f"SELECT NEXTVAL FOR {self.bucket}.`_default`.{sequence_name} as val")
        self.assertEqual(nextval['results'][0]['val'], 1)

        prevval = self.run_cbq_query(f"SELECT PREVVAL FOR {self.bucket}.`_default`.{sequence_name} as val")
        self.assertEqual(prevval['results'][0]['val'], 1)

        prevval = self.run_cbq_query(f"SELECT PREVVAL FOR {self.bucket}.`_default`.{sequence_name} as val")
        self.assertEqual(prevval['results'][0]['val'], 1)

        nextval = self.run_cbq_query(f"SELECT NEXTVAL FOR {self.bucket}.`_default`.{sequence_name} as val")
        self.assertEqual(nextval['results'][0]['val'], 2)

    def test_options(self):
        sequence_name = f"seq_option_{abs(self.start)}"
        expected_default = [{'cache': self.cache, 'cycle': self.cycle, 'increment': self.increment, 'max': self.max, 'min': self.min, 'path': f'`default`:`default`.`_default`.`{sequence_name}`'}]

        self.log.info(f"Sequence name: {sequence_name} with start: {self.start}, increment: {self.increment}, max: {self.max} and min: {self.min}")
        self.run_cbq_query(f"DROP SEQUENCE {self.bucket}.`_default`.{sequence_name} IF EXISTS")
        if self.use_options:
            self.run_cbq_query(f"CREATE SEQUENCE {self.bucket}.`_default`.{sequence_name} WITH {{'cycle': {self.cycle}, 'cache': {self.cache}, 'start': {self.start}, 'increment': {self.increment}, 'max': {self.max}, 'min': {self.min}}}")
        else:
            self.run_cbq_query(f"CREATE SEQUENCE {self.bucket}.`_default`.{sequence_name} NO CYCLE CACHE {self.cache} START WITH {self.start} INCREMENT BY {self.increment} MAXVALUE {self.max} MINVALUE {self.min}")

        result = self.run_cbq_query(f"SELECT `cache`, `cycle`, `increment`, `max`, `min`, `path` FROM system:sequences WHERE name = '{sequence_name}'")
        self.assertEqual(result['results'], expected_default)

        nextval = self.run_cbq_query(f"SELECT NEXTVAL FOR {self.bucket}.`_default`.{sequence_name} as val")
        self.log.info(f"sequence value: {nextval['results'][0]['val']}")
        self.assertEqual(nextval['results'], [{'val': self.start}])

        while nextval['results'][0]['val'] + self.increment <= self.max and nextval['results'][0]['val'] + self.increment >= self.min:
            prevval = self.run_cbq_query(f"SELECT PREVVAL FOR {self.bucket}.`_default`.{sequence_name} as val")
            nextval = self.run_cbq_query(f"SELECT NEXTVAL FOR {self.bucket}.`_default`.{sequence_name} as val")
            self.log.info(f"sequence value: {nextval['results'][0]['val']}")
            self.assertEqual(nextval['results'], [{'val': prevval['results'][0]['val'] + self.increment}])

        try:
            nextval = self.run_cbq_query(f"SELECT NEXTVAL FOR {self.bucket}.`_default`.{sequence_name} as val")
            self.fail("Sequence next value should have failed")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 5010)
            self.assertEqual(error['reason']['message'], f"Sequence 'default:{self.bucket}._default.{sequence_name}' has reached its limit")

    def test_cycle(self):
        sequence_name = f"seq_cycle_{abs(self.start)}"
        expected_default = [{'cache': self.cache, 'cycle': True, 'increment': self.increment, 'max': self.max, 'min': self.min, 'path': f'`default`:`default`.`_default`.`{sequence_name}`'}]

        self.log.info(f"Sequence name: {sequence_name} with start: {self.start}, increment: {self.increment}, max: {self.max} and min: {self.min}")
        self.run_cbq_query(f"DROP SEQUENCE {self.bucket}.`_default`.{sequence_name} IF EXISTS")
        if self.use_options:
            self.run_cbq_query(f"CREATE SEQUENCE {self.bucket}.`_default`.{sequence_name} WITH {{'cycle': True, 'start': {self.start}, 'increment': {self.increment}, 'max': {self.max}, 'min': {self.min}}}")
        else:
            self.run_cbq_query(f"CREATE SEQUENCE {self.bucket}.`_default`.{sequence_name} CYCLE START WITH {self.start} INCREMENT BY {self.increment} MAXVALUE {self.max} MINVALUE {self.min}")

        result = self.run_cbq_query(f"SELECT `cache`, `cycle`, `increment`, `max`, `min`, `path` FROM system:sequences WHERE name = '{sequence_name}'")
        self.assertEqual(result['results'], expected_default)

        nextval = self.run_cbq_query(f"SELECT NEXTVAL FOR {self.bucket}.`_default`.{sequence_name} as val")
        self.log.info(f"sequence value: {nextval['results'][0]['val']}")
        self.assertEqual(nextval['results'], [{'val': self.start}])

        while nextval['results'][0]['val'] + self.increment <= self.max and nextval['results'][0]['val'] + self.increment >= self.min:
            prevval = self.run_cbq_query(f"SELECT PREVVAL FOR {self.bucket}.`_default`.{sequence_name} as val")
            nextval = self.run_cbq_query(f"SELECT NEXTVAL FOR {self.bucket}.`_default`.{sequence_name} as val")
            self.log.info(f"sequence value: {nextval['results'][0]['val']}")
            self.assertEqual(nextval['results'], [{'val': prevval['results'][0]['val'] + self.increment}])

        prevval = self.run_cbq_query(f"SELECT PREVVAL FOR {self.bucket}.`_default`.{sequence_name} as val")
        nextval = self.run_cbq_query(f"SELECT NEXTVAL FOR {self.bucket}.`_default`.{sequence_name} as val")
        self.log.info(f"sequence value: {nextval['results'][0]['val']}")

        if self.increment > 0:
            self.assertTrue(prevval['results'][0]['val'] + self.increment >= self.max)
            self.assertEqual(nextval['results'], [{'val': self.min}])
        else:
            self.assertTrue(prevval['results'][0]['val'] + self.increment <= self.min)
            self.assertEqual(nextval['results'], [{'val': self.max}])

    def test_alter(self):
        sequence_name = f"seq_alter_{abs(self.start)}"
        expected_default = [{'cache': self.cache, 'cycle': False, 'increment': self.increment, 'max': self.max, 'min': self.min, 'path': f'`default`:`default`.`_default`.`{sequence_name}`'}]

        self.run_cbq_query(f"DROP SEQUENCE {self.bucket}.`_default`.{sequence_name} IF EXISTS")
        self.run_cbq_query(f"CREATE SEQUENCE {self.bucket}.`_default`.{sequence_name} NO CYCLE CACHE {self.cache} START WITH {self.start} INCREMENT BY {self.increment} MAXVALUE {self.max} MINVALUE {self.min}")

        result = self.run_cbq_query(f"SELECT `cache`, `cycle`, `increment`, `max`, `min`, `path` FROM system:sequences WHERE name = '{sequence_name}'")
        self.assertEqual(result['results'], expected_default)

        expected_default[0][self.alter_field] = self.alter_value
        if self.alter_field == "min" or self.alter_field == "max":
            alter_clause = f"{self.alter_field}value {self.alter_value}"
        if self.alter_field == "increment":
            alter_clause = f"increment by {self.alter_value}"
        if self.alter_field == "cache":
            alter_clause = f"cache {self.alter_value}"
        if self.alter_field == "restart":
            alter_clause = f"restart with {self.alter_value}"
            self.start = self.alter_value
            del expected_default[0]['restart']
        if self.alter_field == "cycle":
            if self.alter_value:
                alter_clause = f"cycle"
            else:
                alter_clause = f"no cycle"

        alter = self.run_cbq_query(f"ALTER SEQUENCE {self.bucket}.`_default`.{sequence_name} {alter_clause}")
        result = self.run_cbq_query(f"SELECT `cache`, `cycle`, `increment`, `max`, `min`, `path` FROM system:sequences WHERE name = '{sequence_name}'")
        self.assertEqual(result['results'], expected_default, f"expected: {expected_default} - actual: {result['results']}")

        nextval = self.run_cbq_query(f"SELECT NEXTVAL FOR {self.bucket}.`_default`.{sequence_name} as val")
        self.log.info(f"sequence value: {nextval['results'][0]['val']}")
        self.assertEqual(nextval['results'], [{'val': self.start}])

    def test_invalid_option(self):
        sequence_name = f"seq_alter_{abs(self.start)}"
        self.run_cbq_query(f"DROP SEQUENCE {self.bucket}.`_default`.{sequence_name} IF EXISTS")
        try:
            self.run_cbq_query(f"CREATE SEQUENCE {self.bucket}.`_default`.{sequence_name} START {self.start}")
            self.fail('Create sequence should have failed')
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 3000)
            self.assertEqual(error['msg'], "syntax error - line 1, column 54, near '...`.seq_alter_0 START ', at: 0")

        try:
            self.run_cbq_query(f"CREATE SEQUENCE {self.bucket}.`_default`.{sequence_name} WITH {{'started': 10}}")
            self.fail('Create sequence should have failed')
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 19101)
            self.assertEqual(error['reason']['message'], "Invalid option 'started'")


    def test_invalid_value(self):
        sequence_name = f"seq_alter_{abs(self.start)}"
        self.run_cbq_query(f"DROP SEQUENCE {self.bucket}.`_default`.{sequence_name} IF EXISTS")
        try:
            self.run_cbq_query(f"CREATE SEQUENCE {self.bucket}.`_default`.{sequence_name} MAXVALUE '200'")
            self.fail('Create sequence should have failed')
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 3000)
            self.assertEqual(error['msg'], "syntax error - invalid option value (near line 1, column 48)")

        try:
            self.run_cbq_query(f"CREATE SEQUENCE {self.bucket}.`_default`.{sequence_name} WITH {{'max': '200'}}")
            self.fail('Create sequence should have failed')
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 19101)
            self.assertEqual(error['reason']['message'], "Invalid value for 'max'")

    def test_drop(self):
        sequence_name = f"seq_drop_{abs(self.start)}"

        self.run_cbq_query(f"CREATE SCOPE {self.bucket}.scope1 if not exists")
        self.run_cbq_query(f"CREATE SCOPE {self.bucket}.scope2 if not exists")
        self.sleep(3)
        self.run_cbq_query(f"DROP SEQUENCE {self.bucket}.scope1.{sequence_name} IF EXISTS")
        self.run_cbq_query(f"DROP SEQUENCE {self.bucket}.scope2.{sequence_name} IF EXISTS")

        self.run_cbq_query(f"CREATE SEQUENCE {self.bucket}.scope1.{sequence_name}")
        self.run_cbq_query(f"CREATE SEQUENCE {self.bucket}.scope2.{sequence_name}")

        expected = [
            {'cache': 50, 'cycle': False, 'increment': 1, 'max': 9223372036854775807, 'min': -9223372036854775808, 'path': f'`default`:`{self.bucket}`.`scope1`.`seq_drop_0`'}, 
            {'cache': 50, 'cycle': False, 'increment': 1, 'max': 9223372036854775807, 'min': -9223372036854775808, 'path': f'`default`:`{self.bucket}`.`scope2`.`seq_drop_0`'}
        ]

        self.sleep(2)
        result = self.run_cbq_query(f"SELECT `cache`, `cycle`, `increment`, `max`, `min`, `path` FROM system:sequences WHERE name = '{sequence_name}' ORDER by `path`")
        self.assertEqual(expected, result['results'])

        self.run_cbq_query(f"DROP SEQUENCE {self.bucket}.scope1.{sequence_name}")
        self.run_cbq_query(f"DROP SCOPE {self.bucket}.scope2")

        self.sleep(3)
        expected = []
        result = self.run_cbq_query(f"SELECT `cache`, `cycle`, `increment`, `max`, `min`, `path` FROM system:sequences WHERE name = '{sequence_name}'")
        self.assertEqual(expected, result['results'])

    def test_cache_error(self):
        sequence_name = f"seq_alter_{abs(self.start)}"
        self.run_cbq_query(f"DROP SEQUENCE {self.bucket}.`_default`.{sequence_name} IF EXISTS")
        try:
            self.run_cbq_query(f"CREATE SEQUENCE {self.bucket}.`_default`.{sequence_name} CACHE 0")
            self.fail('Create sequence should have failed')
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 19101)
            self.assertEqual(error['reason']['message'], "Invalid cache value 0")

        self.run_cbq_query(f"DROP SEQUENCE {self.bucket}.`_default`.{sequence_name} IF EXISTS")
        try:
            self.run_cbq_query(f"CREATE SEQUENCE {self.bucket}.`_default`.{sequence_name} CACHE -10")
            self.fail('Create sequence should have failed')
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 19101)
            self.assertEqual(error['reason']['message'], "Invalid cache value -10")

    def test_transaction(self):
        sequence_name = f"seq_txn_{abs(self.start)}"
        self.run_cbq_query(f'DELETE FROM {self.bucket} WHERE customer is not missing')
        self.run_cbq_query(f'DELETE FROM {self.bucket} WHERE order_num is not missing')
        self.run_cbq_query(f"DROP SEQUENCE {self.bucket}.`_default`.{sequence_name} IF EXISTS")
        self.run_cbq_query(f"CREATE SEQUENCE {self.bucket}.`_default`.{sequence_name} NO CYCLE CACHE {self.cache} START WITH {self.start} INCREMENT BY {self.increment} MAXVALUE {self.max} MINVALUE {self.min}")

        results = self.run_cbq_query(query="BEGIN WORK", server=self.master)
        txid = results['results'][0]['txid']

        self.run_cbq_query(f'INSERT INTO {self.bucket} VALUES (uuid(), {{"num":NEXT VALUE FOR {self.bucket}.`_default`.{sequence_name},"customer":"Alex"}})', txnid=txid)
        self.run_cbq_query(f'INSERT INTO {self.bucket} VALUES (uuid(), {{"order_num":PREVVAL FOR {self.bucket}.`_default`.{sequence_name},"item_num":1,"description":"Widget One"}})', txnid=txid)

        # get next sequence number outside the transaction
        nextval = self.run_cbq_query(f'SELECT NEXTVAL FOR {self.bucket}.`_default`.{sequence_name} as val')
        self.log.info(f"sequence value: {nextval['results'][0]['val']}")
        self.assertEqual(nextval['results'][0]['val'], self.start + self.increment)

        # back to sequence in transaction
        self.run_cbq_query(f'INSERT INTO {self.bucket} VALUES (uuid(), {{"order_num":PREVVAL FOR {self.bucket}.`_default`.{sequence_name},"item_num":2,"description":"Widget Two"}})', txnid=txid)
        nextval = self.run_cbq_query(f'SELECT NEXTVAL FOR {self.bucket}.`_default`.{sequence_name} as val', txnid=txid)
        self.log.info(f"sequence value: {nextval['results'][0]['val']}")
        self.assertEqual(nextval['results'][0]['val'], self.start + 2*self.increment)

        self.run_cbq_query('COMMIT', txnid=txid)

        expected_result = [{"num": self.start, "items": ["Widget One", "Widget Two"]}]
        result = self.run_cbq_query(f'SELECT o.num, ARRAY_AGG(i.description) items FROM {self.bucket} o, {self.bucket} i WHERE o.num = i.order_num GROUP BY o.num')
        self.assertEqual(result['results'], expected_result)

    def test_prepared(self):
        sequence_name = f"seq_prepared_{abs(self.start)}"
        expected_default = [{'cache': self.cache, 'cycle': False, 'increment': self.increment, 'max': self.max, 'min': self.min, 'path': f'`default`:`default`.`_default`.`{sequence_name}`'}]

        node1 = self.servers[0]
        node2 = self.servers[1]

        self.run_cbq_query(f"DELETE FROM system:prepareds WHERE name = 'SEQ1'")
        self.run_cbq_query(f"DROP SEQUENCE {self.bucket}.`_default`.{sequence_name} IF EXISTS")
        self.run_cbq_query(f"CREATE SEQUENCE {self.bucket}.`_default`.{sequence_name} NO CYCLE CACHE {self.cache} START WITH {self.start} INCREMENT BY {self.increment} MAXVALUE {self.max} MINVALUE {self.min}")
        result = self.run_cbq_query(f"SELECT `cache`, `cycle`, `increment`, `max`, `min`, `path` FROM system:sequences WHERE name = '{sequence_name}'")
        self.assertEqual(result['results'], expected_default)

        self.run_cbq_query(f"PREPARE SEQ1 AS SELECT NEXTVAL FOR {self.bucket}.`_default`.{sequence_name} as val", server=node1)
        
        nextval = self.run_cbq_query(f"EXECUTE SEQ1", server=node1)
        self.log.info(f"sequence value: {nextval['results'][0]['val']}")
        self.assertEqual(nextval['results'], [{'val': self.start}])

        nextval = self.run_cbq_query(f"EXECUTE SEQ1", server=node2)
        self.log.info(f"sequence value: {nextval['results'][0]['val']}")
        self.assertEqual(nextval['results'], [{'val': self.start + self.cache}])

    def test_udf(self):
        sequence_name = f"seq_udf_{abs(self.start)}"
        expected_default = [{'cache': self.cache, 'cycle': False, 'increment': self.increment, 'max': self.max, 'min': self.min, 'path': f'`default`:`default`.`_default`.`{sequence_name}`'}]

        self.run_cbq_query(f"DROP SEQUENCE {self.bucket}.`_default`.{sequence_name} IF EXISTS")
        self.run_cbq_query(f"CREATE SEQUENCE {self.bucket}.`_default`.{sequence_name} NO CYCLE CACHE {self.cache} START WITH {self.start} INCREMENT BY {self.increment} MAXVALUE {self.max} MINVALUE {self.min}")
        result = self.run_cbq_query(f"SELECT `cache`, `cycle`, `increment`, `max`, `min`, `path` FROM system:sequences WHERE name = '{sequence_name}'")
        self.assertEqual(result['results'], expected_default)

        self.run_cbq_query(f"CREATE or REPLACE FUNCTION F1() {{ (SELECT NEXTVAL FOR {self.bucket}.`_default`.{sequence_name} as val) }}")
        
        nextval = self.run_cbq_query(f"EXECUTE FUNCTION default:F1()")
        self.log.info(f"sequence value: {nextval['results']}")
        self.assertEqual(nextval['results'], [[{'val': self.start}]])

        nextval = self.run_cbq_query(f"EXECUTE FUNCTION default:F1()")
        self.log.info(f"sequence value: {nextval['results']}")
        self.assertEqual(nextval['results'], [[{'val': self.start + self.increment}]])

    def test_kill_node(self):
        node1 = self.servers[0]
        node2 = self.servers[1]
        sequence_name = f"seq_kill_{abs(self.start)}"
        self.run_cbq_query(f"DROP SEQUENCE {self.bucket}.`_default`.{sequence_name} IF EXISTS")
        self.run_cbq_query(f"CREATE SEQUENCE {self.bucket}.`_default`.{sequence_name} NO CYCLE CACHE {self.cache} START WITH {self.start} INCREMENT BY {self.increment} MAXVALUE {self.max} MINVALUE {self.min}", server=node2)

        prevval = self.run_cbq_query(f'SELECT PREVVAL FOR {self.bucket}.`_default`.{sequence_name} as val', server=node2)
        self.log.info(f"prev sequence value: {prevval['results']}")
        self.assertEqual(prevval['results'], [{}])

        nextval = self.run_cbq_query(f'SELECT NEXTVAL FOR {self.bucket}.`_default`.{sequence_name} as val', server=node2)
        self.log.info(f"next sequence value: {nextval['results'][0]['val']}")
        self.assertEqual(nextval['results'][0]['val'], self.start)

        nextval = self.run_cbq_query(f'SELECT NEXTVAL FOR {self.bucket}.`_default`.{sequence_name} as val', server=node2)
        self.log.info(f"next sequence value: {nextval['results'][0]['val']}")
        self.assertEqual(nextval['results'][0]['val'], self.start + self.increment)

        # Kill node2
        remote_client = RemoteMachineShellConnection(node2)
        self.log.info(f"Kill process: cbq-engine on node2 ...")
        remote_client.terminate_process(process_name="cbq-engine", force=True)
        self.sleep(30)

        # check after kill
        prevval = self.run_cbq_query(f'SELECT PREVVAL FOR {self.bucket}.`_default`.{sequence_name} as val', server=node2)
        nextval = self.run_cbq_query(f'SELECT NEXTVAL FOR {self.bucket}.`_default`.{sequence_name} as val', server=node2)
        self.log.info(f"prev sequence value: {prevval['results']}")
        self.log.info(f"next sequence value: {nextval['results'][0]['val']}")
        self.assertEqual(prevval['results'], [{}])
        self.assertEqual(nextval['results'][0]['val'], self.start + self.cache)

    def test_concurrency(self):
        sequence_name = f"seq_customer_id"
        self.run_cbq_query(f'DELETE FROM {self.bucket} WHERE customer is not missing')
        self.run_cbq_query(f"DROP SEQUENCE {self.bucket}.`_default`.{sequence_name} IF EXISTS")
        self.run_cbq_query(f"CREATE SEQUENCE {self.bucket}.`_default`.{sequence_name} NO CYCLE CACHE {self.cache} START WITH {self.start} INCREMENT BY {self.increment} MAXVALUE {self.max} MINVALUE {self.min}")
        
        threads = []
        for i in range(self.thread_count):
            thread = threading.Thread(target=self.insert_customer,args=())
            threads.append(thread)

        # Start threads
        for i in range(len(threads)):
            threads[i].start()

        # Wait for threads to finish
        for i in range(len(threads)):
            threads[i].join()

        if self.thread_errors > 0:
            self.fail(f'{self.thread_errors} thread/s had an error. See warning in log above.')
        
        result = self.run_cbq_query(f'SELECT count(*) customer_count FROM {self.bucket} WHERE customer is not missing')
        self.log.info(result['results'])

    def insert_customer(self):
        sequence_name = f"{self.bucket}.`_default`.seq_customer_id"
        insert_query = f'INSERT INTO {self.bucket} (KEY k, VALUE v) SELECT TO_STRING(NEXTVAL FOR {sequence_name}) k, {{"num": NEXTVAL FOR {sequence_name}, "customer": "Customer Name"}} v'
        self.sem.acquire()
        self.log.info("Insert new customer started")
        try:
            result = self.run_cbq_query(insert_query)
        except Exception as ex:
            self.thread_errors += 1
            self.log.warn(f'Thread failed: {ex}')
        self.sem.release()
        self.log.info("Insert new customer ended")

    def test_restart(self):
        sequence_name = f"seq_restart_{abs(self.start)}"
        expected_default = [{'cache': self.cache, 'cycle': False, 'increment': self.increment, 'max': self.max, 'min': self.min, 'path': f'`default`:`default`.`_default`.`{sequence_name}`'}]

        self.run_cbq_query(f"DROP SEQUENCE {self.bucket}.`_default`.{sequence_name} IF EXISTS")
        self.run_cbq_query(f"CREATE SEQUENCE {self.bucket}.`_default`.{sequence_name} NO CYCLE CACHE {self.cache} START WITH {self.start} INCREMENT BY {self.increment} MAXVALUE {self.max} MINVALUE {self.min}")

        result = self.run_cbq_query(f"SELECT `cache`, `cycle`, `increment`, `max`, `min`, `path` FROM system:sequences WHERE name = '{sequence_name}'")
        self.assertEqual(result['results'], expected_default)

        nextval = self.run_cbq_query(f"SELECT NEXTVAL FOR {self.bucket}.`_default`.{sequence_name} as val")
        self.log.info(f"sequence value: {nextval['results'][0]['val']}")
        nextval = self.run_cbq_query(f"SELECT NEXTVAL FOR {self.bucket}.`_default`.{sequence_name} as val")
        self.log.info(f"sequence value: {nextval['results'][0]['val']}")
        nextval = self.run_cbq_query(f"SELECT NEXTVAL FOR {self.bucket}.`_default`.{sequence_name} as val")
        self.log.info(f"sequence value: {nextval['results'][0]['val']}")

        self.run_cbq_query(f"ALTER SEQUENCE {self.bucket}.`_default`.{sequence_name} RESTART")
        result = self.run_cbq_query(f"SELECT `cache`, `cycle`, `increment`, `max`, `min`, `path` FROM system:sequences WHERE name = '{sequence_name}'")
        self.assertEqual(result['results'], expected_default, f"expected: {expected_default} - actual: {result['results']}")

        nextval = self.run_cbq_query(f"SELECT NEXTVAL FOR {self.bucket}.`_default`.{sequence_name} as val")
        self.log.info(f"sequence value: {nextval['results'][0]['val']}")
        self.assertEqual(nextval['results'], [{'val': self.start}])

    def test_insert_nextval(self):
        sequence_name = f"seq_order_{abs(self.start)}"
        expected_default = [{'cache': self.cache, 'cycle': False, 'increment': self.increment, 'max': self.max, 'min': self.min, 'path': f'`default`:`default`.`_default`.`{sequence_name}`'}]

        self.run_cbq_query(f'DELETE FROM {self.bucket} WHERE customer is not missing')
        self.run_cbq_query(f"DROP SEQUENCE {self.bucket}.`_default`.{sequence_name} IF EXISTS")
        self.run_cbq_query(f"CREATE SEQUENCE {self.bucket}.`_default`.{sequence_name} NO CYCLE CACHE {self.cache} START WITH {self.start} INCREMENT BY {self.increment} MAXVALUE {self.max} MINVALUE {self.min}")
        result = self.run_cbq_query(f"SELECT `cache`, `cycle`, `increment`, `max`, `min`, `path` FROM system:sequences WHERE name = '{sequence_name}'")
        self.assertEqual(result['results'], expected_default)

        expected_result = [{'default': {'customer': 'Sam', 'num': self.start}}]
        result = self.run_cbq_query(f'INSERT INTO {self.bucket} VALUES (uuid(),{{"num":NEXT VALUE FOR {self.bucket}.`_default`.{sequence_name}, "customer":"Sam"}}) RETURNING *')
        self.assertEqual(result['results'], expected_result)

        expected_result = [{'id': f'{self.start+self.increment}', 'default': {'customer': 'Bobby', 'num': self.start+self.increment}}]
        result = self.run_cbq_query(f'INSERT INTO {self.bucket} VALUES (TO_STRING(NEXTVAL FOR {self.bucket}.`_default`.{sequence_name}), {{"num":NEXTVAL FOR {self.bucket}.`_default`.{sequence_name}, "customer":"Bobby"}}) RETURNING meta().id,*')
        self.assertEqual(result['results'], expected_result)

        expected_result = [{'id': f'{self.start+2*self.increment}', 'default': {'customer': 'Bobby', 'num': self.start+2*self.increment}}]
        result = self.run_cbq_query(f'INSERT INTO {self.bucket} (KEY k, VALUE v) SELECT TO_STRING(NEXTVAL FOR {self.bucket}.`_default`.{sequence_name}) k, {{"num":NEXTVAL FOR {self.bucket}.`_default`.{sequence_name}, "customer":"Bobby"}} v RETURNING meta().id,*')
        self.assertEqual(result['results'], expected_result)

    def test_no_options(self):
        sequence_name = f"seq_no_options_{abs(self.start)}"
        expected_default = [{'cache': 1, 'cycle': False, 'increment': 1, 'max': +9223372036854775807, 'min': -9223372036854775808, 'path': f'`default`:`default`.`_default`.`{sequence_name}`'}]

        self.run_cbq_query(f"DROP SEQUENCE {self.bucket}.`_default`.{sequence_name} IF EXISTS")
        self.run_cbq_query(f"CREATE SEQUENCE {self.bucket}.`_default`.{sequence_name} NO CYCLE NO CACHE NO MAXVALUE NO MINVALUE")

        result = self.run_cbq_query(f"SELECT `cache`, `cycle`, `increment`, `max`, `min`, `path` FROM system:sequences WHERE name = '{sequence_name}'")
        self.assertEqual(result['results'], expected_default)