import copy
import uuid
import json
from tuqquery.tuq import QueryTests
from deepdiff import DeepDiff
from remote.remote_util import RemoteMachineShellConnection
from membase.api.exception import CBQError

class DMLQueryTests(QueryTests):
    def setUp(self):
        super(DMLQueryTests, self).setUp()
        self.log.info("==============  DMLQueryTests setup has started ==============")
        self.directory = self.input.param("directory", "/tmp/tuq_data")
        self.named_prepare = self.input.param("named_prepare", None)
        self.query_buckets = self.get_query_buckets(check_all_buckets=True)
        if len(self.query_buckets) > 1:
            self.bucket0 = self.query_buckets[0]
            self.bucket1 = self.query_buckets[1]
        for bucket in self.query_buckets:
            self.ensure_primary_indexes_exist()
            self.run_cbq_query("DELETE from {0}".format(bucket))
        self.log.info("-" * 100)
        self.log.info("==============  DMLQueryTests setup has started ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(DMLQueryTests, self).suite_setUp()
        self.log.info("==============  DMLQueryTests suite_setup has started ==============")
        self.log.info("==============  DMLQueryTests suite_setup has started ==============")

    def tearDown(self):
        self.log.info("==============  DMLQueryTests tearDown has started ==============")
        self.log.info("==============  DMLQueryTests tearDown has started ==============")
        super(DMLQueryTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  DMLQueryTests suite_tearDown has started ==============")
        self.log.info("==============  DMLQueryTests suite_tearDown has started ==============")
        super(DMLQueryTests, self).suite_tearDown()

    ###################################################################################################################
    #
    # SANITY
    #
    ###################################################################################################################

    def test_sanity(self):
        self.test_update_where()
        self.test_insert_with_select()
        self.test_delete_where_clause_json()

    ###################################################################################################################
    #
    # INSERT NON-DOC VALUES
    #
    ###################################################################################################################

    def test_insert_non_doc_bool(self):
        expected_item_value = True
        self._common_check('key_bool%s' % str(uuid.uuid4())[:5], expected_item_value)

    def test_insert_non_doc_int(self):
        expected_item_value = 234
        self._common_check('key_int%s' % str(uuid.uuid4())[:5], expected_item_value)

    def test_insert_non_doc_str(self):
        expected_item_value = 'automation_value'
        self._common_check('key_str%s' % str(uuid.uuid4())[:5], expected_item_value)

    def test_insert_non_doc_array(self):
        expected_item_value = ['first', 'second']
        self._common_check('key_array%s' % str(uuid.uuid4())[:5], expected_item_value)

    def test_insert_non_doc_null(self):
        expected_item_value = None
        self._common_check('key_null%s' % str(uuid.uuid4())[:5], expected_item_value)

    # duplicate definition
    # def test_insert_non_doc_long(self):
    #     expected_item_value = 123
    #     self._common_check('key_null%s' % str(uuid.uuid4())[:1] * 256, expected_item_value)

    def test_insert_non_doc_long(self):
        prefix = str(uuid.uuid4())[:4]
        self._common_check_values(['%s%s' % (k, prefix * 60) for k in range(10)],
                                  [False, 123, 'values_auto', ["third", "value"], None] * 2)

    def test_insert_non_doc_values(self):
        prefix = str(uuid.uuid4())[:5]
        self._common_check_values(['%s%s' % (k, prefix) for k in range(10)],
                                  [False, 123, 'values_auto', ["third", "value"], None] * 2)

    ###################################################################################################################
    #
    # INSERT JSON VALUES
    #
    ###################################################################################################################

    def test_insert_json_values(self):
        prefix = str(uuid.uuid4())[:5]
        self._common_check_values(['%s%s' % (k, prefix) for k in range(10)],
                                  [{'name': '%s%s' % (prefix, k)} for k in range(10)])

    def test_insert_returning_elements(self):
        keys = ['%s%s' % (k, str(uuid.uuid4())[:5]) for k in range(10)]
        expected_item_values = [{'name': 'return_%s' % v} for v in range(10)]
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            values = ''
            for k, v in zip(keys, expected_item_values):
                inserted = v
                if isinstance(v, str):
                    inserted = '"%s"' % v
                if v is None:
                    inserted = 'null'
                values += '("%s", %s),' % (k, inserted)
            self.query = 'insert into %s (key , value) VALUES %s RETURNING ELEMENT name' % (query_bucket, values[:-1])
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            # self.assertEqual(sorted(actual_result['results']), sorted([v['name'] for v in expected_item_values]),
            #                 'Results expected:%s, actual: %s' % (expected_item_values, actual_result['results']))
            diffs = DeepDiff(actual_result['results'], [v['name'] for v in expected_item_values], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

    def test_insert_returning_raw(self):
        keys = ['%s%s' % (k, str(uuid.uuid4())[:5]) for k in range(10)]
        expected_item_values = [{'name': 'return_%s' % v} for v in range(10)]
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            values = ''
            for k, v in zip(keys, expected_item_values):
                inserted = v
                if isinstance(v, str):
                    inserted = '"%s"' % v
                if v is None:
                    inserted = 'null'
                values += '("%s", %s),' % (k, inserted)
            self.query = 'insert into %s (key , value) VALUES %s RETURNING RAW name' % (query_bucket, values[:-1])
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            # self.assertEqual(sorted(actual_result['results']), sorted([v['name'] for v in expected_item_values]),
            #    'Results expected:%s, actual: %s' % (expected_item_values, actual_result['results']))
            diffs = DeepDiff(actual_result['results'], [v['name'] for v in expected_item_values], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

    def test_insert_returning_alias(self):
        keys = ['%s%s' % (k, str(uuid.uuid4())[:5]) for k in range(10)]
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            expected_item_values = [{'name': 'return_%s' % v} for v in range(10)]
            values = ''
            for k, v in zip(keys, expected_item_values):
                inserted = v
                if isinstance(v, str):
                    inserted = '"%s"' % v
                if v is None:
                    inserted = 'null'
                values += '("%s", %s),' % (k, inserted)
            self.query = 'insert into %s (key , value) VALUES %s RETURNING name as new_name' % (
            query_bucket, values[:-1])
            actual_result = self.run_cbq_query()
            # modified to support the alias
            expected_item_values = [{'new_name': 'return_%s' % v} for v in range(10)]
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            diffs = DeepDiff(actual_result['results'], expected_item_values, ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

    def test_insert_values_returning_elements(self):
        keys = ['%s%s' % (k, str(uuid.uuid4())[:5]) for k in range(10)]
        expected_item_values = [{'name': 'return_%s' % v} for v in range(10)]
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            values = ''
            for k, v in zip(keys, expected_item_values):
                inserted = v
                if isinstance(v, str):
                    inserted = '"%s"' % v
                if v is None:
                    inserted = 'null'
                values += 'VALUES ("%s", %s),' % (k, inserted)
            self.query = 'insert into %s (key , value) %s RETURNING ELEMENT name' % (query_bucket, values[:-1])
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            # self.assertEqual(sorted(actual_result['results']), sorted([v['name'] for v in expected_item_values]),
            #                 'Results expected:%s, actual: %s' % (expected_item_values, actual_result['results']))
            diffs = DeepDiff(actual_result['results'], [v['name'] for v in expected_item_values], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

    def test_prepared_insert_values_returning_elements(self):
        keys = ['%s%s' % (k, str(uuid.uuid4())[:5]) for k in range(10)]
        expected_item_values = [{'name': 'return_%s' % v} for v in range(10)]
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            values = ''
            for k, v in zip(keys, expected_item_values):
                inserted = v
                if isinstance(v, str):
                    inserted = '"%s"' % v
                if v is None:
                    inserted = 'null'
                values += 'VALUES ("%s", %s),' % (k, inserted)
            self.query = 'insert into %s (key , value) %s RETURNING ELEMENT name' % (query_bucket, values[:-1])
            if self.named_prepare:
                self.named_prepare = "prepare2_" + bucket.name
                self.query = "PREPARE %s from %s" % (self.named_prepare, self.query)
            else:
                self.query = "PREPARE %s" % self.query
            prepared = self.run_cbq_query(query=self.query)['results'][0]
            actual_result = self.run_cbq_query(query=prepared, is_prepared=True)
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            # self.assertEqual(sorted(actual_result['results']), sorted([v['name'] for v in expected_item_values]),
            #    'Results expected:%s, actual: %s' % (expected_item_values, actual_result['results']))
            diffs = DeepDiff(actual_result['results'], [v['name'] for v in expected_item_values], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

    def test_insert_returning_elements_long(self):
        keys = ['%s%s' % (k, str(uuid.uuid4())[:2] * 120) for k in range(10)]
        expected_item_values = [{'name': 'return_%s' % v} for v in range(10)]
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            values = ''
            for k, v in zip(keys, expected_item_values):
                inserted = v
                if isinstance(v, str):
                    inserted = '"%s"' % v
                if v is None:
                    inserted = 'null'
                values += '("%s", %s),' % (k, inserted)
            self.query = 'insert into %s (key , value) VALUES %s RETURNING ELEMENT name' % (query_bucket, values[:-1])
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            # self.assertEqual(sorted(actual_result['results']), sorted([v['name'] for v in expected_item_values]),
            #                 'Results expected:%s, actual: %s' % (expected_item_values, actual_result['results']))
            diffs = DeepDiff(actual_result['results'], [v['name'] for v in expected_item_values], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

    def test_insert_returning_raw_long(self):
        keys = ['%s%s' % (k, str(uuid.uuid4())[:2] * 120) for k in range(10)]
        expected_item_values = [{'name': 'return_%s' % v} for v in range(10)]
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            values = ''
            for k, v in zip(keys, expected_item_values):
                inserted = v
                if isinstance(v, str):
                    inserted = '"%s"' % v
                if v is None:
                    inserted = 'null'
                values += '("%s", %s),' % (k, inserted)
            self.query = 'insert into %s (key , value) VALUES %s RETURNING RAW name' % (query_bucket, values[:-1])
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            # self.assertEqual(sorted(actual_result['results']), sorted([v['name'] for v in expected_item_values]),
            #    'Results expected:%s, actual: %s' % (expected_item_values, actual_result['results']))
            diffs = DeepDiff(actual_result['results'], [v['name'] for v in expected_item_values], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

    def test_insert_values_returning_elements_long(self):
        keys = ['%s%s' % (k, str(uuid.uuid4())[:2] * 120) for k in range(10)]
        expected_item_values = [{'name': 'return_%s' % v} for v in range(10)]
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            values = ''
            for k, v in zip(keys, expected_item_values):
                inserted = v
                if isinstance(v, str):
                    inserted = '"%s"' % v
                if v is None:
                    inserted = 'null'
                values += 'VALUES ("%s", %s),' % (k, inserted)
            self.query = 'insert into %s (key , value) %s RETURNING ELEMENT name' % (query_bucket, values[:-1])
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            # self.assertEqual(sorted(actual_result['results']), sorted([v['name'] for v in expected_item_values]),
            #                 'Results expected:%s, actual: %s' % (expected_item_values, actual_result['results']))
            diffs = DeepDiff(actual_result['results'], [v['name'] for v in expected_item_values], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

    def test_insert_returning_elements_long_value(self):
        keys = ['%s%s' % (k, str(uuid.uuid4())[:1]) for k in range(10)]
        expected_item_values = [{'name': 'return_%s' % str(v) * 240} for v in range(10)]
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            values = ''
            for k, v in zip(keys, expected_item_values):
                inserted = v
                if isinstance(v, str):
                    inserted = '"%s"' % v
                if v is None:
                    inserted = 'null'
                values += '("%s", %s),' % (k, inserted)
            self.query = 'insert into %s (key , value) VALUES %s RETURNING ELEMENT name' % (query_bucket, values[:-1])
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            if self.use_rest:
                # self.assertEqual(sorted(actual_result['results']), sorted([v['name'] for v in expected_item_values]),
                #             'Results expected:%s, actual: %s' % (expected_item_values, actual_result['results']))
                diffs = DeepDiff(actual_result['results'], [v['name'] for v in expected_item_values], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)

    def test_insert_values_returning_elements_long_value(self):
        keys = ['%s%s' % (k, str(uuid.uuid4())[:1]) for k in range(10)]
        expected_item_values = [{'name': 'return_%s' % str(v) * 240} for v in range(10)]
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            values = ''
            for k, v in zip(keys, expected_item_values):
                inserted = v
                if isinstance(v, str):
                    inserted = '"%s"' % v
                if v is None:
                    inserted = 'null'
                values += 'VALUES ("%s", %s),' % (k, inserted)
            self.query = 'insert into %s (key , value) %s RETURNING ELEMENT name' % (query_bucket, values[:-1])
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            # self.assertEqual(sorted(actual_result['results']), sorted([v['name'] for v in expected_item_values]),
            #                 'Results expected:%s, actual: %s' % (expected_item_values, actual_result['results']))
            diffs = DeepDiff(actual_result['results'], [v['name'] for v in expected_item_values], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

    def test_insert_values_prepare_returning_elements(self):
        keys = ['%s%s' % (k, str(uuid.uuid4())[:1]) for k in range(10)]
        expected_item_values = [{'name': 'return_%s' % str(v)} for v in range(10)]
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            values = ''
            for k, v in zip(keys, expected_item_values):
                inserted = v
                if isinstance(v, str):
                    inserted = '"%s"' % v
                if v is None:
                    inserted = 'null'
                values += 'VALUES ("%s", %s),' % (k, inserted)
            query = 'insert into %s (key , value) %s RETURNING ELEMENT name' % (query_bucket, values[:-1])
            prepared = self.run_cbq_query(query='PREPARE %s' % query)['results'][0]
            actual_result = self.run_cbq_query(query=prepared, is_prepared=True)
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            # self.assertEqual(sorted(actual_result['results']), sorted([v['name'] for v in expected_item_values]),
            #                 'Results expected:%s, actual: %s' % (expected_item_values, actual_result['results']))
            diffs = DeepDiff(actual_result['results'], [v['name'] for v in expected_item_values], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

    def test_insert_returning_nulls(self):
        keys = ['%s%s' % (k, str(uuid.uuid4())[:5]) for k in range(10)]
        expected_item_values = [{'name': 'return_%s' % v} for v in range(10)]
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            values = ''
            for k, v in zip(keys, expected_item_values):
                inserted = v
                if isinstance(v, str):
                    inserted = '"%s"' % v
                if v is None:
                    inserted = 'null'
                values += '("%s", %s),' % (k, inserted)
            self.query = 'insert into %s (key , value) VALUES %s RETURNING ELEMENT city' % (query_bucket, values[:-1])
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.assertTrue({v for v in actual_result['results']} == {None},
                            'Results expected:%s, actual: %s' % ({None}, actual_result['results']))

    def test_insert_json(self):
        num_docs = self.input.param('num_docs', 10)
        keys = []
        values = []
        self.fail_if_no_buckets()
        for gen_load in self.gens_load:
            gen = copy.deepcopy(gen_load)
            if len(keys) == num_docs:
                break
            for i in range(gen.end):
                if len(keys) == num_docs:
                    break
                key, value = next(gen)
                key = "insert_json" + key
                for query_bucket in self.query_buckets:
                    self.query = 'INSERT into %s (key, value) VALUES ("%s", %s)' % (query_bucket, key, value)
                    actual_result = self.run_cbq_query()
                    self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
                keys.append(key)
                values.append(json.loads(value))
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            self.query = 'select * from %s %s use keys %s' % (query_bucket, bucket.name, keys)
            actual_result = self.run_cbq_query()
            expected_result = [{bucket.name: doc} for doc in values[:num_docs]]
            actual_result = actual_result['results']
            self._delete_ids(actual_result)
            self._delete_ids(expected_result)
            diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

    def test_prepared_insert_json(self):
        num_docs = self.input.param('num_docs', 10)
        keys = []
        values = []
        self.fail_if_no_buckets()
        for gen_load in self.gens_load:
            gen = copy.deepcopy(gen_load)
            if len(keys) == num_docs:
                break
            for i in range(gen.end):
                if len(keys) == num_docs:
                    break
                # i=i+1
                key, value = next(gen)
                key = "insert_json" + key
                for query_bucket in self.query_buckets:
                    query = 'INSERT into %s (key, value) VALUES ("%s", %s)' % (query_bucket, key, value)
                    if self.named_prepare:
                        prefix = 'insert%s' % str(uuid.uuid4())[:5]
                        i = i + 1
                        self.named_prepare = "prepare_" + prefix + str(i)
                        query = "PREPARE %s from %s" % (self.named_prepare, query)
                    else:
                        query = "PREPARE %s" % query
                    prepared = self.run_cbq_query(query=query)['results'][0]
                    # prepared = self.run_cbq_query(query='PREPARE %s' % query)['results'][0]
                    actual_result = self.run_cbq_query(query=prepared, is_prepared=True)
                    self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
                keys.append(key)
                values.append(json.loads(value))
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            self.query = 'select * from %s %s use keys %s' % (query_bucket, bucket.name, keys)
            actual_result = self.run_cbq_query()
            expected_result = values
            actual_result = actual_result['results']
            actual_result = [doc[bucket.name] for doc in actual_result]
            self._delete_ids(actual_result)
            self._delete_ids(expected_result)
            self.assertEqual(actual_result, expected_result,
                             'Item did not appear')

    def test_prepared_insert_with_select(self):
        num_docs = self.input.param('num_docs', 10)
        keys, values = self._insert_gen_keys(num_docs, prefix="select_i")
        prefix = 'insert%s' % str(uuid.uuid4())[:5]
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            for i in range(num_docs):
                self.query = 'insert into %s (key "%s_%s", value {"name": name}) select name from %s ' \
                             'use keys ["%s"]' % (query_bucket, prefix, str(i), query_bucket, keys[i])
                if self.named_prepare:
                    i = i + 1
                    self.named_prepare = "prepare_" + prefix + str(i) + bucket.name
                    self.query = "PREPARE %s from %s" % (self.named_prepare, self.query)
                else:
                    self.query = "PREPARE %s" % self.query
                prepared = self.run_cbq_query(query=self.query)['results'][0]
                actual_result = self.run_cbq_query(query=prepared, is_prepared=True)
                # actual_result = self.run_cbq_query()
                self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            self.query = 'select * from %s as  %s use keys [%s]' % (query_bucket, bucket.name,
                                                                    ','.join(['"%s_%s"' % (prefix, i) for i in
                                                                              range(num_docs)]))
            actual_result = self.run_cbq_query()
            expected_result = [{bucket.name: {'name': doc['name']}} for doc in values[:num_docs]]
            actual_result = actual_result['results']
            self._delete_ids(actual_result)
            self._delete_ids(expected_result)
            diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

    def test_insert_with_select(self):
        num_docs = self.input.param('num_docs', 10)
        keys, values = self._insert_gen_keys(num_docs, prefix="select_i")
        prefix = 'insert%s' % str(uuid.uuid4())[:5]
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            for i in range(num_docs):
                self.query = 'insert into %s (key "%s_%s", value {"name": name}) select name from %s use keys ["%s"]' % (
                query_bucket, prefix, str(i), query_bucket, keys[i])
                actual_result = self.run_cbq_query()
                self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            self.query = 'select * from %s %s use keys [%s]' % (
            query_bucket, bucket.name, ','.join(['"%s_%s"' % (prefix, i) for i in range(num_docs)]))
            actual_result = self.run_cbq_query()
            expected_result = [{bucket.name: {'name': doc['name']}} for doc in values[:num_docs]]
            # print("--> expected_result:{}".format(expected_result))
            # print("--> actual_result:{}".format(actual_result))
            expected_result = sorted(expected_result, key=(lambda x: x[bucket.name]['name']))
            actual_result = sorted(actual_result['results'], key=(lambda x: x[bucket.name]['name']))
            self._delete_ids(actual_result)
            self._delete_ids(expected_result)
            self.assertEqual(actual_result, expected_result, 'Item did not appear')

    ###################################################################################################################
    #
    # UPSERT NON-DOC VALUES
    #
    ###################################################################################################################

    def test_upsert_non_doc_bool(self):
        expected_item_value = False
        key = 'key_bool%s' % str(uuid.uuid4())[:5]
        self._common_check(key, expected_item_value, upsert=True)
        expected_item_value = True
        self._common_check(key, expected_item_value, upsert=True)

    def test_upsert_non_doc_int(self):
        expected_item_value = 456
        key = 'key_int%s' % str(uuid.uuid4())[:5]
        self._common_check(key, expected_item_value, upsert=True)
        expected_item_value = 1024
        self._common_check(key, expected_item_value, upsert=True)

    def test_upsert_non_doc_str(self):
        key = 'key_str%s' % str(uuid.uuid4())[:5]
        expected_item_value = 'auto_value'
        self._common_check(key, expected_item_value, upsert=True)
        expected_item_value = 'edited_value'
        self._common_check(key, expected_item_value, upsert=True)

    def test_upsert_non_doc_array(self):
        key = 'key_array%s' % str(uuid.uuid4())[:5]
        expected_item_value = ['first1', 'second1']
        self._common_check(key, expected_item_value, upsert=True)
        expected_item_value = ['third', 'fourth']
        self._common_check(key, expected_item_value, upsert=True)

    ###################################################################################################################
    #
    # UPSERT JSON VALUES
    #
    ###################################################################################################################

    def test_upsert_json(self):
        key = 'key_json%s' % str(uuid.uuid4())[:5]
        expected_item_value = {'name': 'Automation_1'}
        self._common_check(key, expected_item_value, upsert=True)
        expected_item_value = {'name': 'Automation_2'}
        self._common_check(key, expected_item_value, upsert=True)

    def test_upsert_json_null(self):
        key = 'key_json%s' % str(uuid.uuid4())[:5]
        expected_item_value = {'name': 'Automation_1'}
        self._common_check(key, expected_item_value, upsert=True)
        expected_item_value = None
        self._common_check(key, expected_item_value, upsert=True)

    def test_upsert_json_long_value(self):
        key = 'key_json%s' % str(uuid.uuid4())[:5]
        expected_item_value = {'name': 'Automation_1'}
        self._common_check(key, expected_item_value, upsert=True)
        expected_item_value = {'name': str(uuid.uuid4())[:4] * 60}
        self._common_check(key, expected_item_value, upsert=True)

    def test_upsert_json_long_keys(self):
        key = 'key_json%s' % (str(uuid.uuid4())[:4] * 60)
        expected_item_value = {'name': 'Automation_1'}
        self._common_check(key, expected_item_value, upsert=True)
        expected_item_value = {'name': 'Automation_2'}
        self._common_check(key, expected_item_value, upsert=True)

    def test_upsert_returning_elements(self):
        keys = ['%s%s' % (k, str(uuid.uuid4())[:5]) for k in range(10)]
        expected_item_values = [{'name': 'return_%s' % v} for v in range(10)]
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            values = ''
            for k, v in zip(keys, expected_item_values):
                inserted = v
                if isinstance(v, str):
                    inserted = '"%s"' % v
                if v is None:
                    inserted = 'null'
                values += '("%s", %s),' % (k, inserted)
            self.query = 'upsert into %s (key , value) VALUES %s RETURNING ELEMENT name' % (query_bucket, values[:-1])
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.assertEqual(sorted(actual_result['results']), sorted([v['name'] for v in expected_item_values]),
                             'Results expected:%s, actual: %s' % (expected_item_values, actual_result['results']))

    def test_upsert_with_select(self):
        num_docs = self.input.param('num_docs', 10)
        keys = []
        values = []
        self.fail_if_no_buckets()
        for gen_load in self.gens_load:
            gen = copy.deepcopy(gen_load)
            if len(keys) == num_docs:
                break
            for i in range(gen.end):
                if len(keys) == num_docs:
                    break
                key, value = next(gen)
                key = "upsert_json" + key
                for query_bucket in self.query_buckets:
                    self.query = 'upsert into %s (key, value) values  ("%s", %s)' % (query_bucket, key, value)
                    actual_result = self.run_cbq_query()
                    self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
                keys.append(key)
                values.append(json.loads(value))
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            self.query = 'select * from %s %s use keys %s' % (query_bucket, bucket.name, keys)
            actual_result = self.run_cbq_query()
            expected_result = [{bucket.name: doc} for doc in values[:num_docs]]
            actual_result = actual_result['results']
            self._delete_ids(actual_result)
            self._delete_ids(expected_result)
            diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

    def test_prepared_upsert_with_select(self):
        num_docs = self.input.param('num_docs', 10)
        keys = []
        values = []
        self.fail_if_no_buckets()
        for gen_load in self.gens_load:
            gen = copy.deepcopy(gen_load)
            if len(keys) == num_docs:
                break
            for i in range(gen.end):
                if len(keys) == num_docs:
                    break
                key, value = next(gen)
                key = "upsert_json" + key
                for query_bucket, bucket in zip(self.query_buckets, self.buckets):
                    self.query = 'upsert into %s (key, value) values  ("%s", %s)' % (query_bucket, key, value)
                    if self.named_prepare:
                        self.named_prepare = "prepare_" + bucket.name + str(uuid.uuid4())[:3]
                        self.query = "PREPARE %s from %s" % (self.named_prepare, self.query)
                    else:
                        self.query = "PREPARE %s" % self.query
                    prepared = self.run_cbq_query(query=self.query)['results'][0]
                    actual_result = self.run_cbq_query(query=prepared, is_prepared=True)
                    self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
                keys.append(key)
                values.append(json.loads(value))
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            self.query = 'select * from %s %s use keys %s' % (query_bucket, bucket.name, keys)
            actual_result = self.run_cbq_query()
            expected_result = [{bucket.name: doc} for doc in values[:num_docs]]
            actual_result = actual_result['results']
            self._delete_ids(actual_result)
            self._delete_ids(expected_result)
            diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

    def test_upsert_returning_elements_long_value(self):
        keys = ['%s%s' % (k, str(uuid.uuid4())[:1]) for k in range(10)]
        expected_item_values = [{'name': 'return_%s' % str(v) * 240} for v in range(10)]
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            values = ''
            for k, v in zip(keys, expected_item_values):
                inserted = v
                if isinstance(v, str):
                    inserted = '"%s"' % v
                if v is None:
                    inserted = 'null'
                values += '("%s", %s),' % (k, inserted)
            self.query = 'upsert into %s (key , value) VALUES %s RETURNING ELEMENT name' % (query_bucket, values[:-1])
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            if self.use_rest:
                self.assertEqual(sorted(actual_result['results']), sorted([v['name'] for v in expected_item_values]),
                                 'Results expected:%s, actual: %s' % (expected_item_values, actual_result['results']))

    def items_check(self, prefix, vls, num_docs):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'select * from %s use keys %s' % (query_bucket, ','.join(["%s%s" % (prefix, i)
                                                                                   for i in range(num_docs)]))
            actual_result = self.run_cbq_query()
            expected_result = vls
            diffs = DeepDiff(actual_result['results'], expected_result, ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

    ###################################################################################################################
    #
    # DELETE
    #
    ###################################################################################################################

    def test_delete_keys_clause(self):
        num_docs = self.input.param('docs_to_delete', 3)
        key_prefix = 'automation%s' % str(uuid.uuid4())[:5]
        value = 'n1ql automation'
        self._common_insert(['%s%s' % (key_prefix, i) for i in range(self.num_items)],
                            [value] * self.num_items)
        keys_to_delete = ['%s%s' % (key_prefix, i) for i in range(num_docs)]
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'delete from %s  use keys %s' % (query_bucket, keys_to_delete)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self._keys_are_deleted(keys_to_delete)

    def test_delete_keys_explain_clause(self):
        num_docs = self.input.param('docs_to_delete', 3)
        key_prefix = 'automation%s' % str(uuid.uuid4())[:5]
        keys_to_delete = ['%s%s' % (key_prefix, i) for i in range(num_docs)]
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'explain delete from %s  use keys %s' % (query_bucket, keys_to_delete)
            actual_result = self.run_cbq_query()
            self.log.info(actual_result["results"])
            plan = self.ExplainPlanHelper(actual_result)
            self.assertTrue(plan["~children"][0]["#operator"] == "KeyScan",
                            "KeysScan is not being used in delete query")

    def test_delete_keys_use_index_clause(self):
        num_docs = self.input.param('docs_to_delete', 3)
        key_prefix = 'automation%s' % str(uuid.uuid4())[:5]
        value = {'name': 'n1ql automation'}
        value_to_delete = {'name': 'n1ql deletion'}
        self._common_insert(['%s%s' % (key_prefix, i) for i in range(self.num_items - num_docs)],
                            [value] * (self.num_items - num_docs))
        self._common_insert(['%s%s' % (key_prefix, i) for i in range(self.num_items - num_docs, self.num_items)],
                            [value_to_delete] * num_docs)
        index_name = 'automation%s' % str(uuid.uuid4())[:5]
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            self.query = 'create index %s on %s(name) USING %s' % (index_name, query_bucket, self.index_type)
            self.run_cbq_query()
            self._wait_for_index_online(bucket, index_name)
        try:
            for query_bucket in self.query_buckets:
                self.query = 'delete from %s  use index(%s using %s) where job_title="n1ql deletion"' % (query_bucket,
                                                                                                         index_name,
                                                                                                         self.index_type)
                actual_result = self.run_cbq_query()
                self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
                self.query = 'select * from %s where job_title="n1ql deletion"' % query_bucket
                actual_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], [], 'Query was not run successfully')
        finally:
            for query_bucket in self.query_buckets:
                self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                try:
                    self.run_cbq_query()
                except:
                    pass

    def test_prepared_delete_keys_use_index_clause(self):
        num_docs = self.input.param('docs_to_delete', 3)
        key_prefix = 'automation%s' % str(uuid.uuid4())[:5]
        value = {'name': 'n1ql automation'}
        value_to_delete = {'name': 'n1ql deletion'}
        self._common_insert(['%s%s' % (key_prefix, i) for i in range(self.num_items - num_docs)],
                            [value] * (self.num_items - num_docs))
        self._common_insert(['%s%s' % (key_prefix, i) for i in range(self.num_items - num_docs, self.num_items)],
                            [value_to_delete] * (num_docs))
        index_name = 'automation%s' % str(uuid.uuid4())[:5]
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            self.query = 'create index %s on %s(name) USING %s' % (index_name, query_bucket, self.index_type)
            self.run_cbq_query()
            self._wait_for_index_online(bucket, index_name)
        try:
            for query_bucket, bucket in zip(self.query_buckets, self.buckets):
                self.query = 'delete from %s  use index(%s using %s) where job_title="n1ql deletion"' % (query_bucket,
                                                                                                         index_name,
                                                                                                         self.index_type)
                if self.named_prepare:
                    self.named_prepare = "prepare_" + bucket.name
                    self.query = "PREPARE %s from %s" % (self.named_prepare, self.query)
                else:
                    self.query = "PREPARE %s" % self.query
                prepared = self.run_cbq_query(query=self.query)['results'][0]
                actual_result = self.run_cbq_query(query=prepared, is_prepared=True)
                # actual_result = self.run_cbq_query()
                self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
                self.query = 'select * from %s where job_title="n1ql deletion"' % query_bucket
                actual_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], [], 'Query was not run successfully')
        finally:
            for query_bucket in self.query_buckets:
                self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                try:
                    self.run_cbq_query()
                except:
                    pass

    def test_delete_where_clause_non_doc(self):
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            try:
                self.run_cbq_query("CREATE PRIMARY INDEX ON `%s` USING %s" % (query_bucket, self.index_type.lower()))
                self.wait_for_index_present(bucket, '#primary', {}, self.index_type.lower())
            except:
                pass

        num_docs = self.input.param('docs_to_delete', 3)
        key_prefix = 'automation%s' % str(uuid.uuid4())[:5]
        value = 'n1ql automation'
        value_to_delete = 'n1ql deletion'
        self._common_insert(['%s%s' % (key_prefix, i) for i in range(self.num_items - num_docs)],
                            [value] * (self.num_items - num_docs))
        self._common_insert(['%s%s' % (key_prefix, i) for i in range(self.num_items - num_docs, self.num_items)],
                            [value_to_delete] * (num_docs))
        keys_to_delete = ['%s%s' % (key_prefix, i) for i in range(self.num_items - num_docs, self.num_items)]

        test_dict = dict()
        index_type = self.index_type.lower()
        indexes = []
        for bucket in self.buckets:
            indexes.append({'name': '#primary', 'bucket': bucket.name, 'fields': [],
                            'state': 'online', 'using': index_type, 'is_primary': True})

        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            test_dict[bucket.name] = {"indexes": indexes, "pre_queries": [],
                                      "queries": ['delete from %s d where d="%s"' % (query_bucket, value_to_delete)],
                                      "post_queries": [],
                                      "asserts": [lambda x: self.assertEqual(x['q_res'][0]['status'], 'success',
                                                                             'Query was not run successfully')],
                                      "cleanups": []}

        self.query_runner(test_dict)
        self._keys_are_deleted(keys_to_delete)

    def test_delete_where_clause_json(self):
        keys, values = self._insert_gen_keys(self.num_items, prefix='delete_where')
        keys_to_delete = [keys[i] for i in range(len(keys)) if values[i]["job_title"] == 'Engineer']
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'delete from %s where job_title="Engineer"' % query_bucket
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self._keys_are_deleted(keys_to_delete)

    def test_delete_where_clause_json_not_equal(self):
        keys, values = self._insert_gen_keys(self.num_items, prefix='delete_where')
        keys_to_delete = [keys[i] for i in range(len(keys)) if values[i]["job_title"] != 'Engineer']
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'delete from %s where job_title!="Engineer"' % query_bucket
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self._keys_are_deleted(keys_to_delete)

    def test_delete_where_clause_json_less_equal(self):
        keys, values = self._insert_gen_keys(self.num_items, prefix='delete_where')
        keys_to_delete = [keys[i] for i in range(len(keys)) if values[i]["join_day"] <= 1]
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'delete from %s where join_day<=1' % query_bucket
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self._keys_are_deleted(keys_to_delete)

    def test_prepared_delete_where_clause_json_less_equal(self):
        keys, values = self._insert_gen_keys(self.num_items, prefix='delete_where')
        keys_to_delete = [keys[i] for i in range(len(keys)) if values[i]["join_day"] <= 1]
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            self._wait_for_index_online(bucket, "#primary")
            self.query = 'delete from %s where join_day <=1' % query_bucket
            if self.named_prepare:
                self.named_prepare = "test_prepared_delete_where" + bucket.name
                self.query = "PREPARE %s from %s" % (self.named_prepare, self.query)
            else:
                self.query = "PREPARE %s" % self.query
            prepared = self.run_cbq_query(query=self.query)['results'][0]
            actual_result = self.run_cbq_query(query=prepared, is_prepared=True)
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self._keys_are_deleted(keys_to_delete)

    def test_delete_where_satisfy_clause_json(self):
        keys, values = self._insert_gen_keys(self.num_items, prefix='delete_sat')
        keys_to_delete = [keys[i] for i in range(len(keys))
                          if len([vm for vm in values[i]["VMs"] if vm["RAM"] == 1]) > 0]
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'delete from %s d where ANY vm IN d.VMs SATISFIES vm.RAM = 1 END' % query_bucket
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self._keys_are_deleted(keys_to_delete)

    def test_delete_limit_keys(self):
        keys, values = self._insert_gen_keys(self.num_items, prefix='delete_limit')
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'select count(*) as actual from %s where job_title="Engineer"' % query_bucket
            self.run_cbq_query()
            self.sleep(5, 'wait for index')
            actual_result = self.run_cbq_query()
            current_docs = actual_result['results'][0]['actual']
            self.query = 'delete from %s where job_title="Engineer" LIMIT 1' % query_bucket
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select count(*) as actual from %s where job_title="Engineer"' % query_bucket
            self.run_cbq_query()
            self.sleep(5, 'wait for index')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['results'][0]['actual'], current_docs - 1, 'Item was not deleted')

    def test_delete_limit_where(self):
        keys, values = self._insert_gen_keys(self.num_items, prefix='delete_limitwhere')
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'select count(*) as actual from %s where job_title="Engineer"' % query_bucket
            self.run_cbq_query()
            self.sleep(5, 'wait for index')
            actual_result = self.run_cbq_query()
            current_docs = actual_result['results'][0]['actual']
            self.query = 'delete from %s where job_title="Engineer" LIMIT 1' % query_bucket
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select count(*) as actual from %s where job_title="Engineer"' % query_bucket
            self.run_cbq_query()
            self.sleep(5, 'wait for index')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['results'][0]['actual'], current_docs - 1, 'Item was not deleted')

    def test_delete_limit_where_offset(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.run_cbq_query(f'CREATE INDEX ix1 IF NOT EXISTS ON {query_bucket}(id ASC)')
            self.run_cbq_query(f'INSERT INTO {query_bucket} (key k, value v) SELECT uuid() as k , {{"name": "San Francisco", "id": d}} as v FROM array_range(1,11) d')
            self.run_cbq_query(f'DELETE FROM {query_bucket} WHERE id is not null LIMIT 3 OFFSET 5')
            result = self.run_cbq_query(f'SELECT id FROM {query_bucket} ORDER BY id')
            expected = [{'id': 1}, {'id': 2}, {'id': 3}, {'id': 4}, {'id': 5}, {'id': 9}, {'id': 10}]
            self.assertEqual(result['results'], expected, f"We expected: {expected} but got: {result['results']}")

    def delete_where_clause_json_hints(self, idx_name):
        keys, values = self._insert_gen_keys(self.num_items, prefix='delete_where_hints')
        keys_to_delete = [keys[i] for i in range(len(keys)) if values[i]["job_title"] == 'Engineer']
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'delete from %s use index(%s using %s) where job_title="Engineer"' % (
                query_bucket, idx_name, self.index_type)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self._keys_are_deleted(keys_to_delete)

    def test_delete_where(self):
        self.key1 = "n1qlDelW1"
        self.key2 = "n1qlDelW2"
        self.doc1 = {"type": "abc"}
        self.doc2 = {"type": "def"}
        self.query = 'INSERT into %s (key , value) VALUES ("%s", %s),("%s",%s)' % (
            self.bucket1, self.key1, self.doc1, self.key2, self.doc2)
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertEqual(actual_result["metrics"]["mutationCount"], 2, "Mutation Count is correct")
        self.query = "delete from %s WHERE type = 'def' " % (self.bucket1)
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result["metrics"]["mutationCount"], 1, "Mutation Count is correct")
        self.query = "delete from %s WHERE type = 'abc' " % (self.bucket1)
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result["metrics"]["mutationCount"], 1, "Mutation Count is correct")

    ###################################################################################################################
    #
    # MERGE
    #
    ###################################################################################################################

    def test_merge_delete_match(self):

        self.assertTrue(len(self.buckets) >= 2, 'Test needs at least 2 buckets')
        keys, _ = self._insert_gen_keys(self.num_items, prefix='merge_delete')
        self.query = 'MERGE INTO %s USING %s sd on key meta().id when matched then delete where meta(sd).id = "%s"' % (
        self.bucket0, self.bucket1, keys[0])
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertEqual(actual_result['metrics']['mutationCount'], 1, 'Merge deleted more data than intended')
        self.query = 'select * from default where meta().id = "%s"' % (keys[0])
        self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertEqual(len(actual_result['results']), 0, 'Merge did not delete the item intended')

    def test_prepared_merge_delete_match(self):
        self.assertTrue(len(self.buckets) >= 2, 'Test needs at least 2 buckets')
        keys, _ = self._insert_gen_keys(self.num_items, prefix='merge_delete')
        query = 'MERGE INTO %s USING %s on key meta().id when matched then delete where meta(%s).id = "%s"' % (
            self.bucket0, self.bucket1, self.bucket1, keys[0])
        prepared = self.run_cbq_query(query='PREPARE %s' % query)['results'][0]
        actual_result = self.run_cbq_query(query=prepared, is_prepared=True)
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.query = 'select * from default where meta().id = "%s"' % (keys[0])
        self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertEqual(len(actual_result['results']), 0, 'Merge did not delete the item intended')

    def test_merge_delete_match_limit(self):
        self.assertTrue(len(self.buckets) >= 2, 'Test needs at least 2 buckets')
        keys, _ = self._insert_gen_keys(self.num_items, prefix='delete_match')
        self.query = 'MERGE INTO %s USING %s on key meta().id when matched then delete limit 1' % (
            self.bucket0, self.bucket1)
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertEqual(actual_result['metrics']['mutationCount'], 1, 'Merge deleted more data than intended')

    def test_merge_delete_where_match(self):
        self.assertTrue(len(self.buckets) >= 2, 'Test needs at least 2 buckets')
        keys, _ = self._insert_gen_keys(self.num_items, prefix='merge_delete_where')
        self.query = 'MERGE INTO %s USING %s s0 on key meta().id when matched then delete where meta(s0).id = "%s" AND s0.name = "%s"' % (
            self.bucket0, self.bucket1, keys[0], "employee-1")
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertEqual(actual_result['metrics']['mutationCount'], 1, 'Merge deleted more data than intended')
        self.query = 'select * from default where meta().id = "%s"' % (keys[0])
        self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertEqual(len(actual_result['results']), 0, 'Merge did not delete the item intended')

    def test_merge_update_match_set(self):
        self.assertTrue(len(self.buckets) >= 2, 'Test needs at least 2 buckets')
        key = "test"
        value = '{"name": "new1"}'
        self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.bucket1, key, value)
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        value = '{"name": "new2"}'
        self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.bucket0, "new1", value)
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        new_name = 'edit'
        self.query = 'MERGE INTO %s b1 USING %s b2 on key name when matched then update set b1.name="%s"' % (
            self.bucket0, self.bucket1, new_name)
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.query = 'SELECT name FROM %s' % (self.bucket0)
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertEqual(actual_result['results'][0]['name'], new_name, 'Name was not updated')

    def test_merge_update_match_unset(self):
        self.assertTrue(len(self.buckets) >= 2, 'Test needs at least 2 buckets')
        key = "test"
        value = '{"name": "new1"}'
        self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.bucket1, key, value)
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        value = '{"name": "new2"}'
        self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.bucket0, "new1", value)
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.query = 'MERGE INTO %s b1 USING %s b2 on key name when matched then update unset b1.name' % (
            self.bucket0, self.bucket1)
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.query = 'SELECT * FROM %s ' % (self.bucket0)
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertTrue('name' not in actual_result['results'], 'Name was not unset')

    def test_merge_not_match_insert(self):
        self.assertTrue(len(self.buckets) >= 2, 'Test needs at least 2 buckets')
        key = "test"
        value = '{"name": "new1"}'
        self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.bucket1, key, value)
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.query = 'MERGE INTO %s b1 USING %s b2 on key b2.%s when not matched then insert %s' % (

            self.bucket0, self.bucket1, 'name', '{"name": "new"}')
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.query = 'SELECT count(*) FROM %s' % (self.bucket0)
        actual_result = self.run_cbq_query()
        self.assertEqual(len(actual_result['results']), 1, 'Query was not run successfully')

    def test_merge_select_not_match_insert(self):
        self.assertTrue(len(self.buckets) >= 2, 'Test needs at least 2 buckets')
        key = "test"
        value = '{"name": "new1"}'
        self.query = 'INSERT into {0} (key , value) VALUES ("{1}", {2})'.format(self.bucket1, key, value)
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.query = 'MERGE INTO {0} b1 USING (select * from {1} b2) as s on key b2.{2} when not matched then insert {3}'.format(
            self.bucket0, self.bucket1, 'name', '{"name": "new"}')
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.query = 'SELECT count(*) FROM %s ' % (self.bucket0)
        actual_result = self.run_cbq_query()
        self.assertEqual(len(actual_result['results']), 1, 'Query was not run successfully')

    ###################################################################################################################
    #
    # UPDATE
    #
    ###################################################################################################################

    def test_update_keys_clause(self):
        num_docs_update = self.input.param('docs_to_update', 3)
        num_docs = self.input.param('num_docs', 10)
        keys, _ = self._insert_gen_keys(num_docs, prefix='update_keys')
        keys_to_update = keys[:num_docs_update]
        self.fail_if_no_buckets()
        updated_value = 'new_name'
        for query_bucket in self.query_buckets:
            self.query = 'update %s use keys %s set name="%s"' % (query_bucket, keys_to_update, updated_value)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select name from %s use keys %s' % (query_bucket, keys_to_update)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['results'], [{'name': updated_value}] * num_docs_update,
                             'Names were not changed')

    def test_update_keys_clause_unset(self):
        num_docs_update = self.input.param('docs_to_update', 3)
        num_docs = self.input.param('num_docs', 10)
        keys, _ = self._insert_gen_keys(num_docs, prefix='updateunset_keys')
        keys_to_update = keys[:num_docs_update]
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'update %s use keys %s unset name' % (query_bucket, keys_to_update)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select name from %s use keys %s' % (query_bucket, keys_to_update)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['results'], [{}] * num_docs_update, 'Names were not unset')

    def test_update_keys_clause_unset_multiple(self):
        num_docs_update = self.input.param('docs_to_update', 3)
        num_docs = self.input.param('num_docs', 10)
        keys, _ = self._insert_gen_keys(num_docs, prefix='update_m_unset_keys')
        keys_to_update = keys[:num_docs_update]
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'update %s use keys %s unset name, join_yr' % (query_bucket, keys_to_update)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select name, join_yr from %s use keys %s' % (query_bucket, keys_to_update)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['results'], [{}] * num_docs_update, 'Names were not unset')

    def test_update_keys_multiple_attrs_clause(self):
        num_docs_update = self.input.param('docs_to_update', 3)
        num_docs = self.input.param('num_docs', 10)
        keys, _ = self._insert_gen_keys(num_docs, prefix='update_multi_keys')
        keys_to_update = keys[:num_docs_update]
        updated_value = 'new_name'
        updated_value_int = 29
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'update %s use keys %s set name="%s", join_day=%s' % (
                query_bucket, keys_to_update, updated_value, updated_value_int)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select name, join_day from %s use keys %s' % (query_bucket, keys_to_update)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['results'],
                             [{'name': updated_value, 'join_day': updated_value_int}] * num_docs_update,
                             'Attrs were not changed')

    def test_update_long_keys(self):
        num_docs_update = self.input.param('docs_to_update', 3)
        num_docs = self.input.param('num_docs', 10)
        keys, _ = self._insert_gen_keys(num_docs, prefix='update_keys' + str(uuid.uuid4())[:4] * 50)
        keys_to_update = keys[:num_docs_update]
        updated_value = 'new_name'
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'update %s use keys %s set name="%s"' % (query_bucket, keys_to_update, updated_value)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select name from %s use keys %s' % (query_bucket, keys_to_update)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['results'], [{'name': updated_value}] * num_docs_update,
                             'Names were not changed')

    def test_update_where(self):
        num_docs_update = self.input.param('docs_to_update', 3)
        num_docs = self.input.param('num_docs', 10)
        _, values = self._insert_gen_keys(num_docs, prefix='update_where' + str(uuid.uuid4())[:4])
        updated_value = 'new_name'
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'update %s set name="%s" where join_day=1 returning name' % (query_bucket, updated_value)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select name from %s where join_day=1' % query_bucket
            actual_result = self.run_cbq_query()
            self.assertFalse([doc for doc in actual_result['results'] if doc['name'] != updated_value],
                             'Names were not changed')

    def test_update_where_not_equal(self):
        num_docs_update = self.input.param('docs_to_update', 3)
        num_docs = self.input.param('num_docs', 10)
        _, values = self._insert_gen_keys(num_docs, prefix='update_where')
        updated_value = 'new_name'
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'update %s set name="%s" where join_day<>1 returning name' % (query_bucket, updated_value)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select name from %s where join_day<>1' % query_bucket
            actual_result = self.run_cbq_query()
            self.assertFalse([doc for doc in actual_result['results'] if doc['name'] != updated_value],
                             'Names were not changed')

    def test_update_where_long_values(self):
        num_docs_update = self.input.param('docs_to_update', 3)
        num_docs = self.input.param('num_docs', 10)
        _, values = self._insert_gen_keys(num_docs, prefix='updatelong' + str(uuid.uuid4())[:5])
        updated_value = 'new_name' * 30
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'update %s set name="%s" where join_day=1 returning element name' % (
                query_bucket, updated_value)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select name from %s where join_day=1' % query_bucket
            actual_result = self.run_cbq_query()
            self.assertFalse([doc for doc in actual_result['results'] if doc['name'] != updated_value],
                             'Names were not changed')

    def test_update_between_where(self):
        num_docs_update = self.input.param('docs_to_update', 3)
        num_docs = self.input.param('num_docs', 10)
        _, values = self._insert_gen_keys(num_docs, prefix='update_where')
        updated_value = 'new_name'
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'update %s set name="%s" where join_day between 1 and 2 returning name' % (
                query_bucket, updated_value)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select name from %s where join_day between 1 and 2' % query_bucket
            actual_result = self.run_cbq_query()
            self.assertFalse([doc for doc in actual_result['results'] if doc['name'] != updated_value],
                             'Names were not changed')

    def test_update_where_limit(self):
        num_docs_update = self.input.param('docs_to_update', 3)
        num_docs = self.input.param('num_docs', 10)
        _, values = self._insert_gen_keys(num_docs, prefix='update_wherelimit')
        updated_value = 'new_name'
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'update %s set name="%s" where join_day=1 limit 1' % (query_bucket, updated_value)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select name from %s where join_day=1' % query_bucket
            actual_result = self.run_cbq_query()
            self.assertTrue(len([doc for doc in actual_result['results'] if doc['name'] == updated_value]) >= 1,
                            'Names were not changed')

    def test_update_keys_for(self):
        num_docs_update = self.input.param('docs_to_update', 3)
        num_docs = self.input.param('num_docs', 10)
        keys, _ = self._insert_gen_keys(num_docs, prefix='update_for')
        keys_to_update = keys[:-num_docs_update]
        updated_value = 'new_name'
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'update %s use keys %s set vm.os="%s" for vm in VMs END returning VMs' % (
                query_bucket, keys_to_update, updated_value)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select VMs from %s use keys %s' % (query_bucket, keys_to_update)
            actual_result = self.run_cbq_query()
            self.assertTrue([row for row in actual_result['results']
                             if len([vm['os'] for vm in row['VMs']
                                     if vm['os'] == updated_value]) == len(row['VMs'])], 'Os of vms were not changed')

    def test_update_keys_for_null(self):
        num_docs_update = self.input.param('docs_to_update', 3)
        num_docs = self.input.param('num_docs', 10)
        keys, _ = self._insert_gen_keys(num_docs, prefix='update_for')
        keys_to_update = keys[:-num_docs_update]
        updated_value = 'null'
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'update %s use keys %s set vm.os="%s" for vm in VMs END returning VMs' % (
                query_bucket, keys_to_update, updated_value)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select VMs from %s use keys %s' % (query_bucket, keys_to_update)
            actual_result = self.run_cbq_query()
            self.assertTrue([row for row in actual_result['results']
                             if len([vm['os'] for vm in row['VMs']
                                     if vm['os'] == updated_value]) == len(row['VMs'])], 'Os of vms were not changed')

    def test_update_keys_for_where(self):
        num_docs_update = self.input.param('docs_to_update', 3)
        num_docs = self.input.param('num_docs', 10)
        keys, _ = self._insert_gen_keys(num_docs, prefix='updatefor_where')
        keys_to_update = keys[:num_docs_update]
        updated_value = 'new_name'
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'update %s use keys %s set vm.os="%s" for vm in VMs when vm.os="ubuntu" END returning VMs' % (
                query_bucket, keys_to_update, updated_value)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select VMs from %s use keys %s' % (query_bucket, keys_to_update)
            actual_result = self.run_cbq_query()
            self.assertTrue([row for row in actual_result['results']
                             if len([vm['os'] for vm in row['VMs']
                                     if vm['os'] == updated_value]) == 1], 'Os of vms were not changed')

    def update_keys_clause_hints(self):
        num_docs = self.input.param('num_docs', 10)
        keys, _ = self._insert_gen_keys(num_docs, prefix='update_keys_hints_%s' % str(uuid.uuid4())[:4])
        updated_value = 'new_name'
        index_name = 'idx_name'
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            self.query = "CREATE INDEX %s ON %s(name) USING %s" % (index_name, query_bucket, self.index_type)
            self.run_cbq_query()
            self._wait_for_index_online(bucket, index_name)
            self.query = 'update %s use index(%s using %s) set name="%s"' % (
                query_bucket, index_name, self.index_type, updated_value)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select distinct name from %s use index(%s using %s)' % (query_bucket, index_name,
                                                                                  self.index_type)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['results'], [{"name": "new_name"}], 'Names were not changed')
            self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
            self.run_cbq_query()

    def update_where_hints(self):
        num_docs = self.input.param('num_docs', 10)
        _, values = self._insert_gen_keys(num_docs, prefix='update_where %s' % str(uuid.uuid4())[:4])
        updated_value = 'new_name'
        index_name = 'idx_name'
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            self.query = "CREATE INDEX %s ON %s(join_day) USING %s" % (index_name, query_bucket, self.index_type)
            self.run_cbq_query()
            self._wait_for_index_online(bucket, index_name)
            self.query = 'update %s use index(%s using %s) set name="%s" where join_day=1 returning name' % (
                query_bucket, index_name, self.index_type, updated_value)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select name from %s use index(%s using %s) where join_day=1' % (
                query_bucket, index_name, self.index_type)
            actual_result = self.run_cbq_query()
            self.assertFalse([doc for doc in actual_result['results'] if doc['name'] != updated_value],
                             'Names were not changed')
            self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
            self.run_cbq_query()

    ###################################################################################################################
    #
    # WITH INDEX
    ###################################################################################################################

    def test_with_hints(self):
        indexes = []
        index_name_prefix = "hint_index_" + str(uuid.uuid4())[:4]
        method_name = self.input.param('to_run', 'test_any')
        index_fields = self.input.param("index_field", '').split(';')
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            for field in index_fields:
                index_name = '%s%s' % (index_name_prefix, field.split('.')[0].split('[')[0])
                self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (
                    index_name, query_bucket, ','.join(field.split(';')), self.index_type)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                indexes.append(index_name)
        try:
            for indx in indexes:
                fn = getattr(self, method_name)
                if "update_where_hints" in str(fn) or "update_keys_clause_hints" in str(fn):
                    fn()
                else:
                    fn(indx)

        finally:
            for query_bucket in self.query_buckets:
                for indx in indexes:
                    self.query = "DROP INDEX %s ON %s USING %s" % (indx, query_bucket, self.index_type)
                try:
                    self.run_cbq_query()
                except:
                    pass

    def test_with_index(self):
        indexes = []
        index_name_prefix = "dml_index_" + str(uuid.uuid4())[:4]
        method_name = self.input.param('to_run', '')
        index_fields = self.input.param("index_field", '').split(';')
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            for field in index_fields:
                index_name = '%s%s' % (index_name_prefix, field.split('.')[0].split('[')[0].replace('~', '_'))
                self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (
                    index_name, query_bucket, ','.join(field.split('~')), self.index_type)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                indexes.append(index_name)
        try:
            for indx in indexes:
                fn = getattr(self, method_name)
                fn()
        finally:
            for query_bucket in self.query_buckets:
                for indx in indexes:
                    self.query = "DROP INDEX %s ON %s USING %s" % (indx, query_bucket, self.index_type)
                try:
                    self.run_cbq_query()
                except:
                    pass

    ###################################################################################################################
    # USE key with positional/named param
    ###################################################################################################################

    def test_use_keys_named_param(self):
        select_query = "SELECT META(g).id, META(g).cas, META(g).expiration, g AS doc FROM $a AS g USE KEYS $b"
        insert_query = "INSERT INTO $a AS d VALUES($b, $c, $d)"
        upsert_query = "UPSERT INTO $a AS d VALUES($b, $c, $d)"
        update_query = "UPDATE $a AS d USE KEYS $b SET d = $c, META(d).expiration = $c.expiration"
        delete_query = "DELETE FROM $a AS d USE KEYS $b"

        params1 = {'$a': '"default"', '$b': '"k101"', '$c': '{"a":"k101"}', '$d': '{"expiration":604800}'}
        params2 = {'$a': '"default"', '$b': '"k101"', '$c': '{"b":"k101"}', '$d': '{"expiration":704800}'}

        self.run_cbq_query(query=insert_query, query_params=params1)
        result = self.run_cbq_query(query=select_query, query_params=params1)
        self.assertEqual(result['results'][0]['doc'], {'a': 'k101'})

        self.run_cbq_query(query=update_query, query_params=params2)
        result = self.run_cbq_query(query=select_query, query_params=params2)
        self.assertEqual(result['results'][0]['doc'], {'b': 'k101'})

        self.run_cbq_query(query=delete_query, query_params=params1)
        result = self.run_cbq_query(query=select_query, query_params=params1)
        self.assertEqual(result['results'], [])

        self.run_cbq_query(query=upsert_query, query_params=params1)
        result = self.run_cbq_query(query=select_query, query_params=params1)
        self.assertEqual(result['results'][0]['doc'], {'a': 'k101'})

    def test_use_keys_pos_param(self):
        select_query = "SELECT META(g).id, META(g).cas, META(g).expiration, g AS doc FROM $1 AS g USE KEYS $2"
        insert_query = "INSERT INTO $1 AS d VALUES($2, $3, $4)"
        upsert_query = "UPSERT INTO $1 AS d VALUES($2, $3, $4)"
        update_query = "UPDATE $1 AS d USE KEYS $2 SET d = $3, META(d).expiration = $4.expiration"
        delete_query = "DELETE FROM $1 AS d USE KEYS $2"

        params1 = {'args': '["default", "k101", {"a":"k101"}, {"expiration":604800}]'}
        params2 = {'args': '["default", "k101", {"b":"k101"}, {"expiration":704800}]'}

        self.run_cbq_query(query=insert_query, query_params=params1)
        result = self.run_cbq_query(query=select_query, query_params=params1)
        self.assertEqual(result['results'][0]['doc'], {'a': 'k101'})

        self.run_cbq_query(query=update_query, query_params=params2)
        result = self.run_cbq_query(query=select_query, query_params=params2)
        self.assertEqual(result['results'][0]['doc'], {'b': 'k101'})

        self.run_cbq_query(query=delete_query, query_params=params1)
        result = self.run_cbq_query(query=select_query, query_params=params1)
        self.assertEqual(result['results'], [])

        self.run_cbq_query(query=upsert_query, query_params=params1)
        result = self.run_cbq_query(query=select_query, query_params=params1)
        self.assertEqual(result['results'][0]['doc'], {'a': 'k101'})
            
    ###################################################################################################################
    # BINARY data
    ###################################################################################################################

    def test_update_binary(self):
        error_code = 12030
        error_message = "UPDATE of binary document is not supported: k01"
        shell = RemoteMachineShellConnection(self.master)
        command = f"{self.path}/cbc create -u Administrator -U 'couchbase://localhost/default' -P password k01 -V '<img src=\"https://www.couchbase.com/webfiles/1616069826389/images/couchbase_logo_black.svg\"/>'"
        shell.execute_command(command)
        fail_update_binary_statements = [
            'UPDATE default d USE KEYS "k01" SET d = d',
            'UPDATE default d USE KEYS "k01" SET d.x = 1'
            ]
        for update_binary in fail_update_binary_statements:
            try:
                self.run_cbq_query(update_binary)
                self.fail(f"Query did not fail as expected with error: {error_message}")
            except CBQError as ex:
                error = self.process_CBQE(ex)
                self.assertEqual(error['code'], error_code)
                self.assertEqual(error['msg'], error_message)
        self.run_cbq_query('INSERT INTO default (KEY, VALUE) VALUES ("k02", {"x":0, "y":10})')
        pass_update_binary_statements = [
            'UPDATE default d SET d.x = 10 WHERE d.y = 10',
            'UPDATE default d SET d.x = 10 WHERE IS_BINARY(d) = false'
        ]
        for update_binary in pass_update_binary_statements:
            try:
                self.run_cbq_query(update_binary)
                result = self.run_cbq_query('SELECT RAW d.x FROM default d WHERE IS_BINARY(d) = false')
                self.assertEqual(result['results'], [10])
            except Exception as ex:
                self.log.error(f"Update with binary failed: {ex}")
                self.fail()

    def test_insert_binary(self):
        error_code = 12030
        error_message = "INSERT of binary document is not supported: k02"
        shell = RemoteMachineShellConnection(self.master)
        command = f"{self.path}/cbc create -u Administrator -U 'couchbase://localhost/default' -P password k01 -V '<img src=\"https://www.couchbase.com/webfiles/1616069826389/images/couchbase_logo_black.svg\"/>'"
        shell.execute_command(command)
        insert_binary = 'INSERT INTO default (KEY "k02", VALUE d) SELECT d FROM default as d USE KEYS "k01"'
        try:
            self.run_cbq_query(insert_binary)
            self.fail(f"Query did not fail as expected with error: {error_message}")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], error_code)
            self.assertEqual(error['msg'], error_message)

    def test_upsert_binary(self):
        error_code = 12030
        error_message = "UPSERT of binary document is not supported: k02"
        shell = RemoteMachineShellConnection(self.master)
        command = f"{self.path}/cbc create -u Administrator -U 'couchbase://localhost/default' -P password k01 -V '<img src=\"https://www.couchbase.com/webfiles/1616069826389/images/couchbase_logo_black.svg\"/>'"
        shell.execute_command(command)
        insert_binary = 'UPSERT INTO default (KEY "k02", VALUE d) SELECT d FROM default as d USE KEYS "k01"'
        try:
            self.run_cbq_query(insert_binary)
            self.fail(f"Query did not fail as expected with error: {error_message}")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], error_code)
            self.assertEqual(error['msg'], error_message)

    ###################################################################################################################
    # LET clauses in DML statements
    ###################################################################################################################

    def test_update_with_let(self):
        """Test UPDATE statement with LET clause"""
        # Create a collection in default._default
        collection_name = "test_collection_let"
        self.run_cbq_query(f'CREATE COLLECTION `{collection_name}` IF NOT EXISTS', query_context='default._default')
        self.sleep(3)

        # Insert test data
        test_data = [
            {"id": "doc1", "name": "John", "age": 30, "salary": 50000},
            {"id": "doc2", "name": "Jane", "age": 25, "salary": 45000},
            {"id": "doc3", "name": "Bob", "age": 35, "salary": 60000}
        ]

        for doc in test_data:
            self.run_cbq_query(f'INSERT INTO default._default.{collection_name} (KEY, VALUE) VALUES ("{doc["id"]}", {json.dumps(doc)})')

        # Test UPDATE with LET clause
        self.query = f'''
        UPDATE default._default.{collection_name} d
        LET bonus = CASE
                WHEN d.age > 30 THEN 5000
                ELSE 2000
            END,
            old_salary = d.salary,
            new_salary = d.salary + bonus
        SET d.salary = new_salary,
            d.bonus = bonus
        WHERE d.age > 25
        RETURNING d.id, d.name, d.salary, d.bonus, old_salary
        '''

        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'UPDATE with LET query was not run successfully')

        # Verify results
        expected_results = [
            {"id": "doc1", "name": "John", "salary": 52000, "bonus": 2000, "old_salary": 50000},
            {"id": "doc3", "name": "Bob", "salary": 65000, "bonus": 5000, "old_salary": 60000}
        ]

        actual_results = sorted(actual_result['results'], key=lambda x: x['id'])
        expected_results = sorted(expected_results, key=lambda x: x['id'])

        self.assertEqual(actual_results, expected_results, 'UPDATE with LET results do not match expected')

        # Clean up
        self.run_cbq_query(f'DROP COLLECTION `{collection_name}` IF EXISTS', query_context='default._default')

    def test_delete_with_let(self):
        """Test DELETE statement with LET clause"""
        # Create a collection in default._default
        collection_name = "test_collection_let_delete"
        self.run_cbq_query(f'CREATE COLLECTION `{collection_name}` IF NOT EXISTS', query_context='default._default')
        self.sleep(3)

        # Insert test data
        test_data = [
            {"id": "doc1", "name": "John", "age": 30, "department": "Engineering"},
            {"id": "doc2", "name": "Jane", "age": 25, "department": "Marketing"},
            {"id": "doc3", "name": "Bob", "age": 35, "department": "Engineering"},
            {"id": "doc4", "name": "Alice", "age": 28, "department": "Sales"}
        ]

        for doc in test_data:
            self.run_cbq_query(f'INSERT INTO default._default.{collection_name} (KEY, VALUE) VALUES ("{doc["id"]}", {json.dumps(doc)})')

        # Test DELETE with LET clause
        self.query = f'''
        DELETE FROM default._default.{collection_name} d
        LET engineering_department = "Engineering",
            senior_age = 30
        WHERE d.department = engineering_department AND d.age > senior_age
        RETURNING d.id, d.name, d.department, d.age
        '''

        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'DELETE with LET query was not run successfully')

        # Verify deleted documents
        expected_deleted = [
            {"id": "doc3", "name": "Bob", "department": "Engineering", "age": 35}
        ]

        self.assertEqual(actual_result['results'], expected_deleted, 'DELETE with LET results do not match expected')

        # Verify remaining documents
        remaining_query = f'SELECT d.id, d.name, d.department, d.age FROM default._default.{collection_name} d ORDER BY d.id'
        remaining_result = self.run_cbq_query(remaining_query)

        expected_remaining = [
            {"id": "doc1", "name": "John", "department": "Engineering", "age": 30},
            {"id": "doc2", "name": "Jane", "department": "Marketing", "age": 25},
            {"id": "doc4", "name": "Alice", "department": "Sales", "age": 28}
        ]

        self.assertEqual(remaining_result['results'], expected_remaining, 'Remaining documents do not match expected')

        # Clean up
        self.run_cbq_query(f'DROP COLLECTION `{collection_name}` IF EXISTS', query_context='default._default')

    def test_merge_with_let(self):
        """Test MERGE statement with LET clause"""
        # Create collections in default._default
        source_collection = "test_collection_let_merge_source"
        target_collection = "test_collection_let_merge_target"

        self.run_cbq_query(f'CREATE COLLECTION `{source_collection}` IF NOT EXISTS', query_context='default._default')
        self.run_cbq_query(f'CREATE COLLECTION `{target_collection}` IF NOT EXISTS', query_context='default._default')
        self.sleep(3)

        # Insert source data
        source_data = [
            {"id": "emp1", "name": "John", "salary": 50000, "bonus_rate": 0.1},
            {"id": "emp2", "name": "Jane", "salary": 45000, "bonus_rate": 0.15},
            {"id": "emp3", "name": "Bob", "salary": 60000, "bonus_rate": 0.12}
        ]

        for doc in source_data:
            self.run_cbq_query(f'INSERT INTO default._default.{source_collection} (KEY, VALUE) VALUES ("{doc["id"]}", {json.dumps(doc)})')

        # Insert target data (some existing, some new)
        target_data = [
            {"id": "emp1", "name": "John", "salary": 48000, "total_comp": 52800},
            {"id": "emp4", "name": "Alice", "salary": 55000, "total_comp": 60500}
        ]

        for doc in target_data:
            self.run_cbq_query(f'INSERT INTO default._default.{target_collection} (KEY, VALUE) VALUES ("{doc["id"]}", {json.dumps(doc)})')

        # Test MERGE with LET clause
        self.query = f'''
        MERGE INTO default._default.{target_collection} t
        USING default._default.{source_collection} s
        ON t.id = s.id
        LET new_salary = s.salary,
            bonus_amount = s.salary * s.bonus_rate,
            total_compensation = new_salary + bonus_amount
        WHEN MATCHED THEN
            UPDATE SET t.salary = new_salary,
                       t.bonus = bonus_amount,
                       t.total_comp = total_compensation
        WHEN NOT MATCHED THEN
            INSERT (KEY s.id, VALUE {{
                "id": s.id,
                "name": s.name,
                "salary": new_salary,
                "bonus": bonus_amount,
                "total_comp": total_compensation
            }})
        RETURNING t.id, t.name, t.salary, t.bonus, t.total_comp
        '''

        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'MERGE with LET query was not run successfully')

        # Verify results
        expected_results = [
            {'id': 'emp1', 'name': 'John', 'salary': 50000, 'bonus': 5000, 'total_comp': 55000},
            {'id': 'emp2', 'name': 'Jane'},
            {'id': 'emp3', 'name': 'Bob'}
        ]

        actual_results = sorted(actual_result['results'], key=lambda x: x['id'])
        expected_results = sorted(expected_results, key=lambda x: x['id'])

        self.assertEqual(actual_results, expected_results, 'MERGE with LET results do not match expected')

        # Verify final state of target collection
        final_query = f'SELECT t.id, t.name, t.salary, t.bonus, t.total_comp FROM default._default.{target_collection} t ORDER BY t.id'
        final_result = self.run_cbq_query(final_query)

        expected_final = [
            {"id": "emp1", "name": "John", "salary": 50000, "bonus": 5000, "total_comp": 55000},
            {"id": "emp2", "name": "Jane"},
            {"id": "emp3", "name": "Bob"},
            {"id": "emp4", "name": "Alice", "salary": 55000, "total_comp": 60500}
        ]

        self.assertEqual(final_result['results'], expected_final, 'Final state of target collection does not match expected')

        # Clean up
        self.run_cbq_query(f'DROP COLLECTION `{source_collection}` IF EXISTS', query_context='default._default')
        self.run_cbq_query(f'DROP COLLECTION `{target_collection}` IF EXISTS', query_context='default._default')

    def test_update_with_let_complex(self):
        """Test UPDATE statement with complex LET clause including array operations"""
        # Create a collection in default._default
        collection_name = "test_collection_let_complex"
        self.run_cbq_query(f'CREATE COLLECTION `{collection_name}` IF NOT EXISTS', query_context='default._default')
        self.sleep(3)

        # Insert test data with arrays
        test_data = [
            {"id": "doc1", "name": "John", "scores": [85, 90, 78], "grades": ["B", "A", "C"]},
            {"id": "doc2", "name": "Jane", "scores": [92, 88, 95], "grades": ["A", "B", "A"]},
            {"id": "doc3", "name": "Bob", "scores": [75, 82, 79], "grades": ["C", "B", "C"]}
        ]

        for doc in test_data:
            self.run_cbq_query(f'INSERT INTO default._default.{collection_name} (KEY, VALUE) VALUES ("{doc["id"]}", {json.dumps(doc)})')

        # Test UPDATE with complex LET clause
        self.query = f'''
        UPDATE default._default.{collection_name} d
        LET avg_score = ARRAY_AVG(d.scores),
            max_score = ARRAY_MAX(d.scores),
            min_score = ARRAY_MIN(d.scores),
            score_range = max_score - min_score,
            performance_level = CASE
                WHEN avg_score >= 90 THEN "Excellent"
                WHEN avg_score >= 80 THEN "Good"
                WHEN avg_score >= 70 THEN "Average"
                ELSE "Needs Improvement"
            END,
            grade_counts = ARRAY_LENGTH(ARRAY_REMOVE(d.grades, 'B', 'C'))
        SET d.average_score = avg_score,
            d.highest_score = max_score,
            d.lowest_score = min_score,
            d.score_range = score_range,
            d.performance = performance_level,
            d.a_grades_count = grade_counts
        WHERE d.name IS NOT MISSING
        RETURNING d.id, d.name, d.average_score, d.performance, d.a_grades_count
        '''

        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'UPDATE with complex LET query was not run successfully')

        # Verify results
        expected_results = [
            {"id": "doc1", "name": "John", "average_score": 84.33333333333333, "performance": "Good", "a_grades_count": 1},
            {"id": "doc2", "name": "Jane", "average_score": 91.66666666666667, "performance": "Excellent", "a_grades_count": 2},
            {"id": "doc3", "name": "Bob", "average_score": 78.66666666666667, "performance": "Average", "a_grades_count": 0}
        ]

        actual_results = sorted(actual_result['results'], key=lambda x: x['id'])
        expected_results = sorted(expected_results, key=lambda x: x['id'])

        self.assertEqual(actual_results, expected_results, 'UPDATE with complex LET results do not match expected')

        # Clean up
        self.run_cbq_query(f'DROP COLLECTION `{collection_name}` IF EXISTS', query_context='default._default')