import time
import copy
import uuid
import collections
import json
from remote.remote_util import RemoteMachineShellConnection
from tuqquery.tuq import QueryTests
from couchbase_helper.documentgenerator import DocumentGenerator


TIMEOUT_DELETED = 300

class DMLQueryTests(QueryTests):
    def setUp(self):
        super(DMLQueryTests, self).setUp()
        self.directory = self.input.param("directory", "/tmp/tuq_data")
        #self.shell.execute_command("killall cbq-engine")
        #for bucket in self.buckets:
        #    self.cluster.bucket_flush(self.master, bucket=bucket,
        #                          timeout=self.wait_timeout * 5)
        #self.create_primary_index_for_3_0_and_greater()

############################################################################################################################
#
# INSERT NON-DOC VALUES
#
############################################################################################################################

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

############################################################################################################################
#
# INSERT JSON VALUES
#
############################################################################################################################

    def test_insert_json(self):
        num_docs = self.input.param('num_docs', 10)
        keys = []
        values = []
        for gen_load in self.gens_load:
            gen = copy.deepcopy(gen_load)
            if len(keys) == num_docs:
                    break
            for i in xrange(gen.end):
                if len(keys) == num_docs:
                    break
                key, value = gen.next()
                key = "insert_json" + key
                for bucket in self.buckets:
                    self.query = 'INSERT into %s (key, value) VALUES ("%s", %s)' % (bucket.name, key, value)
                    actual_result = self.run_cbq_query()
                    self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
                keys.append(key)
                values.append(json.loads(value))
        for bucket in self.buckets:
            self.query = 'select * from %s use keys %s'  % (bucket.name, keys)
            self.run_cbq_query()
            self.sleep(10, 'wait for indexer')
            actual_result = self.run_cbq_query()
            expected_result = sorted([{bucket.name: doc} for doc in values[:num_docs]])
            actual_result = sorted(actual_result['results'])
            self._delete_ids(actual_result)
            self._delete_ids(expected_result)
            self.assertEqual(actual_result, expected_result,
                             'Item did not appear')

    def test_prepared_insert_json(self):
        num_docs = self.input.param('num_docs', 10)
        keys = []
        values = []
        for gen_load in self.gens_load:
            gen = copy.deepcopy(gen_load)
            if len(keys) == num_docs:
                    break
            for i in xrange(gen.end):
                if len(keys) == num_docs:
                    break
                key, value = gen.next()
                key = "insert_json" + key
                for bucket in self.buckets:
                    query = 'INSERT into %s (key, value) VALUES ("%s", %s)' % (bucket.name, key, value)
                    prepared = self.run_cbq_query(query='PREPARE %s' % query)['results'][0]
                    actual_result = self.run_cbq_query(query=prepared, is_prepared=True)
                    self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
                keys.append(key)
                values.append(json.loads(value))
        for bucket in self.buckets:
            self.query = 'select * from %s use keys %s'  % (bucket.name, keys)
            self.run_cbq_query()
            self.sleep(10, 'wait for indexer')
            actual_result = self.run_cbq_query()
            expected_result = sorted([{bucket.name: doc} for doc in values[:num_docs]])
            actual_result = sorted(actual_result['results'])
            self._delete_ids(actual_result)
            self._delete_ids(expected_result)
            self.assertEqual(actual_result, expected_result,
                             'Item did not appear')

    def test_insert_with_select(self):
        num_docs = self.input.param('num_docs', 10)
        keys, values = self._insert_gen_keys(num_docs, prefix="select_i")
        prefix = 'insert%s' % str(uuid.uuid4())[:5]
        for bucket in self.buckets:
            for i in xrange(num_docs):
                self.query = 'insert into %s (key "%s_%s", value {"name": name}) select name from %s use keys ["%s"]'  % (bucket.name, prefix, str(i),
                                                                                              bucket.name, keys[i])
                actual_result = self.run_cbq_query()
                self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        for bucket in self.buckets:
            self.query = 'select * from %s use keys [%s]'  % (bucket.name, ','.join(['"%s_%s"' % (prefix, i) for i in xrange(num_docs)]))
            self.run_cbq_query()
            self.sleep(10, 'wait for indexer')
            actual_result = self.run_cbq_query()
            expected_result = sorted([{bucket.name: {'name': doc['name']}} for doc in values[:num_docs]])
            actual_result = sorted(actual_result['results'])
            self._delete_ids(actual_result)
            self._delete_ids(expected_result)
            self.assertEqual(actual_result, expected_result, 'Item did not appear')

############################################################################################################################
#
# UPSERT NON-DOC VALUES
#
############################################################################################################################

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

############################################################################################################################
#
# UPSERT JSON VALUES
#
############################################################################################################################

    def test_upsert_json(self):
        key = 'key_json%s' % str(uuid.uuid4())[:5]
        expected_item_value = {'name' : 'Automation_1'}
        self._common_check(key, expected_item_value, upsert=True)
        expected_item_value = {'name' : 'Automation_2'}
        self._common_check(key, expected_item_value, upsert=True)

    def test_upsert_with_select(self):
        num_docs = self.input.param('num_docs', 10)
        keys = []
        values = []
        for gen_load in self.gens_load:
            gen = copy.deepcopy(gen_load)
            if len(keys) == num_docs:
                    break
            for i in xrange(gen.end):
                if len(keys) == num_docs:
                    break
                key, value = gen.next()
                key = "upsert_json" + key
                for bucket in self.buckets:
                    self.query = 'upsert into %s (key, value) values  ("%s", %s)' % (bucket.name, key, value)
                    actual_result = self.run_cbq_query()
                    self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
                keys.append(key)
                values.append(json.loads(value))
        for bucket in self.buckets:
            self.query = 'select * from %s use keys %s'  % (bucket.name, keys)
            self.run_cbq_query()
            self.sleep(10, 'wait for indexer')
            actual_result = self.run_cbq_query()
            expected_result = sorted([{bucket.name: doc} for doc in values[:num_docs]])
            actual_result = sorted(actual_result['results'])
            self._delete_ids(actual_result)
            self._delete_ids(expected_result)
            self.assertEqual(actual_result, expected_result,
                             'Item did not appear')

    def test_prepared_upsert_with_select(self):
        num_docs = self.input.param('num_docs', 10)
        keys = []
        values = []
        for gen_load in self.gens_load:
            gen = copy.deepcopy(gen_load)
            if len(keys) == num_docs:
                    break
            for i in xrange(gen.end):
                if len(keys) == num_docs:
                    break
                key, value = gen.next()
                key = "upsert_json" + key
                for bucket in self.buckets:
                    query = 'upsert into %s (key, value) values  ("%s", %s)' % (bucket.name, key, value)
                    prepared = self.run_cbq_query(query='PREPARE %s' % query)['results'][0]
                    actual_result = self.run_cbq_query(query=prepared, is_prepared=True)
                    self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
                keys.append(key)
                values.append(json.loads(value))
        for bucket in self.buckets:
            self.query = 'select * from %s use keys %s'  % (bucket.name, keys)
            self.run_cbq_query()
            self.sleep(10, 'wait for indexer')
            actual_result = self.run_cbq_query()
            expected_result = sorted([{bucket.name: doc} for doc in values[:num_docs]])
            actual_result = sorted(actual_result['results'])
            self._delete_ids(actual_result)
            self._delete_ids(expected_result)
            self.assertEqual(actual_result, expected_result,
                             'Item did not appear')

        def items_check(self, prefix, vls):
            for bucket in self.buckets:
                self.query = 'select * from %s use keys %s'  % (bucket.name, ','.join(["%s%s" % (prefix,i)
                                                                                   for i in xrange(num_docs)]))
                actual_result = self.run_cbq_query()
                expected_result = vls
                self.assertEqual(sorted(actual_result['results']), sorted(expected_result),
                                 'Item did not appear')
############################################################################################################################
#
# DELETE
#
############################################################################################################################

    def test_delete_keys_clause(self):
        num_docs = self.input.param('docs_to_delete', 3)
        key_prefix = 'automation%s' % str(uuid.uuid4())[:5]
        value = 'n1ql automation'
        self._common_insert(['%s%s' % (key_prefix, i) for i in xrange(self.num_items)],
                            [value] * self.num_items)
        keys_to_delete = ['%s%s' % (key_prefix, i) for i in xrange(num_docs)]
        for bucket in self.buckets:
            self.query = 'delete from %s  use keys %s'  % (bucket.name, keys_to_delete)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self._keys_are_deleted(keys_to_delete)

    def test_delete_keys_use_index_clause(self):
        num_docs = self.input.param('docs_to_delete', 3)
        key_prefix = 'automation%s' % str(uuid.uuid4())[:5]
        value = {'name': 'n1ql automation'}
        value_to_delete =  {'name': 'n1ql deletion'}
        self._common_insert(['%s%s' % (key_prefix, i) for i in xrange(self.num_items - num_docs)],
                            [value] * (self.num_items - num_docs))
        self._common_insert(['%s%s' % (key_prefix, i) for i in xrange(self.num_items - num_docs, self.num_items)],
                            [value_to_delete] * (num_docs))
        index_name = 'automation%s' % str(uuid.uuid4())[:5]
        for bucket in self.buckets:
            self.query = 'create index %s on %s(name) USING %s' % (index_name, bucket.name, self.index_type)
            self.run_cbq_query()
        try:
            for bucket in self.buckets:
                self.query = 'delete from %s  use index(%s using %s) where job_title="n1ql deletion"'  % (bucket.name,
                                                                                      index_name, self.index_type)
                actual_result = self.run_cbq_query()
                self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
                self.query = 'select * from %s where job_title="n1ql deletion"'  % (bucket.name)
                actual_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], [], 'Query was not run successfully')
        finally:
            for bucket in self.buckets:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                try:
                    self.run_cbq_query()
                except:
                    pass

    def test_delete_where_clause_non_doc(self):
        num_docs = self.input.param('docs_to_delete', 3)
        key_prefix = 'automation%s' % str(uuid.uuid4())[:5]
        value = 'n1ql automation'
        value_to_delete = 'n1ql deletion'
        self._common_insert(['%s%s' % (key_prefix, i) for i in xrange(self.num_items - num_docs)],
                            [value] * (self.num_items - num_docs))
        self._common_insert(['%s%s' % (key_prefix, i) for i in xrange(self.num_items - num_docs, self.num_items)],
                            [value_to_delete] * (num_docs))
        keys_to_delete = ['%s%s' % (key_prefix, i) for i in xrange(self.num_items - num_docs, self.num_items)]
        for bucket in self.buckets:
            self.query = 'delete from %s d where d="%s"'  % (bucket.name, value_to_delete)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self._keys_are_deleted(keys_to_delete)

    def test_delete_where_clause_json(self):
        keys, values = self._insert_gen_keys(self.num_items, prefix='delete_where')
        keys_to_delete = [keys[i] for i in xrange(len(keys)) if values[i]["job_title"] == 'Engineer']
        for bucket in self.buckets:
            self.query = 'delete from %s where job_title="Engineer"'  % (bucket.name)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self._keys_are_deleted(keys_to_delete)

    def test_prepared_delete_where_clause_json(self):
        keys, values = self._insert_gen_keys(self.num_items, prefix='delete_where')
        keys_to_delete = [keys[i] for i in xrange(len(keys)) if values[i]["job_title"] == 'Engineer']
        for bucket in self.buckets:
            query = 'delete from %s where job_title="Engineer"'  % (bucket.name)
            prepared = self.run_cbq_query(query='PREPARE %s' % query)['results'][0]
            actual_result = self.run_cbq_query(query=prepared, is_prepared=True)
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self._keys_are_deleted(keys_to_delete)

    def test_delete_where_clause_json_not_equal(self):
        keys, values = self._insert_gen_keys(self.num_items, prefix='delete_where')
        keys_to_delete = [keys[i] for i in xrange(len(keys)) if values[i]["job_title"] != 'Engineer']
        for bucket in self.buckets:
            self.query = 'delete from %s where job_title!="Engineer"'  % (bucket.name)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self._keys_are_deleted(keys_to_delete)

    def test_delete_where_clause_json_less_equal(self):
        keys, values = self._insert_gen_keys(self.num_items, prefix='delete_where')
        keys_to_delete = [keys[i] for i in xrange(len(keys)) if values[i]["job_day"] <=1]
        for bucket in self.buckets:
            self.query = 'delete from %s where job_day<=1'  % (bucket.name)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self._keys_are_deleted(keys_to_delete)

    def test_delete_where_satisfy_clause_json(self):
        keys, values = self._insert_gen_keys(self.num_items, prefix='delete_sat')
        keys_to_delete = [keys[i] for i in xrange(len(keys))
                          if len([vm for vm in values[i]["VMs"] if vm["RAM"] == 1]) > 0 ]
        for bucket in self.buckets:
            self.query = 'delete from %s where ANY vm IN %s.VMs SATISFIES vm.RAM = 1 END'  % (bucket.name, bucket.name)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self._keys_are_deleted(keys_to_delete)

    def test_delete_limit_keys(self):
        keys, values = self._insert_gen_keys(self.num_items, prefix='delete_limit')
        for bucket in self.buckets:
            self.query = 'select count(*) as actual from %s where job_title="Engineer"'  % (bucket.name)
            self.run_cbq_query()
            self.sleep(5, 'wait for index')
            actual_result = self.run_cbq_query()
            current_docs = actual_result['results'][0]['actual']
            self.query = 'delete from %s where job_title="Engineer" LIMIT 1'  % (bucket.name)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select count(*) as actual from %s where job_title="Engineer"'  % (bucket.name)
            self.run_cbq_query()
            self.sleep(5, 'wait for index')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['results'][0]['actual'], current_docs - 1, 'Item was not deleted')

    def test_delete_limit_where(self):
        keys, values = self._insert_gen_keys(self.num_items, prefix='delete_limitwhere')
        for bucket in self.buckets:
            self.query = 'select count(*) as actual from %s where job_title="Engineer"'  % (bucket.name)
            self.run_cbq_query()
            self.sleep(5, 'wait for index')
            actual_result = self.run_cbq_query()
            current_docs = actual_result['results'][0]['actual']
            self.query = 'delete from %s where job_title="Engineer" LIMIT 1'  % (bucket.name)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select count(*) as actual from %s where job_title="Engineer"'  % (bucket.name)
            self.run_cbq_query()
            self.sleep(5, 'wait for index')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['results'][0]['actual'], current_docs - 1, 'Item was not deleted')

    def delete_where_clause_json_hints(self, idx_name):
        keys, values = self._insert_gen_keys(self.num_items, prefix='delete_where_hints')
        keys_to_delete = [keys[i] for i in xrange(len(keys)) if values[i]["job_title"] == 'Engineer']
        for bucket in self.buckets:
            self.query = 'delete from %s use index(%s using %s) where job_title="Engineer"'  % (bucket.name, idx_name, self.index_type)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self._keys_are_deleted(keys_to_delete)

############################################################################################################################
#
# MERGE
#
############################################################################################################################

    def test_merge_delete_match(self):
        self.assertTrue(len(self.buckets) >=2, 'Test needs at least 2 buckets')
        keys, _ = self._insert_gen_keys(self.num_items, prefix='merge_delete')
        self.query = 'MERGE INTO %s USING %s on key "%s" when matched then delete'  % (self.buckets[0].name, self.buckets[1].name, keys[0])
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.query = 'SELECT count(*) as rows FROM %s KEY %s'  % (self.buckets[0].name, keys[0])
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertEqual(len(actual_result['results']), 0, 'Query was not run successfully')

    def test_prepared_merge_delete_match(self):
        self.assertTrue(len(self.buckets) >=2, 'Test needs at least 2 buckets')
        keys, _ = self._insert_gen_keys(self.num_items, prefix='merge_delete')
        query = 'MERGE INTO %s USING %s on key "%s" when matched then delete'  % (self.buckets[0].name, self.buckets[1].name, keys[0])
        prepared = self.run_cbq_query(query='PREPARE %s' % query)['results'][0]
        actual_result = self.run_cbq_query(query=prepared, is_prepared=True)
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.query = 'SELECT count(*) as rows FROM %s KEY %s'  % (self.buckets[0].name, keys[0])
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertEqual(len(actual_result['results']), 0, 'Query was not run successfully')

    def test_merge_delete_match_limit(self):
        self.assertTrue(len(self.buckets) >=2, 'Test needs at least 2 buckets')
        keys, _ = self._insert_gen_keys(self.num_items, prefix='delete_match')
        self.query = 'MERGE INTO %s USING %s on key "%s" when matched then delete limit 1'  % (self.buckets[0].name, self.buckets[1].name, keys[0])
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.query = 'SELECT count(*) as rows FROM %s KEY %s'  % (self.buckets[0].name, keys[0])
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertEqual(len(actual_result['results']), 0, 'Query was not run successfully')

    def test_merge_delete_where_match(self):
        self.assertTrue(len(self.buckets) >=2, 'Test needs at least 2 buckets')
        keys, _ = self._insert_gen_keys(self.num_items, prefix='merge_delete_where')
        self.query = 'MERGE INTO %s USING %s on key "%s" when matched then delete where name LIKE "empl%"'  % (self.buckets[0].name, self.buckets[1].name, keys[0])
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.query = 'SELECT count(*) as rows FROM %s KEY %s'  % (self.buckets[0].name, keys[0])
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertEqual(actual_result['results']['rows'], 0, 'Query was not run successfully')

    def test_merge_update_match_set(self):
        self.assertTrue(len(self.buckets) >=2, 'Test needs at least 2 buckets')
        keys, _ = self._insert_gen_keys(self.num_items, prefix='merge_update')
        new_name = 'edited'
        self.query = 'MERGE INTO %s USING %s on key "%s" when matched then update set name="%s"'  % (self.buckets[0].name, self.buckets[1].name, keys[0], new_name)
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.query = 'SELECT name FROM %s KEY %s'  % (self.buckets[0].name, keys[0])
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertEqual(actual_result['results']['name'], new_name, 'Name was not updated')

    def test_merge_update_match_unset(self):
        self.assertTrue(len(self.buckets) >=2, 'Test needs at least 2 buckets')
        keys, _ = self._insert_gen_keys(self.num_items, prefix='merge_unset')
        self.query = 'MERGE INTO %s USING %s on key "%s" when matched then update unset name'  % (self.buckets[0].name, self.buckets[1].name, keys[0])
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.query = 'SELECT * as doc FROM %s KEY %s'  % (self.buckets[0].name, keys[0])
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertTrue('name' not in actual_result['results']['doc'], 'Name was not unset')

    def test_merge_not_match_insert(self):
        self.assertTrue(len(self.buckets) >=2, 'Test needs at least 2 buckets')
        self.query = 'select count(*) as actual from %s where job_title="Engineer"'  % (self.buckets[0].name)
        self.run_cbq_query()
        self.sleep(5, 'wait for index')
        actual_result = self.run_cbq_query()
        current_docs = actual_result['results'][0]['actual']
        new_key = 'NEW'
        self.query = 'MERGE INTO %s USING %s on key %s when not matched then insert %s'  % (
                                                            self.buckets[0].name, self.buckets[1].name, self.buckets[0].name, new_key, '{"name": "new"}')
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.query = 'SELECT count(*) as rows FROM %s KEY %s'  % (self.buckets[0].name, new_key)
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertEqual(actual_result['results']['rows'], 1, 'Query was not run successfully')

    def test_merge_select_not_match_insert(self):
        self.assertTrue(len(self.buckets) >=2, 'Test needs at least 2 buckets')
        self.query = 'select count(*) as actual from %s where job_title="Engineer"'  % (self.buckets[0].name)
        self.run_cbq_query()
        self.sleep(5, 'wait for index')
        actual_result = self.run_cbq_query()
        current_docs = actual_result['results'][0]['actual']
        new_key = 'NEW'
        self.query = 'MERGE INTO %s USING (select * from %s where job_title="Engineer") on key %s when not matched then insert %s'  % (
                                                            self.buckets[0].name, self.buckets[1].name, self.buckets[0].name, new_key, '{"name": "new"}')
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.query = 'SELECT count(*) as rows FROM %s KEY %s'  % (self.buckets[0].name, new_key)
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertEqual(actual_result['results']['rows'], 1, 'Query was not run successfully')
############################################################################################################################
#
# UPDATE
#
############################################################################################################################

    def test_update_keys_clause(self):
        num_docs_update = self.input.param('docs_to_update', 3)
        num_docs = self.input.param('num_docs', 10)
        keys, _ = self._insert_gen_keys(num_docs, prefix='update_keys')
        keys_to_update = keys[:num_docs_update]
        updated_value = 'new_name'
        for bucket in self.buckets:
            self.query = 'update %s use keys %s set name="%s"'  % (bucket.name, keys_to_update, updated_value)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select name from %s use keys %s' % (bucket.name, keys_to_update)
            self.run_cbq_query()
            self.sleep(10, 'wait for index')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['results'],[{'name':updated_value}] * num_docs_update, 'Names were not changed')

    def test_update_where(self):
        num_docs_update = self.input.param('docs_to_update', 3)
        num_docs = self.input.param('num_docs', 10)
        _, values = self._insert_gen_keys(num_docs, prefix='update_where')
        updated_value = 'new_name'
        for bucket in self.buckets:
            self.query = 'update %s set name="%s" where join_day=1 returning name'  % (bucket.name, updated_value)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select name from %s where join_day=1' % (bucket.name)
            self.run_cbq_query()
            self.sleep(10, 'wait for index')
            actual_result = self.run_cbq_query()
            self.assertFalse([doc for doc in actual_result['results'] if doc['name'] != updated_value], 'Names were not changed')

    def test_update_where_limit(self):
        num_docs_update = self.input.param('docs_to_update', 3)
        num_docs = self.input.param('num_docs', 10)
        _, values = self._insert_gen_keys(num_docs, prefix='update_wherelimit')
        updated_value = 'new_name'
        for bucket in self.buckets:
            self.query = 'update %s set name="%s" where join_day=1 limit 1'  % (bucket.name, updated_value)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select name from %s where join_day=1' % (bucket.name)
            self.run_cbq_query()
            self.sleep(10, 'wait for index')
            actual_result = self.run_cbq_query()
            self.assertTrue(len([doc for doc in actual_result['results'] if doc['name'] == updated_value]) >= 1, 'Names were not changed')

    def test_update_keys_for(self):
        num_docs_update = self.input.param('docs_to_update', 3)
        num_docs = self.input.param('num_docs', 10)
        keys, _ = self._insert_gen_keys(num_docs, prefix='update_for')
        keys_to_update = keys[:-num_docs_update]
        updated_value = 'new_name'
        for bucket in self.buckets:
            self.query = 'update %s use keys %s set vm.os="%s" for vm in VMs END returning VMs'  % (bucket.name, keys_to_update, updated_value)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select VMs from %s use keys %s' % (bucket.name, keys_to_update)
            self.run_cbq_query()
            self.sleep(10, 'wait for index')
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
        for bucket in self.buckets:
            self.query = 'update %s use keys %s set vm.os="%s" for vm in VMs when vm.os="ubuntu" END returning VMs'  % (bucket.name, keys_to_update, updated_value)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select VMs from %s use keys %s' % (bucket.name, keys_to_update)
            actual_result = self.run_cbq_query()
            self.assertTrue([row for row in actual_result['results']
                             if len([vm['os'] for vm in row['VMs']
                                     if vm['os'] == updated_value]) == 1], 'Os of vms were not changed')

    def update_keys_clause_hints(self, idx_name):
        num_docs = self.input.param('num_docs', 10)
        keys, _ = self._insert_gen_keys(num_docs, prefix='update_keys_hints')
        updated_value = 'new_name'
        for bucket in self.buckets:
            self.query = 'update %s use index(%s) set name="%s"'  % (bucket.name, idx_name, updated_value)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select name from %s use index(%s using %s)' % (bucket.name, idx_name, self.index_type)
            self.run_cbq_query()
            self.sleep(10, 'wait for index')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['results'],[{'name':updated_value}] * num_docs, 'Names were not changed')

    def update_where_hints(self, idx_name):
        num_docs = self.input.param('num_docs', 10)
        _, values = self._insert_gen_keys(num_docs, prefix='update_where')
        updated_value = 'new_name'
        for bucket in self.buckets:
            self.query = 'update %s use index(%s) set name="%s" where join_day=1 returning name'  % (bucket.name, idx_name, updated_value)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select name from %s use index(%s using %s) where join_day=1' % (bucket.name, idx_name, self.index_type)
            self.run_cbq_query()
            self.sleep(10, 'wait for index')
            actual_result = self.run_cbq_query()
            self.assertFalse([doc for doc in actual_result['results'] if doc['name'] != updated_value], 'Names were not changed')

########################################################################################################################
#
# WITH INDEX
#######################################################################################################################

    def test_with_hints(self):
        indexes = []
        index_name_prefix = "hint_index_" + str(uuid.uuid4())[:4]
        method_name = self.input.param('to_run', 'test_any')
        index_fields = self.input.param("index_field", '').split(';')
        for bucket in self.buckets:
            for field in index_fields:
                index_name = '%s%s' % (index_name_prefix, field.split('.')[0].split('[')[0])
                self.query = "CREATE INDEX %s ON %s(%s) USING %s" % (
                index_name, bucket.name, ','.join(field.split(';')), self.index_type)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                indexes.append(index_name)
        try:
            for indx in indexes:
                fn = getattr(self, method_name)
                fn(indx)
        finally:
            for bucket in self.buckets:
                for indx in indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, indx, self.index_type)
                try:
                    self.run_cbq_query()
                except:
                    pass


############################################################################################################################

    def _insert_gen_keys(self, num_docs, prefix='a1_'):
        def convert(data):
            if isinstance(data, basestring):
                return str(data)
            elif isinstance(data, collections.Mapping):
                return dict(map(convert, data.iteritems()))
            elif isinstance(data, collections.Iterable):
                return type(data)(map(convert, data))
            else:
                return data
        keys = []
        values = []
        for gen_load in self.gens_load:
            gen = copy.deepcopy(gen_load)
            if len(keys) == num_docs:
                    break
            for i in xrange(gen.end):
                if len(keys) == num_docs:
                    break
                key, value = gen.next()
                key = prefix + key
                value = convert(json.loads(value))  
                for bucket in self.buckets:
                    self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (bucket.name, key, value)
                    actual_result = self.run_cbq_query()
                    self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
                keys.append(key)
                values.append(value)
        return keys, values

    def _keys_are_deleted(self, keys):
        end_time = time.time() + TIMEOUT_DELETED
        while time.time() < end_time:
            for bucket in self.buckets:
                self.query = 'select meta(%s).id from %s'  % (bucket.name, bucket.name)
                actual_result = self.run_cbq_query()
                found = False
                for key in keys:
                    if actual_result['results'].count({'id' : key}) != 0:
                        found = True
                        break
                if not found:
                    return
            self.sleep(3)
        self.fail('Keys %s are still present' % keys)

    def _common_insert(self, keys, values):
        for bucket in self.buckets:
            for i in xrange(len(keys)):
                v = '"%s"' % values[i] if isinstance(values[i], str) else values[i]
                self.query = 'INSERT into %s (key , value) VALUES ("%s", "%s")' % (bucket.name, keys[i], values[i])
                actual_result = self.run_cbq_query()
                self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')

    def _common_check(self, key, expected_item_value, upsert=False):
        clause = 'UPSERT' if upsert else 'INSERT'
        for bucket in self.buckets:
            inserted = expected_item_value
            if isinstance(expected_item_value, str):
                inserted = '"%s"' % expected_item_value
            self.query = '%s into %s (key , value) VALUES ("%s", %s)' % (clause, bucket.name, key, inserted)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select * from %s use keys ["%s"]'  % (bucket.name, key)
            try:
                actual_result = self.run_cbq_query()
            except:
                pass
            self.sleep(15, 'Wait for index rebuild')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['results'].count({bucket.name : expected_item_value}), 1,
                             'Item did not appear')

    def load_with_dir(self, generators_load, exp=0, flag=0,
             kv_store=1, only_store_hash=True, batch_size=1, pause_secs=1,
             timeout_secs=30, op_type='create', start_items=0):
        gens_load = {}
        for bucket in self.buckets:
            tmp_gen = []
            for generator_load in generators_load:
                tmp_gen.append(copy.deepcopy(generator_load))
            gens_load[bucket] = copy.deepcopy(tmp_gen)
        items = 0
        for gen_load in gens_load[self.buckets[0]]:
                items += (gen_load.end - gen_load.start)
        shell = RemoteMachineShellConnection(self.master)
        try:
            for bucket in self.buckets:
                self.log.info("%s %s to %s documents..." % (op_type, items, bucket.name))
                self.log.info("Delete directory's content %s/data/default/%s ..." % (self.directory, bucket.name))
                shell.execute_command('rm -rf %s/data/default/*' % self.directory)
                self.log.info("Create directory %s/data/default/%s..." % (self.directory, bucket.name))
                shell.execute_command('mkdir -p %s/data/default/%s' % (self.directory, bucket.name))
                self.log.info("Load %s documents to %s/data/default/%s..." % (items, self.directory, bucket.name))
                for gen_load in gens_load:
                    for i in xrange(gen_load.end):
                        key, value = gen_load.next()
                        out = shell.execute_command("echo '%s' > %s/data/default/%s/%s.json" % (value, self.directory,
                                                                                                bucket.name, key))
                self.log.info("LOAD IS FINISHED")
        finally:
            shell.disconnect()
        self.num_items = items + start_items
        self.log.info("LOAD IS FINISHED")

    def _delete_ids(self, result):
        for item in result:
            if '_id' in item:
                del item['_id']
            for bucket in self.buckets:
                if bucket.name in item and 'id' in item[bucket.name]:
                    del item[bucket.name]['_id']
