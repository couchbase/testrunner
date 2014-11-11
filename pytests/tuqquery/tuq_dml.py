import time
import copy
from remote.remote_util import RemoteMachineShellConnection
from tuqquery.tuq import QueryTests
from couchbase.documentgenerator import DocumentGenerator


TIMEOUT_DELETED = 300

class DMLQueryTests(QueryTests):
    def setUp(self):
        super(DMLQueryTests, self).setUp()
        self.directory = self.input.param("directory", "/tmp/tuq_data")
        self.create_primary_index_for_3_0_and_greater()

############################################################################################################################
#
# INSERT NON-DOC VALUES
#
############################################################################################################################

    def test_insert_non_doc_bool(self):
        expected_item_value = True
        self._common_check('key_bool', expected_item_value)

    def test_insert_non_doc_int(self):
        expected_item_value = 234
        self._common_check('key_int', expected_item_value)

    def test_insert_non_doc_str(self):
        expected_item_value = 'automation_value'
        self._common_check('key_str', expected_item_value)

    def test_insert_non_doc_array(self):
        expected_item_value = ['first', 'second']
        self._common_check('key_array', expected_item_value)

############################################################################################################################
#
# INSERT JSON VALUES
#
############################################################################################################################

    def test_insert_json(self):
        for gen_load in self.gens_load:
            gen = copy.deepcopy(gen_load)
            for i in xrange(gen.end):
                key, value = gen.next()
                for bucket in self.buckets:
                    self.query = 'INSERT into %s key "%s" VALUES %s' % (bucket.name, key, value)
                    actual_result = self.run_cbq_query()
                    self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')

        self.query = 'select * from %s'  % (bucket.name)
        actual_result = self.run_cbq_query()
        full_list = self._generate_full_docs_list(self.gens_load)
        expected_result = [{bucket.name : doc} for doc in full_list]
        self.assertEqual(sorted(actual_result['results']), sorted(expected_result),
                         'Item did not appear')

    def test_insert_with_select(self):
        num_docs = self.input.param('num_docs', 10)
        keys, values = self._insert_gen_keys(num_docs)
        for bucket in self.buckets:
            for i in xrange(num_docs):
                self.query = 'insert into %s key "insert_%s" select name from %s keys ["%s"]'  % (bucket.name, str(i),
                                                                                                  bucket.name, keys[i])
                actual_result = self.run_cbq_query()
                self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')
        for bucket in self.buckets:
            self.query = 'select * from %s keys %s'  % (bucket.name, ','.join(["insert_%s" % i for i in xrange(num_docs)]))
            actual_result = self.run_cbq_query()
            expected_result = values[:num_docs]
            self.assertEqual(sorted(actual_result['results']), sorted(expected_result),
                             'Item did not appear')

############################################################################################################################
#
# UPSERT NON-DOC VALUES
#
############################################################################################################################

    def test_upsert_non_doc_bool(self):
        expected_item_value = True
        self._common_check('key_bool', expected_item_value, upsert=True)
        expected_item_value = False
        self._common_check('key_bool', expected_item_value, upsert=True)

    def test_upsert_non_doc_int(self):
        expected_item_value = 234
        self._common_check('key_int', expected_item_value, upsert=True)
        expected_item_value = 1024
        self._common_check('key_int', expected_item_value, upsert=True)

    def test_upsert_non_doc_str(self):
        expected_item_value = 'automation_value'
        self._common_check('key_str', expected_item_value, upsert=True)
        expected_item_value = 'edited_value'
        self._common_check('key_str', expected_item_value, upsert=True)

    def test_upsert_non_doc_array(self):
        expected_item_value = ['first', 'second']
        self._common_check('key_array', expected_item_value, upsert=True)
        expected_item_value = ['third', 'fourth']
        self._common_check('key_array', expected_item_value, upsert=True)

############################################################################################################################
#
# UPSERT JSON VALUES
#
############################################################################################################################

    def test_upsert_json(self):
        expected_item_value = {'name' : 'Automation_1'}
        self._common_check('key_json', expected_item_value, upsert=True)
        expected_item_value = {'name' : 'Automation_2'}
        self._common_check('key_json', expected_item_value, upsert=True)

    def test_upsert_with_select(self):
        num_docs = self.input.param('num_docs', 10)
        keys = values = []
        for gen_load in self.gens_load:
            gen = copy.deepcopy(gen_load)
            if len(keys) == num_docs:
                    break
            for i in xrange(gen.end):
                if len(keys) == num_docs:
                    break
                key, value = gen.next()
                for bucket in self.buckets:
                    self.query = 'UPSERT into %s key "%s" VALUES %s' % (bucket.name, key, value)
                    actual_result = self.run_cbq_query()
                    self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')
                keys.append(key)
                values.append(value)

        for bucket in self.buckets:
            for i in xrange(num_docs):
                self.query = 'insert into %s key "insert_%s" select name from %s keys ["%s"]'  % (bucket.name, str(i),
                                                                                                  bucket.name, keys[i])
                actual_result = self.run_cbq_query()
                self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')
        items_check('insert_', values[:num_docs])

        for bucket in self.buckets:
            for i in xrange(num_docs):
                self.query = 'insert into %s key "insert_%s" select name from %s use keys ["%s"]'  % (bucket.name, str(i),
                                                                                              bucket.name, keys[num_docs + i])
                actual_result = self.run_cbq_query()
                self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')
        self.items_check('insert_', values[num_docs:num_docs + num_docs])

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
        key_prefix = 'automation'
        value = 'n1ql automation'
        self._common_insert(['%s%s' % (key_prefix, i) for i in xrange(self.num_items)],
                            [value] * len(self.num_items))
        keys_to_delete = ['%s%s' % (key_prefix, i) for i in xrange(num_docs)]
        for bucket in self.buckets:
            self.query = 'delete from %s  keys [%s]'  % (bucket.name,
                                                         map(lambda x: '"%s"' % x, keys_to_delete))
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')
        self._keys_are_deleted(keys_to_delete)

    def test_delete_where_clause_non_doc(self):
        num_docs = self.input.param('docs_to_delete', 3)
        key_prefix = 'automation'
        value = 'n1ql automation'
        value_to_delete = 'n1ql deletion'
        self._common_insert(['%s%s' % (key_prefix, i) for i in xrange(self.num_items - num_docs)],
                            [value] * len(self.num_items - num_docs))
        self._common_insert(['%s%s' % (key_prefix, i) for i in xrange(self.num_items - num_docs, self.num_items)],
                            [value_to_delete] * len(num_docs))
        keys_to_delete = ['%s%s' % (key_prefix, i) for i in xrange(self.num_items - num_docs, self.num_items)]
        for bucket in self.buckets:
            self.query = 'delete from %s where value()=%s'  % (bucket.name, value_to_delete)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')
        self._keys_are_deleted(keys_to_delete)

    def test_delete_where_clause_json(self):
        keys, values = self._insert_gen_keys(self.num_items)
        keys_to_delete = [keys[i] for i in xrange(len(keys)) if values[i]["job_title"] == 'Sales']
        for bucket in self.buckets:
            self.query = 'delete from %s where job_title="Sales"'  % (bucket.name)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')
        self._keys_are_deleted(keys_to_delete)

    def test_delete_where_satisfy_clause_json(self):
        keys, values = self._insert_gen_keys(self.num_items)
        keys_to_delete = [keys[i] for i in xrange(len(keys))
                          if len([vm for vm in values[i]["VMs"] if vm["RAM"] == 5]) > 0 ]
        for bucket in self.buckets:
            self.query = 'delete from %s where ANY vm IN %s.VMs SATISFIES vm.RAM = 5 END'  % (bucket.name, bucket.name)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')
        self._keys_are_deleted(keys_to_delete)

    def test_delete_limit_keys(self):
        keys, values = self._insert_gen_keys(self.num_items)
        for bucket in self.buckets:
            self.query = 'select count(*) as actual from %s where job_title="Sales"'  % (bucket.name, bucket.name)
            actual_result = self.run_cbq_query()
            current_docs = actual_result['results']['actual']
            self.query = 'delete from %s where job_title="Sales" LIMIT 1'  % (bucket.name)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')
            self.query = 'select count(*) as actual from %s where job_title="Sales"'  % (bucket.name, bucket.name)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['results']['actual'], current_docs - 1, 'Item was not deleted')

    def test_delete_limit_where(self):
        keys, values = self._insert_gen_keys(self.num_items)
        for bucket in self.buckets:
            self.query = 'select count(*) as actual from %s where job_title="Sales"'  % (bucket.name, bucket.name)
            actual_result = self.run_cbq_query()
            current_docs = actual_result['results']['actual']
            self.query = 'delete from %s where job_title="Sales" LIMIT 1'  % (bucket.name)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')
            self.query = 'select count(*) as actual from %s where job_title="Sales"'  % (bucket.name, bucket.name)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['results']['actual'], current_docs - 1, 'Item was not deleted')

############################################################################################################################
#
# MERGE
#
############################################################################################################################

    def test_merge_delete_match(self):
        self.assertTrue(len(self.buckets) >=2, 'Test needs at least 2 buckets')
        keys, _ = self._insert_gen_keys(self.num_items)
        self.query = 'MERGE INTO %s USING %s on key %s when matched then delete'  % (self.buckets[0].name, self.buckets[1].name, keys[0])
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')
        self.query = 'SELECT count(*) as rows FROM %s KEY %s'  % (self.buckets[0].name, keys[0])
        self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')
        self.assertEqual(actual_result['results']['rows'], 0, 'Query was not run successfully')

    def test_merge_delete_match_limit(self):
        self.assertTrue(len(self.buckets) >=2, 'Test needs at least 2 buckets')
        keys, _ = self._insert_gen_keys(self.num_items)
        self.query = 'MERGE INTO %s USING %s on key %s when matched then delete limit 1'  % (self.buckets[0].name, self.buckets[1].name, keys[0])
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')
        self.query = 'SELECT count(*) as rows FROM %s KEY %s'  % (self.buckets[0].name, keys[0])
        self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')
        self.assertEqual(actual_result['results']['rows'], 0, 'Query was not run successfully')

    def test_merge_delete_where_match(self):
        self.assertTrue(len(self.buckets) >=2, 'Test needs at least 2 buckets')
        keys, _ = self._insert_gen_keys(self.num_items)
        self.query = 'MERGE INTO %s USING %s on key %s when matched then delete where name LIKE "empl%"'  % (self.buckets[0].name, self.buckets[1].name, keys[0])
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')
        self.query = 'SELECT count(*) as rows FROM %s KEY %s'  % (self.buckets[0].name, keys[0])
        self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')
        self.assertEqual(actual_result['results']['rows'], 0, 'Query was not run successfully')

    def test_merge_update_match_set(self):
        self.assertTrue(len(self.buckets) >=2, 'Test needs at least 2 buckets')
        keys, _ = self._insert_gen_keys(self.num_items)
        new_name = 'edited'
        self.query = 'MERGE INTO %s USING %s on key %s when matched then update set name="%s"'  % (self.buckets[0].name, self.buckets[1].name, keys[0], new_name)
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')
        self.query = 'SELECT name FROM %s KEY %s'  % (self.buckets[0].name, keys[0])
        self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')
        self.assertEqual(actual_result['results']['name'], new_name, 'Name was not updated')

    def test_merge_update_match_unset(self):
        self.assertTrue(len(self.buckets) >=2, 'Test needs at least 2 buckets')
        keys, _ = self._insert_gen_keys(self.num_items)
        self.query = 'MERGE INTO %s USING %s on key %s when matched then update unset name'  % (self.buckets[0].name, self.buckets[1].name, keys[0])
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')
        self.query = 'SELECT * as doc FROM %s KEY %s'  % (self.buckets[0].name, keys[0])
        self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')
        self.assertTrue('name' not in actual_result['results']['doc'], 'Name was not unset')

    def test_merge_not_match_insert(self):
        self.assertTrue(len(self.buckets) >=2, 'Test needs at least 2 buckets')
        new_key = 'NEW'
        self.query = 'MERGE INTO %s USING %s on key %s when not matched then insert into %s key "%s" VALUES %s'  % (
                                                            self.buckets[0].name, self.buckets[1].name, new_key, self.buckets[0].name, new_key, '{"name": "new"}')
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')
        self.query = 'SELECT count(*) as rows FROM %s KEY %s'  % (self.buckets[0].name, new_key)
        self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')
        self.assertEqual(actual_result['results']['rows'], 1, 'Query was not run successfully')

############################################################################################################################

    def _insert_gen_keys(self, num_docs):
        keys = values = []
        for gen_load in self.gens_load:
            gen = copy.deepcopy(gen_load)
            if len(keys) == num_docs:
                    break
            for i in xrange(gen.end):
                if len(keys) == num_docs:
                    break
                key, value = gen.next()
                for bucket in self.buckets:
                    self.query = 'INSERT into %s key "%s" VALUES %s' % (bucket.name, key, value)
                    actual_result = self.run_cbq_query()
                    self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')
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
                self.query = 'INSERT into %s key "%s" VALUES %s' % (bucket.name, keys[i], values[i])
                actual_result = self.run_cbq_query()
                self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')

    def _common_check(self, key, expected_item_value, upsert=False):
        clause = 'UPSERT' if upsert else 'INSERT'
        for bucket in self.buckets:
            inserted = expected_item_value
            if isinstance(expected_item_value, str):
                inserted = '"%s"' % expected_item_value
            self.query = '%s into %s key "%s" VALUES %s' % (clause, bucket.name, key, inserted)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')
            self.query = 'select * from %s'  % (bucket.name)
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
