import time
import copy
from remote.remote_util import RemoteMachineShellConnection
from tuqquery.tuq import QueryTests
from couchbase.documentgenerator import DocumentGenerator



class DMLQueryTests(QueryTests):
    def setUp(self):
        super(DMLQueryTests, self).setUp()
        self.directory = self.input.param("directory", "/tmp/tuq_data")

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

        for i in xrange(num_docs):
            self.query = 'insert into %s key "insert_%s" select name from %s keys ["%s"]'  % (bucket.name, str(i),
                                                                                              bucket.name, keys[i])
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')
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

        def items_check(prefix, vls):
            for bucket in self.buckets:
                self.query = 'select * from %s keys %s'  % (bucket.name, ','.join(["%s%s" % (prefix,i)
                                                                                   for i in xrange(num_docs)]))
                actual_result = self.run_cbq_query()
                expected_result = vls
                self.assertEqual(sorted(actual_result['results']), sorted(expected_result),
                                 'Item did not appear')
        for bucket in self.buckets:
            for i in xrange(num_docs):
                self.query = 'insert into %s key "insert_%s" select name from %s keys ["%s"]'  % (bucket.name, str(i),
                                                                                                  bucket.name, keys[i])
                actual_result = self.run_cbq_query()
                self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')
        items_check('insert_', values[:num_docs])

        for bucket in self.buckets:
            for i in xrange(num_docs):
                self.query = 'insert into %s key "insert_%s" select name from %s keys ["%s"]'  % (bucket.name, str(i),
                                                                                              bucket.name, keys[num_docs + i])
                actual_result = self.run_cbq_query()
                self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')
        items_check('insert_', values[num_docs:num_docs + num_docs])

############################################################################################################################

    def _common_check(self, key, expected_item_value, upsert=False):
        clause = 'UPSERT' if upsert else 'INSERT'
        for bucket in self.buckets:
            self.query = '%s into %s key "%s" VALUES %s' % (clause, bucket.name, key, expected_item_value)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['state'], 'success', 'Query was not run successfully')
            
            self.query = 'select * from %s'  % (bucket.name)
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
