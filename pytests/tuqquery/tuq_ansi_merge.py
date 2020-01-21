from .tuq import QueryTests
from membase.api.exception import CBQError


class QueryANSIMERGETests(QueryTests):
    def setUp(self):
        super(QueryANSIMERGETests, self).setUp()
        self.log.info("==============  QueryANSIMERGETests setup has started ==============")
        self.assertTrue(len(self.buckets) >= 2, 'All tests need at least two buckets to run')
        for bucket in self.buckets:
            self.cluster.bucket_flush(self.master, bucket=bucket,
                                  timeout=self.wait_timeout * 5)
        self.log.info("==============  QueryANSIMERGETests setup has completed ==============")
        self.wait_for_all_indexers_ready()
        self.log_config_info()

    def tearDown(self):
        self.log.info("==============  QueryANSIMERGETests tearDown has started ==============")
        self.log.info("==============  QueryANSIMERGETests tearDown has completed ==============")
        super(QueryANSIMERGETests, self).tearDown()

############################################################################################################################
#
# DELETE
#
############################################################################################################################
    '''Test if you can delete with new ansi merge syntax
        - Load the delete dataset
        - Run the merge query
        - Verify the number of items that were deleted'''
    def test_ansi_merge_delete(self):
        keys, _ = self._insert_gen_keys(self.num_items, prefix='merge_delete')
        self.query = 'MERGE INTO %s USING %s on meta(%s).id == meta(%s).id when matched then delete' % (
        self.buckets[0].name, self.buckets[1].name, self.buckets[0].name, self.buckets[1].name)
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertEqual(actual_result['metrics']['mutationCount'], 1000, 'Merge deleted more data than intended')

    '''Test if you can delete with a where clause with new ansi merge syntax
        - Load the delete dataset
        - Run the merge query
        - Verify the number of items that were deleted'''
    def test_ansi_merge_delete_match(self):
        keys, _ = self._insert_gen_keys(self.num_items, prefix='merge_delete')
        self.query = 'MERGE INTO %s USING %s on meta(%s).id == meta(%s).id when matched then delete where meta(%s).id = "%s"' \
                     % (self.buckets[0].name, self.buckets[1].name, self.buckets[0].name, self.buckets[1].name, self.buckets[1].name, keys[0])
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertEqual(actual_result['metrics']['mutationCount'], 1, 'Merge deleted more data than intended')
        self.query = 'select * from default where meta().id = "%s"' % (keys[0])
        self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertEqual(len(actual_result['results']), 0, 'Merge did not delete the item intended')

    '''Test if limit hint works with new ansi merge syntax
        - Load the delete dataset
        - Run the merge query
        - Verify the number of items that were deleted'''
    def test_ansi_merge_delete_match_limit(self):
        keys, _ = self._insert_gen_keys(self.num_items, prefix='delete_match')
        self.query = 'MERGE INTO %s USING %s on meta(%s).id == meta(%s).id when matched then delete limit 10' \
                     % (self.buckets[0].name, self.buckets[1].name, self.buckets[0].name, self.buckets[1].name)
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertEqual(actual_result['metrics']['mutationCount'], 10, 'Merge deleted more data than intended')

    '''Test multiple predicates in the where clause with ansi merge syntax
        - Load the delete dataset
        - Run the merge query
        - Verify the number of items that were deleted'''
    def test_ansi_merge_delete_where_match(self):
        keys, _ = self._insert_gen_keys(self.num_items, prefix='merge_delete_where')
        self.query = 'MERGE INTO %s USING %s s0 on meta(%s).id == meta(s0).id when matched then delete where meta(s0).id = "%s" AND s0.name = "%s"' \
                     %(self.buckets[0].name, self.buckets[1].name, self.buckets[0].name, keys[0], "employee-1")
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertEqual(actual_result['metrics']['mutationCount'], 1, 'Merge deleted more data than intended')
        self.query = 'select * from default where meta().id = "%s"' % (keys[0])
        self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertEqual(len(actual_result['results']), 0, 'Merge did not delete the item intended')

############################################################################################################################
#
# UPDATE
#
############################################################################################################################
    '''Test update with new ansi syntax
        -Create index on appropriate field
        -Insert the data that will trigger the update
        -Run the ansi merge to trigger the update'''
    def test_ansi_merge_update_match_set(self):
        try:
            self.run_cbq_query(query='CREATE INDEX name on default(name)')

            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[1].name, "test", '{"name": "new1"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[0].name, "new1", '{"name": "new1"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            new_name = 'edit'
            self.query = 'MERGE INTO %s b1 USING %s b2 on b1.name == b2.name when matched then update set b1.name="%s"'  \
                         % (self.buckets[0].name, self.buckets[1].name, new_name)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'SELECT name FROM %s' % self.buckets[0].name
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.assertEqual(actual_result['results'][0]['name'], new_name, 'Name was not updated')
        finally:
            self.run_cbq_query(query="Drop index default.name")

    '''Test unset with new ansi merge syntax
        -Create index on appropriate field
        -Insert the data that will trigger the update
        -Run the ansi merge to trigger the update'''
    def test_ansi_merge_update_match_unset(self):
        try:
            self.run_cbq_query(query='CREATE INDEX name on default(name)')

            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[1].name, "test", '{"name": "new1"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[0].name, "new1", '{"name": "new1"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'MERGE INTO %s b1 USING %s b2 on b1.name == b2.name when matched then update unset b1.name'  \
                         % (self.buckets[0].name, self.buckets[1].name)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'SELECT * FROM %s '  % (self.buckets[0].name)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.assertTrue('name' not in actual_result['results'], 'Name was not unset')
        finally:
            self.run_cbq_query(query="Drop index default.name")

    '''Test hash join hint with new ansi merge syntax
        -Create index on appropriate field
        -Insert the data that will trigger the update
        -Run an explain of the query to ensure hash join is being used
        -Run the ansi merge to trigger the update with hash probe hint
        -Run an explain of the query to ensure hash join is being used 
        -Run the ansi merge to trigger the update with hash build hint'''
    def test_ansi_merge_update_match_set_hash(self):
        try:
            self.run_cbq_query(query='CREATE INDEX name on default(name)')

            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[1].name, "test", '{"name": "new1"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[0].name, "new1", '{"name": "new1"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')

            # Check update merge with hash(probe)
            new_name = 'edit'
            self.query = 'EXPLAIN MERGE INTO %s b1 USING %s b2 use hash(probe) on b1.name == b2.name when matched then update set b1.name="%s"'  \
                         % (self.buckets[0].name, self.buckets[1].name, new_name)
            actual_result = self.run_cbq_query()
            self.assertTrue("HashJoin" in str(actual_result), "HashJoin is not being used")

            self.query = 'MERGE INTO %s b1 USING %s b2 use hash(probe) on b1.name == b2.name when matched then update set b1.name="%s"'  \
                         % (self.buckets[0].name, self.buckets[1].name, new_name)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'SELECT name FROM %s' % self.buckets[0].name
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.assertEqual(actual_result['results'][0]['name'], new_name, 'Name was not updated')

            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[1].name, "test1", '{"name": "new2"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[0].name, "new2", '{"name": "new2"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')

            # Check update merge with hash(build)
            new_name = 'edit1'
            self.query = 'EXPLAIN MERGE INTO %s b1 USING %s b2 use hash(build) on b1.name == b2.name when matched then update set b1.name="%s"'  \
                         % (self.buckets[0].name, self.buckets[1].name, new_name)
            actual_result = self.run_cbq_query()
            self.assertTrue("HashJoin" in str(actual_result), "HashJoin is not being used")

            self.query = 'MERGE INTO %s b1 USING %s b2 use hash(build) on b1.name == b2.name when matched then update set b1.name="%s"'  \
                         % (self.buckets[0].name, self.buckets[1].name, new_name)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'SELECT name FROM %s where name == "edit1"' % self.buckets[0].name
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.assertEqual(actual_result['results'][0]['name'], new_name, 'Name was not updated')
        finally:
            self.run_cbq_query(query="Drop index default.name")

    '''Check hash join hint with new ansi merge syntax when source of the merge is a subquery
        -Create index on appropriate field
        -Insert the data that will trigger the update
        -Run an explain of the query to ensure hash join is being used
        -Run the ansi merge to trigger the update with hash probe hint
        -Run an explain of the query to ensure hash join is being used 
        -Run the ansi merge to trigger the update with hash build hint'''
    def test_ansi_merge_select_update_match_set_hash(self):
        try:
            self.run_cbq_query(query='CREATE INDEX name on default(name)')
            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[1].name, "test", '{"name": "new1"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[0].name, "new1", '{"name": "new1"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')

            # Check update merge with hash(probe)
            new_name = 'edit'
            self.query = 'EXPLAIN MERGE INTO %s b1 USING (select name from %s b2) as s use hash(probe) on b1.name == s.name when matched then update set b1.name="%s"' \
                         % (self.buckets[0].name, self.buckets[1].name, new_name)
            actual_result = self.run_cbq_query()
            self.assertTrue("HashJoin" in str(actual_result), "HashJoin is not being used")

            self.query = 'MERGE INTO %s b1 USING (select name from %s b2) as s use hash(probe) on b1.name == s.name when matched then update set b1.name="%s"' \
                         % (self.buckets[0].name, self.buckets[1].name, new_name)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'SELECT name FROM %s'  % (self.buckets[0].name)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.assertEqual(actual_result['results'][0]['name'], new_name, 'Name was not updated')

            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[1].name, "test1", '{"name": "new2"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[0].name, "new2", '{"name": "new2"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')

            # Check update merge with hash(build)
            new_name = 'edit1'
            self.query = 'EXPLAIN MERGE INTO %s b1 USING (select name from %s b2) as s use hash(build) on b1.name == s.name when matched then update set b1.name="%s"' \
                         % (self.buckets[0].name, self.buckets[1].name, new_name)
            actual_result = self.run_cbq_query()
            self.assertTrue("HashJoin" in str(actual_result), "HashJoin is not being used")

            self.query = 'MERGE INTO %s b1 USING (select name from %s b2) as s use hash(build) on b1.name == s.name when matched then update set b1.name="%s"' \
                         % (self.buckets[0].name, self.buckets[1].name, new_name)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'SELECT name FROM %s where name == "edit1"'  % (self.buckets[0].name)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.assertEqual(actual_result['results'][0]['name'], new_name, 'Name was not updated')
        finally:
            self.run_cbq_query(query="Drop index default.name")

    '''Check hash join hint with new ansi merge syntax when source of the merge is a json doc
        -Create index on appropriate field
        -Insert the data that will trigger the update
        -Run an explain of the query to ensure hash join is being used
        -Run the ansi merge to trigger the update with hash probe hint
        -Run an explain of the query to ensure hash join is being used 
        -Run the ansi merge to trigger the update with hash build hint'''
    def test_ansi_merge_json_update_match_set_hash(self):
        try:
            self.run_cbq_query(query='CREATE INDEX name on default(name)')

            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[0].name, "new1", '{"name": "new1"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')

            # Check update merge with hash(probe)
            new_name = 'edit'
            self.query = 'EXPLAIN MERGE INTO %s b1 USING ({"name":"new1"}) as s use hash(probe) on b1.name == s.name when matched then update set b1.name="%s"'  % (self.buckets[0].name, new_name)
            actual_result = self.run_cbq_query()
            self.assertTrue("HashJoin" in str(actual_result), "HashJoin is not being used")

            self.query = 'MERGE INTO %s b1 USING ({"name":"new1"}) as s use hash(probe) on b1.name == s.name when matched then update set b1.name="%s"'  % (self.buckets[0].name, new_name)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'SELECT name FROM %s' % self.buckets[0].name
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.assertEqual(actual_result['results'][0]['name'], new_name, 'Name was not updated')

            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[0].name, "new2", '{"name": "new2"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')

            # Check update merge with hash(build)
            new_name = 'edit1'
            self.query = 'EXPLAIN MERGE INTO %s b1 USING ({"name":"new2"}) as s use hash(build) on b1.name == s.name when matched then update set b1.name="%s"'  \
                         % (self.buckets[0].name, new_name)
            actual_result = self.run_cbq_query()
            self.assertTrue("HashJoin" in str(actual_result), "HashJoin is not being used")

            self.query = 'MERGE INTO %s b1 USING ({"name":"new2"}) as s use hash(build) on b1.name == s.name when matched then update set b1.name="%s"'  \
                         % (self.buckets[0].name, new_name)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'SELECT name FROM %s where name == "edit1"' % self.buckets[0].name
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.assertEqual(actual_result['results'][0]['name'], new_name, 'Name was not updated')
        finally:
            self.run_cbq_query(query="Drop index default.name")

############################################################################################################################
#
# INSERT
#
############################################################################################################################
    '''Test new insert syntax introduced in ansi merge
        -Create the appropriate index needed for the merge
        -Insert the data to cause an insert to be triggered
        -Run the merge when not matched query and check if data was inserted'''
    def test_ansi_merge_not_match_insert(self):
        try:
            self.run_cbq_query(query='CREATE INDEX name on default(name)')
            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[1].name, "test", '{"name": "new1"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')

            self.query = 'MERGE INTO %s b1 USING %s b2 on b1.%s == b2.%s when not matched then insert %s' \
                         % (self.buckets[0].name, self.buckets[1].name, 'name', 'name', '(KEY "test",VALUE {"name": "new"})')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'SELECT * FROM %s where meta().id == "test"' % self.buckets[0].name
            actual_result = self.run_cbq_query()
            self.assertEqual(len(actual_result['results']), 1, 'Query was not run successfully')
        finally:
            self.run_cbq_query(query="Drop index default.name")

    '''Test use index hint with ansi merge syntax
        -Check the explain plan to see that the index we wanted to used was indeed picked up
        -Run the merge when not matched query and check if data was inserted'''
    def test_ansi_merge_not_match_insert_use_index(self):
        try:
            self.run_cbq_query(query='CREATE INDEX name on default(name)')
            self.run_cbq_query(query='CREATE INDEX name2 on default(name)')

            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[1].name, "test", '{"name": "new1"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')

            self.query = 'EXPLAIN MERGE INTO %s b1 use index(name2) USING %s b2 on b1.%s == b2.%s when not matched then insert %s' \
                         % (self.buckets[0].name, self.buckets[1].name, 'name', 'name', '(KEY "test",VALUE {"name": "new"})')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['results'][0]['plan']['~children'][2]['~child']['~children'][0]['~child']['~children'][0]['index'],
                             'name2', "The incorrect index is being used")

            self.query = 'MERGE INTO %s b1 use index(name2) USING %s b2 on b1.%s == b2.%s when not matched then insert %s' \
                         % (self.buckets[0].name, self.buckets[1].name, 'name', 'name', '(KEY "test",VALUE {"name": "new"})')
            self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'SELECT * FROM %s where meta().id == "test"' % self.buckets[0].name
            actual_result = self.run_cbq_query()
            self.assertEqual(len(actual_result['results']), 1, 'Query was not run successfully')
        finally:
            self.run_cbq_query(query="Drop index default.name")
            self.run_cbq_query(query="Drop index default.name2")

    '''Test use index hint on the source of ansi merge
        -Check the explain plan to see that the index we wanted to used was indeed picked up
        -Run the merge when not matched query and check if data was inserted'''
    def test_ansi_merge_match_update_set_use_index_source(self):
        try:
            self.run_cbq_query(query='CREATE INDEX name on default(name)')
            self.run_cbq_query(query='CREATE INDEX name2 on standard_bucket0(name)')

            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[0].name, "test", '{"name": "new1"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')

            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[1].name, "test", '{"name": "new1"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')

            self.query = 'EXPLAIN MERGE INTO %s b1 USING %s b2 use index(name2) on b1.%s == b2.%s when matched then update set b1.%s = "ajay"' \
                         % ('default', 'standard_bucket0', 'name', 'name', 'name')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['results'][0]['plan']['~children'][0]['index'],
                             'name2', "The incorrect index is being used")
            self.assertEqual(actual_result['results'][0]['plan']['~children'][2]['~child']['~children'][0]['~child']['~children'][0]['index'],
                             'name', "The incorrect index is being used")

            self.query = 'MERGE INTO %s b1 USING %s b2 use index(name2) on b1.%s == b2.%s when matched then update set b1.%s = "ajay"' \
                         % (self.buckets[0].name, self.buckets[1].name, 'name', 'name', 'name' )
            self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')

            self.query = 'SELECT * FROM %s where meta().id == "test"' % self.buckets[0].name
            actual_result = self.run_cbq_query()
            self.assertEqual(len(actual_result['results']), 1, 'Query was not run successfully')
        finally:
            self.run_cbq_query(query="Drop index default.name")
            self.run_cbq_query(query="Drop index standard_bucket0.name2")

    '''Test if you can use UUID function instead of specifying a key value
        -Run the merge when not matched query and check if data was inserted correctly'''
    def test_ansi_merge_not_match_insert_UUID(self):
        try:
            self.run_cbq_query(query='CREATE INDEX name on default(name)')
            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[1].name, "test", '{"name": "new1"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'MERGE INTO %s b1 USING %s b2 on b1.%s == b2.%s when not matched then insert %s' \
                         % (self.buckets[0].name, self.buckets[1].name, 'name', 'name', '(KEY UUID(),VALUE {"name": "new"})')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'SELECT * FROM %s '  % self.buckets[0].name
            actual_result = self.run_cbq_query()
            self.assertEqual(len(actual_result['results']), 1, 'Query was not run successfully')
        finally:
            self.run_cbq_query(query="Drop index default.name")

    '''Test insert syntax with subquery as source of merge
        -Run the merge when not matched query and check if data was inserted correctly'''
    def test_ansi_merge_select_not_match_insert(self):
        try:
            self.run_cbq_query(query='CREATE INDEX name on default(name)')
            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[1].name, "test", '{"name": "new1"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'MERGE INTO %s b1 USING (select * from %s b2) as s on b1.%s == s.%s when not matched then insert %s'  \
                         % (self.buckets[0].name, self.buckets[1].name, 'name', 'name', '(key "test", value {"name": "new"})')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'SELECT * FROM %s where meta().id == "test"'  % self.buckets[0].name
            actual_result = self.run_cbq_query()
            self.assertEqual(len(actual_result['results']), 1, 'Query was not run successfully')
        finally:
            self.run_cbq_query(query="Drop index default.name")

    '''Test insert syntax with json as source of merge
        -Run the merge when not matched query and check if data was inserted correctly'''
    def test_ansi_merge_json_not_match_insert(self):
        try:
            self.run_cbq_query(query='CREATE INDEX name on default(name)')
            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[1].name, "test", '{"name": "new1"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'MERGE INTO %s b1 USING ({"name":"new1"}) as s on b1.%s == s.%s when not matched then insert %s'  \
                         % (self.buckets[0].name, 'name', 'name', '(key "test", value {"name": "new"})')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'SELECT * FROM %s where meta().id == "test"' % self.buckets[0].name
            actual_result = self.run_cbq_query()
            self.assertEqual(len(actual_result['results']), 1, 'Query was not run successfully')
        finally:
            self.run_cbq_query(query="Drop index default.name")

    '''Test insert syntax with hash hint specified, if you specify hash(build) hashjoin will be quietly ignored. This is
        tested in a later test.
        -Check the explain plan to verify a hash join is being used
        -Run the merge and verify that data was inserted
        -Repeat the above for json as source instead of a keyspace
        -Repeat the above for subquery as source instead of a keyspace'''
    def test_ansi_merge_not_match_insert_hash(self):
        try:

            self.run_cbq_query(query='CREATE INDEX name on default(name)')
            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[1].name, "test", '{"name": "new1"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')

            # Test regular merge using hash
            self.query = 'EXPLAIN MERGE INTO %s b1 USING %s b2 use hash(probe) on b1.%s == b2.%s when not matched then insert %s' \
                         % (self.buckets[0].name, self.buckets[1].name, 'name', 'name', '(KEY "test",VALUE {"name": "new"})')
            actual_result = self.run_cbq_query()
            self.assertTrue("HashJoin" in str(actual_result), "HashJoin is not being used")

            self.query = 'MERGE INTO %s b1 USING %s b2 use hash(probe) on b1.%s == b2.%s when not matched then insert %s' \
                         % (self.buckets[0].name, self.buckets[1].name, 'name', 'name', '(KEY "test",VALUE {"name": "new"})')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'SELECT * FROM %s where meta().id == "test"'  % self.buckets[0].name
            actual_result = self.run_cbq_query()
            self.assertEqual(len(actual_result['results']), 1, 'Query was not run successfully')

            # Test JSON use hash
            self.query = 'EXPLAIN MERGE INTO %s b1 USING ({"name":"new1"}) as s use hash(probe) on b1.%s == s.%s when not matched then insert %s'  \
                         % (self.buckets[0].name, 'name', 'name', '(key "test1", value {"name": "new"})')
            actual_result = self.run_cbq_query()
            self.assertTrue("HashJoin" in str(actual_result), "HashJoin is not being used")
            self.query = 'MERGE INTO %s b1 USING ({"name":"new1"}) as s use hash(probe) on b1.%s == s.%s when not matched then insert %s'  \
                         % (self.buckets[0].name, 'name', 'name', '(key "test1", value {"name": "new"})')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'SELECT * FROM %s where meta().id == "test1"' % self.buckets[0].name
            actual_result = self.run_cbq_query()
            self.assertEqual(len(actual_result['results']), 1, 'Query was not run successfully')

            # Test subquery use hash
            self.query = 'EXPLAIN MERGE INTO %s b1 USING (select * from %s b2) as s use hash(probe) on b1.%s == s.%s when not matched then insert %s'  \
                         % (self.buckets[0].name, self.buckets[1].name, 'name', 'name', '(key "test2", value {"name": "new"})')
            actual_result = self.run_cbq_query()
            self.assertTrue("HashJoin" in str(actual_result), "HashJoin is not being used")

            self.query = 'MERGE INTO %s b1 USING (select * from %s b2) as s use hash(probe) on b1.%s == s.%s when not matched then insert %s'  \
                         % (self.buckets[0].name, self.buckets[1].name, 'name', 'name', '(key "test2", value {"name": "new"})')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'SELECT * FROM %s where meta().id == "test2"' % self.buckets[0].name
            actual_result = self.run_cbq_query()
            self.assertEqual(len(actual_result['results']), 1, 'Query was not run successfully')
        finally:
            self.run_cbq_query(query="Drop index default.name")

############################################################################################################################
#
# NEGATIVE
#
############################################################################################################################
    '''Negative test, hash build hint should be ignored by queries , check explain plan to ensure hashjoin is not used
        -Check explain plan to ensure hash join is not being used
        -Attempt to use the keyword hash build inside of the merge statement, query should execute as a nested loop join
        -Check to see if statement succesfully inserted data
        -Repeat above steps for json as source instead of keyspace
        -Repeat above steps for subquery as source instead of keyspace'''
    def test_ansi_merge_not_match_insert_hash_negative(self):
        try:
            self.run_cbq_query(query='CREATE INDEX name on default(name)')
            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[1].name, "test", '{"name": "new1"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')

            # Test regular merge using hash
            self.query = 'EXPLAIN MERGE INTO %s b1 USING %s b2 use hash(build) on b1.%s == b2.%s when not matched then insert %s' \
                         % (self.buckets[0].name, self.buckets[1].name, 'name', 'name', '(KEY "test",VALUE {"name": "new"})')
            actual_result = self.run_cbq_query()
            self.assertFalse("HashJoin" in str(actual_result), "HashJoin is being used")

            self.query = 'MERGE INTO %s b1 USING %s b2 use hash(build) on b1.%s == b2.%s when not matched then insert %s' \
                         % (self.buckets[0].name, self.buckets[1].name, 'name', 'name', '(KEY "test",VALUE {"name": "new"})')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'SELECT * FROM %s where meta().id == "test"' % self.buckets[0].name
            actual_result = self.run_cbq_query()
            self.assertEqual(len(actual_result['results']), 1, 'Query was not run successfully')

            # Test JSON use hash
            self.query = 'EXPLAIN MERGE INTO %s b1 USING ({"name":"new1"}) as s use hash(build) on b1.%s == s.%s when not matched then insert %s'  \
                         % (self.buckets[0].name, 'name', 'name', '(key "test1", value {"name": "new"})')
            actual_result = self.run_cbq_query()
            self.assertFalse("HashJoin" in str(actual_result), "HashJoin is being used")
            self.query = 'MERGE INTO %s b1 USING ({"name":"new1"}) as s use hash(build) on b1.%s == s.%s when not matched then insert %s'  \
                         % (self.buckets[0].name, 'name', 'name', '(key "test1", value {"name": "new"})')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'SELECT * FROM %s where meta().id == "test1"' % self.buckets[0].name
            actual_result = self.run_cbq_query()
            self.assertEqual(len(actual_result['results']), 1, 'Query was not run successfully')

            # Test subquery use hash
            self.query = 'EXPLAIN MERGE INTO %s b1 USING (select * from %s b2) as s use hash(build) on b1.%s == s.%s when not matched then insert %s'  \
                         % (self.buckets[0].name, self.buckets[1].name, 'name', 'name', '(key "test2", value {"name": "new"})')
            actual_result = self.run_cbq_query()
            self.assertFalse("HashJoin" in str(actual_result), "HashJoin is being used")

            self.query = 'MERGE INTO %s b1 USING (select * from %s b2) as s use hash(build) on b1.%s == s.%s when not matched then insert %s'  \
                         % (self.buckets[0].name, self.buckets[1].name, 'name', 'name', '(key "test2", value {"name": "new"})')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'SELECT * FROM %s where meta().id == "test2"' % self.buckets[0].name
            actual_result = self.run_cbq_query()
            self.assertEqual(len(actual_result['results']), 1, 'Query was not run successfully')
        finally:
            self.run_cbq_query(query="Drop index default.name")

    '''Test new syntax without specifying a key value, a key MUST be specified with the new syntax
        -Run the merge statement without a key value specified
        -Check to ensure the correct error is thrown'''
    def test_mixed_merge_not_match_insert_negative(self):
        try:
            self.run_cbq_query(query='CREATE INDEX name on default(name)')
            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[1].name, "test", '{"name": "new1"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')

            self.query = 'MERGE INTO %s b1 USING %s b2 on b1.%s == b2.%s when not matched then insert %s' \
                         % (self.buckets[0].name, self.buckets[1].name, 'name', 'name', '(VALUE {"name": "new"})')
            self.run_cbq_query()
            self.assertFalse(True, "The query should have errored")
        except CBQError as e:
            self.assertTrue("syntax error - at VALUE" in str(e))
        finally:
            self.run_cbq_query(query="Drop index default.name")

    '''Test new syntax without specifying a key value, a key MUST be specified with the new syntax, a different test case
        -Run the merge statement without a key value specified
        -Check to ensure the correct error is thrown'''
    def test_mixed_merge_not_match_insert_negative_old(self):
        try:
            self.run_cbq_query(query='CREATE INDEX name on default(name)')
            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' \
                         % (self.buckets[1].name, "test", '{"name": "new1"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')

            self.query = 'MERGE INTO %s b1 USING %s b2 on b1.%s == b2.%s when not matched then insert %s' \
                         % (self.buckets[0].name, self.buckets[1].name, 'name', 'name', '({"name": "new"})')
            self.run_cbq_query()
            self.assertFalse(True, "The query should have errored")
        except CBQError as e:
            self.assertTrue("MERGE with ON clause must have document key specification in INSERT action." in str(e))
        finally:
            self.run_cbq_query(query="Drop index default.name")

    '''Test whether or not you can specify a key value with the old syntax, this should not be allowed
        -Attempt to specify a key and  a value with the old syntax
        -Check to ensure the correct error is thrown'''
    def test_mixed_merge_not_match_insert_legacy(self):
        try:
            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[1].name, "test", '{"name": "new1"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'MERGE INTO %s b1 USING %s b2 on key b2.%s when not matched then insert %s'  \
                         % (self.buckets[0].name, self.buckets[1].name, 'name', '(Key "test", Value {"name": "new"})')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'SELECT count(*) FROM %s' % self.buckets[0].name
            actual_result = self.run_cbq_query()
            self.assertEqual(len(actual_result['results']), 1, 'Query was not run successfully')
            self.assertFalse(True, "The query should have errored")
        except CBQError as e:
            self.assertTrue("MERGE with ON KEY clause cannot have document key specification in INSERT action." in str(e))

    '''Test if you can use new syntax without the appropriate secondary index, should not be allowed
        -Attempt to run a merge without creating a secondary index
        -Check if the correct error is thrown'''
    def test_mixed_merge_not_match_insert_no_index(self):
        try:
            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[1].name, "test", '{"name": "new1"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')

            self.query = 'MERGE INTO %s b1 USING %s b2 on b1.%s == b2.%s when not matched then insert %s' \
                         % (self.buckets[0].name, self.buckets[1].name, 'name', 'name', '(key "TEST",VALUE {"name": "new"})')
            self.run_cbq_query()
            self.assertFalse(True, "The query should have errored")
        except CBQError as e:
            self.assertTrue("No index available for ANSI join term b1" in str(e))

    '''Test if you can use the index hint with the legacy syntax, should not be allowed
        -Attempt to use the use index hint with an ON KEYS clause merge
        -Check to see the correct error is thrown'''
    def test_mixed_merge_not_match_insert_legacy_index_hint(self):
        try:
            self.run_cbq_query(query='CREATE INDEX name on default(name)')
            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[1].name, "test", '{"name": "new1"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'MERGE INTO %s b1 use index(name) USING %s b2 on key b2.%s when not matched then insert %s' \
                         % (self.buckets[0].name, self.buckets[1].name, 'name', '{"name": "new"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'SELECT count(*) FROM %s'  % (self.buckets[0].name)
            actual_result = self.run_cbq_query()
            self.assertEqual(len(actual_result['results']), 1, 'Query was not run successfully')
            self.assertFalse(True, "The query should have errored")
        except CBQError as e:
            self.assertTrue("MERGE with ON KEY clause cannot have USE INDEX hint specified on target." in str(e))
        finally:
            self.run_cbq_query(query="Drop index default.name")

    '''Test if you can use a json value as the target of a merge, the target of a merge MUST be a keyspace'''
    def test_non_keyspace_json_target(self):
        try:
            keys, _ = self._insert_gen_keys(self.num_items, prefix='merge_delete')
            self.query = 'MERGE INTO ({"name":"new1"}) as s USING %s on meta(s).id == meta(%s).id when matched then delete' % (
            self.buckets[1].name, self.buckets[1].name)
            self.run_cbq_query()
            self.assertFalse(True, "The query should have errored")

        except CBQError as e:
            self.assertTrue("syntax error - at (" in str(e))

    '''Test if you can use a subquery as the target of a merge, the targer of a merge MUST be a keyspace'''
    def test_non_keyspace_select_target(self):
        try:
            keys, _ = self._insert_gen_keys(self.num_items, prefix='merge_delete')
            self.query = 'MERGE INTO (select * from %s b2) as s USING %s on meta(s).id == meta(%s).id when matched then delete' % (
                self.buckets[0].name, self.buckets[1].name, self.buckets[1].name)
            self.run_cbq_query()
            self.assertFalse(True, "The query should have errored")
        except CBQError as e:
            self.assertTrue("syntax error - at (" in str(e))

    '''Test if you can use the index hint on a non keyspace json value, use index hint only works with keyspaces'''
    def test_ansi_merge_json_update_match_set_index_hint(self):
        try:
            self.run_cbq_query(query='CREATE INDEX name on default(name)')
            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[0].name, "new1", '{"name": "new1"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')

            self.query = 'MERGE INTO %s b1 USING ({"name":"new1"}) as s use index(name) on b1.name == s.name when matched then update set b1.name="%s"'  \
                         % (self.buckets[0].name, 'edit')
            self.run_cbq_query()
            self.assertFalse(True, "The query should have errored")
        except CBQError as e:
            self.assertTrue("FROM Expression cannot have USE KEYS or USE INDEX." in str(e))
        finally:
            self.run_cbq_query(query="Drop index default.name")

    '''Test if you can use the index hint on a subquery, use index hint only works with keyspaces'''
    def test_ansi_merge_select_update_match_set_index_hint(self):
        try:
            self.run_cbq_query(query='CREATE INDEX name on default(name)')
            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[0].name, "new1", '{"name": "new1"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')

            self.query = 'MERGE INTO %s b1 USING (select * from %s b2) as s use index(name) on b1.%s == s.%s when not matched then insert %s' % (
                self.buckets[0].name, self.buckets[1].name, 'name', 'name', '(key "test2", value {"name": "new"})')
            self.run_cbq_query()
            self.assertFalse(True, "The query should have errored")
        except CBQError as e:
            self.assertTrue("FROM Subquery cannot have USE KEYS or USE INDEX." in str(e))
        finally:
            self.run_cbq_query(query="Drop index default.name")

    '''Test if the you can use the use hash hint on the target of a merge,
       you should only be able to use the use index hint on the target of a merge'''
    def test_ansi_merge_hints_on_target_hash(self):
        try:
            self.run_cbq_query(query='CREATE INDEX name on default(name)')
            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[1].name, "test", '{"name": "new1"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')

            # Test regular merge using hash
            self.query = 'MERGE INTO %s b1 use hash(probe) USING %s b2 on b1.%s == b2.%s when not matched then insert %s' \
                         % (self.buckets[0].name, self.buckets[1].name, 'name', 'name', '(KEY "test",VALUE {"name": "new"})')
            self.run_cbq_query()
            self.assertFalse(True, "The query should have errored")
        except CBQError as e:
            self.assertTrue("Keyspace reference cannot have join hint (USE HASH or USE NL)in MERGE statement." in str(e))
        finally:
            self.run_cbq_query(query="DROP INDEX default.name")

    '''Test if the you can use the use keys hint on the target of a merge,
       you should only be able to use the use index hint on the target of a merge'''
    def test_ansi_merge_hints_on_target_keys(self):
        try:
            self.run_cbq_query(query='CREATE INDEX name on default(name)')
            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.buckets[1].name, "test", '{"name": "new1"}')
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')

            # Test regular merge using hash
            self.query = 'MERGE INTO %s b1 use keys("test") USING %s b2 on b1.%s == b2.%s when not matched then insert %s' \
                         % (self.buckets[0].name, self.buckets[1].name, 'name', 'name', '(KEY "test",VALUE {"name": "new"})')
            self.run_cbq_query()
            self.assertFalse(True, "The query should have errored")
        except CBQError as e:
            self.assertTrue("Keyspace reference cannot have USE KEYS hint in MERGE statement." in str(e))


