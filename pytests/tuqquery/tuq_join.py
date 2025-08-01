import copy
from .tuq_sanity import QuerySanityTests
import time
from deepdiff import DeepDiff

JOIN_INNER = "INNER"
JOIN_LEFT = "LEFT"
JOIN_RIGHT = "RIGHT"


class JoinTests(QuerySanityTests):
    def setUp(self):
        try:
            self.dataset = 'join'
            super(JoinTests, self).setUp()
            self.gens_tasks = self.gen_docs(type='tasks')
            self.type_join = self.input.param("type_join", JOIN_INNER)
            self.query_buckets = self.get_query_buckets(check_all_buckets=True)
        except Exception as ex:
            self.log.error("ERROR SETUP FAILED: %s" % str(ex))
            raise ex

    def suite_setUp(self):
        super(JoinTests, self).suite_setUp()
        self.load(self.gens_tasks, start_items=self.num_items)

    def tearDown(self):
        super(JoinTests, self).tearDown()

    def suite_tearDown(self):
        super(JoinTests, self).suite_tearDown()

    def test_simple_join_keys(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT employee.name, employee.tasks_ids, new_project.project " + \
                         "FROM %s as employee %s JOIN %s as new_project " % (query_bucket, self.type_join, self.query_buckets[0]) + \
                         "ON KEYS employee.tasks_ids"
            time.sleep(30)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            full_list = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [doc for doc in full_list if not doc]
            expected_result.extend([{"name": doc['name'], "tasks_ids": doc['tasks_ids'], "project": doc['project']}
                                    for doc in full_list if doc and 'project' in doc])
            # expected_result.extend([{"name" : doc['name'], "tasks_ids" : doc['tasks_ids']}
            # for doc in full_list if doc and not 'project' in doc])
            self._verify_results(actual_result, expected_result)

    def test_prepared_simple_join_keys(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT employee.name, employee.tasks_ids, new_project.project " + \
                         "FROM %s as employee %s JOIN %s as new_project " % (query_bucket, self.type_join, self.query_buckets[0]) + \
                         "ON KEYS employee.tasks_ids"
            self.prepared_common_body()

    def test_join_several_keys(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT employee.name, employee.tasks_ids, new_task.project, new_task.task_name " + \
                         "FROM %s as employee %s JOIN %s as new_task " % (query_bucket, self.type_join, self.query_buckets[0]) + \
                         "ON KEYS employee.tasks_ids"
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            full_list = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [doc for doc in full_list if not doc]
            expected_result.extend([{"name": doc['name'], "tasks_ids": doc['tasks_ids'], "project": doc['project'],
                                     "task_name": doc['task_name']}
                                    for doc in full_list if doc and 'project' in doc])
            # expected_result.extend([{"name" : doc['name'], "tasks_ids" : doc['tasks_ids']}
            #                        for doc in full_list if doc and not 'project' in doc])
            self._verify_results(actual_result, expected_result)

    def test_where_join_keys(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT employee.name, employee.tasks_ids, new_project_full.project new_project " + \
                         "FROM %s as employee %s JOIN %s as new_project_full " % (query_bucket, self.type_join, self.query_buckets[0]) + \
                         "ON KEYS employee.tasks_ids WHERE new_project_full.project == 'IT'"
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [{"name": doc['name'], "tasks_ids": doc['tasks_ids'], "new_project": doc['project']}
                               for doc in expected_result if doc and 'project' in doc and doc['project'] == 'IT']
            self._verify_results(actual_result, expected_result)

    def test_bidirectional_join(self):
        default_bucket =  self.query_buckets[0]
        self.query = "create index idxbidirec on %s(join_day)" % default_bucket
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.query = "explain SELECT employee.name, employee.join_day " + \
                     "FROM %s as employee %s JOIN %s as new_project " % (
                         default_bucket, self.type_join, default_bucket) + \
                     "ON KEY new_project.join_day FOR employee where new_project.join_day is not null"
        actual_result = self.run_cbq_query()
        self.assertTrue("covers" in str(actual_result))
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.test_explain_particular_index("idxbidirec")
        self.query = "SELECT employee.name, employee.join_day " + \
                     "FROM %s as employee %s JOIN %s as new_project " % (
                         default_bucket, self.type_join, default_bucket) + \
                     "ON KEY new_project.join_day FOR employee where new_project.join_day is not null"
        actual_result = self.run_cbq_query()
        # self.assertTrue(actual_result['metrics']['resultCount'] == 0, 'Query was not run successfully')
        self.query = "drop index idxbidirec ON %s" % default_bucket
        self.run_cbq_query()

        self.query = "CREATE INDEX ix1 ON %s(docid,name)" % default_bucket
        self.run_cbq_query()
        self.query = 'CREATE INDEX ix2 ON %s(docid,name) where type = "wdoc"' % default_bucket
        self.run_cbq_query()
        self.query = "CREATE INDEX ix3 ON %s(altid, name, DISTINCT ARRAY p FOR p IN phones END)" % default_bucket
        self.run_cbq_query()
        self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (default_bucket, "w001",
                                                                         {"type": "wdoc", "docid": "x001",
                                                                          "name": "wdoc",
                                                                          "phones": ["123-456-7890", "123-456-7891"],
                                                                          "altid": "x001"})
        self.run_cbq_query()
        self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (default_bucket, "pdoc1",
                                                                         {"type": "pdoc", "docid": "x001",
                                                                          "name": "pdoc",
                                                                          "phones": ["123-456-7890", "123-456-7891"],
                                                                          "altid": "x001"})
        self.run_cbq_query()
        self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (default_bucket, "pdoc2",
                                                                         {"type": "pdoc", "docid": "w001",
                                                                          "name": "pdoc",
                                                                          "phones": ["123-456-7890", "123-456-7891"],
                                                                          "altid": "w001"})
        self.run_cbq_query()
        self.query = 'explain SELECT meta(b1).id b1id FROM %s b1 JOIN %s b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > ""' % (default_bucket,  default_bucket)
        actual_result = self.run_cbq_query()
        self.assertTrue("covers" in str(actual_result))
        self.assertTrue("ix1" in str(actual_result))

        self.query = 'SELECT meta(b1).id b1id FROM ' + default_bucket + ' b1 JOIN ' + default_bucket + ' b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > ""'
        actual_result = self.run_cbq_query()

        self.assertTrue(actual_result['results'] == [{'b1id': 'w001'}])
        self.query = 'explain SELECT meta(b1).id b1id, meta(b2).id b2id FROM ' + default_bucket + ' b1 JOIN ' + default_bucket + ' b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > ""'
        actual_result = self.run_cbq_query()
        self.assertTrue("covers" in str(actual_result))
        self.assertTrue("ix1" in str(actual_result))
        self.query = 'SELECT meta(b1).id b1id, meta(b2).id b2id FROM ' + default_bucket + ' b1 JOIN ' + default_bucket + ' b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > ""'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == [{'b1id': 'w001', 'b2id': 'pdoc2'}])
        self.query = 'explain SELECT meta(b1).id b1id, b2.docid FROM ' + default_bucket + ' b1 JOIN ' + default_bucket + ' b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > ""'
        actual_result = self.run_cbq_query()
        self.assertTrue("covers" in str(actual_result))
        self.assertTrue("ix1" in str(actual_result))
        self.query = 'SELECT meta(b1).id b1id, b2.docid FROM ' + default_bucket + ' b1 JOIN ' + default_bucket + ' b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > ""'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == [{'docid': 'w001', 'b1id': 'w001'}])
        self.query = 'explain SELECT meta(b1).id b1id, b2.name FROM ' + default_bucket + ' b1 JOIN ' + default_bucket + ' b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > ""'
        actual_result = self.run_cbq_query()
        self.assertTrue("covers" in str(actual_result))
        self.assertTrue("ix1" in str(actual_result))
        self.query = 'SELECT meta(b1).id b1id, b2.name FROM ' + default_bucket + ' b1 JOIN ' + default_bucket + ' b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > ""'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == [{'b1id': 'w001', 'name': 'pdoc'}])
        self.query = 'explain SELECT meta(b1).id b1id, b2.name, b3.docid FROM ' + default_bucket + ' b1 JOIN ' + default_bucket + ' b2 ON KEY b2.docid FOR b1 JOIN ' + default_bucket + ' b3 ON KEY b3.docid FOR b1 WHERE meta(b1).id > ""'
        actual_result = self.run_cbq_query()
        self.assertTrue("covers" in str(actual_result))
        self.assertTrue("ix1" in str(actual_result))
        self.query = 'SELECT meta(b1).id b1id, b2.name, b3.docid FROM ' + default_bucket + ' b1 JOIN ' + default_bucket + ' b2 ON KEY b2.docid FOR b1 JOIN ' + default_bucket + ' b3 ON KEY b3.docid FOR b1 WHERE meta(b1).id > ""'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == [{'docid': 'w001', 'b1id': 'w001', 'name': 'pdoc'}])
        self.query = 'explain SELECT meta(b1).id b1id, b2.name, b3.docid  FROM ' + default_bucket + ' b1 JOIN ' + default_bucket + ' b2 ON KEY b2.docid FOR b1 JOIN ' + default_bucket + ' b3 ON KEY b3.docid FOR b2 WHERE meta(b1).id > ""'
        actual_result = self.run_cbq_query()
        self.assertTrue("covers" in str(actual_result))
        self.assertTrue("ix1" in str(actual_result))
        self.query = 'SELECT meta(b1).id b1id, b2.name, b3.docid  FROM ' + default_bucket + ' b1 JOIN ' + default_bucket + ' b2 ON KEY b2.docid FOR b1 JOIN ' + default_bucket + ' b3 ON KEY b3.docid FOR b2 WHERE meta(b1).id > "";'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['metrics']['resultCount'] == 0)
        self.query = 'explain SELECT meta(b1).id b1id, meta(b2).id, b2.name  FROM ' + default_bucket + ' b1 JOIN ' + default_bucket + ' b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > "" AND b2.type = "wdoc"'
        actual_result = self.run_cbq_query()
        self.assertTrue("covers" in str(actual_result))
        self.assertTrue("ix2" in str(actual_result))
        self.query = 'SELECT meta(b1).id b1id, meta(b2).id, b2.name  FROM ' + default_bucket + ' b1 JOIN ' + default_bucket + ' b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > "" AND b2.type = "wdoc"'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['metrics']['resultCount'] == 0)
        self.query = 'explain SELECT meta(b1).id b1id, b2.name, b3.docid FROM ' + default_bucket + ' b1 JOIN ' + default_bucket + ' b2 ON KEY b2.docid FOR b1 JOIN ' + default_bucket + ' b3 ON KEY b3.docid FOR b1 WHERE meta(b1).id > "" AND b2.type = "wdoc"'
        actual_result = self.run_cbq_query()
        self.assertTrue("covers" in str(actual_result))
        self.assertTrue("ix2" in str(actual_result))
        self.query = 'SELECT meta(b1).id b1id, b2.name, b3.docid FROM ' + default_bucket + ' b1 JOIN ' + default_bucket + ' b2 ON KEY b2.docid FOR b1 JOIN ' + default_bucket + ' b3 ON KEY b3.docid FOR b1 WHERE meta(b1).id > "" AND b2.type = "wdoc"'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['metrics']['resultCount'] == 0)
        self.query = 'explain SELECT meta(b1).id b1id, b2.name, b3.docid FROM ' + default_bucket + ' b1 JOIN ' + default_bucket + ' b2 ON KEY b2.docid FOR b1 JOIN ' + default_bucket + ' b3 ON KEY b3.docid FOR b2 WHERE meta(b1).id > "" AND b2.type = "wdoc"'
        actual_result = self.run_cbq_query()
        self.assertTrue("covers" in str(actual_result))
        self.assertTrue("ix2" in str(actual_result))
        self.query = 'SELECT meta(b1).id b1id, b2.name, b3.docid FROM ' + default_bucket + ' b1 JOIN ' + default_bucket + ' b2 ON KEY b2.docid FOR b1 JOIN ' + default_bucket + ' b3 ON KEY b3.docid FOR b2 WHERE meta(b1).id > "" AND b2.type = "wdoc"'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['metrics']['resultCount'] == 0)
        self.query = 'SELECT meta(b1).id b1id, b2 from ' + default_bucket + ' b1 JOIN ' + default_bucket + ' b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > ""'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == [{'b1id': 'w001',
                                                      'b2': {'phones': ['123-456-7890', '123-456-7891'], 'type': 'pdoc',
                                                             'docid': 'w001', 'name': 'pdoc', 'altid': 'w001'}}])

        self.query = 'explain SELECT meta(b1).id b1id from ' + default_bucket + ' b1 NEST ' + default_bucket + ' b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > ""'
        actual_result = self.run_cbq_query()
        self.assertTrue("covers" in str(actual_result))
        self.assertTrue("ix1" in str(actual_result))
        self.assertTrue("(`b2`.`docid`)" in str(actual_result))
        self.query = 'SELECT meta(b1).id b1id from ' + default_bucket + ' b1 NEST ' + default_bucket + ' b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > ""'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == [{'b1id': 'w001'}])
        self.query = 'explain SELECT meta(b1).id b1id, b2 from ' + default_bucket + ' b1 NEST ' + default_bucket + ' b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > ""'
        actual_result = self.run_cbq_query()
        self.assertTrue("covers" in str(actual_result))
        self.assertTrue("ix1" in str(actual_result))
        self.assertTrue("(`b2`.`docid`)" in str(actual_result))
        self.query = 'SELECT meta(b1).id b1id, b2 from ' + default_bucket + ' b1 NEST ' + default_bucket + ' b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > ""'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == [{'b1id': 'w001', 'b2': [
            {'phones': ['123-456-7890', '123-456-7891'], 'type': 'pdoc', 'docid': 'w001', 'name': 'pdoc',
             'altid': 'w001'}]}])
        self.query = 'delete from ' + default_bucket + ' use keys["w001","pdoc1","pdoc2"]'
        self.run_cbq_query()

    def test_basic_nest_join(self):
        default_bucket =  self.query_buckets[0]
        self.query = 'insert into ' + default_bucket + ' values("a_12345",{ "_id": "a_12345", "_type": "service" })'
        self.run_cbq_query()
        self.query = 'insert into ' + default_bucket + ' values("b_12345", { "_id": "b_12345", "parent": "a_12345", ' \
                                                       '"data": { "a": "b", "c": "d" } }) '
        self.run_cbq_query()
        self.query = 'insert into ' + default_bucket + ' values("b_12346", { "_id": "b_12346", "parent": "a_12345", ' \
                                                       '"data": { "6": "3", "d": "f" } }) '
        self.run_cbq_query()
        self.query = 'CREATE INDEX idx_parent ON ' + default_bucket + '( parent )'
        self.run_cbq_query()
        self.query = 'SELECT * FROM ' + default_bucket + ' a NEST ' + default_bucket + ' b ON KEY b.parent FOR a'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == ([{'a': {'_type': 'service', '_id': 'a_12345'}, 'b': [
            {'_id': 'b_12345', 'data': {'a': 'b', 'c': 'd'}, 'parent': 'a_12345'},
            {'_id': 'b_12346', 'data': {'d': 'f', '6': '3'}, 'parent': 'a_12345'}]}]))
        self.query = 'SELECT * FROM ' + default_bucket + ' a join ' + default_bucket + ' b ON KEY b.parent FOR a'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == ([{'a': {'_type': 'service', '_id': 'a_12345'},
                                                       'b': {'_id': 'b_12345', 'data': {'a': 'b', 'c': 'd'},
                                                             'parent': 'a_12345'}},
                                                      {'a': {'_type': 'service', '_id': 'a_12345'},
                                                       'b': {'_id': 'b_12346', 'data': {'d': 'f', '6': '3'},
                                                             'parent': 'a_12345'}}]))
        self.query = 'delete from ' + default_bucket + ' use keys ["a_12345","b_12345","b_12346"]'
        self.run_cbq_query()

    def test_where_join_keys_covering(self):
        created_indexes = []
        ind_list = ["one"]
        index_name = "one"
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            for ind in ind_list:
                index_name = "coveringindex%s" % ind
                if ind == "one":
                    self.query = "CREATE INDEX %s ON %s(name, tasks_ids,job_title)  USING %s" % (
                        index_name, query_bucket, self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                created_indexes.append(index_name)
        for query_bucket in self.query_buckets:
            try:
                self.query = "EXPLAIN SELECT employee.name, employee.tasks_ids, employee.job_title new_project " + \
                             "FROM %s as employee %s JOIN %s as new_project_full " % (
                                 query_bucket, self.type_join, self.query_buckets[0]) + \
                             "ON KEYS employee.tasks_ids WHERE employee.name == 'employee-9'"
                if self.covering_index:
                    self.check_explain_covering_index(index_name[0])
                self.query = "SELECT employee.name , employee.tasks_ids " + \
                             "FROM %s as employee %s JOIN %s as new_project_full " % (
                                 query_bucket, self.type_join, self.query_buckets[0]) + \
                             "ON KEYS employee.tasks_ids WHERE employee.name == 'employee-9' limit 10"
                actual_result = self.run_cbq_query()
                actual_result = actual_result['results']
                expected_result = self._generate_full_joined_docs_list(join_type=self.type_join)
                expected_result = [{"name": doc['name'], "tasks_ids": doc['tasks_ids']
                                    }
                                   for doc in expected_result if doc and 'name' in doc and
                                   doc['name'] == 'employee-9']
                expected_result = expected_result[0:10]
                self.query = "create primary index on %s" % query_bucket
                self.run_cbq_query()
                self.query = "SELECT employee.name , employee.tasks_ids " + \
                             "FROM %s as employee %s JOIN %s as new_project_full " % (
                                 query_bucket, self.type_join, self.query_buckets[0]) + \
                             "ON KEYS employee.tasks_ids WHERE employee.name == 'employee-9' limit 10"
                result = self.run_cbq_query()
                diffs = DeepDiff(actual_result, result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                self.query = "drop primary index on %s" % query_bucket
                self.run_cbq_query()
                # self.assertTrue(expected_result == actual_result)
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                    self.run_cbq_query()

    def test_where_join_keys_not_equal(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT employee.join_day, employee.tasks_ids, new_project_full.project new_project " + \
                         "FROM %s as employee %s JOIN %s as new_project_full " % (query_bucket, self.type_join, self.query_buckets[0]) + \
                         "ON KEYS employee.tasks_ids WHERE employee.join_day != 2"
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [{"join_day": doc['join_day'], "tasks_ids": doc['tasks_ids'],
                                "new_project": doc['project']}
                               for doc in expected_result if doc and 'join_day' in doc and
                               doc['join_day'] != 2]
            self._verify_results(actual_result, expected_result)

    def test_where_join_keys_between(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT employee.join_day, employee.tasks_ids, new_project_full.project new_project " + \
                         "FROM %s as employee %s JOIN %s as new_project_full " % (query_bucket, self.type_join, self.query_buckets[0]) + \
                         "ON KEYS employee.tasks_ids WHERE employee.join_day between 1 and 2"
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [{"join_day": doc['join_day'], "tasks_ids": doc['tasks_ids'],
                                "new_project": doc['project']}
                               for doc in expected_result if doc and 'join_day' in doc and
                               doc['join_day'] <= 2]
            self._verify_results(actual_result, expected_result)

    def test_where_join_keys_not_equal_more_less(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT employee.join_day, employee.tasks_ids, new_project_full.project new_project " + \
                         "FROM %s as employee %s JOIN %s as new_project_full " % (query_bucket, self.type_join, self.query_buckets[0]) + \
                         "ON KEYS employee.tasks_ids WHERE employee.join_day <> 2"
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [{"join_day": doc['join_day'], "tasks_ids": doc['tasks_ids'],
                                "new_project": doc['project']}
                               for doc in expected_result if doc and 'join_day' in doc and
                               doc['join_day'] != 2]
            self._verify_results(actual_result, expected_result)

    def test_where_join_keys_equal_less(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT employee.join_day, employee.tasks_ids, new_project_full.project new_project " + \
                         "FROM %s as employee %s JOIN %s as new_project_full " % (query_bucket, self.type_join, self.query_buckets[0]) + \
                         "ON KEYS employee.tasks_ids WHERE employee.join_day <= 2"
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [{"join_day": doc['join_day'], "tasks_ids": doc['tasks_ids'],
                                "new_project": doc['project']}
                               for doc in expected_result if doc and 'join_day' in doc and
                               doc['join_day'] <= 2]
            self._verify_results(actual_result, expected_result)

    def test_where_join_keys_equal_more(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT employee.join_day, employee.tasks_ids, new_project_full.project new_project " + \
                         "FROM %s as employee %s JOIN %s as new_project_full " % (query_bucket, self.type_join, self.query_buckets[0]) + \
                         "ON KEYS employee.tasks_ids WHERE employee.join_day <= 2"
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [{"join_day": doc['join_day'], "tasks_ids": doc['tasks_ids'],
                                "new_project": doc['project']}
                               for doc in expected_result if doc and 'join_day' in doc and
                               doc['join_day'] <= 2]
            self._verify_results(actual_result, expected_result)

    def test_where_join_keys_equal_more_covering(self):
        created_indexes = []
        ind_list = ["one"]
        index_name = "one"
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            for ind in ind_list:
                index_name = "coveringindex%s" % ind
                if ind == "one":
                    self.query = "CREATE INDEX %s ON %s(join_day, tasks_ids, job_title)  USING %s" % (
                        index_name, query_bucket, self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                created_indexes.append(index_name)
        for query_bucket in self.query_buckets:
            self.query = "EXPLAIN SELECT employee.join_day, employee.tasks_ids, new_project_full.project new_project " + \
                         "FROM %s as employee %s JOIN %s as new_project_full " % (query_bucket, self.type_join, self.query_buckets[0]) + \
                         "ON KEYS employee.tasks_ids WHERE employee.join_day <= 2 order by employee.join_day limit 10"
            if self.covering_index:
                self.check_explain_covering_index(index_name[0])
            self.query = "SELECT employee.join_day, employee.tasks_ids, new_project_full.project new_project " + \
                         "FROM %s as employee %s JOIN %s as new_project_full " % (query_bucket, self.type_join, self.query_buckets[0]) + \
                         "ON KEYS employee.tasks_ids WHERE employee.join_day <= 2  order by employee.join_day limit 10"
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [{"join_day": doc['join_day'], "tasks_ids": doc['tasks_ids'],
                                "new_project": doc['project']}
                               for doc in expected_result if doc and 'join_day' in doc and
                               doc['join_day'] <= 2]
            expected_result = expected_result[0:10]
            for index_name in created_indexes:
                self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                self.run_cbq_query()
            self.query = "CREATE PRIMARY INDEX ON %s" % query_bucket
            self.run_cbq_query()
            self.sleep(15, 'wait for index')
            self.query = "SELECT employee.join_day, employee.tasks_ids, new_project_full.project new_project " + \
                         "FROM %s as employee %s JOIN %s as new_project_full " % (query_bucket, self.type_join, self.query_buckets[0]) + \
                         "ON KEYS employee.tasks_ids WHERE employee.join_day <= 2  order by employee.join_day limit 10"
            result = self.run_cbq_query()
            diffs = DeepDiff(actual_result, result['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
            self.query = "DROP PRIMARY INDEX ON %s" % query_bucket
            self.run_cbq_query()

    def test_join_unnest_alias(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT task2 FROM %s emp1 JOIN %s" % (query_bucket, query_bucket) + \
                         " task ON KEYS emp1.tasks_ids UNNEST emp1.tasks_ids as task2"
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = self._generate_full_joined_docs_list()
            expected_result = [{"task2": task} for doc in expected_result
                               for task in doc['tasks_ids']]
            self._verify_results(actual_result, expected_result)

    def test_unnest(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT emp.name, task FROM %s emp %s UNNEST emp.tasks_ids task" % (
                query_bucket, self.type_join)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = self.generate_full_docs_list(self.gens_load)
            expected_result = [{"task": task, "name": doc["name"]}
                               for doc in expected_result for task in doc['tasks_ids']]
            if self.type_join.upper() == JOIN_LEFT:
                expected_result.extend([{}] * self.gens_tasks[-1].end)

            self._verify_results(actual_result, expected_result)

    def test_unnest_covering(self):
        created_indexes = []
        ind_list = ["one"]
        index_name = "one"
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            for ind in ind_list:
                index_name = "coveringindex%s" % ind
                if ind == "one":
                    self.query = "CREATE INDEX %s ON %s(name, task, tasks_ids)  USING %s" % (
                        index_name, query_bucket, self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                created_indexes.append(index_name)
        for query_bucket in self.query_buckets:
            try:
                self.query = "EXPLAIN SELECT emp.name, task FROM %s emp %s UNNEST emp.tasks_ids task where emp.name is not null" % (
                    query_bucket, self.type_join)
                if self.covering_index:
                    self.check_explain_covering_index(index_name[0])
                self.query = "SELECT emp.name, task FROM %s emp %s UNNEST emp.tasks_ids task where emp.name is not null" % (
                    query_bucket, self.type_join)
                actual_result = self.run_cbq_query()
                actual_result = actual_result['results']
                expected_result = self.generate_full_docs_list(self.gens_load)
                expected_result = [{"task": task, "name": doc["name"]} for doc in expected_result for task in
                                   doc['tasks_ids']]
                if self.type_join.upper() == JOIN_LEFT:
                    expected_result.extend([{}] * self.gens_tasks[-1].end)
                try:
                    self.query = "create primary index if not exists on %s" % query_bucket
                    self.run_cbq_query()
                    self.sleep(15, 'wait for index')
                except Exception as e:
                    if "The index #primary already exists." in str(e):
                        continue
                    else:
                        self.fail("Index failed to be created! {0}".format(str(e)))
                self.query = "SELECT emp.name, task FROM %s emp use index (`#primary`) %s UNNEST emp.tasks_ids task where emp.name is not null" % (
                    query_bucket, self.type_join)
                result = self.run_cbq_query()

                diffs = DeepDiff(actual_result, result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
                # self._verify_results(actual_result, expected_result)
            finally:
                self.query = "drop primary index on %s" % query_bucket
                self.run_cbq_query()
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                    self.run_cbq_query()

    def test_prepared_unnest(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT emp.name, task FROM %s emp %s UNNEST emp.tasks_ids task" % (
                query_bucket, self.type_join)
            self.prepared_common_body()

    ##############################################################################################
    #
    #   SUBQUERY
    ##############################################################################################

    def test_subquery_count(self):
        for query_bucket in self.query_buckets:
            self.query = "select name, ARRAY_LENGTH((select task_name  from %s d use keys %s)) as cn from %s" % (
                query_bucket, str(['test_task-%s' % i for i in range(0, 29)]),
                query_bucket)
            self.run_cbq_query()
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            expected_result = [{'name': doc['name'], 'cn': 29} for doc in all_docs_list]
            expected_result.extend([{'cn': 29}] * 29)
            self._verify_results(actual_result, expected_result)

    def test_subquery_select(self):
        for query_bucket in self.query_buckets:
            self.query = "select task_name, (select count(task_name) cn from %s d use keys %s) as names from %s" % (
                query_bucket, str(['test_task-%s' % i for i in range(0, 29)]),
                query_bucket)
            self.run_cbq_query()
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result_subquery = {"cn": 29}
            expected_result = [{'names': [expected_result_subquery]}] * len(
                self.generate_full_docs_list(self.gens_load))
            expected_result.extend([{'task_name': doc['task_name'], 'names': [expected_result_subquery]}
                                    for doc in self.generate_full_docs_list(self.gens_tasks)])
            self._verify_results(actual_result, expected_result)

    def test_prepared_subquery_select(self):
        for query_bucket in self.query_buckets:
            self.query = "select task_name, (select count(task_name) cn from %s d use keys %s) as names from %s" % (
                query_bucket, str(['test_task-%s' % i for i in range(0, 29)]),
                query_bucket)
            self.prepared_common_body()

    def test_subquery_where_aggr(self):
        for query_bucket in self.query_buckets:
            self.query = "select name, join_day from %s where join_day =" % query_bucket + \
                         " (select AVG(join_day) as average from %s d use keys %s)[0].average" % (query_bucket,
                                                                                                  str([
                                                                                                      'query-test-Sales-2010-1-1-%s' % i
                                                                                                      for i in
                                                                                                      range(0,
                                                                                                            self.docs_per_day)]))
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = [{'name': doc['name'], 'join_day': doc['join_day']} for doc in all_docs_list if
                               doc['join_day'] == 1]
            self._verify_results(actual_result, expected_result)

    def test_subquery_where_in(self):
        for query_bucket in self.query_buckets:
            self.query = "select name, join_day from %s where join_day IN " % query_bucket + \
                         " (select ARRAY_AGG(join_day) as average from %s d use keys %s)[0].average" % (query_bucket,
                                                                                                        str([
                                                                                                            'query-test-Sales-2010-1-1-%s' % i
                                                                                                            for i in
                                                                                                            range(0,
                                                                                                                  self.docs_per_day)]))
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = [{'name': doc['name'], 'join_day': doc['join_day']}
                               for doc in all_docs_list if doc['join_day'] == 1]
            self._verify_results(actual_result, expected_result)

    def test_where_in_subquery(self):
        for query_bucket in self.query_buckets:
            self.query = "select name, tasks_ids from %s where tasks_ids[0] IN" % query_bucket + \
                         " (select ARRAY_AGG(DISTINCT task_name) as names from %s d " % query_bucket + \
                         "use keys %s where project='MB')[0].names" % '["test_task-1", "test_task-2"]'
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = [{'name': doc['name'], 'tasks_ids': doc['tasks_ids']}
                               for doc in all_docs_list if doc['tasks_ids'] in ['test_task-1', 'test_task-2']]
            self._verify_results(actual_result, expected_result)

    def test_where_in_subquery_not_equal(self):
        for query_bucket in self.query_buckets:
            self.query = "select name, tasks_ids from %s where tasks_ids[0] IN" % query_bucket + \
                         " (select ARRAY_AGG(DISTINCT task_name) as names from %s d " % query_bucket + \
                         "use keys %s where project!='AB')[0].names" % '["test_task-1", "test_task-2"]'
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = [{'name': doc['name'], 'tasks_ids': doc['tasks_ids']}
                               for doc in all_docs_list if
                               ('test_task-1' in doc['tasks_ids'] or 'test_task-2' in doc['tasks_ids'])]
            self._verify_results(actual_result, expected_result)

    def test_where_in_subquery_equal_more(self):
        for query_bucket in self.query_buckets:
            self.query = "select name, tasks_ids,join_day from %s where join_day>=2 and tasks_ids[0] IN" % query_bucket + \
                         " (select ARRAY_AGG(DISTINCT task_name) as names from %s d " % query_bucket + \
                         "use keys %s where project!='AB')[0].names" % '["test_task-1", "test_task-2"]'
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = [{'name': doc['name'], 'tasks_ids': doc['tasks_ids'], 'join_day': doc['join_day']}
                               for doc in all_docs_list if
                               ('test_task-1' in doc['tasks_ids'] or 'test_task-2' in doc['tasks_ids']) and doc[
                                   'join_day'] >= 2]
            self._verify_results(actual_result, expected_result)

    def test_where_in_subquery_equal_less(self):
        for query_bucket in self.query_buckets:
            self.query = "select name, tasks_ids,join_day from %s where join_day<=2 and tasks_ids[0] IN" % query_bucket + \
                         " (select ARRAY_AGG(DISTINCT task_name) as names from %s d " % query_bucket + \
                         "use keys %s where project!='AB')[0].names" % '["test_task-1", "test_task-2"]'
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = [{'name': doc['name'], 'tasks_ids': doc['tasks_ids'], 'join_day': doc['join_day']}
                               for doc in all_docs_list if
                               ('test_task-1' in doc['tasks_ids'] or 'test_task-2' in doc['tasks_ids']) and doc[
                                   'join_day'] <= 2]
            self._verify_results(actual_result, expected_result)

    def test_where_in_subquery_between(self):
        for query_bucket in self.query_buckets:
            self.query = "select name, tasks_ids, join_day from %s where (join_day between 1 and 12) and tasks_ids[0] IN" % query_bucket + \
                         " (select ARRAY_AGG(DISTINCT task_name) as names from %s d " % query_bucket + \
                         "use keys %s where project!='AB')[0].names" % '["test_task-1", "test_task-2"]'
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = [{'name': doc['name'], 'tasks_ids': doc['tasks_ids'], 'join_day': doc['join_day']}
                               for doc in all_docs_list if
                               ('test_task-1' in doc['tasks_ids'] or 'test_task-2' in doc['tasks_ids']) and doc[
                                   'join_day'] <= 12]
            self._verify_results(actual_result, expected_result)

    def test_subquery_exists(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT name FROM %s d1 WHERE " % query_bucket + \
                         "EXISTS (SELECT * FROM %s d  use keys toarray(d1.tasks_ids[0]))" % query_bucket
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            tasks_ids = [doc["task_name"] for doc in self.generate_full_docs_list(self.gens_tasks)]
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = [{'name': doc['name']} for doc in all_docs_list if doc['tasks_ids'][0] in tasks_ids]
            self._verify_results(actual_result, expected_result)

    def test_subquery_exists_where(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT name FROM %s d1 WHERE " % query_bucket + \
                         "EXISTS (SELECT * FROM %s d use keys toarray(d1.tasks_ids[0]) where d.project='MB')" % query_bucket
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            tasks_ids = [doc["task_name"] for doc in self.generate_full_docs_list(self.gens_tasks) if
                         doc['project'] == 'MB']
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = [{'name': doc['name']} for doc in all_docs_list if doc['tasks_ids'][0] in tasks_ids]
            self._verify_results(actual_result, expected_result)

    def test_subquery_exists_and(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT name FROM %s d1 WHERE " % query_bucket + \
                         "EXISTS (SELECT * FROM %s d  use keys toarray(d1.tasks_ids[0])) and join_mo>5" % query_bucket
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            tasks_ids = [doc["task_name"] for doc in self.generate_full_docs_list(self.gens_tasks)]
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = [{'name': doc['name']} for doc in all_docs_list if
                               doc['tasks_ids'][0] in tasks_ids and doc['join_mo'] > 5]
            self._verify_results(actual_result, expected_result)

    def test_subquery_from(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT TASKS.task_name FROM (SELECT task_name, project FROM %s WHERE project = 'CB') as TASKS" % query_bucket
            all_docs_list = self.generate_full_docs_list(self.gens_tasks)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = [{'task_name': doc['task_name']} for doc in all_docs_list if doc['project'] == 'CB']
            self._verify_results(actual_result, expected_result)

    def test_subquery_from_join(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT EMP.name Name, TASK.project proj FROM (SELECT tasks_ids, name FROM " + \
                         "%s WHERE join_mo>10) as EMP %s JOIN %s TASK ON KEYS EMP.tasks_ids" % (
                             query_bucket, self.type_join, query_bucket)
            all_docs_list = self._generate_full_joined_docs_list(join_type=self.type_join)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = [{'Name': doc['name'], 'proj': doc['project']} for doc in all_docs_list if
                               doc['join_mo'] > 10]
            self._verify_results(actual_result, expected_result)

    ##############################################################################################
    #
    #   KEY
    ##############################################################################################

    def test_keys(self):
        for query_bucket in self.query_buckets:
            keys_select = []
            generator = copy.deepcopy(self.gens_tasks[0])
            for i in range(5):
                key, _ = next(generator)
                keys_select.append(key)
            self.query = 'select task_name FROM %s USE KEYS %s' % (query_bucket, keys_select)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            full_list = self.generate_full_docs_list(self.gens_tasks, keys=keys_select)
            expected_result = [{"task_name": doc['task_name']} for doc in full_list]
            self._verify_results(actual_result, expected_result)

            keys_select.extend(["wrong"])
            self.query = 'select task_name FROM %s USE KEYS %s' % (query_bucket, keys_select)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            self._verify_results(actual_result, expected_result)

            self.query = 'select task_name FROM %s USE KEYS ["wrong_one","wrong_second"]' % query_bucket
            actual_result = self.run_cbq_query()
            self.assertFalse(actual_result['results'], "Having a wrong key query returned some result")

    def test_key_array(self):
        for query_bucket in self.query_buckets:
            gen_select = copy.deepcopy(self.gens_tasks[0])
            key_select, value_select = next(gen_select)
            self.query = 'SELECT * FROM %s d USE KEYS ARRAY emp._id FOR emp IN [%s] END' % (query_bucket, value_select)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = self.generate_full_docs_list(self.gens_tasks, keys=[key_select])
            expected_result = [{'d': doc} for doc in expected_result]
            self._verify_results(actual_result, expected_result)

            key2_select, value2_select = next(gen_select)
            self.query = 'SELECT * FROM %s d USE KEYS ARRAY emp._id FOR emp IN [%s,%s] END' % (
                query_bucket, value_select, value2_select)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = self.generate_full_docs_list(self.gens_tasks, keys=[key_select, key2_select])
            expected_result = [{'d': doc} for doc in expected_result]
            self._verify_results(actual_result, expected_result)

    ##############################################################################################
    #
    #   NEST
    ##############################################################################################

    def test_simple_nest_keys(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT * FROM %s emp %s NEST %s tasks ON KEYS emp.tasks_ids" % (
                query_bucket, self.type_join, query_bucket)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            actual_result = {item['emp']['_id']: item for item in actual_result}
            full_list = self._generate_full_nested_docs_list(join_type=self.type_join)
            expected_result = [{"emp": doc['item'], "tasks": doc['items_nested']} for doc in full_list if
                               doc and 'items_nested' in doc]
            expected_result.extend([{"emp": doc['item']} for doc in full_list if 'items_nested' not in doc])
            expected_result = {item['emp']['_id']: item for item in expected_result}
            self.assertEqual(expected_result, actual_result, "Results are not matching")

    def test_simple_nest_key(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT * FROM %s emp %s NEST %s tasks KEY emp.tasks_ids[0]" % (
                query_bucket, self.type_join, query_bucket)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: self._get_for_sort(doc))
            self._delete_ids(actual_result)
            full_list = self._generate_full_nested_docs_list(particular_key=0, join_type=self.type_join)
            expected_result = [{"emp": doc['item'], "tasks": doc['items_nested']} for doc in full_list if
                               doc and 'items_nested' in doc]
            expected_result.extend([{"emp": doc['item']} for doc in full_list if 'items_nested' not in doc])
            expected_result = sorted(expected_result, key=lambda doc: self._get_for_sort(doc))
            self._delete_ids(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_nest_keys_with_array(self):
        for query_bucket in self.query_buckets:
            self.query = "select emp.name, ARRAY item.project FOR item in items end projects " + \
                         "FROM %s emp %s NEST %s items " % (
                             query_bucket, self.type_join, query_bucket) + \
                         "ON KEYS emp.tasks_ids"
            actual_result = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_result['results'], key='projects')
            full_list = self._generate_full_nested_docs_list(join_type=self.type_join)
            expected_result = [{"name": doc['item']['name'],
                                "projects": [nested_doc['project'] for nested_doc in doc['items_nested']]}
                               for doc in full_list if doc and 'items_nested' in doc]
            expected_result.extend([{} for doc in full_list if 'items_nested' not in doc])
            expected_result = self.sort_nested_list(expected_result, key='projects')
            self._verify_results(actual_result, expected_result)

    def test_prepared_nest_keys_with_array(self):
        for query_bucket in self.query_buckets:
            self.query = "select emp.name, ARRAY item.project FOR item in items end projects " + \
                         "FROM %s emp %s NEST %s items " % (
                             query_bucket, self.type_join, query_bucket) + \
                         "ON KEYS emp.tasks_ids"
            self.prepared_common_body()

    def test_nest_keys_where(self):
        for query_bucket in self.query_buckets:
            self.query = "select emp.name, ARRAY item.project FOR item in items end projects " + \
                         "FROM %s emp %s NEST %s items " % (
                             query_bucket, self.type_join, query_bucket) + \
                         "ON KEYS emp.tasks_ids where ANY item IN items SATISFIES item.project == 'CB' end"
            actual_result = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_result['results'], key='projects')
            actual_result = sorted(actual_result, key=lambda doc: (doc['name'], doc['projects']))
            full_list = self._generate_full_nested_docs_list(join_type=self.type_join)
            expected_result = [{"name": doc['item']['name'],
                                "projects": [nested_doc['project'] for nested_doc in doc['items_nested']]}
                               for doc in full_list if doc and 'items_nested' in doc and
                               len([nested_doc for nested_doc in doc['items_nested']
                                    if nested_doc['project'] == 'CB']) > 0]
            expected_result = self.sort_nested_list(expected_result, key='projects')
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'], doc['projects']))
            self._verify_results(actual_result, expected_result)

    def test_nest_keys_where_not_equal(self):
        for query_bucket in self.query_buckets:
            self.query = "select emp.name, ARRAY item.project FOR item in items end projects " + \
                         "FROM %s emp %s NEST %s items " % (
                             query_bucket, self.type_join, query_bucket) + \
                         "ON KEYS emp.tasks_ids where ANY item IN items SATISFIES item.project != 'CB' end"
            actual_result = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_result['results'], key='projects')
            actual_result = sorted(actual_result, key=lambda doc: (doc['name'], doc['projects']))
            full_list = self._generate_full_nested_docs_list(join_type=self.type_join)
            expected_result = [{"name": doc['item']['name'],
                                "projects": [nested_doc['project'] for nested_doc in doc['items_nested']]}
                               for doc in full_list if doc and 'items_nested' in doc and
                               len([nested_doc for nested_doc in doc['items_nested']
                                    if nested_doc['project'] != 'CB']) > 0]
            expected_result = self.sort_nested_list(expected_result, key='projects')
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'], doc['projects']))
            self._verify_results(actual_result, expected_result)

    def test_nest_keys_where_between(self):
        for query_bucket in self.query_buckets:
            self.query = "select emp.name, emp.join_day, ARRAY item.project FOR item in items end projects " + \
                         "FROM %s emp %s NEST %s items " % (
                             query_bucket, self.type_join, query_bucket) + \
                         "ON KEYS emp.tasks_ids where emp.join_day between 2 and 4"
            actual_result = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_result['results'], key='projects')
            actual_result = sorted(actual_result, key=lambda doc: (doc['name'], doc['projects']))
            full_list = self._generate_full_nested_docs_list(join_type=self.type_join)
            expected_result = [{"name": doc['item']['name'], "join_day": doc['item']['join_day'],
                                "projects": [nested_doc['project'] for nested_doc in doc['items_nested']]}
                               for doc in full_list
                               if doc and 'join_day' in doc['item'] and
                               2 <= doc['item']['join_day'] <= 4]
            expected_result = self.sort_nested_list(expected_result, key='projects')
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'], doc['projects']))
            self._verify_results(actual_result, expected_result)

    def test_nest_keys_where_less_more_equal(self):
        for query_bucket in self.query_buckets:
            self.query = "select emp.name, emp.join_day, emp.join_yr, ARRAY item.project FOR item in items end projects " + \
                         "FROM %s emp %s NEST %s items " % (
                             query_bucket, self.type_join, query_bucket) + \
                         "ON KEYS emp.tasks_ids where emp.join_day <= 4 and emp.join_yr>=2010"
            actual_result = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_result['results'], key='projects')
            full_list = self._generate_full_nested_docs_list(join_type=self.type_join)
            expected_result = [{"name": doc['item']['name'], "join_day": doc['item']['join_day'],
                                'join_yr': doc['item']['join_yr'],
                                "projects": [nested_doc['project'] for nested_doc in doc['items_nested']]}
                               for doc in full_list
                               if doc and 'join_day' in doc['item'] and
                               doc['item']['join_day'] <= 4 and doc['item']['join_yr'] >= 2010]
            expected_result = self.sort_nested_list(expected_result, key='projects')
            self._verify_results(actual_result, expected_result)

    def test_dual(self):
        self.query = "select 1"
        actual_result = self.run_cbq_query()
        self.query = "select 1 from system:dual"
        expected_result = self.run_cbq_query()
        self._verify_results(actual_result['results'], expected_result['results'])

    def test_cartesian_join(self):
        self.run_cbq_query("CREATE INDEX adv_job_title ON `default`(`job_title`)")
        join_query = 'select a.name from default as a, default as b where a.job_title = b.job_title and a.join_yr = 2011 and a.join_mo = 1 and a.job_title = "Engineer" order by a.`_id` limit 3'
        expected_result = [{"name": "employee-1"}, {"name": "employee-1"}, {"name": "employee-1"}]
        actual_result = self.run_cbq_query(join_query)
        self._verify_results(actual_result['results'], expected_result)

    def test_lateral_join(self):
        self.run_cbq_query("CREATE INDEX adv_job_title IF NOT EXISTS ON `default`(`job_title`)")
        join_query = 'SELECT a.name FROM default as a, LATERAL (SELECT name FROM default b WHERE a.job_title = b.job_title and b.join_yr = 2011 and b.join_mo = 1 and b.job_title = "Engineer" ORDER BY a.`_id`) as c LIMIT 3'
        expected_result = [{"name": "employee-1"}, {"name": "employee-1"}, {"name": "employee-1"}]
        actual_result = self.run_cbq_query(join_query)
        self._verify_results(actual_result['results'], expected_result)

        join_query = 'SELECT a.name FROM default as a JOIN LATERAL default b ON b.job_title WHERE a.join_yr = 2011 and a.join_mo = 1 and a.job_title = "Engineer" ORDER BY a.`_id` LIMIT 3'
        actual_result = self.run_cbq_query(join_query)
        self._verify_results(actual_result['results'], expected_result)

        join_query = 'SELECT a.name FROM default as a, LATERAL default b WHERE a.job_title = b.job_title and a.join_yr = 2011 and a.join_mo = 1 and a.job_title = "Engineer" ORDER BY a.`_id` LIMIT 3'
        actual_result = self.run_cbq_query(join_query)
        self._verify_results(actual_result['results'], expected_result)

    def test_MB59084(self):
        upsert = 'UPSERT INTO default (KEY k, VALUE v) SELECT "k00"||TO_STR(d) AS k, {"c1":d, "c2":d, "c3":d} AS v FROM ARRAY_RANGE(1,10) AS d'
        index = 'CREATE INDEX ix1 ON default(c1,c2, c3)'
        self.run_cbq_query(upsert)
        self.run_cbq_query(index)

        udf1 = 'CREATE OR REPLACE FUNCTION f11(a) {(SELECT l, r FROM default AS l JOIN default AS r USE NL ON l.c3=r.c3 WHERE l.c1 > 0 AND r.c1 > 0 AND r.c2 = a)}'
        udf2 = 'CREATE OR REPLACE FUNCTION f12(a) {(SELECT l, r FROM default AS l JOIN default AS r  ON l.c3=r.c3 WHERE l.c1 > 0 AND r.c1 > 0 AND r.c2 = a)}'

        self.run_cbq_query(udf1)
        self.run_cbq_query(udf2)

        expected_result = [
            {"$1": [{"l": {"c1": 1,"c2": 1,"c3": 1}, "r": {"c1": 1,"c2": 1,"c3": 1}}]},
            {"$1": [{"l": {"c1": 2,"c2": 2,"c3": 2}, "r": {"c1": 2,"c2": 2,"c3": 2}}]},
            {"$1": [{"l": {"c1": 3,"c2": 3,"c3": 3}, "r": {"c1": 3,"c2": 3,"c3": 3}}]},
            {"$1": [{"l": {"c1": 4,"c2": 4,"c3": 4}, "r": {"c1": 4,"c2": 4,"c3": 4}}]},
            {"$1": [{"l": {"c1": 5,"c2": 5,"c3": 5}, "r": {"c1": 5,"c2": 5,"c3": 5}}]},
            {"$1": [{"l": {"c1": 6,"c2": 6,"c3": 6}, "r": {"c1": 6,"c2": 6,"c3": 6}}]},
            {"$1": [{"l": {"c1": 7,"c2": 7,"c3": 7}, "r": {"c1": 7,"c2": 7,"c3": 7}}]},
            {"$1": [{"l": {"c1": 8,"c2": 8,"c3": 8}, "r": {"c1": 8,"c2": 8,"c3": 8}}]},
            {"$1": [{"l": {"c1": 9,"c2": 9,"c3": 9}, "r": {"c1": 9,"c2": 9,"c3": 9}}]}
        ]
        result1 = self.run_cbq_query('SELECT f11(t.c1) FROM default AS t WHERE t.c1 > 0')
        result2 = self.run_cbq_query('SELECT f12(t.c1) FROM default AS t WHERE t.c1 > 0')

        self.assertEqual(expected_result, result1['results'])
        self.assertEqual(expected_result, result2['results'])

        expected_result2 = [
            {"l": {"c1": 1,"c2": 1,"c3": 1},"r": {"c1": 1,"c2": 1,"c3": 1}},
            {"l": {"c1": 2,"c2": 2,"c3": 2},"r": {"c1": 2,"c2": 2,"c3": 2}},
            {"l": {"c1": 3,"c2": 3,"c3": 3},"r": {"c1": 3,"c2": 3,"c3": 3}}
        ]
        result3 = self.run_cbq_query('WITH a AS ([1,2,3]) SELECT l, r FROM default l JOIN default r USE NL ON l.c3 = r.c3 WHERE l.c1 > 0 and r.c1 > 0 AND r.c2 IN a')
        self.assertEqual(expected_result2, result3['results'])

    def test_MB62254(self):
        query = 'select * from {} a left join {} b on false'
        result = self.run_cbq_query(query)
        self.assertEqual(result['results'], [{"a": {}}])

    def test_MB63024(self):
        upsert = 'UPSERT INTO default VALUES("k01", {"cid": "c01", "status":"active", "pid": "p01"})'
        index = 'CREATE INDEX ix11 ON default(cid, status, pid)'
        self.run_cbq_query(upsert)
        self.run_cbq_query(index)

        join_query = 'SELECT a.* FROM default AS a LEFT JOIN (SELECT b.pid, COUNT(1) AS cnt FROM default AS b WHERE b.cid > "c01" AND b.status IN [\'active\'] GROUP BY b.pid) AS m USE NL ON m.pid = a.cid WHERE a.cid IS NOT NULL'
        result = self.run_cbq_query(join_query)
        expected = [{"cid": "c01", "pid": "p01", "status": "active"}]
        self.assertEqual(expected, result['results'], f"Expect: {expected} but actual: {result['results']}")

    def test_MB63673(self):
        upsert = 'CREATE INDEX ix20 ON default(peroid.startDateTime, peroid.endDateTime)'
        index = 'UPSERT INTO default VALUES("m01", { "peroid": { "endDateTime": "2019-10-31T05:37:00.059Z", "startDateTime": "2019-10-01T05:37:00.059Z" }, "factor":0.1})'
        self.run_cbq_query(upsert)
        self.run_cbq_query(index)
        data = [{"Factor":0.5,"peroid":{"endDateTime":"2019-10-31T05:37:00.059Z","startDateTime":"2019-10-01T05:37:00.059Z"}}]
        merge_query = f"MERGE INTO default AS m USING {data} p ON DATE_DIFF_STR(m.peroid.startDateTime, p.peroid.startDateTime, 'millisecond') = 0 AND DATE_DIFF_STR(m.peroid.endDateTime, p.peroid.endDateTime, 'millisecond') = 0 WHEN MATCHED THEN UPDATE SET m.factor = p.Factor RETURNING *"
        result = self.run_cbq_query(merge_query)
        self.log.info(result['metrics'])
        self.assertTrue(result['metrics']['mutationCount'], 1)

    def test_MB63647(self):
        index = 'CREATE INDEX ix31 ON default(c1,c2)'
        upsert1 = 'UPSERT INTO default VALUES("k01",{"c1":1, "c2":1})'
        upsert2 = 'UPSERT INTO default VALUES("k02",{"c1":2, "c2":2})'
        self.run_cbq_query(index)
        self.run_cbq_query(upsert1)
        self.run_cbq_query(upsert2)
        query = 'SELECT d2.* FROM (SELECT c2, MAX(c1) AS c1 FROM default AS d WHERE d.c1 > 0 GROUP BY c2) AS d1 JOIN default d2 ON d1.c2 = d2.c2 AND d1.c1 = d2.c1'
        self.run_cbq_query(query)

    # MB-63947
    def test_leftjoin_subquery(self):
        left_join = 'SELECT c1.c11, sub.c32 FROM default.s1.c1 LEFT JOIN ( SELECT c2.c21, c3.c32 FROM default.s1.c2 LEFT JOIN default.s1.c3 ON c2.c22 = c3.c32 ) AS sub ON c1.c11 = sub.c21 WHERE c1.c11 > 0 ORDER BY c1.c11, sub.c32'
        index1 = 'CREATE INDEX ix1 ON default.s1.c1(c11, c12)'
        index2 = 'CREATE INDEX ix2 ON default.s1.c2(c21 INCLUDE MISSING)'
        index3 = 'CREATE INDEX ix3 ON default.s1.c3(c32, c31)'
        insert1 = 'INSERT INTO default.s1.c1 (key _k, value _v) SELECT "k1" || lpad(tostring(i), 3, "0") as _k, {"c11": i, "c12": i+10, "s11": lpad(tostring(i), 1024, "0"), "s12": lpad(tostring(i), 1024, "0")} as _v FROM array_range(0,3) as i'
        insert2 = 'INSERT INTO default.s1.c2 (key _k, value _v) SELECT "k2" || lpad(tostring(i), 3, "0") as _k, {"c21": i, "c22": i+20, "s21": lpad(tostring(i), 1024, "0"), "s22": lpad(tostring(i), 1024, "0")} as _v FROM array_range(0,5) as i'
        insert3 = 'INSERT INTO default.s1.c3 (key _k, value _v) SELECT "k3" || lpad(tostring(i), 4, "0") as _k, {"c31": i, "c32": IMod(i,211), "s31": lpad(tostring(i), 1024, "0"), "s32": lpad(tostring(i), 1024, "0")} as _v FROM array_range(0,1005) as i'
        # create scope and collections
        self.run_cbq_query('CREATE SCOPE default.s1 IF not exists')
        self.sleep(3)
        self.run_cbq_query('CREATE COLLECTION default.s1.c1 IF not exists')
        self.run_cbq_query('CREATE COLLECTION default.s1.c2 IF not exists')
        self.run_cbq_query('CREATE COLLECTION default.s1.c3 IF not exists')
        # insert documents
        self.run_cbq_query(insert1)
        self.run_cbq_query(insert2)
        self.run_cbq_query(insert3)
        # create indexes
        self.run_cbq_query(index1)
        self.run_cbq_query(index2)
        self.run_cbq_query(index3)
        # run query
        expected = [
            {"c11": 1,"c32": 21},
            {"c11": 1,"c32": 21},
            {"c11": 1,"c32": 21},
            {"c11": 1,"c32": 21},
            {"c11": 1,"c32": 21},
            {"c11": 2,"c32": 22},
            {"c11": 2,"c32": 22},
            {"c11": 2,"c32": 22},
            {"c11": 2,"c32": 22},
            {"c11": 2,"c32": 22}
        ]
        result = self.run_cbq_query(left_join)
        self.assertEqual(result['results'], expected)

    def test_MB65976(self):
        # create collection mb65976 if not exists
        self.query = "CREATE COLLECTION mb65976 IF NOT EXISTS"
        self.run_cbq_query(query_context='default._default')

        # drop primary index on mb65976 collection if exists
        self.query = "DROP PRIMARY INDEX IF EXISTS ON mb65976"
        self.run_cbq_query(query_context='default._default')

        # insert documents into mb65976 collection
        self.run_cbq_query('UPSERT INTO mb65976 VALUES ("k_001", {"type": "airline", "c20":1, "c21": 10})', query_context='default._default')
        self.run_cbq_query('UPSERT INTO mb65976 VALUES ("k_002", {"type": "airline", "c20":2, "c21": 20})', query_context='default._default')
        self.run_cbq_query('UPSERT INTO mb65976 VALUES ("k_003", {"type": "airline", "c20":3, "c21": 30})', query_context='default._default')
        self.run_cbq_query('UPSERT INTO mb65976 VALUES ("k_011", {"type": "airport", "docType": "0", "c1": 1, "c2": 2, "c3":3, "c4": 4})', query_context='default._default')
        self.run_cbq_query('UPSERT INTO mb65976 VALUES ("k_012", {"type": "airport", "docType": "0", "c1": 2, "c2": 4, "c3":6, "c4": 8})', query_context='default._default')

        # create index on mb65976 collection if not exists
        self.run_cbq_query("CREATE INDEX ix11 IF NOT EXISTS ON mb65976 (c20,c21) PARTITION BY hash(c20) WHERE type = 'airline' AND c20 != c21", query_context='default._default')
        self.run_cbq_query("CREATE INDEX ix12 IF NOT EXISTS ON mb65976 (c1, c2,c3,c4) WHERE type = 'airport' AND docType = '0'", query_context='default._default')

        # explain query with use_cbo=True
        explain_query = '''EXPLAIN SELECT 1 FROM mb65976 AS a 
        LEFT JOIN mb65976 AS b USE HASH(BUILD) ON b.type = "airport" AND a.c20 = b.c1 AND b.docType = "0"
        WHERE a.type = "airline" AND a.c20 != a.c21 AND b.c1 IS MISSING'''
        explain_result = self.run_cbq_query(explain_query, query_context='default._default', query_params={'use_cbo': True})
        self.log.info(f"Explain result: {explain_result}")

        # Check explain contains ix11 and ix12 and not sequentialscan index
        self.log.info("Check explain uses ix11 and ix12 and not sequentialscan index")
        self.assertTrue('ix11' in str(explain_result), "ix11 is not in explain result")
        self.assertTrue('ix12' in str(explain_result), "ix12 is not in explain result")
        self.assertTrue('sequentialscan' not in str(explain_result), "sequentialscan is in explain result")

    def test_MB65949(self):
        try:
            # create collection mb65949 if not exists
            self.run_cbq_query("CREATE COLLECTION mb65949 IF NOT EXISTS", query_context='default._default')
            self.run_cbq_query("DROP PRIMARY INDEX IF EXISTS ON mb65949", query_context='default._default')

            # insert documents into mb65949 collection
            insert_query = '''UPSERT INTO mb65949 (key _k, value _v)
            SELECT "key" || lpad(tostring(i), 10, "0") AS _k,
            {"c1": i, "c2": imod(i, 8191), "c3": imod(i, 160000), "c4": imod(Random(), 100001), "c5": lpad(tostring(i), 256, "0"), "c6": lpad(tostring(i), 2048, "0")} AS _v
            FROM ARRAY_RANGE(0, 512000) AS i'''
            self.run_cbq_query(insert_query, query_context='default._default')

            # update statistics for c2 field since we need histogram for c2 field
            self.run_cbq_query("UPDATE STATISTICS FOR mb65949(c2)", query_context='default._default')

            # create index on mb65949 collection
            self.run_cbq_query("CREATE INDEX ix1 ON mb65949 (c3, c4, c1, c5)", query_context='default._default')

            # wait for stats to be updated
            self.sleep(10)

            # explain query with use_cbo=True
            explain_query = '''EXPLAIN SELECT /*+ ORDERED */ ta.c3, META(ta).id AS docId
            FROM (SELECT c3, MAX(c4) AS c4 FROM mb65949 WHERE c3 IS NOT MISSING AND c4 IS NOT MISSING GROUP BY c3) AS tb
            INNER JOIN mb65949 ta ON ta.c3 = tb.c3 AND ta.c4 = tb.c4
            WHERE ta.c1 <= 300 AND ta.c2 >= 10'''
            explain_result = self.run_cbq_query(explain_query, query_context='default._default', query_params={'use_cbo': True})
            self.log.info(f"Explain result: {explain_result}")

            # check explain does not contain NestedLoopJoin but contains HashJoin
            self.log.info("Check explain does not contain NestedLoopJoin but contains HashJoin")
            self.assertTrue('NestedLoopJoin' not in str(explain_result), "NestedLoopJoin is in explain result")
            self.assertTrue('HashJoin' in str(explain_result), "HashJoin is not in explain result")
        finally:
            self.run_cbq_query("DROP COLLECTION mb65949 IF EXISTS", query_context='default._default')

    def test_MB65746(self):
        try:
            # create collection mb65746 and indexes if not exists
            self.run_cbq_query("CREATE COLLECTION mb65746 IF NOT EXISTS", query_context='default._default')
            self.run_cbq_query("DROP INDEX ix1 IF EXISTS ON mb65746", query_context='default._default')
            self.run_cbq_query("DROP INDEX ix2 IF EXISTS ON mb65746", query_context='default._default')
            self.run_cbq_query("CREATE INDEX ix1 ON mb65746(status,owner,type)", query_context='default._default')
            self.run_cbq_query("CREATE INDEX ix2 ON mb65746(type,date,owner) WHERE type NOT IN ['a1','a2']", query_context='default._default')
            
            # run explain query with use_cbo=False
            explain_query = "EXPLAIN SELECT * FROM mb65746 WHERE type='xx' AND owner='{owner}' AND status IN ['open','close']"
            explain_result = self.run_cbq_query(explain_query, query_context='default._default', query_params={'use_cbo': False})
            self.log.info(f"Explain result: {explain_result}")

            # Check explain does not contain IntersectScan and contains ix1
            self.log.info("Check explain does not contain IntersectScan and contains ix1")
            self.assertTrue('IntersectScan' not in str(explain_result), "IntersectScan is in explain result")
            self.assertTrue('ix1' in str(explain_result), "ix1 is not in explain result")
        finally:
            self.run_cbq_query("DROP COLLECTION mb65746 IF EXISTS", query_context='default._default')

    def test_MB67432(self):
        """
        Test for MB-67432: Ensure the queries do not panic.
        """
        queries = [
            # Original query
            '''
                SELECT n.*
                FROM (SELECT a1
                        FROM {"c10":1, "a1":[{"c20":"xyz"}]} AS d
                        UNNEST d.a1) AS n
                JOIN [{"c1":1}, {"c1":1},{"c1":1},{"c1":1},{"c1":1}] AS r1 ON true;
            ''',
            # Second query: join on r1.c1 = n.c10
            '''
                SELECT n.*
                FROM (SELECT d.c10, a1
                      FROM {"c10":1, "a1":[{"c20":"xyz"}]} AS d
                      UNNEST d.a1) AS n
                JOIN [{"c1":1}, {"c1":1},{"c1":1},{"c1":1},{"c1":1}] AS r1 ON r1.c1 = n.c10;
            ''',
            # Third query: WITH, LEFT JOIN
            '''
                SELECT r.*
                FROM (WITH w1 AS (ARRAY {"c1":1} FOR v IN ARRAY_RANGE(0,5) END),
                           w2 AS (SELECT d.c10, a1 FROM {"c10":1, "a1":[{"c20":"xyz"}]} AS d UNNEST d.a1)
                      SELECT n.* FROM w2 AS n LEFT JOIN w1 AS r1 ON r1.c1 = n.c10
                  ) AS r;
            ''',
            # Fourth query: UNNEST with LEFT JOIN and ON true
            '''
                SELECT n.*, r1.c1 AS r1_c1
                FROM (SELECT d.c10, a1
                      FROM {"c10":1, "a1":[{"c20":"xyz"}, {"c20":"abc"}]} AS d
                      UNNEST d.a1) AS n
                LEFT JOIN [{"c1":1}, {"c1":2}] AS r1 ON true;
            ''',
            # Fifth query: JOIN with array of objects, ON with complex expression
            '''
                SELECT n.*, r1.c1
                FROM (SELECT d.c10, a1
                      FROM {"c10":1, "a1":[{"c20":"xyz"}, {"c20":"abc"}]} AS d
                      UNNEST d.a1) AS n
                JOIN [{"c1":1}, {"c1":2}] AS r1 ON r1.c1 = n.c10 AND n.a1.c20 = "xyz";
            ''',
            # Sixth query: UNNEST, JOIN, and WHERE clause
            '''
                SELECT n.*, r1.c1
                FROM (SELECT d.c10, a1
                      FROM {"c10":1, "a1":[{"c20":"xyz"}, {"c20":"abc"}]} AS d
                      UNNEST d.a1) AS n
                JOIN [{"c1":1}, {"c1":2}] AS r1 ON r1.c1 = n.c10
                WHERE n.a1.c20 = "abc";
            ''',
            # Seventh query: WITH, multiple UNNESTs, and JOIN
            '''
                WITH w1 AS (ARRAY {"c1":1, "c2":v} FOR v IN ARRAY_RANGE(0,2) END),
                     w2 AS (SELECT d.c10, a1 FROM {"c10":1, "a1":[{"c20":"xyz"}, {"c20":"abc"}]} AS d UNNEST d.a1)
                SELECT n.*, r1.c2
                FROM w2 AS n
                JOIN w1 AS r1 ON r1.c1 = n.c10 AND r1.c2 = 1;
            ''',
            # Eighth query: UNNEST, JOIN, and subquery in FROM
            '''
                SELECT n.*, r1.c1
                FROM (SELECT d.c10, a1
                      FROM {"c10":1, "a1":[{"c20":"xyz"}, {"c20":"abc"}]} AS d
                      UNNEST d.a1) AS n
                JOIN (SELECT RAW {"c1":1} UNION SELECT RAW {"c1":2}) AS r1 ON r1.c1 = n.c10;
            '''
        ]
        for idx, query in enumerate(queries):
            try:
                result = self.run_cbq_query(query)
                self.log.info(f"MB-67432 query {idx+1} result: {result}")
                # Ensure the query returns results and does not panic
                self.assertIsInstance(result, dict)
                self.assertIn('results', result)
            except Exception as e:
                self.fail(f"MB-67432 query {idx+1} caused an exception: {e}")

    def test_left_join_between_non_existing_field(self):
        """
        MB-51736: LEFT JOIN with BETWEEN operator on non-existing field.
        This test verifies that queries with BETWEEN vs >= and <= operators
        on non-existing fields return correct results.
        """
        scope_name = "s1_leftjoin_nonexist"
        try:
            # Create scope and collections in default bucket
            self.run_cbq_query(f"CREATE SCOPE default.{scope_name} IF NOT EXISTS")
            self.sleep(3)
            self.run_cbq_query(f"CREATE COLLECTION default.{scope_name}.c1 IF NOT EXISTS")
            self.run_cbq_query(f"CREATE COLLECTION default.{scope_name}.c2 IF NOT EXISTS")

            # Insert documents with {"id": 1} in both collections
            self.run_cbq_query(f'INSERT INTO default.{scope_name}.c1 (key, value) VALUES ("doc1", {{"id": 1}})')
            self.run_cbq_query(f'INSERT INTO default.{scope_name}.c2 (key, value) VALUES ("doc2", {{"id": 1}})')

            # Create indexes as specified
            self.run_cbq_query(f"CREATE PRIMARY INDEX ON `default`.`{scope_name}`.`c1`")
            self.run_cbq_query(f"CREATE INDEX `idx1` ON `default`.`{scope_name}`.`c2`(`id`)")

            # Wait for indexes to be ready
            self.sleep(5)

            # Test query 1: LEFT JOIN with BETWEEN operator on non-existing field
            query1 = f"""
            SELECT lhs.id
            FROM default.{scope_name}.c1 lhs
            LEFT JOIN default.{scope_name}.c2 rhs 
            ON lhs.id = rhs.id
                AND rhs.x between 1 and 10
            """
            result1 = self.run_cbq_query(query1)
            self.log.info(f"Query 1 (BETWEEN) result: {result1}")

            # Test query 2: LEFT JOIN with >= and <= operators on non-existing field
            query2 = f"""
            SELECT lhs.id
            FROM default.{scope_name}.c1 lhs
            LEFT JOIN default.{scope_name}.c2 rhs 
            ON lhs.id = rhs.id
                AND rhs.x >= 1 and rhs.x <= 10
            """
            result2 = self.run_cbq_query(query2)
            self.log.info(f"Query 2 (>= and <=) result: {result2}")

            # Both queries should return the same result: 1 document with id: 1
            # The LEFT JOIN should preserve the left side document even when the right side
            # condition fails due to non-existing field
            expected_result = [{"id": 1}]

            self.assertEqual(result1['results'], expected_result, 
                           f"Query 1 (BETWEEN) should return {expected_result}, but got {result1['results']}")
            self.assertEqual(result2['results'], expected_result, 
                           f"Query 2 (>= and <=) should return {expected_result}, but got {result2['results']}")

            # Verify both queries return the same result
            self.assertEqual(result1['results'], result2['results'], 
                           "Both queries should return the same result")
        finally:
            # Clean up
            try:
                self.run_cbq_query(f"DROP SCOPE `default`.`{scope_name}` IF EXISTS")
            except Exception as e:
                self.log.warning(f"Cleanup failed: {e}")