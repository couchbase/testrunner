import copy
from tuqquery.tuq import QueryTests
from .tuq_sanity import QuerySanityTests
import time
from deepdiff import DeepDiff

JOIN_INNER = "INNER"
JOIN_LEFT = "LEFT"
JOIN_RIGHT = "RIGHT"


class JoinTests(QuerySanityTests, QueryTests):
    def setUp(self):
        try:
            self.dataset = 'join'
            super(JoinTests, self).setUp()
            self.gens_tasks = self.gen_docs(type='tasks')
            self.type_join = self.input.param("type_join", JOIN_INNER)
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
        for bucket in self.buckets:
            self.query = "SELECT employee.name, employee.tasks_ids, new_project.project " +\
            "FROM %s as employee %s JOIN default as new_project " % (bucket.name, self.type_join) +\
            "ON KEYS employee.tasks_ids"
            time.sleep(30)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            full_list = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [doc for doc in full_list if not doc]
            expected_result.extend([{"name" : doc['name'], "tasks_ids" : doc['tasks_ids'], "project" : doc['project']}
                                    for doc in full_list if doc and 'project' in doc])
            #expected_result.extend([{"name" : doc['name'], "tasks_ids" : doc['tasks_ids']}
                                    #for doc in full_list if doc and not 'project' in doc])
            self._verify_results(actual_result, expected_result)

    def test_prepared_simple_join_keys(self):
        for bucket in self.buckets:
            self.query = "SELECT employee.name, employee.tasks_ids, new_project.project " +\
            "FROM %s as employee %s JOIN default as new_project " % (bucket.name, self.type_join) +\
            "ON KEYS employee.tasks_ids"
            self.prepared_common_body()

    def test_join_several_keys(self):
        for bucket in self.buckets:
            self.query = "SELECT employee.name, employee.tasks_ids, new_task.project, new_task.task_name " +\
            "FROM %s as employee %s JOIN default as new_task " % (bucket.name, self.type_join) +\
            "ON KEYS employee.tasks_ids"
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            full_list = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [doc for doc in full_list if not doc]
            expected_result.extend([{"name" : doc['name'], "tasks_ids" : doc['tasks_ids'], "project" : doc['project'], "task_name" : doc['task_name']}
                                    for doc in full_list if doc and 'project' in doc])
            #expected_result.extend([{"name" : doc['name'], "tasks_ids" : doc['tasks_ids']}
            #                        for doc in full_list if doc and not 'project' in doc])
            self._verify_results(actual_result, expected_result)


    def test_where_join_keys(self):
        for bucket in self.buckets:
            self.query = "SELECT employee.name, employee.tasks_ids, new_project_full.project new_project " +\
            "FROM %s as employee %s JOIN default as new_project_full " % (bucket.name, self.type_join) +\
            "ON KEYS employee.tasks_ids WHERE new_project_full.project == 'IT'"
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [{"name" : doc['name'], "tasks_ids" : doc['tasks_ids'], "new_project" : doc['project']}
                               for doc in expected_result if doc and 'project' in doc and doc['project'] == 'IT']
            self._verify_results(actual_result, expected_result)

    def test_bidirectional_join(self):
            self.query = "create index idxbidirec on %s(join_day)" % self.buckets[0].name
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = "explain SELECT employee.name, employee.join_day " +\
            "FROM %s as employee %s JOIN %s as new_project " % (self.buckets[0].name, self.type_join, self.buckets[0].name) +\
            "ON KEY new_project.join_day FOR employee where new_project.join_day is not null"
            actual_result = self.run_cbq_query()
            self.assertTrue("covers" in str(actual_result))
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.test_explain_particular_index("idxbidirec")
            self.query = "SELECT employee.name, employee.join_day " +\
            "FROM %s as employee %s JOIN %s as new_project " % (self.buckets[0].name, self.type_join, self.buckets[0].name)  +\
            "ON KEY new_project.join_day FOR employee where new_project.join_day is not null"
            actual_result = self.run_cbq_query()
            #self.assertTrue(actual_result['metrics']['resultCount'] == 0, 'Query was not run successfully')
            self.query = "drop index %s.idxbidirec" %self.buckets[0].name;
            self.run_cbq_query()

            self.query = "CREATE INDEX ix1 ON default(docid,name)"
            self.run_cbq_query()
            self.query = 'CREATE INDEX ix2 ON default(docid,name) where type = "wdoc"'
            self.run_cbq_query()
            self.query = "CREATE INDEX ix3 ON default(altid, name, DISTINCT ARRAY p FOR p IN phones END)"
            self.run_cbq_query()
            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % ("default", "w001", {"type":"wdoc", "docid":"x001","name":"wdoc","phones":["123-456-7890", "123-456-7891"],"altid":"x001"})
            self.run_cbq_query()
            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % ("default", "pdoc1", {"type":"pdoc", "docid":"x001","name":"pdoc","phones":["123-456-7890", "123-456-7891"],"altid":"x001"})
            self.run_cbq_query()
            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % ("default", "pdoc2", {"type":"pdoc", "docid":"w001","name":"pdoc","phones":["123-456-7890", "123-456-7891"],"altid":"w001"})
            self.run_cbq_query()
            self.query = 'explain SELECT meta(b1).id b1id FROM default b1 JOIN default b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > ""'
            actual_result= self.run_cbq_query()
            self.assertTrue("covers" in str(actual_result))
            self.assertTrue("ix1" in str(actual_result))

            self.query = 'SELECT meta(b1).id b1id FROM default b1 JOIN default b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > ""'
            actual_result= self.run_cbq_query()

            self.assertTrue(actual_result['results']==[{'b1id': 'w001'}])
            self.query = 'explain SELECT meta(b1).id b1id, meta(b2).id b2id FROM default b1 JOIN default b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > ""'
            actual_result= self.run_cbq_query()
            self.assertTrue("covers" in str(actual_result))
            self.assertTrue("ix1" in str(actual_result))
            self.query = 'SELECT meta(b1).id b1id, meta(b2).id b2id FROM default b1 JOIN default b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > ""'
            actual_result= self.run_cbq_query()
            self.assertTrue(actual_result['results']==[{'b1id': 'w001', 'b2id': 'pdoc2'}])
            self.query = 'explain SELECT meta(b1).id b1id, b2.docid FROM default b1 JOIN default b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > ""'
            actual_result= self.run_cbq_query()
            self.assertTrue("covers" in str(actual_result))
            self.assertTrue("ix1" in str(actual_result))
            self.query = 'SELECT meta(b1).id b1id, b2.docid FROM default b1 JOIN default b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > ""'
            actual_result= self.run_cbq_query()
            self.assertTrue(actual_result['results']==[{'docid': 'w001', 'b1id': 'w001'}])
            self.query = 'explain SELECT meta(b1).id b1id, b2.name FROM default b1 JOIN default b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > ""'
            actual_result= self.run_cbq_query()
            self.assertTrue("covers" in str(actual_result))
            self.assertTrue("ix1" in str(actual_result))
            self.query = 'SELECT meta(b1).id b1id, b2.name FROM default b1 JOIN default b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > ""'
            actual_result= self.run_cbq_query()
            self.assertTrue(actual_result['results']==[{'b1id': 'w001', 'name': 'pdoc'}])
            self.query = 'explain SELECT meta(b1).id b1id, b2.name, b3.docid FROM default b1 JOIN default b2 ON KEY b2.docid FOR b1 JOIN default b3 ON KEY b3.docid FOR b1 WHERE meta(b1).id > ""'
            actual_result= self.run_cbq_query()
            self.assertTrue("covers" in str(actual_result))
            self.assertTrue("ix1" in str(actual_result))
            self.query = 'SELECT meta(b1).id b1id, b2.name, b3.docid FROM default b1 JOIN default b2 ON KEY b2.docid FOR b1 JOIN default b3 ON KEY b3.docid FOR b1 WHERE meta(b1).id > ""'
            actual_result= self.run_cbq_query()
            self.assertTrue(actual_result['results']==[{'docid': 'w001', 'b1id': 'w001', 'name': 'pdoc'}])
            self.query = 'explain SELECT meta(b1).id b1id, b2.name, b3.docid  FROM default b1 JOIN default b2 ON KEY b2.docid FOR b1 JOIN default b3 ON KEY b3.docid FOR b2 WHERE meta(b1).id > ""'
            actual_result= self.run_cbq_query()
            self.assertTrue("covers" in str(actual_result))
            self.assertTrue("ix1" in str(actual_result))
            self.query = 'SELECT meta(b1).id b1id, b2.name, b3.docid  FROM default b1 JOIN default b2 ON KEY b2.docid FOR b1 JOIN default b3 ON KEY b3.docid FOR b2 WHERE meta(b1).id > "";'
            actual_result= self.run_cbq_query()
            self.assertTrue(actual_result['metrics']['resultCount']==0)
            self.query = 'explain SELECT meta(b1).id b1id, meta(b2).id, b2.name  FROM default b1 JOIN default b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > "" AND b2.type = "wdoc"'
            actual_result= self.run_cbq_query()
            self.assertTrue("covers" in str(actual_result))
            self.assertTrue("ix2" in str(actual_result))
            self.query = 'SELECT meta(b1).id b1id, meta(b2).id, b2.name  FROM default b1 JOIN default b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > "" AND b2.type = "wdoc"'
            actual_result= self.run_cbq_query()
            self.assertTrue(actual_result['metrics']['resultCount']==0)
            self.query = 'explain SELECT meta(b1).id b1id, b2.name, b3.docid FROM default b1 JOIN default b2 ON KEY b2.docid FOR b1 JOIN default b3 ON KEY b3.docid FOR b1 WHERE meta(b1).id > "" AND b2.type = "wdoc"'
            actual_result= self.run_cbq_query()
            self.assertTrue("covers" in str(actual_result))
            self.assertTrue("ix2" in str(actual_result))
            self.query = 'SELECT meta(b1).id b1id, b2.name, b3.docid FROM default b1 JOIN default b2 ON KEY b2.docid FOR b1 JOIN default b3 ON KEY b3.docid FOR b1 WHERE meta(b1).id > "" AND b2.type = "wdoc"'
            actual_result= self.run_cbq_query()
            self.assertTrue(actual_result['metrics']['resultCount']==0)
            self.query = 'explain SELECT meta(b1).id b1id, b2.name, b3.docid FROM default b1 JOIN default b2 ON KEY b2.docid FOR b1 JOIN default b3 ON KEY b3.docid FOR b2 WHERE meta(b1).id > "" AND b2.type = "wdoc"'
            actual_result= self.run_cbq_query()
            self.assertTrue("covers" in str(actual_result))
            self.assertTrue("ix2" in str(actual_result))
            self.query = 'SELECT meta(b1).id b1id, b2.name, b3.docid FROM default b1 JOIN default b2 ON KEY b2.docid FOR b1 JOIN default b3 ON KEY b3.docid FOR b2 WHERE meta(b1).id > "" AND b2.type = "wdoc"'
            actual_result= self.run_cbq_query()
            self.assertTrue(actual_result['metrics']['resultCount']==0)
            self.query = 'SELECT meta(b1).id b1id, b2 from default b1 JOIN default b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > ""'
            actual_result=self.run_cbq_query()
            self.assertTrue(actual_result['results']==[{'b1id': 'w001', 'b2': {'phones': ['123-456-7890', '123-456-7891'], 'type': 'pdoc', 'docid': 'w001', 'name': 'pdoc', 'altid': 'w001'}}])

            self.query = 'explain SELECT meta(b1).id b1id from default b1 NEST default b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > ""'
            actual_result=self.run_cbq_query()
            self.assertTrue("covers" in str(actual_result))
            self.assertTrue("ix1" in str(actual_result))
            self.assertTrue("(`b2`.`docid`)" in str(actual_result))
            self.query = 'SELECT meta(b1).id b1id from default b1 NEST default b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > ""'
            actual_result=self.run_cbq_query()
            self.assertTrue(actual_result['results']==[{'b1id': 'w001'}])
            self.query = 'explain SELECT meta(b1).id b1id, b2 from default b1 NEST default b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > ""'
            actual_result=self.run_cbq_query()
            self.assertTrue("covers" in str(actual_result))
            self.assertTrue("ix1" in str(actual_result))
            self.assertTrue("(`b2`.`docid`)" in str(actual_result))
            self.query = 'SELECT meta(b1).id b1id, b2 from default b1 NEST default b2 ON KEY b2.docid FOR b1 WHERE meta(b1).id > ""'
            actual_result=self.run_cbq_query()
            self.assertTrue( actual_result['results']== [{'b1id': 'w001', 'b2': [{'phones': ['123-456-7890', '123-456-7891'], 'type': 'pdoc', 'docid': 'w001', 'name': 'pdoc', 'altid': 'w001'}]}] )
            self.query = 'delete from default use keys["w001","pdoc1","pdoc2"]'
            self.run_cbq_query()

    def test_basic_nest_join(self):
           self.query = 'insert into default values("a_12345",{ "_id": "a_12345", "_type": "service" })'
           self.run_cbq_query()
           self.query = 'insert into default values("b_12345", { "_id": "b_12345", "parent": "a_12345", "data": { "a": "b", "c": "d" } })'
           self.run_cbq_query()
           self.query = 'insert into default values("b_12346", { "_id": "b_12346", "parent": "a_12345", "data": { "6": "3", "d": "f" } })'
           self.run_cbq_query()
           self.query = 'CREATE INDEX idx_parent ON default( parent )'
           self.run_cbq_query()
           self.query = 'SELECT * FROM default a NEST default b ON KEY b.parent FOR a'
           actual_result = self.run_cbq_query()
           self.assertTrue(actual_result['results']==([{'a': {'_type': 'service', '_id': 'a_12345'}, 'b': [{'_id': 'b_12345', 'data': {'a': 'b', 'c': 'd'}, 'parent': 'a_12345'}, {'_id': 'b_12346', 'data': {'d': 'f', '6': '3'}, 'parent': 'a_12345'}]}]))
           self.query = 'SELECT * FROM default a join default b ON KEY b.parent FOR a'
           actual_result = self.run_cbq_query()
           self.assertTrue(actual_result['results']==([{'a': {'_type': 'service', '_id': 'a_12345'}, 'b': {'_id': 'b_12345', 'data': {'a': 'b', 'c': 'd'}, 'parent': 'a_12345'}}, {'a': {'_type': 'service', '_id': 'a_12345'}, 'b': {'_id': 'b_12346', 'data': {'d': 'f', '6': '3'}, 'parent': 'a_12345'}}]))
           self.query = 'delete from default use keys ["a_12345","b_12345","b_12346"]'
           self.run_cbq_query()

    def test_where_join_keys_covering(self):
        created_indexes = []
        ind_list = ["one"]
        index_name="one"
        for bucket in self.buckets:
            for ind in ind_list:
                index_name = "coveringindex%s" % ind
                if ind =="one":
                    self.query = "CREATE INDEX %s ON %s(name, tasks_ids,job_title)  USING %s" % (index_name, bucket.name, self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                created_indexes.append(index_name)
        for bucket in self.buckets:
          try:
            self.query = "EXPLAIN SELECT employee.name, employee.tasks_ids, employee.job_title new_project " +\
                         "FROM %s as employee %s JOIN default as new_project_full " % (bucket.name, self.type_join) +\
                         "ON KEYS employee.tasks_ids WHERE employee.name == 'employee-9'"
            if self.covering_index:
                self.check_explain_covering_index(index_name[0])
            self.query = "SELECT employee.name , employee.tasks_ids " +\
                         "FROM %s as employee %s JOIN default as new_project_full " % (bucket.name, self.type_join) +\
                         "ON KEYS employee.tasks_ids WHERE employee.name == 'employee-9' limit 10"
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [{"name" : doc['name'], "tasks_ids" : doc['tasks_ids']
                                }
            for doc in expected_result if doc and 'name' in doc and\
                                          doc['name'] == 'employee-9']
            expected_result = expected_result[0:10]
            self.query = "create primary index on %s" %bucket.name
            self.run_cbq_query()
            self.query = "SELECT employee.name , employee.tasks_ids " +\
                         "FROM %s as employee %s JOIN default as new_project_full " % (bucket.name, self.type_join) +\
                         "ON KEYS employee.tasks_ids WHERE employee.name == 'employee-9' limit 10"
            result = self.run_cbq_query()
            diffs = DeepDiff(actual_result, result['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
          finally:
            self.query = "drop primary index on %s" %bucket.name
            self.run_cbq_query()
            #self.assertTrue(expected_result == actual_result)
            for index_name in created_indexes:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_where_join_keys_not_equal(self):
        for bucket in self.buckets:
            self.query = "SELECT employee.join_day, employee.tasks_ids, new_project_full.project new_project " +\
            "FROM %s as employee %s JOIN default as new_project_full " % (bucket.name, self.type_join) +\
            "ON KEYS employee.tasks_ids WHERE employee.join_day != 2"
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [{"join_day" : doc['join_day'], "tasks_ids" : doc['tasks_ids'],
                                "new_project" : doc['project']}
                               for doc in expected_result if doc and 'join_day' in doc and\
                               doc['join_day'] != 2]
            self._verify_results(actual_result, expected_result)

    def test_where_join_keys_between(self):
        for bucket in self.buckets:
            self.query = "SELECT employee.join_day, employee.tasks_ids, new_project_full.project new_project " +\
            "FROM %s as employee %s JOIN default as new_project_full " % (bucket.name, self.type_join) +\
            "ON KEYS employee.tasks_ids WHERE employee.join_day between 1 and 2"
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [{"join_day" : doc['join_day'], "tasks_ids" : doc['tasks_ids'],
                                "new_project" : doc['project']}
                               for doc in expected_result if doc and 'join_day' in doc and\
                               doc['join_day'] <= 2]
            self._verify_results(actual_result, expected_result)

    def test_where_join_keys_not_equal_more_less(self):
        for bucket in self.buckets:
            self.query = "SELECT employee.join_day, employee.tasks_ids, new_project_full.project new_project " +\
            "FROM %s as employee %s JOIN default as new_project_full " % (bucket.name, self.type_join) +\
            "ON KEYS employee.tasks_ids WHERE employee.join_day <> 2"
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [{"join_day" : doc['join_day'], "tasks_ids" : doc['tasks_ids'],
                                "new_project" : doc['project']}
                               for doc in expected_result if doc and 'join_day' in doc and\
                               doc['join_day'] != 2]
            self._verify_results(actual_result, expected_result)

    def test_where_join_keys_equal_less(self):
        for bucket in self.buckets:
            self.query = "SELECT employee.join_day, employee.tasks_ids, new_project_full.project new_project " +\
            "FROM %s as employee %s JOIN default as new_project_full " % (bucket.name, self.type_join) +\
            "ON KEYS employee.tasks_ids WHERE employee.join_day <= 2"
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [{"join_day" : doc['join_day'], "tasks_ids" : doc['tasks_ids'],
                                "new_project" : doc['project']}
                               for doc in expected_result if doc and 'join_day' in doc and\
                               doc['join_day'] <= 2]
            self._verify_results(actual_result, expected_result)

    def test_where_join_keys_equal_more(self):
        for bucket in self.buckets:
            self.query = "SELECT employee.join_day, employee.tasks_ids, new_project_full.job_title new_project " +\
            "FROM %s as employee %s JOIN default as new_project_full " % (bucket.name, self.type_join) +\
            "ON KEYS employee.tasks_ids WHERE employee.join_day <= 2"
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [{"join_day" : doc['join_day'], "tasks_ids" : doc['tasks_ids'],
                                "new_project" : doc['job_title']}
                               for doc in expected_result if doc and 'join_day' in doc and\
                               doc['join_day'] <= 2]
            self._verify_results(actual_result, expected_result)

    def test_where_join_keys_equal_more_covering(self):
        created_indexes = []
        ind_list = ["one"]
        index_name = "one"
        for bucket in self.buckets:
            for ind in ind_list:
                index_name = "coveringindex%s" % ind
                if ind =="one":
                    self.query = "CREATE INDEX %s ON %s(join_day, tasks_ids, job_title)  USING %s" % (index_name, bucket.name, self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                created_indexes.append(index_name)
        for bucket in self.buckets:
            self.query = "EXPLAIN SELECT employee.join_day, employee.tasks_ids, new_project_full.job_title new_project " +\
                         "FROM %s as employee %s JOIN default as new_project_full " % (bucket.name, self.type_join) +\
                         "ON KEYS employee.tasks_ids WHERE employee.join_day <= 2 order by employee.join_day limit 10"
            if self.covering_index:
                self.check_explain_covering_index(index_name[0])
            self.query = "SELECT employee.join_day, employee.tasks_ids, new_project_full.job_title new_project " +\
                         "FROM %s as employee %s JOIN default as new_project_full " % (bucket.name, self.type_join) +\
                         "ON KEYS employee.tasks_ids WHERE employee.join_day <= 2  order by employee.join_day limit 10"
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [{"join_day" : doc['join_day'], "tasks_ids" : doc['tasks_ids'],
                                "new_project" : doc['job_title']}
            for doc in expected_result if doc and 'join_day' in doc and\
                                          doc['join_day'] <= 2]
            expected_result = expected_result[0:10]
            #self.assertTrue(actual_result, expected_result)
            for index_name in created_indexes:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()
            self.query = "CREATE PRIMARY INDEX ON %s" % bucket.name
            self.run_cbq_query()
            self.sleep(15, 'wait for index')
            self.query = "SELECT employee.join_day, employee.tasks_ids, new_project_full.job_title new_project " +\
                         "FROM %s as employee %s JOIN default as new_project_full " % (bucket.name, self.type_join) +\
                         "ON KEYS employee.tasks_ids WHERE employee.join_day <= 2  order by employee.join_day limit 10"
            result = self.run_cbq_query()
            diffs = DeepDiff(actual_result, result['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
            self.query = "DROP PRIMARY INDEX ON %s" % bucket.name
            self.run_cbq_query()


    def test_join_unnest_alias(self):
        for bucket in self.buckets:
            self.query = "SELECT task2 FROM %s emp1 JOIN %s" % (bucket.name, bucket.name) +\
            " task ON KEYS emp1.tasks_ids UNNEST emp1.tasks_ids as task2"
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = self._generate_full_joined_docs_list()
            expected_result = [{"task2" : task} for doc in expected_result
                               for task in doc['tasks_ids']]
            self._verify_results(actual_result, expected_result)

    def test_unnest(self):
        for bucket in self.buckets:
            self.query = "SELECT emp.name, task FROM %s emp %s UNNEST emp.tasks_ids task" % (bucket.name, self.type_join)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = self.generate_full_docs_list(self.gens_load)
            expected_result = [{"task" : task, "name" : doc["name"]}
                               for doc in expected_result for task in doc['tasks_ids']]
            if self.type_join.upper() == JOIN_LEFT:
                expected_result.extend([{}] * self.gens_tasks[-1].end)

            self._verify_results(actual_result, expected_result)

    def test_unnest_covering(self):
        created_indexes = []
        ind_list = ["one"]
        index_name = "one"
        for bucket in self.buckets:
            for ind in ind_list:
                index_name = "coveringindex%s" % ind
                if ind == "one":
                    self.query = "CREATE INDEX %s ON %s(name, task, tasks_ids)  USING %s" % (index_name, bucket.name, self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                created_indexes.append(index_name)
        for bucket in self.buckets:
          try:
            self.query = "EXPLAIN SELECT emp.name, task FROM %s emp %s UNNEST emp.tasks_ids task where emp.name is not null" % (bucket.name, self.type_join)
            if self.covering_index:
                self.check_explain_covering_index(index_name[0])
            self.query = "SELECT emp.name, task FROM %s emp %s UNNEST emp.tasks_ids task where emp.name is not null" % (bucket.name, self.type_join)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = self.generate_full_docs_list(self.gens_load)
            expected_result = [{"task": task, "name": doc["name"]} for doc in expected_result for task in doc['tasks_ids']]
            if self.type_join.upper() == JOIN_LEFT:
                expected_result.extend([{}] * self.gens_tasks[-1].end)

            self.query = "create primary index on %s" % bucket.name
            self.run_cbq_query()
            self.sleep(15, 'wait for index')
            self.query = "SELECT emp.name, task FROM %s emp use index (`#primary`) %s UNNEST emp.tasks_ids task where emp.name is not null" % (bucket.name, self.type_join)
            result = self.run_cbq_query()

            diffs = DeepDiff(actual_result, result['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
            #self._verify_results(actual_result, expected_result)
          finally:
            self.query= "drop primary index on %s" % bucket.name
            self.run_cbq_query()
            for index_name in created_indexes:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name, self.index_type)
                self.run_cbq_query()

    def test_prepared_unnest(self):
        for bucket in self.buckets:
            self.query = "SELECT emp.name, task FROM %s emp %s UNNEST emp.tasks_ids task" % (bucket.name, self.type_join)
            self.prepared_common_body()

##############################################################################################
#
#   SUBQUERY
##############################################################################################

    def test_subquery_count(self):
        for bucket in self.buckets:
            self.query = "select name, ARRAY_LENGTH((select task_name  from %s d use keys %s)) as cn from %s" % (bucket.name, str(['test_task-%s' % i for i in range(0, 29)]),
                                                                                                                    bucket.name)
            self.run_cbq_query()
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            expected_result= [{'name': doc['name'],'cn' : 29} for doc in all_docs_list]
            expected_result.extend([{'cn' : 29}] * 29)
            self._verify_results(actual_result, expected_result)

    def test_subquery_select(self):
        for bucket in self.buckets:
            self.query = "select task_name, (select count(task_name) cn from %s d use keys %s) as names from %s" % (bucket.name, str(['test_task-%s' % i for i in range(0, 29)]),
                                                                                                                    bucket.name)
            self.run_cbq_query()
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result_subquery = {"cn" : 29}
            expected_result = [{'names' : [expected_result_subquery]}] * len(self.generate_full_docs_list(self.gens_load))
            expected_result.extend([{'task_name': doc['task_name'], 'names' : [expected_result_subquery]}
                                    for doc in self.generate_full_docs_list(self.gens_tasks)])
            self._verify_results(actual_result, expected_result)

    def test_prepared_subquery_select(self):
        for bucket in self.buckets:
            self.query = "select task_name, (select count(task_name) cn from %s d use keys %s) as names from %s" % (bucket.name, str(['test_task-%s' % i for i in range(0, 29)]),
                                                                                                                    bucket.name)
            self.prepared_common_body()

    def test_subquery_where_aggr(self):
        for bucket in self.buckets:
            self.query = "select name, join_day from %s where join_day =" % (bucket.name) +\
            " (select AVG(join_day) as average from %s d use keys %s)[0].average" % (bucket.name,
                                                                               str(['query-test-Sales-2010-1-1-%s' % i
                                                                                    for i in range(0, self.docs_per_day)]))
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = [{'name' : doc['name'], 'join_day' : doc['join_day']} for doc in all_docs_list if doc['join_day'] == 1]
            self._verify_results(actual_result, expected_result)

    def test_subquery_where_in(self):
        for bucket in self.buckets:
            self.query = "select name, join_day from %s where join_day IN " % (bucket.name) +\
            " (select ARRAY_AGG(join_day) as average from %s d use keys %s)[0].average" % (bucket.name,
                                                                               str(['query-test-Sales-2010-1-1-%s' % i
                                                                                    for i in range(0, self.docs_per_day)]))
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = [{'name' : doc['name'], 'join_day' : doc['join_day']}
                               for doc in all_docs_list if doc['join_day'] == 1]
            self._verify_results(actual_result, expected_result)

    def test_where_in_subquery(self):
        for bucket in self.buckets:
            self.query = "select name, tasks_ids from %s where tasks_ids[0] IN" % bucket.name +\
            " (select ARRAY_AGG(DISTINCT task_name) as names from %s d " % bucket.name +\
            "use keys %s where project='MB')[0].names" % ('["test_task-1", "test_task-2"]')
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = [{'name' : doc['name'], 'tasks_ids' : doc['tasks_ids']}
                               for doc in all_docs_list if doc['tasks_ids'] in ['test_task-1', 'test_task-2']]
            self._verify_results(actual_result, expected_result)

    def test_where_in_subquery_not_equal(self):
        for bucket in self.buckets:
            self.query = "select name, tasks_ids from %s where tasks_ids[0] IN" % bucket.name +\
            " (select ARRAY_AGG(DISTINCT task_name) as names from %s d " % bucket.name +\
            "use keys %s where project!='AB')[0].names" % ('["test_task-1", "test_task-2"]')
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = [{'name' : doc['name'], 'tasks_ids' : doc['tasks_ids']}
                               for doc in all_docs_list if ('test_task-1' in doc['tasks_ids'] or 'test_task-2' in doc['tasks_ids'])]
            self._verify_results(actual_result, expected_result)

    def test_where_in_subquery_equal_more(self):
        for bucket in self.buckets:
            self.query = "select name, tasks_ids,join_day from %s where join_day>=2 and tasks_ids[0] IN" % bucket.name +\
            " (select ARRAY_AGG(DISTINCT task_name) as names from %s d " % bucket.name +\
            "use keys %s where project!='AB')[0].names" % ('["test_task-1", "test_task-2"]')
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = [{'name' : doc['name'], 'tasks_ids' : doc['tasks_ids'], 'join_day': doc['join_day']}
                               for doc in all_docs_list if ('test_task-1' in doc['tasks_ids'] or 'test_task-2' in doc['tasks_ids']) and doc['join_day'] >=2]
            self._verify_results(actual_result, expected_result)

    def test_where_in_subquery_equal_less(self):
        for bucket in self.buckets:
            self.query = "select name, tasks_ids,join_day from %s where join_day<=2 and tasks_ids[0] IN" % bucket.name +\
            " (select ARRAY_AGG(DISTINCT task_name) as names from %s d " % bucket.name +\
            "use keys %s where project!='AB')[0].names" % ('["test_task-1", "test_task-2"]')
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = [{'name' : doc['name'], 'tasks_ids' : doc['tasks_ids'], 'join_day': doc['join_day']}
                               for doc in all_docs_list if ('test_task-1' in doc['tasks_ids'] or 'test_task-2' in doc['tasks_ids']) and doc['join_day'] <=2]
            self._verify_results(actual_result, expected_result)

    def test_where_in_subquery_between(self):
        for bucket in self.buckets:
            self.query = "select name, tasks_ids, join_day from %s where (join_day between 1 and 12) and tasks_ids[0] IN" % bucket.name +\
            " (select ARRAY_AGG(DISTINCT task_name) as names from %s d " % bucket.name +\
            "use keys %s where project!='AB')[0].names" % ('["test_task-1", "test_task-2"]')
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = [{'name' : doc['name'], 'tasks_ids' : doc['tasks_ids'], 'join_day': doc['join_day']}
                               for doc in all_docs_list if ('test_task-1' in doc['tasks_ids'] or 'test_task-2' in doc['tasks_ids']) and doc['join_day'] <=12]
            self._verify_results(actual_result, expected_result)

    def test_subquery_exists(self):
        for bucket in self.buckets:
            self.query = "SELECT name FROM %s d1 WHERE " % bucket.name +\
            "EXISTS (SELECT * FROM %s d  use keys toarray(d1.tasks_ids[0]))" % bucket.name
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            tasks_ids = [doc["task_name"] for doc in self.generate_full_docs_list(self.gens_tasks)]
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = [{'name' : doc['name']} for doc in all_docs_list if doc['tasks_ids'][0] in tasks_ids]
            self._verify_results(actual_result, expected_result)

    def test_subquery_exists_where(self):
        for bucket in self.buckets:
            self.query = "SELECT name FROM %s d1 WHERE " % bucket.name +\
            "EXISTS (SELECT * FROM %s d use keys toarray(d1.tasks_ids[0]) where d.project='MB')" % bucket.name
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            tasks_ids = [doc["task_name"] for doc in self.generate_full_docs_list(self.gens_tasks) if doc['project'] == 'MB']
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = [{'name' : doc['name']} for doc in all_docs_list if doc['tasks_ids'][0] in tasks_ids]
            self._verify_results(actual_result, expected_result)

    def test_subquery_exists_and(self):
        for bucket in self.buckets:
            self.query = "SELECT name FROM %s d1 WHERE " % bucket.name +\
            "EXISTS (SELECT * FROM %s d  use keys toarray(d1.tasks_ids[0])) and join_mo>5" % bucket.name
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            tasks_ids = [doc["task_name"] for doc in self.generate_full_docs_list(self.gens_tasks)]
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = [{'name' : doc['name']} for doc in all_docs_list if doc['tasks_ids'][0] in tasks_ids and doc['join_mo']>5]
            self._verify_results(actual_result, expected_result)

    def test_subquery_from(self):
        for bucket in self.buckets:
            self.query = "SELECT TASKS.task_name FROM (SELECT task_name, project FROM %s WHERE project = 'CB') as TASKS" % bucket.name
            all_docs_list = self.generate_full_docs_list(self.gens_tasks)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = [{'task_name' : doc['task_name']} for doc in all_docs_list if doc['project'] == 'CB']
            self._verify_results(actual_result, expected_result)

    def test_subquery_from_join(self):
        for bucket in self.buckets:
            self.query = "SELECT EMP.name Name, TASK.project proj FROM (SELECT tasks_ids, name FROM "+\
            "%s WHERE join_mo>10) as EMP %s JOIN %s TASK ON KEYS EMP.tasks_ids" % (bucket.name, self.type_join, bucket.name)
            all_docs_list = self._generate_full_joined_docs_list(join_type=self.type_join)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = [{'Name' : doc['name'], 'proj' : doc['project']} for doc in all_docs_list if doc['join_mo'] > 10]
            self._verify_results(actual_result, expected_result)
##############################################################################################
#
#   KEY
##############################################################################################

    def test_keys(self):
        for bucket in self.buckets:
            keys_select = []
            generator = copy.deepcopy(self.gens_tasks[0])
            for i in range(5):
                key, _ = next(generator)
                keys_select.append(key)
            self.query = 'select task_name FROM %s USE KEYS %s' % (bucket.name, keys_select)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            full_list = self.generate_full_docs_list(self.gens_tasks, keys=keys_select)
            expected_result = [{"task_name" : doc['task_name']} for doc in full_list]
            self._verify_results(actual_result, expected_result)

            keys_select.extend(["wrong"])
            self.query = 'select task_name FROM %s USE KEYS %s' % (bucket.name, keys_select)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            self._verify_results(actual_result, expected_result)

            self.query = 'select task_name FROM %s USE KEYS ["wrong_one","wrong_second"]' % (bucket.name)
            actual_result = self.run_cbq_query()
            self.assertFalse(actual_result['results'], "Having a wrong key query returned some result")

    def test_key_array(self):
        for bucket in self.buckets:
            gen_select = copy.deepcopy(self.gens_tasks[0])
            key_select, value_select = next(gen_select)
            self.query = 'SELECT * FROM %s USE KEYS ARRAY emp._id FOR emp IN [%s] END' % (bucket.name, value_select)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = self.generate_full_docs_list(self.gens_tasks, keys=[key_select])
            expected_result = [{bucket.name : doc} for doc in expected_result]
            self._verify_results(actual_result, expected_result)

            key2_select, value2_select = next(gen_select)
            self.query = 'SELECT * FROM %s USE KEYS ARRAY emp._id FOR emp IN [%s,%s] END' % (bucket.name, value_select, value2_select)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = self.generate_full_docs_list(self.gens_tasks, keys=[key_select, key2_select])
            expected_result = [{bucket.name : doc} for doc in expected_result]
            self._verify_results(actual_result, expected_result)

##############################################################################################
#
#   NEST
##############################################################################################


    def test_simple_nest_keys(self):
        for bucket in self.buckets:
            self.query = "SELECT * FROM %s emp %s NEST %s tasks ON KEYS emp.tasks_ids" % (bucket.name, self.type_join, bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            self._delete_ids(actual_result)
            actual_result = self.sort_nested_list(actual_result, key='task_name')
            actual_result = sorted(actual_result, key=lambda doc: self._get_for_sort(doc))
            full_list = self._generate_full_nested_docs_list(join_type=self.type_join)
            expected_result = [{"emp" : doc['item'], "tasks" : doc['items_nested']} for doc in full_list if doc and 'items_nested' in doc]
            expected_result.extend([{"emp" : doc['item']} for doc in full_list if not 'items_nested' in doc])
            self._delete_ids(expected_result)
            expected_result = self.sort_nested_list(expected_result, key='task_name')
            expected_result = sorted(expected_result, key=lambda doc: self._get_for_sort(doc))
            self._verify_results(actual_result, expected_result)

    def test_simple_nest_key(self):
        for bucket in self.buckets:
            self.query = "SELECT * FROM %s emp %s NEST %s tasks KEY emp.tasks_ids[0]" % (bucket.name, self.type_join, bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: self._get_for_sort(doc))
            self._delete_ids(actual_result)
            full_list = self._generate_full_nested_docs_list(particular_key=0, join_type=self.type_join)
            expected_result = [{"emp" : doc['item'], "tasks" : doc['items_nested']} for doc in full_list if doc and 'items_nested' in doc]
            expected_result.extend([{"emp" : doc['item']} for doc in full_list if not 'items_nested' in doc])
            expected_result = sorted(expected_result, key=lambda doc: self._get_for_sort(doc))
            self._delete_ids(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_nest_keys_with_array(self):
        for bucket in self.buckets:
            self.query = "select emp.name, ARRAY item.project FOR item in items end projects " +\
                         "FROM %s emp %s NEST %s items " % (
                                                    bucket.name, self.type_join, bucket.name) +\
                         "ON KEYS emp.tasks_ids"
            actual_result = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_result['results'], key='projects')
            full_list = self._generate_full_nested_docs_list(join_type=self.type_join)
            expected_result = [{"name" : doc['item']['name'],
                                "projects" : [nested_doc['project'] for nested_doc in doc['items_nested']]}
                               for doc in full_list if doc and 'items_nested' in doc]
            expected_result.extend([{} for doc in full_list if not 'items_nested' in doc])
            expected_result = self.sort_nested_list(expected_result, key='projects')
            self._verify_results(actual_result, expected_result)

    def test_prepared_nest_keys_with_array(self):
        for bucket in self.buckets:
            self.query = "select emp.name, ARRAY item.project FOR item in items end projects " +\
                         "FROM %s emp %s NEST %s items " % (
                                                    bucket.name, self.type_join, bucket.name) +\
                         "ON KEYS emp.tasks_ids"
            self.prepared_common_body()

    def test_nest_keys_where(self):
        for bucket in self.buckets:
            self.query = "select emp.name, ARRAY item.project FOR item in items end projects " +\
                         "FROM %s emp %s NEST %s items " % (
                                                    bucket.name, self.type_join, bucket.name) +\
                         "ON KEYS emp.tasks_ids where ANY item IN items SATISFIES item.project == 'CB' end"
            actual_result = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_result['results'], key='projects')
            actual_result = sorted(actual_result, key=lambda doc: (doc['name'], doc['projects']))
            full_list = self._generate_full_nested_docs_list(join_type=self.type_join)
            expected_result = [{"name" : doc['item']['name'],
                                "projects" : [nested_doc['project'] for nested_doc in doc['items_nested']]}
                               for doc in full_list if doc and 'items_nested' in doc and\
                               len([nested_doc for nested_doc in doc['items_nested']
                                    if nested_doc['project'] == 'CB']) > 0]
            expected_result = self.sort_nested_list(expected_result, key='projects')
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'], doc['projects']))
            self._verify_results(actual_result, expected_result)

    def test_nest_keys_where_not_equal(self):
        for bucket in self.buckets:
            self.query = "select emp.name, ARRAY item.project FOR item in items end projects " +\
                         "FROM %s emp %s NEST %s items " % (
                                                    bucket.name, self.type_join, bucket.name) +\
                         "ON KEYS emp.tasks_ids where ANY item IN items SATISFIES item.project != 'CB' end"
            actual_result = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_result['results'], key='projects')
            actual_result = sorted(actual_result, key=lambda doc: (doc['name'], doc['projects']))
            full_list = self._generate_full_nested_docs_list(join_type=self.type_join)
            expected_result = [{"name" : doc['item']['name'],
                                "projects" : [nested_doc['project'] for nested_doc in doc['items_nested']]}
                               for doc in full_list if doc and 'items_nested' in doc and\
                               len([nested_doc for nested_doc in doc['items_nested']
                                    if nested_doc['project'] != 'CB']) > 0]
            expected_result = self.sort_nested_list(expected_result, key='projects')
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'], doc['projects']))
            self._verify_results(actual_result, expected_result)

    def test_nest_keys_where_between(self):
        for bucket in self.buckets:
            self.query = "select emp.name, emp.join_day, ARRAY item.project FOR item in items end projects " +\
                         "FROM %s emp %s NEST %s items " % (
                                                    bucket.name, self.type_join, bucket.name) +\
                         "ON KEYS emp.tasks_ids where emp.join_day between 2 and 4"
            actual_result = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_result['results'], key='projects')
            actual_result = sorted(actual_result, key=lambda doc: (doc['name'], doc['projects']))
            full_list = self._generate_full_nested_docs_list(join_type=self.type_join)
            expected_result = [{"name" : doc['item']['name'], "join_day" : doc['item']['join_day'],
                                "projects" : [nested_doc['project'] for nested_doc in doc['items_nested']]}
                               for doc in full_list
                               if doc and 'join_day' in doc['item'] and \
                               doc['item']['join_day'] >= 2 and doc['item']['join_day'] <=4]
            expected_result = self.sort_nested_list(expected_result, key='projects')
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'], doc['projects']))
            self._verify_results(actual_result, expected_result)

    def test_nest_keys_where_less_more_equal(self):
        for bucket in self.buckets:
            self.query = "select emp.name, emp.join_day, emp.join_yr, ARRAY item.project FOR item in items end projects " +\
                         "FROM %s emp %s NEST %s items " % (
                                                    bucket.name, self.type_join, bucket.name) +\
                         "ON KEYS emp.tasks_ids where emp.join_day <= 4 and emp.join_yr>=2010"
            actual_result = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_result['results'], key='projects')
            full_list = self._generate_full_nested_docs_list(join_type=self.type_join)
            expected_result = [{"name" : doc['item']['name'], "join_day" : doc['item']['join_day'],
                                'join_yr': doc['item']['join_yr'],
                                "projects" : [nested_doc['project'] for nested_doc in doc['items_nested']]}
                               for doc in full_list
                               if doc and 'join_day' in doc['item'] and\
                               doc['item']['join_day'] <=4 and doc['item']['join_yr']>=2010]
            expected_result = self.sort_nested_list(expected_result, key='projects')
            self._verify_results(actual_result, expected_result)

    def test_dual(self):
        self.query = "select 1"
        actual_result = self.run_cbq_query()
        self.query = "select 1 from system:dual"
        expected_result = self.run_cbq_query()
        self._verify_results(actual_result, expected_result)
