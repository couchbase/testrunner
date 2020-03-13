from .tuq import QueryTests
from membase.api.exception import CBQError


class QuerySubqueryTests(QueryTests):

    def setUp(self):
        super(QuerySubqueryTests, self).setUp()
        self.log.info("==============  QuerySubqueryTests setup has started ==============")
        self.array_indexing = True
        self.query_bucket = self.get_query_buckets()[0]
        self.log.info("==============  QuerySubqueryTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QuerySubqueryTests, self).suite_setUp()
        self.log.info("==============  QuerySubqueryTests suite_setup has started ==============")
        self.log.info("==============  QuerySubqueryTests suite_setup has completed ==============")

    def tearDown(self):
        super(QuerySubqueryTests, self).tearDown()
        self.log.info("==============  QuerySubqueryTests teardown has started ==============")
        self.log.info("==============  QuerySubqueryTests teardown has completed ==============")

    def suite_tearDown(self):
        super(QuerySubqueryTests, self).suite_tearDown()
        self.log.info("==============  QuerySubqueryTests suite_teardown has started ==============")
        self.log.info("==============  QuerySubqueryTests suite_teardown has completed ==============")

    def test_constant_expressions(self):
        self.query = 'select a from [] as a'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == [])
        self.query = 'SELECT a FROM ["abc", 1, 2.5 ] AS a'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == [{'a': 'abc'}, {'a': 1}, {'a': 2.5}])
        self.query = 'SELECT a FROM [{"x":11},{"x":12},"abc"] AS a'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == [{'a': {'x': 11}}, {'a': {'x': 12}}, {'a': 'abc'}])
        self.query = 'SELECT a.x FROM [{"x":11},"abc",{"x":12}] AS a'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == [{'x': 11}, {}, {'x': 12}])
        self.query = 'SELECT p.x FROM {"p":[{"x":11},{"x":12}],"q":"abc","r":null}.p'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == [{'x': 11}, {'x': 12}])
        self.query = 'SELECT q FROM {"p":[{"x":11},{"x":12}],"q":"abc","r":null}.q'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == [{'q': "abc"}])
        self.query = 'SELECT r FROM {"p":[{"x":11},{"x":12}],"q":"abc","r":null}.r'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == [{'r': None}])
        self.query = 'SELECT s FROM {"p":[{"x":11},{"x":12}],"q":"abc","r":null}.s'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == [])

    def test_aggregating_correlated_subquery(self):
        self.query = 'SELECT meta().id, (SELECT RAW SUM(VMs.memory) FROM d.VMs AS VMs)[0] AS total FROM ' + \
                     self.query_bucket + ' d order by total'
        actual_result = self.run_cbq_query()
        self.query = 'SELECT meta().id, (SELECT RAW SUM(VMs.memory) FROM d.VMs)[0] AS total FROM ' + \
                     self.query_bucket + ' d order by total'
        expected_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == expected_result['results'])

    def test_sort_array_correlated_subquery(self):
        self.query = 'select meta().id, name ,(SELECT VMs FROM d.VMs where VMs.RAM=10 ORDER BY VMs.os) test from ' + \
                     self.query_bucket + ' d order by meta().id limit 1'
        actual_result1 = self.run_cbq_query()
        self.assertTrue(actual_result1['results'] == [{'test': [
            {'VMs': {'RAM': 10, 'os': 'centos', 'name': 'vm_12', 'memory': 10}},
            {'VMs': {'RAM': 10, 'os': 'macos', 'name': 'vm_13', 'memory': 10}},
            {'VMs': {'RAM': 10, 'os': 'ubuntu', 'name': 'vm_10', 'memory': 10}},
            {'VMs': {'RAM': 10, 'os': 'windows', 'name': 'vm_11', 'memory': 10}}],
            'id': 'query-testemployee10153.187782713003-0',
            'name': [{'FirstName': 'employeefirstname-9'},
                     {'MiddleName': 'employeemiddlename-9'},
                     {'LastName': 'employeelastname-9'}]}])
        self.query = 'select meta().id, name ,(SELECT VMs FROM d.VMs where VMs.RAM=10 ORDER BY VMs.os DESC)' \
                     ' test from ' + self.query_bucket + ' d order by meta().id limit 1'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == [{'test': [
            {'VMs': {'RAM': 10, 'os': 'windows', 'name': 'vm_11', 'memory': 10}},
            {'VMs': {'RAM': 10, 'os': 'ubuntu', 'name': 'vm_10', 'memory': 10}},
            {'VMs': {'RAM': 10, 'os': 'macos', 'name': 'vm_13', 'memory': 10}},
            {'VMs': {'RAM': 10, 'os': 'centos', 'name': 'vm_12', 'memory': 10}}],
            'id': 'query-testemployee10153.187782713003-0',
            'name': [{'FirstName': 'employeefirstname-9'},
                     {'MiddleName': 'employeemiddlename-9'},
                     {'LastName': 'employeelastname-9'}]}])

    def test_delete_subquery(self):
        self.query = 'insert into  ' + self.query_bucket + \
                     '  (key k,value doc)  select to_string(email)|| UUID() as k , ' \
                     'hobbies as doc from  ' + self.query_bucket + \
                     '  where email is not missing and VMs[0].os is not missing and' \
                     ' hobbies is not missing'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['metrics']['mutationCount'] == 10080)
        self.query = 'delete from  ' + self.query_bucket + \
                     ' d where "sports" IN (select RAW OBJECT_NAMES(h)[0] FROM d.hobby h)'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['metrics']['mutationCount'] == 10080)
        self.query = 'DELETE FROM  ' + self.query_bucket + '  a WHERE  "centos" IN (SELECT RAW VMs.os FROM a.VMs)'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['metrics']['mutationCount'] == 10080)

    def test_update_subquery_in_where_clause(self):
        self.gens_load = self.gen_docs(self.docs_per_day)
        self.load(self.gens_load, flag=self.item_flag)
        updated_value = "new_name"
        self.query = 'UPDATE ' + self.query_bucket + \
                     ' a set name = "{0}" where "centos" in ( SELECT RAW VMs.os FROM a.VMs) ' \
                     'limit 2 returning a.name '.format(updated_value)
        actual_result = self.run_cbq_query()
        self.assertTrue(len([doc for doc in actual_result['results'] if doc['name'] == updated_value]) == 2,
                        'Names were not changed correctly')

    def test_update_subquery_in_set_clause(self):
        self.query = 'UPDATE ' + self.query_bucket + \
                     ' a set name = ( SELECT RAW VMs.os FROM a.VMs ) where "centos" in ( SELECT RAW VMs.os' \
                     ' FROM a.VMs) ' \
                     'limit 1 returning a.name '
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == [{'name': ['ubuntu', 'windows', 'centos', 'macos']}])

    def test_update_set_subquery_for(self):
        self.query = 'update ' + self.query_bucket + ' a use keys "query-testemployee10153.187782713003-0" set ' \
                                                     'vm.os="new_os" for vm in ' \
                                                     '( SELECT RAW VMs FROM a.VMs )' \
                                                     'when vm.os = ( SELECT RAW r FROM {"p":[{"x":11},{"x":12}],' \
                                                     '"q":"abc","r":"windows"}.r )[0] END returning VMs '
        actual_result = self.run_cbq_query()
        self.assertTrue(
            actual_result['results'] == [{'VMs': [{'RAM': 10, 'os': 'ubuntu', 'name': 'vm_10', 'memory': 10},
                                                  {'RAM': 10, 'os': 'new_os', 'name': 'vm_11', 'memory': 10},
                                                  {'RAM': 10, 'os': 'centos', 'name': 'vm_12', 'memory': 10},
                                                  {'RAM': 10, 'os': 'macos', 'name': 'vm_13', 'memory': 10}]}])

    def test_subquery_let(self):
        self.query = 'select meta().id,total from  ' + self.query_bucket + \
                     ' d let total = (SELECT RAW SUM(VMs.memory) FROM d.VMs AS VMs)[0] order by meta().id limit 10'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == [{'total': 40, 'id': 'query-testemployee10153.187782713003-0'},
                                                     {'total': 40, 'id': 'query-testemployee10153.187782713003-1'},
                                                     {'total': 40, 'id': 'query-testemployee10153.187782713003-2'},
                                                     {'total': 40, 'id': 'query-testemployee10153.187782713003-3'},
                                                     {'total': 40, 'id': 'query-testemployee10153.187782713003-4'},
                                                     {'total': 40, 'id': 'query-testemployee10153.187782713003-5'},
                                                     {'total': 44, 'id': 'query-testemployee10194.855617030253-0'},
                                                     {'total': 44, 'id': 'query-testemployee10194.855617030253-1'},
                                                     {'total': 44, 'id': 'query-testemployee10194.855617030253-2'},
                                                     {'total': 44, 'id': 'query-testemployee10194.855617030253-3'}])
        self.query = 'SELECT meta().id, (SELECT RAW SUM(item.memory) FROM items as item)[0] total FROM ' + \
                     self.query_bucket + ' d LET items = (SELECT VMs.* FROM d.VMs ORDER BY VMs.memory) ' \
                                         'order by meta().id limit 5'
        actual_result1 = self.run_cbq_query()
        self.assertTrue(actual_result1['results'] == [{'total': 40, 'id': 'query-testemployee10153.187782713003-0'},
                                                      {'total': 40, 'id': 'query-testemployee10153.187782713003-1'},
                                                      {'total': 40, 'id': 'query-testemployee10153.187782713003-2'},
                                                      {'total': 40, 'id': 'query-testemployee10153.187782713003-3'},
                                                      {'total': 40, 'id': 'query-testemployee10153.187782713003-4'}])

    def test_subquery_letting(self):
        self.query = 'select meta().id,total from  ' + self.query_bucket + \
                     ' GROUP BY meta().id LETTING total = COUNT(META().id) order by meta().id limit 3'
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{"id": "query-testemployee10153.187782713003-0", "total": 1},
                                                    {"id": "query-testemployee10153.187782713003-1", "total": 1},
                                                    {"id": "query-testemployee10153.187782713003-2", "total": 1}])
        self.query = 'select meta().id,total from  ' + self.query_bucket + \
                     ' d GROUP BY meta().id LETTING total = (SELECT RAW SUM(VMs.memory) FROM d.VMs AS VMs)[0] ' \
                     'order by meta().id limit 10'
        try:
            self.run_cbq_query()
            self.fail("Query should have failed")
        except CBQError as e:
            self.assertTrue('Expression (correlated (select raw sum((`VMs`.`memory`)) from '
                            '(`d`.`VMs`) as `VMs`)[0]) must depend only on group keys or aggregates.' in str(e),
                            "Incorrect error message: \n" + str(e))

    def test_update_unset(self):
        self.query = 'UPDATE ' + self.query_bucket + \
                     ' a unset name  where "windows" in ( SELECT RAW VMs.os FROM a.VMs) limit 2 ' \
                     'returning a.*,meta().id '
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == [
            {'VMs': [{'RAM': 10, 'memory': 10, 'name': 'vm_10', 'os': 'ubuntu'},
                     {'RAM': 10, 'memory': 10, 'name': 'vm_11', 'os': 'windows'},
                     {'RAM': 10, 'memory': 10, 'name': 'vm_12', 'os': 'centos'},
                     {'RAM': 10, 'memory': 10, 'name': 'vm_13', 'os': 'macos'}],
             '_id': 'query-testemployee10153.187782713003-0',
             'address': [[{'city': 'Delhi'}, {'street': '18th street'}],
                         [{'apartment': 123, 'country': 'INDIA'}]],
             'department': 'Support',
             'email': '9-mail@couchbase.com',
             'hobbies': {'hobby': [{'sports': ['Cricket', 'Football', 'Basketball']},
                                   {'dance': ['classical', 'contemporary', 'hip hop']}, 'art']},
             'id': 'query-testemployee10153.187782713003-0',
             'join_yr': [2012, 2014, 2016],
             'mutated': 0,
             'tasks': [{'Developer': ['Storage', 'IOS'],
                        'Marketing': [{'region1': 'International', 'region2': 'South'},
                                      {'region2': 'South'}]}, 'Sales', 'QA']},
            {'VMs': [{'RAM': 10, 'memory': 10, 'name': 'vm_10', 'os': 'ubuntu'},
                     {'RAM': 10, 'memory': 10, 'name': 'vm_11', 'os': 'windows'},
                     {'RAM': 10, 'memory': 10, 'name': 'vm_12', 'os': 'centos'},
                     {'RAM': 10, 'memory': 10, 'name': 'vm_13', 'os': 'macos'}],
             '_id': 'query-testemployee10153.187782713003-1',
             'address': [[{'city': 'Delhi'}, {'street': '18th street'}],
                         [{'apartment': 123, 'country': 'INDIA'}]],
             'department': 'Support',
             'email': '9-mail@couchbase.com',
             'hobbies': {'hobby': [{'sports': ['Cricket', 'Football', 'Basketball']},
                                   {'dance': ['classical', 'contemporary', 'hip hop']}, 'art']},
             'id': 'query-testemployee10153.187782713003-1',
             'join_yr': [2012, 2014, 2016], 'mutated': 0,
             'tasks': [{'Developer': ['Storage', 'IOS'],
                        'Marketing': [{'region1': 'International', 'region2': 'South'},
                                      {'region2': 'South'}]}, 'Sales', 'QA']}])

    def test_namespace_keyspace(self):
        self.query = 'select * from  ' + self.query_bucket + ' d order by name limit 1'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == [
            {'d':
                 {'name':
                      [{'FirstName': 'employeefirstname-1'},
                       {'MiddleName': 'employeemiddlename-1'},
                       {'LastName': 'employeelastname-1'}],
                  'department': 'Manager',
                  'join_yr': [2013, 2014, 2010],
                  'email': '1-mail@couchbase.com',
                  'hobbies':
                      {'hobby': [{'sports': ['Cricket', 'Badminton', 'American Football']},
                                 {'dance': ['salsa', 'contemporary', 'hip hop']},
                                 'art']},
                  'tasks': [{'Developer': ['Storage', 'IOS'],
                             'Marketing': [{'region1': 'North', 'region2': 'West'},
                                           {'region2': 'North'}]}, 'Sales', 'QA'],
                  'VMs': [{'RAM': 4, 'os': 'ubuntu', 'name': 'vm_4', 'memory': 4},
                          {'RAM': 4, 'os': 'windows', 'name': 'vm_5', 'memory': 4},
                          {'RAM': 4, 'os': 'centos', 'name': 'vm_6', 'memory': 4},
                          {'RAM': 4, 'os': 'macos', 'name': 'vm_7', 'memory': 4}],
                  'address': [[{'city': 'Delhi'}, {'street': '18th street'}],
                              [{'apartment': 123, 'country': 'USA'}]],
                  '_id': 'query-testemployee11166.096114837153-0', 'mutated': 0}}])

    # below test fails
    # def test_subquery_negative_USE_KEYS_INDEX(self):
    #     self.query = 'select meta().id,total from  ' + self.query_bucket + ' d let total = (SELECT RAW
    #     SUM(VMs.memory) FROM d.VMs AS VMs use keys "query-testemployee10153.187782713003-1")[0]
    #     order by meta().id limit 10'
    #     actual_result = self.run_cbq_query()
    #     self.query = 'select meta().id,total from  ' + self.query_bucket + ' d let total = (SELECT RAW
    #     SUM(VMs.memory) FROM d.VMs AS VMs use index(`#primary`))[0] order by meta().id limit 10'
    #     actual_result = self.run_cbq_query()
    #     print actual_result

    def test_merge_subquery(self):
        self.query = 'UPDATE ' + self.query_bucket + ' a set id = UUID() where "centos" in ( SELECT RAW VMs.os FROM ' \
                                                     'a.VMs) returning a.id '
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['metrics']['mutationCount'] == 10080)
        self.query = 'MERGE INTO ' + self.query_bucket + ' USING ' + self.query_bucket + \
                     ' d ON KEY id WHEN NOT MATCHED THEN INSERT {d.id} RETURNING *'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['metrics']['mutationCount'] == 10080)
        self.query = 'MERGE INTO ' + self.query_bucket + ' USING (SELECT "s" || id || UUID() AS id FROM  ' + \
                     self.query_bucket + ' ) o ON KEY o.id WHEN NOT MATCHED ' \
                                         'THEN INSERT {o.id} RETURNING *;'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['metrics']['mutationCount'] == 20160)
        self.query = 'MERGE INTO ' + self.query_bucket + ' USING (SELECT "se" || id || UUID() || "123" AS id,(SELECT ' \
                                                         'RAW SUM(VMs.memory) FROM ' \
                                                         'd.VMs)[0] as total from  ' + \
                     self.query_bucket + ' d ) o ON KEY o.id WHEN NOT MATCHED THEN INSERT {o.id,o.total} RETURNING *;'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['metrics']['mutationCount'] == 40320)
        self.query = 'MERGE INTO ' + self.query_bucket + ' d USING [{"id":"c1235"},{"id":"c1236"}] o ON KEY id WHEN ' \
                                                         ' NOT MATCHED THEN INSERT {o.id} RETURNING * '
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == [{'d': {'id': 'c1235'}}, {'d': {'id': 'c1236'}}])

    def test_correlated_queries_predicate_exists(self):
        self.query = 'SELECT name, id FROM ' + self.query_bucket + ' d WHERE EXISTS (SELECT 1 FROM d.VMs WHERE ' \
                                                                   'VMs.memory > 10 order by meta(d).id)' \
                                                                   ' order by meta().id limit 2'
        actual_result = self.run_cbq_query()
        self.log.info("test_correlated_queries_predicate_exists is {0}".format(actual_result['results']))
        # self.assertTrue(actual_result['results']==[{u'id': u'352e1533-eabb-4fed-a4a4-2236f7a690c7', u'name':
        # [{u'FirstName': u'employeefirstname-4'}, {u'MiddleName': u'employeemiddlename-4'}, {u'LastName':
        # u'employeelastname-4'}]}, {u'id': u'c6326d5b-64e0-4f65-ab00-85465472774d', u'name': [{u'FirstName':
        # u'employeefirstname-4'}, {u'MiddleName': u'employeemiddlename-4'}, {u'LastName': u'employeelastname-4'}]}])

    def test_correlated_queries_predicate_not_exists(self):
        self.query = 'SELECT name, id FROM ' + self.query_bucket + ' d WHERE NOT EXISTS (SELECT 1 FROM d.VMs' \
                                                                   ' WHERE VMs.memory < 10 order by meta(d).id) order ' \
                                                                   'by meta().id limit 2 '
        self.run_cbq_query()
        actual_result = self.run_cbq_query()
        self.log.info("test_correlated_queries_predicate_not_exists is {0}".format(actual_result['results']))
        # self.assertTrue(actual_result['results']==[{u'id': u'00002fb9-7b42-45ae-b864-c21e87563dac'},
        # {u'id': u'0011e2e6-7788-4582-b6ac-4185549dc838'}])

    def test_correlated_queries_in_clause(self):
        self.query = 'SELECT name, id FROM ' + self.query_bucket + ' d WHERE "windows" IN (SELECT RAW VMs.os FROM ' \
                                                                   'd.VMs) order by meta(d).id limit 2 '
        actual_result = self.run_cbq_query()
        self.log.info("test_correlated_queries_in_clause is {0}".format(actual_result['results']))
        # self.assertTrue(actual_result['results']==[{u'id': u'feaa4880-b117-4de6-9b3c-d1dd21c64abe'},
        # {u'id': u'930e38d0-c35f-4e73-991e-db1a0758b8a9'}])
        self.query = 'SELECT name, id FROM ' + self.query_bucket + ' d WHERE 10 < (SELECT RAW VMs.memory FROM d.VMs)  ' \
                                                                   'order by meta().id limit 2 '
        actual_result = self.run_cbq_query()
        self.log.info("test_correlated_queries_in_clause2 is {0}".format(actual_result['results']))
        # self.assertTrue(actual_result['results']==[{u'id': u'00148e19-1203-4f48-aa3d-2751b57fec8d'},
        # {u'id': u'0018f09f-9726-4f6f-b872-afa3f7510254'}])

    def test_subquery_joins(self):
        self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.query_bucket, "w001",
                                                                         {"type": "wdoc", "docid": "x001",
                                                                          "name": "wdoc",
                                                                          "phones": ["123-456-7890", "123-456-7891"],
                                                                          "altid": "x001"})
        self.run_cbq_query()
        self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.query_bucket, "pdoc1",
                                                                         {"type": "pdoc", "docid": "x001",
                                                                          "name": "pdoc",
                                                                          "phones": ["123-456-7890", "123-456-7891"],
                                                                          "altid": "x001"})
        self.run_cbq_query()
        self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (self.query_bucket, "pdoc2",
                                                                         {"type": "pdoc", "docid": "w001",
                                                                          "name": "pdoc",
                                                                          "phones": ["123-456-7890", "123-456-7891"],
                                                                          "altid": "w001"})
        self.run_cbq_query()
        self.query = 'SELECT b1.b1id,b2.name FROM (select d.*,meta(d).id b1id from ' + self.query_bucket + \
                     ' d) b1 JOIN ' + self.query_bucket + ' b2 ON KEYS b1.docid where b1.b1id > ""'
        actual_result_with_subquery = self.run_cbq_query()
        self.query = 'SELECT meta(b1).id b1id, b2.name FROM ' + self.query_bucket + ' b1 JOIN ' + self.query_bucket + \
                     ' b2 ON KEYS b1.docid WHERE meta(b1).id > ""'
        expected_result = self.run_cbq_query()
        self.assertTrue(actual_result_with_subquery['results'] == expected_result['results'])

    def test_subquery_explain(self):
        self.query = 'explain SELECT q FROM {"p":[{"x":11},{"x":12}],"q":"abc","r":null}.q'
        actual_result = self.run_cbq_query()
        plan = self.ExplainPlanHelper(actual_result)
        self.assertTrue(plan['~children'][0]['#operator'] == 'ExpressionScan')
