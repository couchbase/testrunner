from .tuq import QueryTests
from membase.api.exception import CBQError


class QuerySubqueryTests(QueryTests):

    def setUp(self):
        super(QuerySubqueryTests, self).setUp()
        self.log.info("==============  QuerySubqueryTests setup has started ==============")
        self.array_indexing = True
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
        self.assertTrue(actual_result['results']==[])
        self.query = 'SELECT a FROM ["abc", 1, 2.5 ] AS a'
        actual_result =self.run_cbq_query()
        self.assertTrue(actual_result['results']==[{'a': 'abc'}, {'a': 1}, {'a': 2.5}])
        self.query = 'SELECT a FROM [{"x":11},{"x":12},"abc"] AS a'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results']==[{'a': {'x': 11}}, {'a': {'x': 12}}, {'a': 'abc'}])
        self.query = 'SELECT a.x FROM [{"x":11},"abc",{"x":12}] AS a'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results']==[{'x': 11}, {}, {'x': 12}])
        self.query = 'SELECT p.x FROM {"p":[{"x":11},{"x":12}],"q":"abc","r":null}.p'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results']==[{'x': 11}, {'x': 12}])
        self.query = 'SELECT q FROM {"p":[{"x":11},{"x":12}],"q":"abc","r":null}.q'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results']==[{'q': "abc"}])
        self.query = 'SELECT r FROM {"p":[{"x":11},{"x":12}],"q":"abc","r":null}.r'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results']==[{'r': None}])
        self.query = 'SELECT s FROM {"p":[{"x":11},{"x":12}],"q":"abc","r":null}.s'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results']==[])

    def test_aggregating_correlated_subquery(self):
        self.query = 'SELECT meta().id, (SELECT RAW SUM(VMs.memory) FROM default.VMs AS VMs)[0] AS total FROM default order by total'
        actual_result = self.run_cbq_query()
        self.query = 'SELECT meta().id, (SELECT RAW SUM(VMs.memory) FROM default.VMs)[0] AS total FROM default order by total'
        expected_result = self.run_cbq_query()
        self.assertTrue(actual_result['results']==expected_result['results'])

    def test_sort_array_correlated_subquery(self):
        self.query = 'select meta().id, name ,(SELECT VMs FROM default.VMs where VMs.RAM=10 ORDER BY VMs.os) test from default order by meta().id limit 1'
        actual_result1 = self.run_cbq_query()
        self.assertTrue(actual_result1['results']== [{'test': [{'VMs': {'RAM': 10, 'os': 'centos', 'name': 'vm_12', 'memory': 10}}, {'VMs': {'RAM': 10, 'os': 'macos', 'name': 'vm_13', 'memory': 10}}, {'VMs': {'RAM': 10, 'os': 'ubuntu', 'name': 'vm_10', 'memory': 10}}, {'VMs': {'RAM': 10, 'os': 'windows', 'name': 'vm_11', 'memory': 10}}],
                                                      'id': 'query-testemployee10153.1877827-0', 'name': [{'FirstName': 'employeefirstname-9'}, {'MiddleName': 'employeemiddlename-9'}, {'LastName': 'employeelastname-9'}]}])
        self.query = 'select meta().id, name ,(SELECT VMs FROM default.VMs where VMs.RAM=10 ORDER BY VMs.os DESC) test from default order by meta().id limit 1'
        actual_result = self.run_cbq_query()
        self.assertTrue( actual_result['results'] ==  [{'test': [{'VMs': {'RAM': 10, 'os': 'windows', 'name': 'vm_11', 'memory': 10}}, {'VMs': {'RAM': 10, 'os': 'ubuntu', 'name': 'vm_10', 'memory': 10}}, {'VMs': {'RAM': 10, 'os': 'macos', 'name': 'vm_13', 'memory': 10}}, {'VMs': {'RAM': 10, 'os': 'centos', 'name': 'vm_12', 'memory': 10}}],
                                                        'id': 'query-testemployee10153.1877827-0', 'name': [{'FirstName': 'employeefirstname-9'}, {'MiddleName': 'employeemiddlename-9'}, {'LastName': 'employeelastname-9'}]}])

    def test_delete_subquery(self):
        self.query = 'insert into {0} (key k,value doc)  select to_string(email)|| UUID() as k , hobbies as doc from {0} where email is not missing and VMs[0].os is not missing and hobbies is not missing'.format("default")
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['metrics']['mutationCount']==10080)
        self.query = 'delete from {0} d where "sports" IN (select RAW OBJECT_NAMES(h)[0] FROM d.hobby h)'.format("default")
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['metrics']['mutationCount']==10080)
        self.query = 'DELETE FROM {0} a WHERE  "centos" IN (SELECT RAW VMs.os FROM a.VMs)'.format("default")
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['metrics']['mutationCount']==10080)

    def test_update_subquery_in_where_clause(self):
        self.gens_load = self.gen_docs(self.docs_per_day)
        self.load(self.gens_load, flag=self.item_flag)
        updated_value = "new_name"
        self.query = 'UPDATE default a set name = "{0}" where "centos" in ( SELECT RAW VMs.os FROM a.VMs) limit 2 returning a.name '.format(updated_value)
        actual_result = self.run_cbq_query()
        self.assertTrue(len([doc for doc in actual_result['results'] if doc['name'] == updated_value]) == 2, 'Names were not changed correctly')

    def test_update_subquery_in_set_clause(self):
        self.query = 'UPDATE default a set name = ( SELECT RAW VMs.os FROM a.VMs ) where "centos" in ( SELECT RAW VMs.os FROM a.VMs) limit 1 returning a.name '
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == [{'name': ['ubuntu', 'windows', 'centos', 'macos']}])

    def test_update_set_subquery_for(self):
        self.query = 'update default a use keys "query-testemployee10153.1877827-0" set vm.os="new_os" for vm in ' \
                     '( SELECT RAW VMs FROM a.VMs )' \
                     ' when vm.os = ( SELECT RAW r FROM {"p":[{"x":11},{"x":12}],"q":"abc","r":"windows"}.r )[0] END returning VMs'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results']==[{'VMs': [{'RAM': 10, 'os': 'ubuntu', 'name': 'vm_10', 'memory': 10},
                                                             {'RAM': 10, 'os': 'new_os', 'name': 'vm_11', 'memory': 10}, {'RAM': 10, 'os': 'centos', 'name': 'vm_12', 'memory': 10}, {'RAM': 10, 'os': 'macos', 'name': 'vm_13', 'memory': 10}]}])

    def test_subquery_let(self):
        self.query = 'select meta().id,total from {0} let total = (SELECT RAW SUM(VMs.memory) FROM default.VMs AS VMs)[0] order by meta().id limit 10'.format('default')
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results']==[{'total': 40, 'id': 'query-testemployee10153.1877827-0'}, {'total': 40, 'id': 'query-testemployee10153.1877827-1'}, {'total': 40, 'id': 'query-testemployee10153.1877827-2'}, {'total': 40, 'id': 'query-testemployee10153.1877827-3'}, {'total': 40, 'id': 'query-testemployee10153.1877827-4'}, {'total': 40, 'id': 'query-testemployee10153.1877827-5'}, {'total': 44, 'id': 'query-testemployee10194.855617-0'}, {'total': 44, 'id': 'query-testemployee10194.855617-1'}, {'total': 44, 'id': 'query-testemployee10194.855617-2'}, {'total': 44, 'id': 'query-testemployee10194.855617-3'}])
        self.query= 'SELECT meta().id, (SELECT RAW SUM(item.memory) FROM items as item)[0] total FROM ' \
                    'default LET items = (SELECT VMs.* FROM default.VMs ORDER BY VMs.memory) order by meta().id limit 5'
        actual_result1 = self.run_cbq_query()
        self.assertTrue(actual_result1['results']== [{'total': 40, 'id': 'query-testemployee10153.1877827-0'}, {'total': 40, 'id': 'query-testemployee10153.1877827-1'}, {'total': 40, 'id': 'query-testemployee10153.1877827-2'}, {'total': 40, 'id': 'query-testemployee10153.1877827-3'}, {'total': 40, 'id': 'query-testemployee10153.1877827-4'}])

    def test_subquery_letting(self):
        self.query = 'select meta().id,total from {0} GROUP BY meta().id LETTING total = COUNT(META().id) order by meta().id limit 3'.format('default')
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{"id": "query-testemployee10153.1877827-0", "total": 1}, {"id": "query-testemployee10153.1877827-1", "total": 1}, {"id": "query-testemployee10153.1877827-2", "total": 1}])
        self.query = 'select meta().id,total from {0} GROUP BY meta().id LETTING total = (SELECT RAW SUM(VMs.memory) FROM default.VMs AS VMs)[0] order by meta().id limit 10'.format('default')
        try:
            self.run_cbq_query()
            self.fail("Query should have failed")
        except CBQError as e:
            self.assertTrue('Expression (correlated (select raw sum((`VMs`.`memory`)) from (`default`.`VMs`) as `VMs`)[0]) must depend only on group keys or aggregates.' in str(e), "Incorrect error message: \n" + str(e))

    def test_update_unset(self):
        self.query = 'UPDATE default a unset name  where "windows" in ( SELECT RAW VMs.os FROM a.VMs) limit 2 returning a.*,meta().id '
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'] == [{'tasks': [{'Marketing': [{'region2': 'International', 'region1': 'South'}, {'region2': 'South'}], 'Developer': ['IOS', 'Indexing']}, 'Sales', 'QA'], 'id': 'query-testemployee10153.1877827-0', 'address': [[{'city': 'Delhi'}, {'street': '12th street'}], [{'country': 'EUROPE', 'apartment': 123}]], 'VMs': [{'RAM': 10, 'os': 'ubuntu', 'name': 'vm_10', 'memory': 10}, {'RAM': 10, 'os': 'windows', 'name': 'vm_11', 'memory': 10}, {'RAM': 10, 'os': 'centos', 'name': 'vm_12', 'memory': 10}, {'RAM': 10, 'os': 'macos', 'name': 'vm_13', 'memory': 10}], 'mutated': 0, 'hobbies': {'hobby': [{'sports': ['Badminton', 'Football', 'Basketball']}, {'dance': ['hip hop', 'bollywood', 'contemporary']}, 'art']}, 'department': 'Support', 'join_yr': [2013, 2015, 2012], '_id': 'query-testemployee10153.1877827-0', 'email': '9-mail@couchbase.com'}, {'tasks': [{'Marketing': [{'region2': 'International', 'region1': 'South'}, {'region2': 'South'}], 'Developer': ['IOS', 'Indexing']}, 'Sales', 'QA'], 'id': 'query-testemployee10153.1877827-1', 'address': [[{'city': 'Delhi'}, {'street': '12th street'}], [{'country': 'EUROPE', 'apartment': 123}]], 'VMs': [{'RAM': 10, 'os': 'ubuntu', 'name': 'vm_10', 'memory': 10}, {'RAM': 10, 'os': 'windows', 'name': 'vm_11', 'memory': 10}, {'RAM': 10, 'os': 'centos', 'name': 'vm_12', 'memory': 10}, {'RAM': 10, 'os': 'macos', 'name': 'vm_13', 'memory': 10}], 'mutated': 0, 'hobbies': {'hobby': [{'sports': ['Badminton', 'Football', 'Basketball']}, {'dance': ['hip hop', 'bollywood', 'contemporary']}, 'art']}, 'department': 'Support', 'join_yr': [2013, 2015, 2012], '_id': 'query-testemployee10153.1877827-1', 'email': '9-mail@couchbase.com'}])

    def test_namespace_keyspace(self):
        self.query = 'select * from default:default order by name limit 1'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results']==[{'default': {'tasks': [{'Marketing': [{'region2': 'South', 'region1': 'East'}, {'region2': 'North'}], 'Developer': ['Android', 'Query']}, 'Sales', 'QA'], 'name': [{'FirstName': 'employeefirstname-1'}, {'MiddleName': 'employeemiddlename-1'}, {'LastName': 'employeelastname-1'}], 'address': [[{'city': 'San Francisco'}, {'street': '12th street'}], [{'country': 'USA', 'apartment': 123}]], 'VMs': [{'RAM': 4, 'os': 'ubuntu', 'name': 'vm_4', 'memory': 4}, {'RAM': 4, 'os': 'windows', 'name': 'vm_5', 'memory': 4}, {'RAM': 4, 'os': 'centos', 'name': 'vm_6', 'memory': 4}, {'RAM': 4, 'os': 'macos', 'name': 'vm_7', 'memory': 4}], 'mutated': 0, 'hobbies': {'hobby': [{'sports': ['ski', 'Cricket', 'Badminton']}, {'dance': ['hip hop', 'bollywood', 'salsa']}, 'art']}, 'department': 'Manager', 'join_yr': [2012, 2011, 2014], '_id': 'query-testemployee11166.0961148-0', 'email': '1-mail@couchbase.com'}}])

    # below test fails
    # def test_subquery_negative_USE_KEYS_INDEX(self):
    #     self.query = 'select meta().id,total from {0} let total = (SELECT RAW SUM(VMs.memory) FROM default.VMs AS VMs use keys "query-testemployee10153.1877827-1")[0] order by meta().id limit 10'.format('default')
    #     actual_result = self.run_cbq_query()
    #     self.query = 'select meta().id,total from {0} let total = (SELECT RAW SUM(VMs.memory) FROM default.VMs AS VMs use index(`#primary`))[0] order by meta().id limit 10'.format('default')
    #     actual_result = self.run_cbq_query()
    #     print actual_result

    def test_merge_subquery(self):
        self.query = 'UPDATE default a set id = UUID() where "centos" in ( SELECT RAW VMs.os FROM a.VMs) returning a.id '
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['metrics']['mutationCount']==10080)
        self.query = 'MERGE INTO default USING default d ON KEY id WHEN NOT MATCHED THEN INSERT {d.id} RETURNING *'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['metrics']['mutationCount']==10080)
        self.query = 'MERGE INTO default USING (SELECT "s" || id || UUID() AS id FROM default) o ON KEY o.id WHEN NOT MATCHED ' \
                     'THEN INSERT {o.id} RETURNING *;'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['metrics']['mutationCount']==20160)
        self.query = 'MERGE INTO default USING (SELECT "se" || id || UUID() || "123" AS id,(SELECT RAW SUM(VMs.memory) FROM ' \
                     'default.VMs)[0] as total from default) ' \
                     'o ON KEY o.id WHEN NOT MATCHED ' \
                     'THEN INSERT {o.id,o.total} RETURNING *;'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['metrics']['mutationCount']==40320)
        self.query = 'MERGE INTO default USING [{"id":"c1235"},{"id":"c1236"}] o ON KEY id WHEN NOT MATCHED THEN INSERT {o.id} RETURNING *'
        actual_result= self.run_cbq_query()
        self.assertTrue(actual_result['results']==[{'default': {'id': 'c1235'}}, {'default': {'id': 'c1236'}}])

    def test_correlated_queries_predicate_exists(self):
        self.query = 'SELECT name, id FROM default WHERE EXISTS (SELECT 1 FROM default.VMs WHERE VMs.memory > 10 order by meta(default).id)' \
                     ' order by meta().id limit 2'
        actual_result = self.run_cbq_query()
        self.log.info("test_correlated_queries_predicate_exists is {0}".format(actual_result['results']))
        #self.assertTrue(actual_result['results']==[{u'id': u'352e1533-eabb-4fed-a4a4-2236f7a690c7', u'name': [{u'FirstName': u'employeefirstname-4'}, {u'MiddleName': u'employeemiddlename-4'}, {u'LastName': u'employeelastname-4'}]}, {u'id': u'c6326d5b-64e0-4f65-ab00-85465472774d', u'name': [{u'FirstName': u'employeefirstname-4'}, {u'MiddleName': u'employeemiddlename-4'}, {u'LastName': u'employeelastname-4'}]}])

    def test_correlated_queries_predicate_not_exists(self):
        self.query = 'SELECT name, id FROM default WHERE NOT EXISTS (SELECT 1 FROM default.VMs' \
                     ' WHERE VMs.memory < 10 order by meta(default).id) order by meta().id limit 2'
        self.run_cbq_query()
        actual_result = self.run_cbq_query()
        self.log.info("test_correlated_queries_predicate_not_exists is {0}".format(actual_result['results']))
        #self.assertTrue(actual_result['results']==[{u'id': u'00002fb9-7b42-45ae-b864-c21e87563dac'}, {u'id': u'0011e2e6-7788-4582-b6ac-4185549dc838'}])

    def test_correlated_queries_in_clause(self):
        self.query = 'SELECT name, id FROM default WHERE "windows" IN (SELECT RAW VMs.os FROM default.VMs) order by meta(default).id limit 2'
        actual_result = self.run_cbq_query()
        self.log.info("test_correlated_queries_in_clause is {0}".format(actual_result['results']))
        #self.assertTrue(actual_result['results']==[{u'id': u'feaa4880-b117-4de6-9b3c-d1dd21c64abe'}, {u'id': u'930e38d0-c35f-4e73-991e-db1a0758b8a9'}])
        self.query = 'SELECT name, id FROM default WHERE 10 < (SELECT RAW VMs.memory FROM default.VMs)  order by meta().id limit 2'
        actual_result = self.run_cbq_query()
        self.log.info("test_correlated_queries_in_clause2 is {0}".format(actual_result['results']))
        #self.assertTrue(actual_result['results']==[{u'id': u'00148e19-1203-4f48-aa3d-2751b57fec8d'}, {u'id': u'0018f09f-9726-4f6f-b872-afa3f7510254'}])

    def test_subquery_joins(self):
        self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % ("default", "w001", {"type":"wdoc", "docid":"x001","name":"wdoc","phones":["123-456-7890", "123-456-7891"],"altid":"x001"})
        self.run_cbq_query()
        self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % ("default", "pdoc1", {"type":"pdoc", "docid":"x001","name":"pdoc","phones":["123-456-7890", "123-456-7891"],"altid":"x001"})
        self.run_cbq_query()
        self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % ("default", "pdoc2", {"type":"pdoc", "docid":"w001","name":"pdoc","phones":["123-456-7890", "123-456-7891"],"altid":"w001"})
        self.run_cbq_query()
        self.query = 'SELECT b1.b1id,b2.name FROM (select d.*,meta(d).id b1id from default d) b1 JOIN default b2 ON KEYS b1.docid where b1.b1id > ""'
        actual_result_with_subquery = self.run_cbq_query()
        self.query = 'SELECT meta(b1).id b1id, b2.name FROM default b1 JOIN default b2 ON KEYS b1.docid WHERE meta(b1).id > ""'
        expected_result= self.run_cbq_query()
        self.assertTrue(actual_result_with_subquery['results'] == expected_result['results'])

    def test_subquery_explain(self):
        self.query = 'explain SELECT q FROM {"p":[{"x":11},{"x":12}],"q":"abc","r":null}.q'
        actual_result = self.run_cbq_query()
        plan = self.ExplainPlanHelper(actual_result)
        self.assertTrue(plan['~children'][0]['#operator']=='ExpressionScan')












