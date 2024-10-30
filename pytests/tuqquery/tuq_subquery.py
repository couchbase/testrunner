from .tuq import QueryTests
from membase.api.exception import CBQError
from remote.remote_util import RemoteMachineShellConnection
from deepdiff import DeepDiff
import json


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

    def test_MB63274(self):
        self.run_cbq_query('CREATE INDEX ix1 ON default(type, META().id)')
        self.run_cbq_query('UPSERT INTO default (KEY t.id, VALUE t) '
                           'SELECT {"type":"doc", "id":"aa_"||TO_STR(d), "a1": ARRAY {"ac0":IMOD(d1,1), '
                           '"ac1":IMOD(d1,10), "ac2":IMOD(d1,2), "ac3":IMOD(d1,3)} FOR d1 IN ARRAY_RANGE(0,5) END  } AS t '
                           'FROM ARRAY_RANGE(0,5) AS d')
        expected_results = self.run_cbq_query(query='SELECT u.ac1, COUNT(1) AS cnt FROM (SELECT a1 FROM default AS t '
                                                    'WHERE type = "doc" AND META().id LIKE "aa_%" ) AS d UNNEST d.a1 AS u '
                                                    'GROUP BY u.ac1')
        actual_results = self.run_cbq_query(query='SELECT u.ac1, COUNT(1) AS cnt FROM '
                                                  '(SELECT a1 FROM default AS t WHERE type = "doc" AND META().id LIKE "aa_%" ) AS d '
                                                  'UNNEST d.a1 AS u GROUP BY u.ac1 '
                                                  'UNION SELECT u.ac1, COUNT(1) AS cnt '
                                                  'FROM (SELECT a1 FROM default AS t WHERE type = "doc" AND META().id LIKE "aa_%" ) AS d '
                                                  'UNNEST d.a1 AS u GROUP BY u.ac1')
        diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    ''' When a cte is killed, the subquery it runs should also be killed'''
    def test_cte_subquery_cancel(self):
        self.run_cbq_query('CREATE INDEX ix1 ON default(c1)')
        self.run_cbq_query('UPSERT INTO default  (KEY _k, VALUE _v) '
                           'SELECT  "k0"||TO_STR(d) AS _k , {"c1":d, "c2":2*d, "c3":3*d} AS _v '
                           'FROM ARRAY_RANGE(1,1000) AS d')
        try:
            self.run_cbq_query('WITH aa AS (WITH a1 AS (SELECT RAW COUNT(l.c2) '
                               'FROM default AS l JOIN default AS r ON l.c1 < r.c1 JOIN default r1 ON r.c1 < r1.c1 WHERE l.c1 >= 0) '
                               'SELECT a1) SELECT aa',query_params={'timeout':"5s"})
        except CBQError as e:
            self.assertTrue('Timeout 5s exceeded' in str(e))
        self.sleep(1)
        num_requests = self.get_num_requests("default:ix1",self.nodes_init)
        if num_requests == 0:
            self.fail("Requests should be non zero as a query has run against this index")
        self.log.info(f"Number of requests before sleep: {num_requests}")
        self.sleep(10)
        new_num_requests = self.get_num_requests("default:ix1",self.nodes_init)
        self.log.info(f"Number of requests after sleep: {new_num_requests}")
        self.assertEqual(num_requests,new_num_requests, "The number of requests should be the same, it is not, please check")

    def test_native_recursive_cte_subquery_cancel(self):
        self.run_cbq_query('INSERT INTO default VALUES ("e1", {"employee_id": 1,"employee_name": "John"}),'
                           'VALUES ("e2",  {"employee_id": 2,"employee_name": "Emily","manager_id": 1}),'
                           'VALUES ("e3",  {"employee_id": 3,"employee_name": "Mike","manager_id": 1}),'
                           'VALUES ("e4", {"employee_id": 4,"employee_name": "Sarah","manager_id": 2}),'
                           'VALUES ("e5", {"employee_id": 5,"employee_name": "Alex","manager_id": 3}),'
                           'VALUES ("e6",  {"employee_id": 6,"employee_name": "Lisa","manager_id": 2})'
                           'RETURNING *;')
        self.run_cbq_query('CREATE INDEX e_idx ON default(manager_id INCLUDE MISSING, employee_id, employee_name);')
        try:
            self.run_cbq_query('WITH recursive emplLevel AS '
                               '(SELECT e.employee_id, e.employee_name, 0 lvl FROM default e WHERE manager_id IS MISSING '
                               'UNION SELECT e1.employee_id, e1.employee_name, e1.manager_id, emplLevel.lvl+1 lvl '
                               'FROM default e1 JOIN emplLevel '
                               'ON e1.manager_id = emplLevel.employee_id) SELECT * FROM emplLevel;',
                               query_params={'timeout':"1s"})
        except CBQError as e:
            self.assertTrue('Timeout 1s exceeded' in str(e))
        self.sleep(1)
        num_requests = self.get_num_requests("default:e_idx",self.nodes_init)
        if num_requests == 0:
            self.fail("Requests should be non zero as a query has run against this index")
        self.log.info(f"Number of requests before sleep: {num_requests}")
        self.sleep(10)
        new_num_requests = self.get_num_requests("default:e_idx",self.nodes_init)
        self.log.info(f"Number of requests after sleep: {new_num_requests}")
        self.assertEqual(num_requests,new_num_requests, "The number of requests should be the same, it is not, please check")

    def test_builtin_recursive_cte_subquery_cancel(self):
        self.run_cbq_query('UPSERT INTO default (KEY _k, VALUE _v) '
                           'SELECT  "k0"||TO_STR(d) AS _k , {"c1":d, "c2":2*d, "c3":3*d} AS _v '
                           'FROM ARRAY_RANGE(1,1000) AS d')
        self.run_cbq_query('CREATE INDEX native_idx ON default(c1)')
        try:
            self.run_cbq_query('select RECURSIVE_CTE( '
                               '"SELECT  COUNT(l.c2) as count_1 FROM default AS l JOIN default AS r ON l.c1 < r.c1 '
                               'JOIN default r1 ON r.c1 < r1.c1 '
                               'WHERE l.c1 >= 0", "SELECT 1+a.count_1 as count_1 from $anchor a where a.count_1 < 10000");',
                               query_params={'timeout':"5s"})
        except CBQError as e:
            self.assertTrue('Timeout 5s exceeded' in str(e))
        self.sleep(1)
        num_requests = self.get_num_requests("default:native_idx",self.nodes_init)
        if num_requests == 0:
            self.fail("Requests should be non zero as a query has run against this index")
        self.log.info(f"Number of requests before sleep: {num_requests}")
        self.sleep(10)
        new_num_requests = self.get_num_requests("default:native_idx",self.nodes_init)
        self.log.info(f"Number of requests after sleep: {new_num_requests}")
        self.assertEqual(num_requests,new_num_requests, "The number of requests should be the same, it is not, please check")

    def test_subquery_cancel(self):
        self.run_cbq_query('CREATE INDEX subquery_cancel ON default(c1)')
        self.run_cbq_query('UPSERT INTO default  (KEY _k, VALUE _v) '
                           'SELECT  "k0"||TO_STR(d) AS _k , {"c1":d, "c2":2*d, "c3":3*d} AS _v '
                           'FROM ARRAY_RANGE(1,1000) AS d')
        try:
            self.run_cbq_query('SELECT * from (SELECT RAW COUNT(l.c2) '
                               'FROM default AS l JOIN default AS r ON l.c1 < r.c1 JOIN default r1 ON r.c1 < r1.c1 WHERE l.c1 >= 0) d'
                               ,query_params={'timeout': "5s"})
        except CBQError as e:
            self.assertTrue('Timeout 5s exceeded' in str(e))
        self.sleep(1)
        num_requests = self.get_num_requests("default:subquery_cancel",self.nodes_init)
        if num_requests == 0:
            self.fail("Requests should be non zero as a query has run against this index")
        self.log.info(f"Number of requests before sleep: {num_requests}")
        self.sleep(10)
        new_num_requests = self.get_num_requests("default:subquery_cancel",self.nodes_init)
        self.log.info(f"Number of requests after sleep: {new_num_requests}")
        self.assertEqual(num_requests, new_num_requests,
                         "The number of requests should be the same, it is not, please check")

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
        # self.gens_load = self.gen_docs(self.docs_per_day)
        # self.load(self.gens_load, flag=self.item_flag)
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
            self.assertTrue('must depend only on group keys or aggregates' in str(e),
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

    def test_correlated_projection(self):
        correlated_query = f"SELECT D.address[0][0].city, \
            (SELECT raw count(email) FROM {self.query_bucket} WHERE department = 'Tester' AND address[0][0].city = D.address[0][0].city)[0] as _count \
            FROM {self.query_bucket} D WHERE department = 'Developer' AND hobbies.hobby[0].sports[0] = 'Basketball' ORDER BY _count DESC LIMIT 5"
        # create secondary index
        self.run_cbq_query(f"CREATE INDEX idx_dept ON {self.query_bucket}(department)")
        try:
            self.run_cbq_query(correlated_query)
        except Exception as e:
            self.log.error(f"Correlated query failed: {e}")
            self.fail()
        finally:
            self.run_cbq_query(f"DROP INDEX idx_dept ON {self.query_bucket}")

    def test_correlated_where(self):
        correlated_query = f"SELECT Distinct D.address[0][0].city FROM {self.query_bucket} D \
            WHERE department = 'Developer' AND hobbies.hobby[0].sports[0] = 'Basketball' \
            AND 50 < (SELECT raw count(email) FROM {self.query_bucket} WHERE department = 'Tester' AND address[0][0].city = D.address[0][0].city)[0] \
            ORDER BY _count desc LIMIT 5"
        # create secondary index
        self.run_cbq_query(f"CREATE INDEX idx_dept ON {self.query_bucket}(department)")
        try:
            self.run_cbq_query(correlated_query)
        except Exception as e:
            self.log.error(f"Correlated query failed: {e}")
            self.fail()
        finally:
            self.run_cbq_query(f"DROP INDEX idx_dept ON {self.query_bucket}")

    def test_correlated_with(self):
        correlated_query = f"WITH FirstJoin AS (SELECT VALUE MIN(ARRAY_MIN(join_yr)) FROM {self.query_bucket} WHERE department = 'Developer' ), \
            TESTER AS (SELECT address FROM {self.query_bucket} WHERE department = 'Tester' and ARRAY_MIN(join_yr) = FirstJoin[0]) \
            SELECT distinct T.address[0][0].city as city FROM TESTER T ORDER BY city"
        # create secondary index
        self.run_cbq_query(f"CREATE INDEX idx_dept ON {self.query_bucket}(department)")
        try:
            self.run_cbq_query(correlated_query)
        except Exception as e:
            self.log.error(f"Correlated query failed: {e}")
            self.fail()
        finally:
            self.run_cbq_query(f"DROP INDEX idx_dept ON {self.query_bucket}")

    def test_correlated_negative(self):
        correlated_query = f"SELECT D.address[0][0].city, \
            (SELECT raw count(email) FROM {self.query_bucket} WHERE department = 'Tester' AND address[0][0].city = D.address[0][0].city)[0] as _count \
            FROM {self.query_bucket} D WHERE department = 'Developer' AND hobbies.hobby[0].sports[0] = 'Basketball' ORDER BY _count DESC LIMIT 5"
        error_code = 5370
        error_message = "Unable to run subquery - cause: Correlated subquery's keyspace (default) cannot have more than 1000 documents without appropriate secondary index"
        try:
            self.run_cbq_query(query=correlated_query)
            self.fail(f"Query did not fail as expected with error: {error_message}")
        except CBQError as ex:
            error = self.process_CBQE(ex, 0)
            self.assertEqual(error['code'], error_code)
            self.assertEqual(error['msg'], error_message)

    def test_MB57903(self):
        udf = 'CREATE OR REPLACE FUNCTION `FN_Test_Bad3` (data) LANGUAGE INLINE AS ( ( SELECT RAW (SELECT RAW data) ) ) ;'
        self.run_cbq_query(udf)
        result = self.run_cbq_query('select a,test from [1,2,3] a let test = FN_Test_Bad3(a)')
        self.assertEqual(result['results'], [{"a": 1, "test": [[1]]}, {"a": 2, "test": [[2]]}, {"a": 3,"test": [[3]]}])

        result = self.run_cbq_query('select a, (SELECT RAW (SELECT RAW a) ) AS b  from [1,2,3] a')
        self.assertEqual(result['results'], [{"a": 1, "b": [[1]]}, {"a": 2, "b": [[2]]}, {"a": 3,"b": [[3]]}])

    def test_MB60011(self):
        self.run_cbq_query(f'delete from {self.query_bucket}')
        self.run_cbq_query(f'CREATE PRIMARY INDEX if not exists ON {self.query_bucket}')
        self.run_cbq_query(f'INSERT INTO {self.query_bucket} VALUES("k01", {{"c1": 1, "c2": 1}})')

        # when used as covering index scan
        result = self.run_cbq_query(f'SELECT RAW (SELECT 1 FROM {self.query_bucket} AS d WHERE META(d).id = META(t).id ) FROM {self.query_bucket} t')
        self.log.info(f"result is {result['results']}")
        self.assertEqual(result['results'], [[{'$1': 1}]])

        # when used as non-covering index scan but with index spans
        result = self.run_cbq_query(f'SELECT RAW (SELECT d.c1 FROM {self.query_bucket} AS d WHERE META(d).id = META(t).id ) FROM {self.query_bucket} t')
        self.log.info(f"result is {result['results']}")
        self.assertEqual(result['results'], [[{'c1': 1}]])

    def test_raw_subquery(self):
        subquery = 'SELECT RAW event FROM ( SELECT RAW { "date": MILLIS("2024-06-17T03:59:00Z") } ) event ORDER BY event.date'
        result = self.run_cbq_query(subquery)
        self.log.info(f"result is {result['results']}")
        self.assertEqual(result['results'], [{"date": 1718596740000}])