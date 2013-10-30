import logger
import json
import uuid
import copy
import math
import re

import testconstants
from datetime import date
import datetime
from remote.remote_util import RemoteMachineShellConnection
from basetestcase import BaseTestCase
from couchbase.documentgenerator import DocumentGenerator
from membase.api.exception import CBQError
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import MemcachedClientHelper

class QueryTests(BaseTestCase):
    def setUp(self):
        if not self._testMethodName == 'suite_setUp':
            self.skip_buckets_handle = True
        super(QueryTests, self).setUp()
        self.version = self.input.param("cbq_version", "git_repo")
        if self.input.tuq_client and "client" in self.input.tuq_client:
            self.shell = RemoteMachineShellConnection(self.input.tuq_client["client"])
        else:
            self.shell = RemoteMachineShellConnection(self.master)
        if not self._testMethodName == 'suite_setUp':
            self._start_command_line_query(self.master)
        self.use_rest = self.input.param("use_rest", True)
        self.max_verify = self.input.param("max_verify", None)
        self.buckets = RestConnection(self.master).get_buckets()
        docs_per_day = self.input.param("doc-per-day", 49)
        self.item_flag = self.input.param("item_flag", 4042322160)
        self.gens_load = self.generate_docs(docs_per_day)

    def suite_setUp(self):
        try:
            self.load(self.gens_load, flag=self.item_flag)
            if not self.input.param("skip_build_tuq", False):
                self._build_tuq(self.master)
            self.skip_buckets_handle = True
        except:
            self.tearDown()

    def tearDown(self):
        if self._testMethodName == 'suite_tearDown':
            self.skip_buckets_handle = False
        super(QueryTests, self).tearDown()

    def suite_tearDown(self):
        if hasattr(self, 'shell'):
            self.shell.execute_command("killall /tmp/tuq/cbq-engine")
            self.shell.execute_command("killall tuqtng")
            self.shell.disconnect()


##############################################################################################
#
#   SIMPLE CHECKS
##############################################################################################
    def test_simple_check(self):
        for bucket in self.buckets:
            self.query = 'FROM %s SELECT name, email ORDER BY name,email ASC' % (bucket.name)
            actual_result = self.run_cbq_query()

            full_list = self._generate_full_docs_list(self.gens_load)
            expected_list = [{"name" : doc["name"], "email" : doc["email"]} for doc in full_list]
            expected_list_sorted = sorted(expected_list, key=lambda doc: (doc['name'], doc['email']))
            self._verify_results(actual_result['resultset'], expected_list_sorted)

    def test_simple_negative_check(self):
        queries_errors = {'SELECT name FROM {0} WHERE COUNT({0}.name)>3' :
                          'Aggregate function not allowed here',
                          'SELECT *.name FROM {0}' : 'Parse Error - syntax error',
                          'SELECT *.* FROM {0} ... ERROR' : 'Parse Error - syntax error',
                          'FROM %s SELECT name WHERE id=null' : 'Parse Error - syntax error',}
        self.negative_common_body(queries_errors)

    def test_consistent_simple_check(self):
        queries = ['SELECT name, join_day, join_mo FROM %s '
                    'WHERE name IS NOT NULL AND join_day<10 '
                    'OR join_mo = 6 ORDER BY join_day, join_mo',
                   'SELECT name, join_day, join_mo FROM %s '
                    'WHERE join_mo = 6 OR name IS NOT NULL AND '
                    'join_day<10 ORDER BY join_day, join_mo']
        for bucket in self.buckets:
            actual_result1 = self.run_cbq_query(queries[0] % bucket.name)
            actual_result2 = self.run_cbq_query(queries[1] % bucket.name)
            self.assertTrue(actual_result1['resultset'] == actual_result2['resultset'],
                              "Results are inconsistent.Difference: %s %s %s %s" %(
                                    len(actual_result1['resultset']), len(actual_result2['resultset']),
                                    actual_result1['resultset'][100], actual_result2['resultset'][100]))

    def test_simple_nulls(self):
        queries = ['SELECT id FROM %s WHERE id=NULL or id="null"']
        for bucket in self.buckets:
            for query in queries:
                actual_result = self.run_cbq_query(query % (bucket.name))
                self._verify_results(actual_result['resultset'], [])

##############################################################################################
#
#   LIMIT OFFSET CHECKS
##############################################################################################

    def test_limit_offset(self):
        for bucket in self.buckets:
            self.query = 'SELECT DISTINCT name FROM %s ORDER BY name LIMIT 10' % (bucket.name)
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [ {"name" : doc["name"]} for doc in full_list ]
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: doc['name'])
            self.assertEquals(actual_result['resultset'], expected_result[:10],
                              "Results are incorrect.Actual %s.\n Expected: %s.\n" % (
                                        actual_result['resultset'], expected_result))
            self.query = 'SELECT DISTINCT name FROM %s ORDER BY name LIMIT 10 OFFSET 10' % (bucket.name)
            actual_result = self.run_cbq_query()
            self.assertEquals(actual_result['resultset'], expected_result[10:20],
                              "Results are incorrect.Actual %s.\n Expected: %s.\n" % (
                                        actual_result['resultset'], expected_result))

    def test_limit_offset_zero(self):
        for bucket in self.buckets:
            self.query = 'SELECT DISTINCT name FROM %s ORDER BY name LIMIT 0' % (bucket.name)
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [ {"name" : doc["name"]} for doc in full_list ]
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: doc['name'])
            self.assertEquals(actual_result['resultset'], [],
                              "Results are incorrect.Actual %s.\n Expected: %s.\n" % (
                                        actual_result['resultset'], expected_result))
            self.query = 'SELECT DISTINCT name FROM %s ORDER BY name LIMIT 10 OFFSET 0' % (bucket.name)
            actual_result = self.run_cbq_query()
            self.assertEquals(actual_result['resultset'], expected_result[:10],
                              "Results are incorrect.Actual %s.\n Expected: %s.\n" % (
                                        actual_result['resultset'], expected_result))

    def test_limit_offset_negative_check(self):
        queries_errors = {'SELECT DISTINCT name FROM {0} LIMIT -1' :
                          'Parse Error - syntax error',
                          'SELECT DISTINCT name FROM {0} LIMIT 1.1' :
                          'Parse Error - syntax error',
                          'SELECT DISTINCT name FROM {0} OFFSET -1' :
                          'Parse Error - syntax error',
                          'SELECT DISTINCT name FROM {0} OFFSET 1.1' :
                          'Parse Error - syntax error'}
        self.negative_common_body(queries_errors)

##############################################################################################
#
#   ALIAS CHECKS
##############################################################################################

    def test_simple_alias(self):
        for bucket in self.buckets:
            self.query = 'SELECT COUNT(name) AS COUNT_EMPLOYEE FROM %s' % (bucket.name)
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [ { "COUNT_EMPLOYEE": len(full_list)  } ]
            self.assertEquals(actual_result['resultset'], expected_result,
                              "Results are incorrect.Actual %s.\n Expected: %s.\n" % (
                                        actual_result['resultset'], expected_result))

            self.query = 'SELECT COUNT(*) + 1 AS COUNT_EMPLOYEE FROM %s' % (bucket.name)
            actual_result = self.run_cbq_query()
            expected_result = [ { "COUNT_EMPLOYEE": len(full_list) + 1 } ]
            self.assertEquals(actual_result['resultset'], expected_result,
                              "Results are incorrect.Actual %s.\n Expected: %s.\n" % (
                                        actual_result['resultset'], expected_result))

    def test_simple_negative_alias(self):
        queries_errors = {'SELECT name._last_name as *' : 'Parse Error - syntax error',
                          'SELECT name._last_name as DATABASE ?' : 'Parse Error - syntax error',
                          'SELECT nodes AS NULL FROM {0}' : 'Parse Error - syntax error',
                          'SELECT email as name, name FROM {0}' :
                                'alias name is defined more than once',
                          'SELECT tasks_points AS points, points.task1 FROM {0}' :
                                'Alias points cannot be referenced',
                          'SELECT tasks_points.task1 AS points_new FROM {0} AS test ' +
                           'WHERE points_new >0' : "Alias points_new cannot be referenced",
                          'SELECT DISTINCT tasks_points AS points_new FROM {0} AS test ' +
                           'ORDER BY points_new':
                                'Expression points_new is not in the list',
                          'SELECT tasks_points AS points FROM {0} AS test GROUP BY points':
                                'Alias points cannot be referenced',
                          'SELECT test.tasks_points as points FROM {0} AS TEST ' +
                           'GROUP BY TEST.points' :
                                'The expression TEST is not satisfied by these dependencies',
                          'SELECT test.tasks_points as points FROM {0} AS TEST ' +
                           'GROUP BY tasks_points AS GROUP_POINT' :
                                'parse_error',
                          'SELECT COUNT(tasks_points) as COUNT_NEW_POINT, COUNT(name) ' +
                           'as COUNT_EMP  FROM {0} AS TEST GROUP BY name ' +
                           'HAVING COUNT_NEW_POINT >0' :
                                'Alias COUNT_NEW_POINT cannot be referenced'}
        self.negative_common_body(queries_errors)

    def test_alias_from_clause(self):
        queries = ['SELECT tasks_points.task1 AS points FROM %s AS test ORDER BY points' ,
                   'SELECT tasks_points.task1 AS points FROM %s AS test WHERE test.join_day >0'
                   ' ORDER BY points',
                   'SELECT tasks_points.task1 AS points FROM %s AS test '
                       'WHERE FLOOR(test.test_rate) >0 ORDER BY points',
                   'SELECT tasks_points.task1 AS points FROM %s AS test '
                   'GROUP BY test.tasks_points.task1 ORDER BY points']
        for bucket in self.buckets:
            for query in queries:
                full_list = self._generate_full_docs_list(self.gens_load)
                expected_result = [{"points" : doc["tasks_points"]["task1"]} for doc in full_list]
                if query.find('GROUP BY') != -1:
                    expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
                expected_result = sorted(expected_result, key=lambda doc: doc['points'])
                actual_result = self.run_cbq_query(query % (bucket.name))
                self._verify_results(actual_result['resultset'], expected_result)

    def test_alias_from_clause_group(self):
        for bucket in self.buckets:
            self.query = 'SELECT tasks_points.task1 AS points FROM %s AS test ' %(bucket.name) +\
                         'GROUP BY test.tasks_points.task1 ORDER BY points'
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"points" : doc["tasks_points"]["task1"]} for doc in full_list]
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: doc['points'])
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['resultset'], expected_result)

    def test_alias_order_desc(self):
        for bucket in self.buckets:
            self.query = 'SELECT name AS name_new FROM %s AS test ORDER BY name_new DESC' %(
                                                                                bucket.name)
            actual_result = self.run_cbq_query()

            full_list = self._generate_full_docs_list(self.gens_load)
            expected_list = [{"name_new" : doc["name"]} for doc in full_list]
            expected_result = sorted(expected_list, key=lambda doc: doc['name_new'],
                                     reverse=True)
            self._verify_results(actual_result['resultset'], expected_result)

    def test_alias_order_asc(self):
        for bucket in self.buckets:
            self.query = 'SELECT name AS name_new FROM %s AS test ORDER BY name_new DESC' %(
                                                                                bucket.name)
            actual_result = self.run_cbq_query()

            full_list = self._generate_full_docs_list(self.gens_load)
            expected_list = [{"name_new" : doc["name"]} for doc in full_list]
            expected_result = sorted(expected_list, key=lambda doc: doc['name_new'],
                                     reverse=True)
            self._verify_results(actual_result['resultset'], expected_result)

    def test_alias_aggr_fn(self):
        for bucket in self.buckets:
            self.query = 'SELECT COUNT(TEST.name) from %s AS TEST' %(bucket.name)
            actual_result = self.run_cbq_query()

            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"$1" : len(full_list)}]
            self._verify_results(actual_result['resultset'], expected_result)

    def test_alias_over(self):
        for bucket in self.buckets:
            self.query = 'SELECT count(skill) FROM %s AS emp OVER skill IN emp.skills' %(
                                                                            bucket.name)
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"$1" : len(full_list) * len(full_list[0]["skills"])}]
            self._verify_results(actual_result['resultset'], expected_result)

##############################################################################################
#
#   ORDER BY CHECKS
##############################################################################################

    def test_order_by_check(self):
        for bucket in self.buckets:
            self.query = 'SELECT name, job_title, tasks_points.task1 FROM %s'  % (bucket.name) +\
            ' ORDER BY job_title, name, tasks_points.task1'
            actual_result = self.run_cbq_query()

            full_list = self._generate_full_docs_list(self.gens_load)
            expected_list = [{"name" : doc["name"], "job_title" : doc["job_title"],
                              "task1" : doc["tasks_points"]["task1"]}
                             for doc in full_list]
            expected_result = sorted(expected_list, key=lambda doc: (doc['job_title'],
                                                                     doc["name"],
                                                                     doc["task1"]))
            self._verify_results(actual_result['resultset'], expected_result)
            self.query = 'SELECT name, job_title FROM %s'  % (bucket.name) +\
            ' ORDER BY tasks_points.task1, job_title, name'
            actual_result = self.run_cbq_query()
            result_sorted = sorted(expected_list, key=lambda doc: (doc["task1"],
                                                                     doc['job_title'],
                                                                     doc["name"]))
            expected_result = [{"name" : doc["name"], "job_title" : doc["job_title"]}
                               for doc in result_sorted]
            self._verify_results(actual_result['resultset'], expected_result)
            self.query = 'SELECT name, job_title, tasks_points.task1 AS CONTACT' +\
            ' FROM %s ORDER BY 2, 1, 3' % (bucket.name)
            actual_result = self.run_cbq_query()
            expected_list = [{"name" : doc["name"], "job_title" : doc["job_title"],
                              "CONTACT" : doc["tasks_points"]["task1"]}
                             for doc in full_list]
            expected_result = sorted(expected_list, key=lambda doc: (doc['job_title'],
                                                                     doc["name"],
                                                                     doc["CONTACT"]))
            self._verify_results(actual_result['resultset'], expected_result)

    def test_order_by_alias(self):
        for bucket in self.buckets:
            self.query = 'SELECT job_title, tasks_points AS points FROM %s'  % (bucket.name) +\
            ' AS test ORDER BY job_title DESC, points DESC'
            actual_result = self.run_cbq_query()

            full_list = self._generate_full_docs_list(self.gens_load)
            expected_list = [{"job_title" : doc["job_title"],
                              "points" : doc["tasks_points"]}
                             for doc in full_list]
            expected_result = sorted(expected_list, key=lambda doc: (doc['job_title'],
                                                                     doc["points"]),
                                     reverse=True)
            self._verify_results(actual_result['resultset'], expected_result)

    def test_order_by_alias_arrays(self):
        for bucket in self.buckets:
            self.query = 'SELECT job_title, tasks_points, skills[0] AS SKILL FROM %s'  % (
                                                                            bucket.name) +\
            ' AS TEST ORDER BY SKILL, job_title, TEST.tasks_points'
            actual_result = self.run_cbq_query()

            full_list = self._generate_full_docs_list(self.gens_load)
            expected_list = [{"job_title" : doc["job_title"],
                              "tasks_points" : doc["tasks_points"],
                              "SKILL" : doc["skills"][0]}
                             for doc in full_list]
            expected_result = sorted(expected_list, key=lambda doc: (doc["SKILL"],
                                                                     doc['job_title'],
                                                                     doc["tasks_points"]))
            self._verify_results(actual_result['resultset'], expected_result)

    def test_order_by_alias_aggr_fn(self):
        for bucket in self.buckets:
            self.query = 'SELECT join_yr, join_mo, count(*) AS emp_per_month from %s'% (
                                                                            bucket.name) +\
            ' WHERE join_mo>7 GROUP BY join_yr, join_mo ORDER BY emp_per_month, join_mo, join_yr'  
            actual_result = self.run_cbq_query()

            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"join_yr" : doc["join_yr"],
                              "join_mo" : doc["join_mo"],
                              "emp_per_month" : len([x for x in full_list if
                                                     x['join_yr'] == doc["join_yr"] and\
                                                     x["join_mo"] == doc["join_mo"]])}
                             for doc in full_list
                             if doc["join_mo"] > 7]
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: (doc["emp_per_month"],
                                                                     doc['join_mo'],
                                                                     doc["join_yr"]))
            self._verify_results(actual_result['resultset'], expected_result)

    def test_order_by_aggr_fn(self):
        for bucket in self.buckets:
            self.query = 'SELECT job_title AS TITLE FROM %s GROUP'  % (bucket.name) +\
            ' BY job_title ORDER BY MIN(join_mo), job_title'
            actual_result = self.run_cbq_query()

            full_list = self._generate_full_docs_list(self.gens_load)
            tmp_titles = set([doc['job_title'] for doc in full_list])
            expected_list = [{"job_title" : title,
                              "min_value" :
                              min([doc["join_mo"] for doc in full_list
                                   if doc["job_title"]==title])}
                             for title in tmp_titles]

            expected_result = sorted(expected_list, key=lambda doc: (doc['min_value'],
                                                                     doc['job_title']))
            expected_result = [{"TITLE" : doc["job_title"]} for doc in expected_result]
            self._verify_results(actual_result['resultset'], expected_result)

    def test_order_by_precedence(self):
        for bucket in self.buckets:
            self.query = 'SELECT job_title, join_yr FROM %s'  % (bucket.name) +\
            ' ORDER BY job_title, join_yr'
            actual_result = self.run_cbq_query()

            full_list = self._generate_full_docs_list(self.gens_load)
            expected_list = [{"job_title" : doc["job_title"],
                              "join_yr" : doc["join_yr"]}
                             for doc in full_list]
            expected_result = sorted(expected_list, key=lambda doc: (doc['job_title'],
                                                                     doc["join_yr"]))
            self._verify_results(actual_result['resultset'], expected_result)

            self.query = 'SELECT job_title, join_yr FROM %s'  % (bucket.name) +\
            ' ORDER BY join_yr, job_title'
            actual_result = self.run_cbq_query()

            expected_list = [{"job_title" : doc["job_title"],
                              "join_yr" : doc["join_yr"]}
                             for doc in full_list]
            expected_result = sorted(expected_list, key=lambda doc: (doc['join_yr'],
                                                                     doc["job_title"]))
            self._verify_results(actual_result['resultset'], expected_result)

    def test_order_by_over(self):
        for bucket in self.buckets:
            self.query = 'SELECT name, VMs FROM %s AS employee ' % (bucket.name) +\
                        'WHERE ANY vm.RAM > 5 AND vm.os = "ubuntu" ' +\
                        'OVER vm IN employee.VMs end ORDER BY name, VMs[0].RAM'
            actual_result = self.run_cbq_query()

            full_list = self._generate_full_docs_list(self.gens_load)
            expected_list = [{"name" : doc["name"], "VMs" : doc["VMs"]}
                              for doc in full_list
                              if len([vm for vm in doc["VMs"]
                                      if vm["RAM"] > 5 and vm["os"] == 'ubuntu']) > 0]
            expected_result = sorted(expected_list, key=lambda doc: (doc['name'],
                                                                     doc["VMs"][0]["RAM"]))
            self._verify_results(actual_result['resultset'], expected_result)

##############################################################################################
#
#   DISTINCT
##############################################################################################

    def test_distinct(self):
        for bucket in self.buckets:
            self.query = 'SELECT DISTINCT job_title FROM %s ORDER BY job_title'  % (bucket.name)
            actual_result = self.run_cbq_query()

            full_list = self._generate_full_docs_list(self.gens_load)
            tmp_titles = set([doc['job_title'] for doc in full_list])
            expected_result = [{"job_title" : title}
                             for title in tmp_titles]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result['resultset'], expected_result)

    def test_distinct_nested(self):
        for bucket in self.buckets:
            self.query = 'SELECT DISTINCT tasks_points.task1 FROM %s '  % (bucket.name) +\
                         'ORDER BY tasks_points.task1'
            full_list = self._generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            tmp = set([doc['tasks_points']['task1'] for doc in full_list])
            expected_result = [{"task1" : point} for point in tmp]
            expected_result = sorted(expected_result, key=lambda doc: (doc['task1']))
            self._verify_results(actual_result['resultset'], expected_result)

            self.query = 'SELECT DISTINCT skills[0] as skill' +\
                         ' FROM %s ORDER BY skills[0]'  % (bucket.name)
            actual_result = self.run_cbq_query()
            tmp = set([doc['skills'][0] for doc in full_list])
            expected_result = [{"skill" : point} for point in tmp]
            expected_result = sorted(expected_result, key=lambda doc: (doc['skill']))
            self._verify_results(actual_result['resultset'], expected_result)

            self.query = 'SELECT DISTINCT tasks_points.* ' +\
                         'FROM %s'  % (bucket.name)
            actual_result = self.run_cbq_query()
            expected_result = [doc['tasks_points'] for doc in full_list]
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc:
                                     (doc['task1'], doc['task2']))
            actual_result = sorted(actual_result['resultset'], key=lambda doc:
                                     (doc['task1'], doc['task2']))
            self._verify_results(actual_result, expected_result)

    def test_distinct_negative(self):
        queries_errors = {'SELECT name FROM {0} ORDER BY DISTINCT name' : 'Parse Error - syntax error',
                          'SELECT name FROM {0} GROUP BY DISTINCT name' : 'Parse Error - syntax error',
                          'SELECT ANY tasks_points FROM {0}' : 'Parse Error - syntax error'}
        self.negative_common_body(queries_errors)

    def test_any(self):
        for bucket in self.buckets:
            self.query = "SELECT name, email FROM %s WHERE "  % (bucket.name) +\
                         "(ANY skill = 'skill2010' OVER skill IN %s.skills end) " % (
                                                                      bucket.name) +\
                         "AND (ANY vm.RAM = 5 OVER vm IN %s.VMs end) " % (
                                                                bucket.name) +\
                         "AND  NOT (job_title = 'Sales') ORDER BY name"
            full_list = self._generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            expected_result = [{"name" : doc['name'], "email" : doc["email"]}
                               for doc in full_list
                               if len([skill for skill in doc["skills"]
                                       if skill == 'skill2010']) > 0 and\
                                  len([vm for vm in doc["VMs"]
                                       if vm["RAM"] == 5]) > 0 and\
                                  doc["job_title"] != 'Sales']
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['resultset'], expected_result)

    def test_any_no_in_clause(self):
        for bucket in self.buckets:
            self.query = "SELECT name, email FROM %s WHERE "  % (bucket.name) +\
                         "(ANY skills = 'skill2010' OVER %s.skills end) " % (
                                                                bucket.name) +\
                         "AND (ANY VMs.RAM = 5 OVER %s.VMs end) " % (
                                                            bucket.name) +\
                         "AND  NOT (job_title = 'Sales') ORDER BY name"
            full_list = self._generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            expected_result = [{"name" : doc['name'], "email" : doc["email"]}
                               for doc in full_list
                               if len([skill for skill in doc["skills"]
                                       if skill == 'skill2010']) > 0 and\
                                  len([vm for vm in doc["VMs"]
                                       if vm["RAM"] == 5]) > 0 and\
                                  doc["job_title"] != 'Sales']
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['resultset'], expected_result)

    def test_any_external(self):
        for bucket in self.buckets:
            self.query = 'SELECT name FROM %s WHERE '  % (bucket.name) +\
                         'ANY job_title = x OVER x IN ["Support", "Management"] END ' +\
                         'ORDER BY name'
            full_list = self._generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            expected_result = [{"name" : doc['name']}
                               for doc in full_list
                               if doc["job_title"] in ["Support", "Management"]]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['resultset'], expected_result)

    def test_all(self):
        for bucket in self.buckets:
            self.query = "SELECT name FROM %s WHERE " % (bucket.name) +\
                         "(ALL CEIL(vm.memory) > 5 OVER vm IN %s.VMs END)" % (
                                                            bucket.name) +\
                         " ORDER BY name"
            full_list = self._generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            expected_result = [{"name" : doc['name']}
                               for doc in full_list
                               if len([vm for vm in doc["VMs"]
                                       if math.ceil(vm['memory']) > 5]) ==\
                                  len(doc["VMs"])]

            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['resultset'], expected_result)

    def test_all_no_in_clause(self):
        for bucket in self.buckets:
            self.query = "SELECT name FROM %s WHERE " % (bucket.name) +\
                         "(ALL CEIL(VMs.memory) > 5 OVER %s.VMs END)" % (
                                                        bucket.name) +\
                         " ORDER BY name"
            full_list = self._generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            expected_result = [{"name" : doc['name']}
                               for doc in full_list
                               if len([vm for vm in doc["VMs"]
                                       if math.ceil(vm['memory']) > 5]) ==\
                                  len(doc["VMs"])]

            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['resultset'], expected_result)

    def test_array(self):
        for bucket in self.buckets:
            self.query = "SELECT ARRAY vm.memory OVER vm IN VMs END AS vm_memories" +\
            " FROM %s WHERE VMs IS NOT NULL "  % (bucket.name)
            full_list = self._generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'], key=lambda doc: (doc['vm_memories']))
            expected_result = [{"vm_memories" : [vm["memory"] for vm in doc['VMs']]}
                               for doc in full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['vm_memories']))
            self._verify_results(actual_result, expected_result)

##############################################################################################
#
#   LIKE
##############################################################################################

    def test_like(self):
        for bucket in self.buckets:
            self.query = "SELECT name FROM {0} WHERE job_title LIKE 'S%' ORDER BY name".format(bucket.name)
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"name" : doc['name']} for doc in full_list
                               if doc["job_title"].startswith('S')]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['resultset'], expected_result)

            self.query = "SELECT name FROM {0} WHERE job_title LIKE '%u%' ORDER BY name".format(
                                                                            bucket.name)
            actual_result = self.run_cbq_query()
            expected_result = [{"name" : doc['name']} for doc in full_list
                               if doc["job_title"].find('u') != -1]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['resultset'], expected_result)

            self.query = "SELECT name FROM {0} WHERE job_title NOT LIKE 'S%' ORDER BY name".format(
                                                                            bucket.name)
            actual_result = self.run_cbq_query()
            expected_result = [{"name" : doc['name']} for doc in full_list
                               if not doc["job_title"].startswith('S')]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['resultset'], expected_result)

            self.query = "SELECT name FROM {0} WHERE job_title NOT LIKE '_ales' ORDER BY name".format(
                                                                            bucket.name)
            actual_result = self.run_cbq_query()
            expected_result = [{"name" : doc['name']} for doc in full_list
                               if not (doc["job_title"].endswith('ales') and\
                               len(doc["job_title"]) == 5)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['resultset'], expected_result)

    def test_like_negative(self):
        queries_errors = {"SELECT tasks_points FROM {0} WHERE tasks_points.* LIKE '_1%'" :
                           'Parse Error - syntax error'}
        self.negative_common_body(queries_errors)

    def test_like_any(self):
        for bucket in self.buckets:
            self.query = "SELECT name, email FROM %s WHERE (ANY vm.os " % (bucket.name) +\
            "LIKE '%bun%' OVER vm IN " +\
            "%s.VMs END) AND (ANY skill = 'skill2010' " % (bucket.name) +\
            "OVER skill IN %s.skills END) ORDER BY name"% (bucket.name)
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"name" : doc['name'], "email" : doc["email"]}
                               for doc in full_list
                               if len([vm for vm in doc["VMs"]
                                       if vm["os"].find('bun') != -1]) > 0 and\
                                  len([skill for skill in doc["skills"]
                                       if skill == 'skill2010']) > 0]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['resultset'], expected_result)

    def test_like_all(self):
        for bucket in self.buckets:
            self.query = "SELECT name, email FROM %s WHERE (ALL vm.os" % (bucket.name) +\
                         " NOT LIKE '%cent%' OVER vm IN " +\
                         "%s.VMs END) AND (ANY skill =" % (bucket.name) +\
                         " 'skill2010' OVER skill IN %s.skills END) ORDER BY name" % (
                                                                      bucket.name)
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"name" : doc['name'], "email" : doc["email"]}
                               for doc in full_list
                               if len([vm for vm in doc["VMs"]
                                     if vm["os"].find('cent') == -1]) == len(doc["VMs"]) and\
                                  len([skill for skill in doc["skills"]
                                       if skill == 'skill2010']) > 0]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['resultset'], expected_result)

    def test_like_aliases(self):
        for bucket in self.buckets:
            self.query = "select name AS NAME from %s " % (bucket.name) +\
            "AS EMPLOYEE where EMPLOYEE.name LIKE '_mpl%' ORDER BY name"
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"NAME" : doc['name']} for doc in full_list
                               if doc["name"].find('mpl') == 1]
            expected_result = sorted(expected_result, key=lambda doc: (doc['NAME']))
            self._verify_results(actual_result['resultset'], expected_result)

    def test_like_wildcards(self):
        for bucket in self.buckets:
            self.query = "SELECT email FROM %s WHERE email " % (bucket.name) +\
                         "LIKE '%\@%\.%' ORDER BY email"
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"email" : doc['email']} for doc in full_list
                               if re.match(r'.*@.*\..*', doc['email'])]
            expected_result = sorted(expected_result, key=lambda doc: (doc['email']))
            self._verify_results(actual_result['resultset'], expected_result)

            self.query = "SELECT email FROM %s WHERE email" % (bucket.name) +\
                         " NOT LIKE '%\@%\.' ORDER BY email"
            actual_result = self.run_cbq_query()
            expected_result = [{"email" : doc['email']} for doc in full_list
                               if re.match(r'.*@.*\.$', doc['email']) is None]
            expected_result = sorted(expected_result, key=lambda doc: (doc['email']))
            self._verify_results(actual_result['resultset'], expected_result)

##############################################################################################
#
#   GROUP BY
##############################################################################################

    def test_group_by(self):
        for bucket in self.buckets:
            self.query = "SELECT tasks_points.task1 AS task from %s " % (bucket.name) +\
                         "WHERE join_mo>7 GROUP BY tasks_points.task1 " +\
                         "ORDER BY tasks_points.task1"
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"task" : doc['tasks_points']["task1"]}
                               for doc in full_list
                               if doc["join_mo"] > 7]
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['task']))
            self._verify_results(actual_result['resultset'], expected_result)

            self.query = "SELECT tasks_points.task1 AS task from %s " % (bucket.name) +\
                         "WHERE join_mo>7 GROUP BY 1 " +\
                         "ORDER BY tasks_points.task1"
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['resultset'], expected_result)

    def test_group_by_having(self):
        for bucket in self.buckets:
            self.query = "from %s WHERE join_mo>7 GROUP BY tasks_points.task1 " % (bucket.name) +\
                         "HAVING COUNT(tasks_points.task1) > 0 SELECT tasks_points.task1 " +\
                         "AS task ORDER BY tasks_points.task1"
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"task" : doc['tasks_points']["task1"]}
                               for doc in full_list
                               if doc["join_mo"] > 7]
            expected_result = [doc for doc in expected_result
                               if expected_result.count(doc) > 0]
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['task']))
            self._verify_results(actual_result['resultset'], expected_result)

    def test_group_by_aggr_fn(self):
        for bucket in self.buckets:
            self.query = "SELECT tasks_points.task1 AS task from %s " % (bucket.name) +\
                         "WHERE join_mo>7 GROUP BY tasks_points.task1 " +\
                         "HAVING COUNT(tasks_points.task1) > 0 AND "  +\
                         "(MIN(join_day)=1 OR MAX(join_yr=2011)) " +\
                         "ORDER BY tasks_points.task1"
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            tmp_groups = set([doc['tasks_points']["task1"] for doc in full_list])
            expected_result = [{"task" : group} for group in tmp_groups
                               if [doc['tasks_points']["task1"]
                                   for doc in full_list].count(group) >0 and\
                               (min([doc["join_day"] for doc in full_list
                                     if doc['tasks_points']["task1"] == group]) == 1 or\
                                max([doc["join_yr"] for doc in full_list
                                     if doc['tasks_points']["task1"] == group]) == 2011)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['task']))
            self._verify_results(actual_result['resultset'], expected_result)

    def test_group_by_over(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title, AVG(test_rate) as avg_rate FROM %s " % (bucket.name) +\
                         "WHERE (ANY skill = 'skill2010' OVER skill IN %s.skills end) " % (
                                                                      bucket.name) +\
                         "AND (ANY vm.RAM = 5 OVER vm IN %s.VMs end) "  % (
                                                                      bucket.name) +\
                         "GROUP BY job_title ORDER BY job_title"
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            tmp_groups = set([doc["job_title"] for doc in full_list])
            expected_result = [{"job_title" : doc['job_title'],
                                "test_rate" : doc["test_rate"]}
                               for doc in full_list
                               if 'skill2010' in doc["skills"] and\
                                  len([vm for vm in doc["VMs"] if vm["RAM"] == 5]) >0]
            expected_result = [{"job_title" : group,
                                "avg_rate" : math.fsum([doc["test_rate"]
                                                         for doc in expected_result
                                             if doc["job_title"] == group]) /
                                             len([doc["test_rate"] for doc in expected_result
                                             if doc["job_title"] == group])}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result['resultset'], expected_result)

    def test_group_by_negative(self):
        queries_errors = {"SELECT tasks_points from {0} WHERE tasks_points.task2>3"
                          " GROUP BY tasks_points.*" :
                           "Parse Error - syntax error",
                           "SELECT tasks_points from {0} AS TEST "
                           "WHERE tasks_points.task2>3 GROUP BY TEST.tasks_points.task1":
                           "The expression TEST is not satisfied by these dependencies",
                           "SELECT tasks_points.task1 AS task from {0} WHERE join_mo>7"
                           " GROUP BY task" : "Alias task cannot be referenced",
                           "SELECT tasks_points.task1 AS task from {0} WHERE join_mo>7"
                           " GROUP BY tasks_points.task1 HAVING COUNT(task) > 0" :
                           "Alias task cannot be referenced",
                           "from {0} WHERE join_mo>7 GROUP BY tasks_points.task1 "
                           "SELECT tasks_points.task1 AS task HAVING COUNT(tasks_points.task1) > 0" :
                           "Parse Error - syntax error"}
        self.negative_common_body(queries_errors)

##############################################################################################
#
#   SCALAR FN
##############################################################################################

    def test_ceil(self):
        for bucket in self.buckets:
            self.query = "select name, ceil(test_rate) as rate from %s"  % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'],
                                   key=lambda doc: (doc['name'], doc['rate']))
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"name" : doc['name'], "rate" : math.ceil(doc['test_rate'])}
                               for doc in full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['rate']))
            self._verify_results(actual_result, expected_result)

            self.query = "select name from %s where ceil(test_rate) > 5"  % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'], key=lambda doc: (doc['name']))
            expected_result = [{"name" : doc['name']} for doc in full_list
                               if math.ceil(doc['test_rate']) > 5]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

    def test_floor(self):
        for bucket in self.buckets:
            self.query = "select name, floor(test_rate) as rate from %s"  % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'],
                                   key=lambda doc: (doc['name'], doc['rate']))
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"name" : doc['name'], "rate" : math.floor(doc['test_rate'])}
                               for doc in full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['rate']))
            self._verify_results(actual_result, expected_result)

            self.query = "select name from %s where floor(test_rate) > 5"  % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'], key=lambda doc: (doc['name']))
            expected_result = [{"name" : doc['name']} for doc in full_list
                               if math.floor(doc['test_rate']) > 5]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

            self.query = "select name from %s where floor(job_title) > 5"  %(bucket.name)
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['resultset'], [])

    def test_greatest(self):
        for bucket in self.buckets:
            self.query = "select name, GREATEST(skills[0], skills[1]) as SKILL from %s"  % (
                                                                                bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'],
                                   key=lambda doc: (doc['name'], doc['SKILL']))
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"name" : doc['name'], "SKILL" :
                                (doc['skills'][0], doc['skills'][1])[doc['skills'][0]<doc['skills'][1]]}
                               for doc in full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['SKILL']))
            self._verify_results(actual_result, expected_result)

    def test_least(self):
        for bucket in self.buckets:
            self.query = "select name, LEAST(skills[0], skills[1]) as SKILL from %s"  % (
                                                                            bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'],
                                   key=lambda doc: (doc['name'], doc['SKILL']))
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"name" : doc['name'], "SKILL" :
                                (doc['skills'][0], doc['skills'][1])[doc['skills'][0]>doc['skills'][1]]}
                               for doc in full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['SKILL']))
            self._verify_results(actual_result, expected_result)

    def test_meta(self):
        for bucket in self.buckets:
            self.query = 'SELECT distinct name FROM %s WHERE META().type = "json"'  % (
                                                                            bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'],
                                   key=lambda doc: (doc['name']))
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"name" : doc['name']} for doc in full_list]
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

            self.query = "SELECT distinct name FROM %s WHERE META().id IS NOT NULL"  % (
                                                                                   bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'], key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

    def test_meta_flags(self):
        for bucket in self.buckets:
            self.query = 'SELECT DISTINCT META().flags as flags FROM %s'  % (
                                                                bucket.name)
            actual_result = self.run_cbq_query()
            expected_result = [{"flags" : self.item_flag}]
            self._verify_results(actual_result['resultset'], expected_result)

    def test_meta_cas(self):
        for bucket in self.buckets:
            self.query = 'SELECT META().cas as cas, META().id as id FROM %s'  % (bucket.name)
            actual_result = self.run_cbq_query()
            keys = [doc['id'] for doc in actual_result['resultset']]
            actual_result = [{"cas": int(doc['cas'])} for doc in actual_result['resultset']]
            actual_result = sorted(actual_result,
                                   key=lambda doc: (doc['cas']))
            client = MemcachedClientHelper.direct_client(self.master, bucket.name)
            expected_result = []
            for key in keys:
                _, cas, _ = client.get(key.encode('utf-8'))
                expected_result.append({"cas" : cas})
            expected_result = sorted(expected_result, key=lambda doc: (doc['cas']))
            self._verify_results(actual_result, expected_result)

    def test_length(self):
        for bucket in self.buckets:
            self.query = 'Select name, email from %s where LENGTH(job_title) = 5'  % (
                                                                            bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'],
                                   key=lambda doc: (doc['name']))
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"name" : doc['name'], "email" : doc['email']}
                               for doc in full_list
                               if len(doc['job_title']) == 5]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

    def test_upper(self):
        for bucket in self.buckets:
            self.query = 'SELECT DISTINCT UPPER(job_title) as JOB from %s'  % (
                                                                        bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'],
                                   key=lambda doc: (doc['JOB']))
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"JOB" : doc['job_title'].upper()}
                               for doc in full_list]
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['JOB']))
            self._verify_results(actual_result, expected_result)

    def test_lower(self):
        for bucket in self.buckets:
            self.query = "SELECT distinct email from %s" % (bucket.name) +\
                         " WHERE LOWER(job_title) < 't'"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'],
                                   key=lambda doc: (doc['email']))
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"email" : doc['email'].lower()}
                               for doc in full_list
                               if doc['job_title'].lower() < 't']
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['email']))
            self._verify_results(actual_result, expected_result)

    def test_round(self):
        for bucket in self.buckets:
            self.query = "select name, round(test_rate) as rate from %s" % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'],
                                   key=lambda doc: (doc['name'], doc['rate']))
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"name": doc["name"], "rate" : round(doc['test_rate'])}
                               for doc in full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['rate']))
            self._verify_results(actual_result, expected_result)

    def test_substr(self):
        for bucket in self.buckets:
            self.query = "select name, SUBSTR(email, 7) as DOMAIN from %s" % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'],
                                   key=lambda doc: (doc['name'], doc['DOMAIN']))
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"name": doc["name"], "DOMAIN" : doc['email'][7:]}
                               for doc in full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['DOMAIN']))
            self._verify_results(actual_result, expected_result)

    def test_trunc(self):
        for bucket in self.buckets:
            self.query = "select name, TRUNC(test_rate, 0) as rate from %s" % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'],
                                   key=lambda doc: (doc['name'], doc['rate']))
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"name": doc["name"], "rate" : math.trunc(doc['test_rate'])}
                               for doc in full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['rate']))
            self._verify_results(actual_result, expected_result)

    def test_first(self):
        for bucket in self.buckets:
            self.query = "select name, FIRST vm.os over vm in VMs end as OS from %s" % (
                                                                           bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'],
                                   key=lambda doc: (doc['name'], doc['OS']))
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"name": doc["name"], "OS" : doc['VMs'][0]["os"]}
                               for doc in full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['OS']))
            self._verify_results(actual_result, expected_result)

##############################################################################################
#
#   AGGR FN
##############################################################################################

    def test_sum(self):
        for bucket in self.buckets:
            self.query = "SELECT join_mo, SUM(tasks_points.task1) as points_sum" +\
                        " FROM %s WHERE join_mo < 5 GROUP BY join_mo " % (bucket.name) +\
                        "ORDER BY join_mo"
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            tmp_groups = set([doc['join_mo'] for doc in full_list
                              if doc['join_mo'] < 5])
            expected_result = [{"join_mo" : group,
                                "points_sum" : math.fsum([doc['tasks_points']['task1']
                                                          for doc in full_list
                                                          if doc['join_mo'] == group])}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['join_mo']))
            self._verify_results(actual_result['resultset'], expected_result)

            self.query = "SELECT join_mo, SUM(test_rate) as rate FROM %s " % (bucket.name) +\
                         "as employees WHERE job_title='Sales' GROUP BY join_mo " +\
                         "HAVING SUM(employees.test_rate) > 0 and " +\
                         "SUM(test_rate) < 100000"
            actual_result = self.run_cbq_query()
            actual_result = [{"join_mo" : doc["join_mo"], "rate" : int(doc["rate"])} for doc in actual_result]
            actual_result = sorted(actual_result['resultset'], key=lambda doc: (doc['join_mo']))
            tmp_groups = set([doc['join_mo'] for doc in full_list])
            expected_result = [{"join_mo" : group,
                                "rate" : int(math.fsum([doc['test_rate']
                                                          for doc in full_list
                                                          if doc['join_mo'] == group and\
                                                             doc['job_title'] == 'Sales']))}
                               for group in tmp_groups
                               if math.fsum([doc['test_rate']
                                            for doc in full_list
                                            if doc['join_mo'] == group and\
                                            doc['job_title'] == 'Sales'] ) > 0 and\
                                  math.fsum([doc['test_rate']
                                            for doc in full_list
                                            if doc['join_mo'] == group and\
                                            doc['job_title'] == 'Sales'] ) < 100000]
            expected_result = sorted(expected_result, key=lambda doc: (doc['join_mo']))
            self._verify_results(actual_result, expected_result)

    def test_avg(self):
        for bucket in self.buckets:
            self.query = "SELECT join_mo, AVG(tasks_points.task1) as points_avg" +\
                        " FROM %s WHERE join_mo < 5 GROUP BY join_mo " % (bucket.name) +\
                        "ORDER BY join_mo"
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            tmp_groups = set([doc['join_mo'] for doc in full_list
                              if doc['join_mo'] < 5])
            expected_result = [{"join_mo" : group,
                                "points_avg" : math.fsum([doc['tasks_points']['task1']
                                                          for doc in full_list
                                                          if doc['join_mo'] == group]) /
                                                len([doc['tasks_points']['task1']
                                                          for doc in full_list
                                                          if doc['join_mo'] == group])}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['join_mo']))
            self._verify_results(actual_result['resultset'], expected_result)

            self.query = "SELECT join_mo, AVG(test_rate) as rate FROM %s " % (bucket.name) +\
                         "as employees WHERE job_title='Sales' GROUP BY join_mo " +\
                         "HAVING AVG(employees.test_rate) > 0 and " +\
                         "SUM(test_rate) < 100000"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'], key=lambda doc: (doc['join_mo']))
            tmp_groups = set([doc['join_mo'] for doc in full_list])
            expected_result = [{"join_mo" : group,
                                "rate" : math.fsum([doc['test_rate']
                                                          for doc in full_list
                                                          if doc['join_mo'] == group and\
                                                             doc['job_title'] == 'Sales'])/
                                               len([doc['test_rate']
                                                          for doc in full_list
                                                          if doc['join_mo'] == group and\
                                                             doc['job_title'] == 'Sales'])}
                               for group in tmp_groups
                               if (math.fsum([doc['test_rate']
                                            for doc in full_list
                                            if doc['join_mo'] == group and\
                                            doc['job_title'] == 'Sales'])/
                                    len([doc['test_rate']
                                         for doc in full_list
                                         if doc['join_mo'] == group and\
                                         doc['job_title'] == 'Sales'])> 0)  and\
                                  math.fsum([doc['test_rate']
                                            for doc in full_list
                                            if doc['join_mo'] == group and\
                                            doc['job_title'] == 'Sales'])  < 100000]
            expected_result = sorted(expected_result, key=lambda doc: (doc['join_mo']))
            self._verify_results(actual_result, expected_result)

    def test_min(self):
        for bucket in self.buckets:
            self.query = "SELECT join_mo, MIN(test_rate) as rate FROM %s " % (bucket.name) +\
                         "as employees WHERE job_title='Sales' GROUP BY join_mo " +\
                         "HAVING MIN(employees.test_rate) > 0 and " +\
                         "SUM(test_rate) < 100000"
            full_list = self._generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'], key=lambda doc: (doc['join_mo']))
            tmp_groups = set([doc['join_mo'] for doc in full_list])
            expected_result = [{"join_mo" : group,
                                "rate" : min([doc['test_rate']
                                              for doc in full_list
                                              if doc['join_mo'] == group and\
                                              doc['job_title'] == 'Sales'])}
                               for group in tmp_groups
                               if min([doc['test_rate']
                                       for doc in full_list
                                       if doc['join_mo'] == group and\
                                       doc['job_title'] == 'Sales']) > 0 and\
                                  math.fsum([doc['test_rate']
                                            for doc in full_list
                                            if doc['join_mo'] == group and\
                                            doc['job_title'] == 'Sales']) < 100000]
            expected_result = sorted(expected_result, key=lambda doc: (doc['join_mo']))
            self._verify_results(actual_result, expected_result)

    def test_max(self):
        for bucket in self.buckets:
            self.query = "SELECT join_mo, MAX(test_rate) as rate FROM %s " % (bucket.name) +\
                         "as employees WHERE job_title='Sales' GROUP BY join_mo " +\
                         "HAVING MAX(employees.test_rate) > 0 and " +\
                         "SUM(test_rate) < 100000"
            full_list = self._generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'], key=lambda doc: (doc['join_mo']))
            tmp_groups = set([doc['join_mo'] for doc in full_list])
            expected_result = [{"join_mo" : group,
                                "rate" : max([doc['test_rate']
                                              for doc in full_list
                                              if doc['join_mo'] == group and\
                                              doc['job_title'] == 'Sales'])}
                               for group in tmp_groups
                               if max([doc['test_rate']
                                       for doc in full_list
                                       if doc['join_mo'] == group and\
                                       doc['job_title'] == 'Sales'] ) > 0 and\
                                  math.fsum([doc['test_rate']
                                            for doc in full_list
                                            if doc['join_mo'] == group and\
                                            doc['job_title'] == 'Sales'] ) < 100000]
            expected_result = sorted(expected_result, key=lambda doc: (doc['join_mo']))
            self._verify_results(actual_result, expected_result)

    def test_array_agg_distinct(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title, array_agg(DISTINCT name) as names" +\
            " FROM %s GROUP BY job_title" % (bucket.name)
            full_list = self._generate_full_docs_list(self.gens_load)
            actual_list = self.run_cbq_query()
            actual_result = []
            for key, value in actual_list['resultset']:
                if isinstance(value, list):
                    actual_result.append({key : value.sort()})
                else:
                    actual_result.append({key : value})
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

            tmp_groups = set([doc['job_title'] for doc in full_list])
            expected_list = [{"job_title" : group,
                                "names" : set([x["name"] for x in full_list
                                               if x["job_title"] == group])}
                               for group in tmp_groups]
            expected_result = []
            for key, value in expected_list:
                if isinstance(value, list):
                    expected_result.append({key : value.sort()})
                else:
                    expected_result.append({key : value})
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

    def test_array_agg(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title, array_agg(name) as names" +\
            " FROM %s GROUP BY job_title" % (bucket.name)
            full_list = self._generate_full_docs_list(self.gens_load)
            actual_list = self.run_cbq_query()
            actual_result = []
            for key, value in actual_list['resultset']:
                if isinstance(value, list):
                    actual_result.append({key : value.sort()})
                else:
                    actual_result.append({key : value})
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

            tmp_groups = set([doc['job_title'] for doc in full_list])
            expected_list = [{"job_title" : group,
                                "names" : [x["name"] for x in full_list
                                               if x["job_title"] == group]}
                               for group in tmp_groups]
            expected_result = []
            for key, value in expected_list:
                if isinstance(value, list):
                    expected_result.append({key : value.sort()})
                else:
                    expected_result.append({key : value})
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

##############################################################################################
#
#   EXPRESSIONS
##############################################################################################

    def test_case(self):
        for bucket in self.buckets:
            self.query = "SELECT name, CASE WHEN join_mo < 3 OR join_mo > 11 THEN 'winter'" +\
                         " WHEN join_mo < 6 AND join_mo > 2 THEN 'spring' " +\
                         "WHEN join_mo < 9 AND join_mo > 5 THEN 'summer' " +\
                         "ELSE 'autumn' END AS period FROM %s" % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'], key=lambda doc: (
                                                                       doc['name'],
                                                                       doc['period']))
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"name" : doc['name'],
                                "period" : ((('autumn','summer')[doc['join_mo'] in [6,7,8]],
                                             'spring')[doc['join_mo'] in [3,4,5]],'winter')
                                             [doc['join_mo'] in [12,1,2]]}
                               for doc in full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['period']))
            self._verify_results(actual_result, expected_result)

    def test_case_arithm(self):
        self.query = "SELECT CASE WHEN 1+1=3 THEN 7+7 WHEN 2+2=5 THEN 8+8 ELSE 2 END"
        actual_result = self.run_cbq_query()
        expected_result = [{"$1" : 2}]
        self._verify_results(actual_result['resultset'], expected_result)

    def test_logic_expr(self):
        for bucket in self.buckets:
            self.query = "SELECT tasks_points.task1 as task FROM %s WHERE " % (bucket.name)+\
            "tasks_points.task1 > 1 AND tasks_points.task1 < 4"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'], key=lambda doc: (
                                                                       doc['task']))
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"task" : doc['tasks_points']['task1']}
                               for doc in full_list
                               if doc['tasks_points']['task1'] > 1 and\
                                  doc['tasks_points']['task1'] < 4]
            expected_result = sorted(expected_result, key=lambda doc: (doc['task']))
            self._verify_results(actual_result, expected_result)

    def test_comparition_expr(self):
        for bucket in self.buckets:
            self.query = "SELECT tasks_points.task1 as task FROM %s WHERE " % (bucket.name)+\
            "tasks_points.task1 > tasks_points.task1"
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['resultset'], [])

    def test_arithm(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title, SUM(test_rate) % COUNT(distinct join_yr)" +\
            " as avg_per_year from {0} group by job_title".format(bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = [{"job_title": doc["job_title"],
                              "avg_per_year" : round(doc["avg_per_year"],2)}
                              for doc in actual_result['resultset']]
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))
            full_list = self._generate_full_docs_list(self.gens_load)
            tmp_groups = set([doc['job_title'] for doc in full_list])
            expected_result = [{"job_title" : group,
                                "avg_per_year" : math.fsum([doc['test_rate']
                                                             for doc in full_list
                                                             if doc['job_title'] == group]) %\
                                                  len(set([doc['join_yr'] for doc in full_list
                                                           if doc['job_title'] == group]))}
                               for group in tmp_groups]
            expected_result = [{"job_title": doc["job_title"],
                              "avg_per_year" : round(doc["avg_per_year"],2)}
                              for doc in expected_result]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

            self.query = "SELECT job_title, (SUM(tasks_points.task1) +" +\
            " SUM(tasks_points.task2)) % COUNT(distinct join_yr) as avg_per_year" +\
            " from {0} group by job_title".format(bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = [{"job_title": doc["job_title"],
                              "avg_per_year" : round(doc["avg_per_year"],2)}
                              for doc in actual_result['resultset']]
            actual_result = sorted(actual_result, key=lambda doc: (
                                                                       doc['job_title']))
            tmp_groups = set([doc['job_title'] for doc in full_list])
            expected_result = [{"job_title" : group,
                                "avg_per_year" : (math.fsum([doc['tasks_points']['task1']
                                                             for doc in full_list
                                                             if doc['job_title'] == group])+\
                                                  math.fsum([doc['tasks_points']['task2']
                                                             for doc in full_list
                                                             if doc['job_title'] == group]))%\
                                                  len(set([doc['join_yr'] for doc in full_list
                                                           if doc['job_title'] == group]))}
                               for group in tmp_groups]
            expected_result = [{"job_title": doc["job_title"],
                              "avg_per_year" : round(doc["avg_per_year"],2)}
                              for doc in expected_result]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

##############################################################################################
#
#   EXPLAIN
##############################################################################################

    def test_explain(self):
        for bucket in self.buckets:
            self.query = "EXPLAIN SELECT * FROM %s" % (bucket.name)
            res = self.run_cbq_query()
            self.assertTrue(res["resultset"][0]["input"]["type"] == "fetch",
                            "Type should be fetch, but is: %s" % res["resultset"])
            self.assertTrue(res["resultset"][0]["input"]["input"]["type"] == "scan",
                            "Type should be scan, but is: %s" % res["resultset"])
            self.assertTrue(res["resultset"][0]["input"]["input"]["index"] == "#alldocs",
                            "Type should be #alldocs, but is: %s" % res["resultset"])

##############################################################################################
#
#   DATETIME
##############################################################################################

    def test_now(self):
        self.query = "select now_str() as now"
        now = datetime.datetime.now()
        today = date.today()
        res = self.run_cbq_query()
        expected = "%s-%s-%sT%s:" % (today.year, today.month, today.day, now.hour)
        self.assertTrue(res["resultset"]["now"].startswith(expected),
                        "Result expected: %s. Actual %s" % (expected, res["resultset"]))

    def test_hours(self):
        self.query = 'select date_part("hour", now_str()) as hour, ' +\
        'date_part("minute", now_str()) as minute, date_part("second",' +\
        ' now_str()) as sec, date_part("milliseconds", now_str()) as msec'
        now = datetime.datetime.now()
        res = self.run_cbq_query()
        self.assertTrue(res["resultset"]["hour"] == now.hour,
                        "Result expected: %s. Actual %s" % (now.hour, res["resultset"]))
        self.assertTrue("minute" in res["resultset"], "No minute field")
        self.assertTrue("second" in res["resultset"], "No minute field")
        self.assertTrue("msec" in res["resultset"], "No minute field")

    def test_where(self):
        for bucket in self.buckets:
            self.query = 'select name, join_yr, join_mo, join_day from %s' % (bucket.name) +\
            ' where date_part("month", now_str()) < join_mo AND date_part("year", now_str())' +\
            ' > join_yr AND date_part("day", now_str()) < join_day'
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result["resultset"], key=lambda doc: (doc['name'],
                                                                                doc['join_yr'],
                                                                                doc['join_mo'],
                                                                                doc['join_day']))
            full_list = self._generate_full_docs_list(self.gens_load)
            today = date.today()
            expected_result = [{"name" : doc['name'], "join_yr" : doc['join_yr'],
                                "join_mo" : doc['join_mo'], "join_day" : doc['join_day']}
                               for doc in full_list
                               if doc['join_yr'] > today.year and\
                               doc['join_mo'] < today.month and\
                               doc['join_day'] < today.day]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                        doc['join_yr'],
                                                                        doc['join_mo'],
                                                                        doc['join_day']))
            self._verify_results(actual_result, expected_result)

##############################################################################################
#
#   COMMON FUNCTIONS
##############################################################################################

    def negative_common_body(self, queries_errors={}):
        if not queries_errors:
            self.fail("No queries to run!")
        for bucket in self.buckets:
            for query, error in queries_errors.iteritems():
                try:
                    actual_result = self.run_cbq_query(query.format(bucket.name))
                except CBQError as ex:
                    self.log.error(ex)
                    self.assertTrue(str(ex).find(error) != -1,
                                    "Error is incorrect.Actual %s.\n Expected: %s.\n" %(
                                                                str(ex).split(':')[-1], error))
                else:
                    self.fail("There was no errors. Error expected: %s" % error)

    def run_cbq_query(self, query=None, min_output_size=10, server=None):
        if query is None:
            query = self.query
        if server is None:
           server = self.master
           if self.input.tuq_client and "client" in self.input.tuq_client:
               server = self.tuq_client
        if self.use_rest:
            result = RestConnection(server).query_tool(query)
        else:
            if self.version == "git_repo":
                output = self.shell.execute_commands_inside("$GOPATH/src/github.com/couchbaselabs/tuqtng/" +\
                                                            "tuq_client/tuq_client " +\
                                                            "-engine=http://%s:8093/" % server.ip,
                                                       subcommands=[query,],
                                                       min_output_size=20,
                                                       end_msg='tuq_client>')
            else:
                output = self.shell.execute_commands_inside("/tmp/tuq/cbq -engine=http://%s:8093/" % server.ip,
                                                           subcommands=[query,],
                                                           min_output_size=20,
                                                           end_msg='cbq>')
            result = self._parse_query_output(output)
        if 'error' in result:
            raise CBQError(result["error"], server.ip)
        self.log.info("TOTAL ELAPSED TIME: %s" % [param["message"]
                        for param in result["info"] if param["key"] == "total_elapsed_time"])
        return result

    def build_url(self, version):
        info = self.shell.extract_remote_info()
        type = info.distribution_type.lower()
        if type in ["ubuntu", "centos", "red hat"]:
            url = "https://s3.amazonaws.com/packages.couchbase.com/releases/couchbase-query/dp1/"
            url += "couchbase-query_%s_%s_linux.tar.gz" %(
                                version, info.architecture_type)
        #TODO for windows
        return url

    def _build_tuq(self, server):
        if self.version == "git_repo":
            os = self.shell.extract_remote_info().type.lower()
            if os != 'windows':
                goroot = testconstants.LINUX_GOROOT
                gopath = testconstants.LINUX_GOPATH
            else:
                goroot = testconstants.WINDOWS_GOROOT
                gopath = testconstants.WINDOWS_GOPATH
            if self.input.tuq_client and "gopath" in self.input.tuq_client:
                gopath = self.input.tuq_client["gopath"]
            if self.input.tuq_client and "goroot" in self.input.tuq_client:
                goroot = self.input.tuq_client["goroot"]
            cmd = "rm -rf {0}/src/github.com".format(gopath)
            self.shell.execute_command(cmd)
            cmd= 'export GOROOT={0} && export GOPATH={1} &&'.format(goroot, gopath) +\
                ' export PATH=$PATH:$GOROOT/bin && ' +\
                'go get github.com/couchbaselabs/tuqtng;' +\
                'cd $GOPATH/src/github.com/couchbaselabs/tuqtng; ' +\
                'go get -d -v ./...; cd .'
            self.shell.execute_command(cmd)
            cmd = 'export GOROOT={0} && export GOPATH={1} &&'.format(goroot, gopath) +\
                ' export PATH=$PATH:$GOROOT/bin && ' +\
                'cd $GOPATH/src/github.com/couchbaselabs/tuqtng; go build; cd .'
            self.shell.execute_command(cmd)
            cmd = 'export GOROOT={0} && export GOPATH={1} &&'.format(goroot, gopath) +\
                ' export PATH=$PATH:$GOROOT/bin && ' +\
                'cd $GOPATH/src/github.com/couchbaselabs/tuqtng/tuq_client; go build; cd .'
            self.shell.execute_command(cmd)
        else:
            cbq_url = self.build_url(self.version)
            #TODO for windows
            cmd = "cd /tmp; mkdir tuq;cd tuq; wget {0} -O tuq.tar.gz;".format(cbq_url)
            cmd += "tar -xvf tuq.tar.gz;rm -rf tuq.tar.gz"
            self.shell.execute_command(cmd)

    def _start_command_line_query(self, server):
        if self.version == "git_repo":
            os = self.shell.extract_remote_info().type.lower()
            if os != 'windows':
                gopath = testconstants.LINUX_GOPATH
            else:
                gopath = testconstants.WINDOWS_GOPATH
            if self.input.tuq_client and "gopath" in self.input.tuq_client:
                gopath = self.input.tuq_client["gopath"]
            if os == 'windows':
                cmd = "cd %s/src/github.com/couchbaselabs/tuqtng/; " % (gopath) +\
                "./tuqtng.exe -couchbase http://%s:%s/ >/dev/null 2>&1 &" %(
                                                                server.ip, server.port)
            else:
                cmd = "cd %s/src/github.com/couchbaselabs/tuqtng/; " % (gopath) +\
                "./tuqtng -couchbase http://%s:%s/ >/dev/null 2>&1 &" %(
                                                                server.ip, server.port)
            self.shell.execute_command(cmd)
        else:
            #TODO for windows
            cmd = "cd /tmp/tuq;./cbq-engine -couchbase http://%s:%s/ >/dev/null 2>&1 &" %(
                                                                server.ip, server.port)
            self.shell.execute_command(cmd)

    def _parse_query_output(self, output):
        if output.find("cbq>") == 0:
            output = output[output.find("cbq>") + 4:].strip()
        if output.find("tuq_client>") == 0:
            output = output[output.find("tuq_client>") + 11:].strip()
        if output.find("cbq>") != -1:
            output = output[:output.find("cbq>")].strip()
        if output.find("tuq_client>") != -1:
            output = output[:output.find("tuq_client>")].strip()
        return json.loads(output)

    def generate_docs(self, docs_per_day, start=0):
        generators = []
        types = ['Engineer', 'Sales', 'Support']
        join_yr = [2010, 2011]
        join_mo = xrange(1, 12 + 1)
        join_day = xrange(1, 28 + 1)
        template = '{{ "name":"{0}", "join_yr":{1}, "join_mo":{2}, "join_day":{3},'
        template += ' "email":"{4}", "job_title":"{5}", "test_rate":{8}, "skills":{9},'
        template += '"VMs": {10},'
        template += ' "tasks_points" : {{"task1" : {6}, "task2" : {7}}}}}'
        for info in types:
            for year in join_yr:
                for month in join_mo:
                    for day in join_day:
                        prefix = str(uuid.uuid4())[:7]
                        name = ["employee-%s" % (str(day))]
                        email = ["%s-mail@couchbase.com" % (str(day))]
                        vms = [{"RAM": month, "os": "ubuntu",
                                "name": "vm_%s" % month, "memory": month},
                               {"RAM": month, "os": "windows",
                                "name": "vm_%s"% (month + 1), "memory": month}]
                        generators.append(DocumentGenerator("query-test" + prefix,
                                               template,
                                               name, [year], [month], [day],
                                               email, [info], range(1,10), range(1,10),
                                               [float("%s.%s" % (month, month))],
                                               [["skill%s" % y for y in join_yr]],
                                               [vms],
                                               start=start, end=docs_per_day))
        return generators

    def load(self, generators_load, exp=0, flag=0,
             kv_store=1, only_store_hash=True, batch_size=1, pause_secs=1,
             timeout_secs=30, op_type='create'):
        gens_load = {}
        for bucket in self.buckets:
            tmp_gen = []
            for generator_load in generators_load:
                tmp_gen.append(copy.deepcopy(generator_load))
            gens_load[bucket] = copy.deepcopy(tmp_gen)
        tasks = []
        items = 0
        for gen_load in gens_load[self.buckets[0]]:
                items += (gen_load.end - gen_load.start)

        for bucket in self.buckets:
            self.log.info("%s %s to %s documents..." % (op_type, items, bucket.name))
            tasks.append(self.cluster.async_load_gen_docs(self.master, bucket.name,
                                             gens_load[bucket],
                                             bucket.kvs[kv_store], op_type, exp, flag,
                                             only_store_hash, batch_size, pause_secs,
                                             timeout_secs))
        for task in tasks:
            task.result()
        self.num_items = items
        self.verify_cluster_stats(self.servers[:self.nodes_init])
        self.log.info("LOAD IS FINISHED")

    def _generate_full_docs_list(self, gens_load):
        all_docs_list = []
        for gen_load in gens_load:
            doc_gen = copy.deepcopy(gen_load)
            while doc_gen.has_next():
                _, val = doc_gen.next()
                try:
                    val = json.loads(val)
                except TypeError:
                    pass
                all_docs_list.append(val)
        return all_docs_list

    def _verify_results(self, actual_result, expected_result):
        self.assertEquals(len(actual_result), len(expected_result),
                          "Results are incorrect.Actual num %s. Expected num: %s.\n" % (
                                            len(actual_result), len(expected_result)))
        if self.max_verify is not None:
            actual_result = actual_result[:self.max_verify]
            expected_result = expected_result[:self.max_verify]

        msg = "Results are incorrect.\n Actual first and last 100:  %s.\n ... \n %s" +\
        "Expected first and last 100: %s.\n  ... \n %s"
        self.assertTrue(actual_result == expected_result,
                          msg % (actual_result[:100],actual_result[-100:],
                                 expected_result[:100],expected_result[-100:]))
