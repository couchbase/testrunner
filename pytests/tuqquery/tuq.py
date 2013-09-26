import logger
import json
import uuid
import copy
import math
import re

from remote.remote_util import RemoteMachineShellConnection
from basetestcase import BaseTestCase
from couchbase.documentgenerator import DocumentGenerator
from membase.api.exception import CBQError
from membase.api.rest_client import RestConnection

class QueryTests(BaseTestCase):
    def setUp(self):
        super(QueryTests, self).setUp()
        self.shell = RemoteMachineShellConnection(self.master)
        self.version = self.input.param("cbq_version", "dev_preview1")
        self.use_rest = self.input.param("use_rest", False)
        self.max_verify = self.input.param("max_verify", None)
        if self._testMethodName in ['suite_tearDown', 'suite_setUp']:
            return
        try:
            docs_per_day = self.input.param("doc-per-day", 49)
            self.gens_load = self.generate_docs(docs_per_day)
            self.load(self.gens_load)
        except:
            self.tearDown()

    def suite_setUp(self):
        self._start_command_line_query(self.master)

    def tearDown(self):
        super(QueryTests, self).tearDown()

    def suite_tearDown(self):
        if hasattr(self, 'shell'):
            self.shell.execute_command("killall /tmp/tuq/cbq-engine")
            self.shell.execute_command("rm -rf /tmp/tuq")
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
                          'SELECT UNIQUE tasks_points from {0}' : ''}
        self.negative_common_body(queries_errors)

    def test_consistent_simple_check(self):
        queries = ['SELECT name, join_day, join_mo FROM %s \
                    WHERE join_day<10 AND join_mo IN (1, 6) \
                    OR name NOT NULL ORDER BY join_day',
                   'SELECT name, join_day, join_mo FROM %s \
                    WHERE join_mo IN (1, 6) OR name NOT NULL AND \
                    join_day<10 ORDER BY join_day']
        for bucket in self.buckets:
            actual_result1 = self.run_cbq_query(queries[0] % bucket.name)
            actual_result2 = self.run_cbq_query(queries[1] % bucket.name)
            self.assertEquals(actual_result1['resultset'], actual_result2['resultset'],
                              "Results are inconsistent.Difference: %s" %(
                                    actual_result1['resultset'] - actual_result2['resultset']))

    def test_simple_nulls(self):
        queries = ['FROM %s SELECT name WHERE id=null',
           'SELECT id FROM %s WHERE id=NULL or id="null"']
        for bucket in self.buckets:
            for query in queries:
                actual_result = self.run_cbq_query(query % (bucket.name))
                self._verify_results(actual_result['resultset'], [])

##############################################################################################
#
#   ALIAS CHECKS
##############################################################################################

    def test_simple_alias(self):
        for bucket in self.buckets:
            self.query = 'SELECT COUNT(name) AS COUNT_EMPLOYEE FROM %s' % (bucket.name)
            actual_result = self.run_cbq_query()
            expected_result = [ { "COUNT_EMPLOYEE": self.num_items  } ]
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
                                'Alias points cannot be referenced',
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
                expected_result = sorted(expected_result, key=lambda doc: doc['points'])
                actual_result = self.run_cbq_query(query % (bucket.name))
                self._verify_results(actual_result['resultset'], expected_result)

    def test_alias_from_clause_group(self):
        for bucket in self.buckets:
            self.query = 'SELECT tasks_points.task1 AS points FROM %s AS test ' +\
                         'GROUP BY test.tasks_points.task1 ORDER BY points' % (bucket.name)
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = set([{"points" : doc["tasks_points"]["task1"]} for doc in full_list])
            expected_result = sorted(expected_result, key=lambda doc: doc['points'])
            expected_result = [{"points" : doc["points"]} for doc in expected_result]
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['resultset'], expected_result)

    def test_alias_order_desc(self):
        for bucket in self.buckets:
            self.query = 'SELECT name AS name_new FROM %s AS test ORDER BY name_new DESC' %(
                                                                                bucket.name)
            actual_result = self.run_cbq_query()

            full_list = self._generate_full_docs_list(self.gens_load)
            expected_list = [{"name" : doc["name"]} for doc in full_list]
            expected_result = sorted(expected_list, key=lambda doc: doc['name'],
                                     reverse=True)
            self._verify_results(actual_result['resultset'], expected_result)

    def test_alias_order_asc(self):
        for bucket in self.buckets:
            self.query = 'SELECT name AS name_new FROM %s AS test ORDER BY name_new DESC' %(
                                                                                bucket.name)
            actual_result = self.run_cbq_query()

            full_list = self._generate_full_docs_list(self.gens_load)
            expected_list = [{"name" : doc["name"]} for doc in full_list]
            expected_result = sorted(expected_list, key=lambda doc: doc['name'],
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
            expected_result = [{"$1" : len(full_list)}]
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
            ' AS test ORDER BY points DESC'
            actual_result = self.run_cbq_query()

            full_list = self._generate_full_docs_list(self.gens_load)
            expected_list = [{"job_title" : doc["job_title"],
                              "tasks_points" : doc["tasks_points"]}
                             for doc in full_list]
            expected_result = sorted(expected_list, key=lambda doc: (doc['job_title'],
                                                                     doc["tasks_points"]),
                                     reverse=True)
            self._verify_results(actual_result['resultset'], expected_result)

    def test_order_by_alias_arrays(self):
        for bucket in self.buckets:
            self.query = 'SELECT job_title, tasks_points, skills[0] AS SKILL FROM %s'  % (
                                                                            bucket.name) +\
            ' AS TEST ORDER BY TEST.tasks_points, SKILL'
            actual_result = self.run_cbq_query()

            full_list = self._generate_full_docs_list(self.gens_load)
            expected_list = [{"job_title" : doc["job_title"],
                              "tasks_points" : doc["tasks_points"],
                              "SKILL" : doc["skills"][0]}
                             for doc in full_list]
            expected_result = sorted(expected_list, key=lambda doc: (doc['tasks_points'],
                                                                     doc["skills"][0]),
                                     reverse=True)
            self._verify_results(actual_result['resultset'], expected_result)

    def test_order_by_aggr_fn(self):
        for bucket in self.buckets:
            self.query = 'SELECT job_title AS TITLE FROM %s GROUP'  % (bucket.name) +\
            ' BY job_title ORDER BY MIN(join_mo)'
            actual_result = self.run_cbq_query()

            full_list = self._generate_full_docs_list(self.gens_load)
            tmp_titles = set([doc['job_title'] for doc in full_list])
            expected_list = [{"job_title" : doc["job_title"],
                              "min_value" :
                              min([doc["join_month"] for doc in full_list
                                   if doc["job_title"]==title])}
                             for title in tmp_titles]

            expected_result = sorted(expected_list, key=lambda doc: (doc['min_value']))
            expected_result = [{"TITLE" : doc["job_title"]} for doc in expected_result]
            self._verify_results(actual_result['resultset'], expected_result)

    def test_order_by_precedence(self):
        for bucket in self.buckets:
            self.query = 'SELECT job_title, join_yr, FROM %s'  % (bucket.name) +\
            ' ORDER BY job_title, join_yr'
            actual_result = self.run_cbq_query()

            full_list = self._generate_full_docs_list(self.gens_load)
            expected_list = [{"job_title" : doc["job_title"],
                              "join_yr" : doc["join_yr"]}
                             for doc in full_list]
            expected_result = sorted(expected_list, key=lambda doc: (doc['job_title'],
                                                                     doc["join_yr"]))
            self._verify_results(actual_result['resultset'], expected_result)

            self.query = 'SELECT job_title, join_yr, FROM %s'  % (bucket.name) +\
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
            self.query = 'SELECT name, VMs FROM %s '  % (bucket.name) +\
            'AS employees WHERE ANY vm.RAM > 5 AND vm.os = "ubuntu" '
            'OVER vm IN default.VMs END ORDER BY name'
            actual_result = self.run_cbq_query()

            full_list = self._generate_full_docs_list(self.gens_load)
            expected_list = [{"name" : doc["name"], "VMs" : doc["VMs"]}
                              for doc in full_list
                              if len([vm["RAM"] > 5 and vm["os"] == 'ubuntu'
                                for vm in doc["VMs"]]) > 0]

            expected_result = sorted(expected_list, key=lambda doc: (doc['name']))
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
                         ' FROM %s ORDER BY skill'  % (bucket.name, bucket.name)
            actual_result = self.run_cbq_query()
            tmp = set([doc['skills'][0] for doc in full_list])
            expected_result = [{"skill" : point} for point in tmp]
            expected_result = sorted(expected_result, key=lambda doc: (doc['skill']))
            self._verify_results(actual_result['resultset'], expected_result)

            self.query = 'SELECT DISTINCT tasks_points.* ' +\
                         'FROM %s'  % (bucket.name)
            actual_result = self.run_cbq_query()
            tmp = set([doc['tasks_points']['task1'] for doc in full_list])
            expected_result = [{"task1" : point} for point in tmp]
            self._verify_results(actual_result['resultset'], expected_result)

    def test_distinct_negative(self):
        queries_errors = {'SELECT name FROM {0} ORDER BY DISTINCT name' : 'Parse Error - syntax error',
                          'SELECT name FROM {0} GROUP BY DISTINCT name' : 'Parse Error - syntax error',
                          'SELECT ANY tasks_points FROM {0}' : 'Parse Error - syntax error'}
        self.negative_common_body(queries_errors)

    def test_any(self):
        for bucket in self.buckets:
            self.query = "SELECT name, email FROM %s WHERE "  % (bucket.name) +\
                         "(ANY skill = 'skill2010' OVER skill IN default.skills end) " +\
                         "AND (ANY vm.RAM = 5 OVER vm IN default.VMs end) " +\
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

    def test_all(self):
        for bucket in self.buckets:
            self.query = "SELECT name FROM %s WHERE " % (bucket.name) +\
                         "(ALL CEIL(vm.memory) > 5 OVER vm IN default.VMs END)" +\
                         " ORDER BY name"
            full_list = self._generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            expected_result = [{"name" : doc['name']}
                               for doc in full_list
                               if len([vm for vm in doc["VMs"]
                                       if math.ceil(vm['memory']) < 5]) ==\
                                  len(doc["VMs"])]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['resultset'], expected_result)

##############################################################################################
#
#   LIKE
##############################################################################################

    def test_like(self):
        for bucket in self.buckets:
            self.query = "SELECT name FROM %s WHERE job_title LIKE 'S%' ORDER BY name"  % (bucket.name)
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"name" : doc['name']} for doc in full_list
                               if doc["job_title"].startswith('S')]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['resultset'], expected_result)

            self.query = "SELECT name FROM %s WHERE job_title LIKE '%u%' ORDER BY name"  %(
                                                                            bucket.name)
            actual_result = self.run_cbq_query()
            expected_result = [{"name" : doc['name']} for doc in full_list
                               if doc["job_title"].find('u') != -1]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['resultset'], expected_result)

            self.query = "SELECT name FROM %s WHERE job_title NOT LIKE 'S%' ORDER BY name"  %(
                                                                            bucket.name)
            actual_result = self.run_cbq_query()
            expected_result = [{"name" : doc['name']} for doc in full_list
                               if not doc["job_title"].startswith('S')]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['resultset'], expected_result)

            self.query = "SELECT name FROM %s WHERE join_yr NOT LIKE '_010' ORDER BY name"  %(
                                                                            bucket.name)
            actual_result = self.run_cbq_query()
            expected_result = [{"name" : doc['name']} for doc in full_list
                               if not str(doc["join_yr"]).endswith('010') and\
                               len(str(doc["join_yr"])) == 4]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['resultset'], expected_result)

    def test_like_negative(self):
        queries_errors = {"SELECT tasks_points FROM {0} WHERE tasks_points.* LIKE '_1%'" :
                           'Parse Error - syntax error'}
        self.negative_common_body(queries_errors)

    def test_like_any(self):
        for bucket in self.buckets:
            self.query = "SELECT name, email FROM %s WHERE (ANY vm.os " % (bucket.name)
            "LIKE '%bun%' OVER vm IN default.VMs END) AND (ANY skill = 'skill2010' "
            "OVER skill IN default.skills END) ORDER BY name"
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"name" : doc['name'], "email" : doc["email"]}
                               for doc in full_list
                               if len([vm for vm in doc["VMs"]
                                       if vm["os"].find('bun') != 0]) > 0 and\
                                  len([skill for skill in doc["skills"]
                                       if skill == 'skill2010']) > 0]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['resultset'], expected_result)

    def test_like_all(self):
        for bucket in self.buckets:
            self.query = "SELECT name, email FROM %s WHERE (ALL vm.os" % (bucket.name) +\
                         " NOT LIKE '%cent%' OVER vm IN default.VMs END) AND (ANY skill =" +\
                         " 'skill2010' OVER skill IN default.skills END) ORDER BY name" 
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{"name" : doc['name'], "email" : doc["email"]}
                               for doc in full_list
                               if len([vm for vm in doc["VMs"]
                                     if vm["os"].find('cent') == 0]) == len(doc["VMs"]) and\
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
                               if doc["name"].find('mpl') != 0]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
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
                               if re.match(r'.*@.*\..*', doc['email']) is None]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
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
            expected_result = set(expected_result)
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
            expected_result = set(expected_result)
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
                         "WHERE (ANY skill = 'skill2010' OVER skill IN default.skills end) " +\
                         "AND (ANY vm.RAM = 5 OVER vm IN default.VMs end) "  +\
                         "GROUP BY job_title ORDER BY job_title"
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            tmp_groups = set([doc["job_title"] for doc in full_list])
            expected_result = [{"job_title" : group,
                                "avg_rate" : math.fsum([doc["test_rate"] for doc in full_list
                                             if doc["job_title"] == group]) /
                                             len([doc["test_rate"] for doc in full_list
                                             if doc["job_title"] == group])}
                               for group in tmp_groups
                               if len([doc for doc in full_list
                                       if doc["job_title"] == group and\
                                       'skill2010' in doc["skills"]]) >0 and\
                                  len([doc for doc in full_list
                                       if doc["job_title"] == group and\
                                       [vm for vm in doc["VMs"] if vm["RAM"] == 5]]) >0]
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

    def run_cbq_query(self, query=None, min_output_size=10):
        if query is None:
            query = self.query
        if self.use_rest:
            result = RestConnection(self.master).query_tool(query)
        else:
            if self.version == "git_repo":
                output = self.shell.execute_commands_inside("$GOPATH/src/github.com/couchbaselabs/tuqtng/"
                                                            "tuq_client/tuq_client "
                                                            "-engine=http://localhost:8093/",
                                                       subcommands=[query,],
                                                       min_output_size=20,
                                                       end_msg='tuq_client>')
            else:
                output = self.shell.execute_commands_inside("/tmp/tuq/cbq -engine=http://localhost:8093/",
                                                           subcommands=[query,],
                                                           min_output_size=20,
                                                           end_msg='cbq>')
            result = self._parse_query_output(output)
        if 'error' in result:
            raise CBQError(result["error"], self.master.ip)
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

    def _start_command_line_query(self, server):
        if self.version == "git_repo":
            if self.shell.file_exists('$GOPATH/src/github.com/couchbaselabs/tuqtng',
                                   'tuqtng'):
                cmd = "cd $GOPATH/src/github.com/couchbaselabs/tuqtng; git pull origin"
                self.shell.execute_command(cmd)
                cmd = "rm -rf $GOPATH/src/github.com/couchbaselabs/tuqtng/tugtng "
                "$GOPATH/src/github.com/couchbaselabs/tuqtng/tuq_client/tuq_client"
                self.shell.execute_command(cmd)
            else:
                cmd= 'go get github.com/couchbaselabs/tuqtng;'
                'cd $GOPATH/src/github.com/couchbaselabs/tuqtng; go get -d -v ./...'
                self.shell.execute_command(cmd)
            cmd = "cd $GOPATH/src/github.com/couchbaselabs/tuqtng; go build"
            self.shell.execute_command(cmd)
            cmd = "cd $GOPATH/src/github.com/couchbaselabs/tuqtng/tuq_client; go build"
            self.shell.execute_command(cmd)
        else:
            cbq_url = self.build_url(self.version)
            #TODO for windows
            cmd = "cd /tmp; mkdir tuq;cd tuq; wget {0} -O tuq.tar.gz;".format(cbq_url)
            cmd += "tar -xvf tuq.tar.gz;rm -rf tuq.tar.gz"
            self.shell.execute_command(cmd)
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
        self._verify_stats_all_buckets(self.servers[:self.nodes_init])
        self.log.info("LOAD IS FINISHED")

    def _generate_full_docs_list(self, gens_load):
        all_docs_list = []
        for gen_load in gens_load:
            doc_gen = copy.deepcopy(gen_load)
            while doc_gen.has_next():
                _, val = doc_gen.next()
                all_docs_list.append(json.loads(val))
        return all_docs_list

    def _verify_results(self, actual_result, expected_result):
        self.assertEquals(len(actual_result), len(expected_result),
                          "Results are incorrect.Actual num %s. Expected num: %s.\n" % (
                                            len(actual_result), len(expected_result)))
        if self.max_verify is not None:
            actual_result = actual_result[:self.max_verify]
            expected_result = expected_result[:self.max_verify]

        self.assertTrue(actual_result == expected_result,
                          "Results are incorrect.Actual %s.\n Expected: %s.\n" % (
                                            actual_result, expected_result))
